"""The real worker loop against a fake orchestrator (gRPC) and real MinIO."""

import threading
import time

import numpy as np
import pytest
from conftest import FakeOrchestrator, Objects, make_task, start_fake

from dmean_worker.main import run_worker
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.queue.client import TaskQueueClient
from dmean_worker.rpc import task_queue_pb2 as pb
from dmean_worker.settings import Settings
from dmean_worker.storage.csv_format import format_row, parse_input, read_partial
from dmean_worker.storage.s3 import Storage

pytestmark = pytest.mark.integration


class RunningWorker:
    def __init__(self, settings: Settings, storage: Storage, address: str, slowdown: float = 0.0):
        self.draining = threading.Event()
        self.stop_now = threading.Event()
        self.client = TaskQueueClient(address, "test-worker")
        runner = TaskRunner(storage, slowdown)
        self.thread = threading.Thread(
            target=run_worker,
            args=(settings, self.client, runner, self.draining, self.stop_now),
            daemon=True,
        )

    def __enter__(self) -> "RunningWorker":
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.draining.set()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive(), "worker did not drain"
        self.client.close()


def wait_until(check: object, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not check():  # type: ignore[operator]
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)


def test_leases_runs_and_reports_done(
    fake: FakeOrchestrator, objects: Objects, settings: Settings, storage: Storage
) -> None:
    data = np.random.default_rng(1).random((3, 50))
    keys = [objects.put(f"in{i}.csv", format_row(row)) for i, row in enumerate(data)]
    fake.queue.tasks.append(make_task(11, keys, objects.key("result.csv"), columns=50, final=True))

    with RunningWorker(settings, storage, fake.address):
        wait_until(lambda: len(fake.queue.reports) == 1)

    [report] = fake.queue.reports
    assert (report.task_id, report.attempt, report.WhichOneof("outcome")) == (11, 1, "done")
    assert report.done.output_key == objects.key("result.csv")
    means = parse_input(storage.get_text(objects.key("result.csv")), 50)
    np.testing.assert_allclose(means, data.mean(axis=0), rtol=1e-14)
    assert report.done.size_bytes == len(storage.get_text(objects.key("result.csv")).encode())


def test_runs_tasks_one_after_another(
    fake: FakeOrchestrator, objects: Objects, settings: Settings, storage: Storage
) -> None:
    a = objects.put("a.csv", "1,2,3\n")
    b = objects.put("b.csv", "4,5,6\n")
    fake.queue.tasks.extend(
        [make_task(1, [a], objects.key("p1.csv")), make_task(2, [b], objects.key("p2.csv"))]
    )
    with RunningWorker(settings, storage, fake.address):
        wait_until(lambda: len(fake.queue.reports) == 2)
    assert [r.task_id for r in fake.queue.reports] == [1, 2]
    assert read_partial(storage.get_text(objects.key("p2.csv")), 3)[1] == 1


def test_bad_csv_is_reported_as_user_input(
    fake: FakeOrchestrator, objects: Objects, settings: Settings, storage: Storage
) -> None:
    bad = objects.put("bad.csv", "1,2\n")
    fake.queue.tasks.append(make_task(5, [bad], objects.key("out.csv")))
    with RunningWorker(settings, storage, fake.address):
        wait_until(lambda: len(fake.queue.reports) == 1)
    failed = fake.queue.reports[0].failed
    assert (failed.failure_type, failed.retryable) == (pb.USER_INPUT, False)
    assert "expected 3 values, got 2" in failed.error


def test_heartbeats_while_running_and_stop_abandons_quietly(
    fake: FakeOrchestrator, objects: Objects, settings: Settings, storage: Storage
) -> None:
    keys = [objects.put(f"f{i}.csv", "1,2,3\n") for i in range(6)]
    fake.queue.tasks.append(make_task(9, keys, objects.key("out.csv"), heartbeat_seconds=1))
    fake.queue.stop_tasks.add(9)  # e.g. the job failed meanwhile
    # slow worker: 0.5 s per file → the heartbeat (every 1 s) arrives mid-task and says STOP
    with RunningWorker(settings, storage, fake.address, slowdown=0.5):
        wait_until(lambda: 9 in fake.queue.heartbeats)
        time.sleep(1.0)
    assert fake.queue.reports == []  # abandoned: nothing to report
    assert storage.size_if_exists(objects.key("out.csv")) is None


def test_first_signal_finishes_the_task_second_hands_it_back(
    fake: FakeOrchestrator, objects: Objects, settings: Settings, storage: Storage
) -> None:
    keys = [objects.put(f"g{i}.csv", "1,2,3\n") for i in range(10)]
    fake.queue.tasks.append(make_task(21, keys, objects.key("a.csv")))
    fake.queue.tasks.append(make_task(22, keys * 4, objects.key("b.csv")))
    # 0.2 s per file: each task outlasts its first heartbeat (1 s), which proves the worker holds
    # it (a task the fake has handed out may still be in flight to the worker)

    worker = RunningWorker(settings, storage, fake.address, slowdown=0.2)
    worker.__enter__()
    wait_until(lambda: 21 in fake.queue.heartbeats)
    worker.draining.set()  # first signal mid-task: it finishes and reports done
    worker.thread.join(timeout=10)
    assert not worker.thread.is_alive()
    worker.client.close()
    [report] = fake.queue.reports
    assert (report.task_id, report.WhichOneof("outcome")) == (21, "done")
    assert fake.queue.leased == 1  # and it leased nothing more

    worker = RunningWorker(settings, storage, fake.address, slowdown=0.2)
    worker.__enter__()
    wait_until(lambda: 22 in fake.queue.heartbeats)
    worker.draining.set()
    worker.stop_now.set()  # second signal: abandon now, hand the task back
    worker.thread.join(timeout=5)
    assert not worker.thread.is_alive()
    worker.client.close()
    failed = fake.queue.reports[1].failed
    assert (fake.queue.reports[1].task_id, failed.retryable) == (22, True)
    assert storage.size_if_exists(objects.key("b.csv")) is None


def test_report_is_held_while_the_orchestrator_is_down(
    objects: Objects, settings: Settings, storage: Storage
) -> None:
    """The worker finishes a task, but the orchestrator fails reports and then goes down
    completely; when it is back on the same address, the report still arrives."""
    from conftest import FakeTaskQueue

    queue = FakeTaskQueue(fail_reports=True)
    server, port = start_fake(queue)
    key = objects.put("a.csv", "1,2,3\n")
    queue.tasks.append(make_task(3, [key], objects.key("out.csv"), heartbeat_seconds=60))

    with RunningWorker(settings, storage, f"127.0.0.1:{port}"):
        wait_until(lambda: queue.report_attempts >= 1)  # first report refused
        server.stop(grace=None)  # then fully down
        time.sleep(1.5)
        queue.fail_reports = False
        server2, _ = start_fake(queue, port)  # back on the same address
        try:
            wait_until(lambda: len(queue.reports) == 1, timeout=15)
        finally:
            server2.stop(grace=None)
    assert queue.reports[0].task_id == 3
    assert queue.report_attempts >= 2


def test_drains_promptly_while_idle(
    fake: FakeOrchestrator, settings: Settings, storage: Storage
) -> None:
    worker = RunningWorker(settings, storage, fake.address)
    worker.__enter__()
    time.sleep(0.3)  # sitting in a long-poll
    started = time.monotonic()
    worker.__exit__()
    assert time.monotonic() - started < 2
