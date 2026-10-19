"""Worker against real Postgres, MinIO and Redis. Needs `docker compose up`."""

import threading
import time

import numpy as np
import pytest
import redis
from conftest import Db, IntegrationSettings, collect_reports
from dramatiq.brokers.redis import RedisBroker

from dmean_worker.main import build_worker
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.queue.messages import FailureType, ReportEvent, run_task_message
from dmean_worker.storage.csv_format import format_row, parse_input, read_partial, write_partial
from dmean_worker.storage.s3 import Storage

pytestmark = pytest.mark.integration


def test_map_task_writes_partial_and_reports(
    db: Db, runner: TaskRunner, broker: RedisBroker, storage: Storage
) -> None:
    job_id, job_uuid = db.job(no_of_files=10, columns=3)
    task_id = db.task(job_id, "map", db.inputs(job_id, ["1,2,3", "4,5,6", "7,8,9"]), 3)

    runner.run(task_id, last_attempt=False)

    sums, count = read_partial(storage.get_text(f"partials/{job_uuid}/{task_id}.csv"), 3)
    assert (sums.tolist(), count) == ([12.0, 15.0, 18.0], 3)
    started, done = collect_reports(broker, 2)
    assert started.event is ReportEvent.STARTED
    assert done.event is ReportEvent.DONE
    assert done.output_key == f"partials/{job_uuid}/{task_id}.csv"
    assert done.size_bytes == len(write_partial(sums, 3))


def test_final_map_writes_the_mean(db: Db, runner: TaskRunner, storage: Storage) -> None:
    job_id, job_uuid = db.job(no_of_files=2, columns=3)
    task_id = db.task(job_id, "map", db.inputs(job_id, ["1,2,3", "4,5,6"]), 2, final=True)

    runner.run(task_id, last_attempt=False)

    assert storage.get_text(f"results/{job_uuid}.csv") == "2.5,3.5,4.5\n"


def test_final_merge_of_partials(db: Db, runner: TaskRunner, storage: Storage) -> None:
    rng = np.random.default_rng(5)
    data = rng.random((5, 100))
    job_id, job_uuid = db.job(no_of_files=5, columns=100)
    p1 = db.partial(job_id, job_uuid, write_partial(data[:2].sum(axis=0), 2), 2)
    p2 = db.partial(job_id, job_uuid, write_partial(data[2:].sum(axis=0), 3), 3)
    task_id = db.task(job_id, "merge", [p1, p2], 5, final=True)

    runner.run(task_id, last_attempt=False)

    means = parse_input(storage.get_text(f"results/{job_uuid}.csv"), 100)
    np.testing.assert_allclose(means, data.mean(axis=0), rtol=1e-14)


@pytest.mark.parametrize("status", ["done", "failed", "skipped"])
def test_finished_task_is_skipped(
    db: Db, runner: TaskRunner, broker: RedisBroker, storage: Storage, status: str
) -> None:
    job_id, job_uuid = db.job(no_of_files=2, columns=3)
    task_id = db.task(job_id, "map", db.inputs(job_id, ["1,2,3", "4,5,6"]), 2, status=status)

    runner.run(task_id, last_attempt=False)

    assert collect_reports(broker, 0) == []
    assert storage.size_if_exists(f"partials/{job_uuid}/{task_id}.csv") is None


def test_task_of_a_failed_job_is_skipped(db: Db, runner: TaskRunner, broker: RedisBroker) -> None:
    job_id, _ = db.job(no_of_files=2, columns=3, failed=True)
    task_id = db.task(job_id, "map", db.inputs(job_id, ["1,2,3", "4,5,6"]), 2)

    runner.run(task_id, last_attempt=False)

    assert collect_reports(broker, 0) == []


def test_bad_csv_fails_as_user_input_and_writes_nothing(
    db: Db, runner: TaskRunner, broker: RedisBroker, storage: Storage
) -> None:
    job_id, job_uuid = db.job(no_of_files=2, columns=3)
    task_id = db.task(job_id, "map", db.inputs(job_id, ["1,2,3", "1,2"]), 2)

    runner.run(task_id, last_attempt=False)  # non-retryable: reported, not raised

    _, failed = collect_reports(broker, 2)
    assert failed.event is ReportEvent.FAILED
    assert failed.failure_type is FailureType.USER_INPUT
    assert failed.retryable is False
    assert failed.error is not None and "expected 3 values, got 2" in failed.error
    assert storage.size_if_exists(f"partials/{job_uuid}/{task_id}.csv") is None


def test_existing_output_is_reported_without_recomputing(
    db: Db, runner: TaskRunner, broker: RedisBroker, storage: Storage
) -> None:
    job_id, job_uuid = db.job(no_of_files=10, columns=3)
    # inputs are NOT uploaded: reading them would fail, so a success proves nothing was recomputed
    task_id = db.task(job_id, "map", db.inputs(job_id, ["", ""], upload=False), 2)
    size = storage.put_text(f"partials/{job_uuid}/{task_id}.csv", write_partial(np.ones(3), 2))

    runner.run(task_id, last_attempt=False)

    _, done = collect_reports(broker, 2)
    assert done.event is ReportEvent.DONE
    assert done.size_bytes == size


def test_storage_error_is_retried_and_reported_on_the_last_attempt(
    db: Db, runner: TaskRunner, broker: RedisBroker
) -> None:
    job_id, _ = db.job(no_of_files=10, columns=3)
    task_id = db.task(job_id, "map", db.inputs(job_id, ["1,2,3"], upload=False), 1)

    with pytest.raises(Exception, match="NoSuchKey"):
        runner.run(task_id, last_attempt=False)  # raise → Dramatiq retries
    assert [r.event for r in collect_reports(broker, 1)] == [ReportEvent.STARTED]

    with pytest.raises(Exception, match="NoSuchKey"):
        runner.run(task_id, last_attempt=True)
    started, failed = collect_reports(broker, 2)
    assert started.event is ReportEvent.STARTED
    assert failed.event is ReportEvent.FAILED
    assert failed.failure_type is FailureType.INTERNAL
    assert failed.error is not None and failed.error.startswith("retries exhausted")


def test_worker_process_end_to_end(
    db: Db, settings: IntegrationSettings, broker: RedisBroker, storage: Storage
) -> None:
    """Real Dramatiq path: message on `tasks` → worker → output in MinIO → report on `results`."""
    rng = np.random.default_rng(11)
    data = rng.random((4, 1000))
    job_id, job_uuid = db.job(no_of_files=4, columns=1000)
    task_id = db.task(
        job_id, "map", db.inputs(job_id, [format_row(r) for r in data]), 4, final=True
    )

    worker, reader = build_worker(settings, "it-worker")
    assert worker.queue_prefetch == 1
    heartbeats = redis.Redis.from_url(settings.redis_url)
    worker.start()
    try:
        broker.enqueue(run_task_message(task_id))
        reports = collect_reports(broker, 2)
        assert [r.event for r in reports] == [ReportEvent.STARTED, ReportEvent.DONE]
        assert heartbeats.exists("worker:it-worker") == 1
    finally:
        stopper = threading.Thread(target=worker.stop, kwargs={"timeout": 5000})
        stopper.start()
        stopper.join(timeout=15)
        reader.close()

    means = parse_input(storage.get_text(f"results/{job_uuid}.csv"), 1000)
    np.testing.assert_allclose(means, data.mean(axis=0), rtol=1e-14)
    time.sleep(0.2)
    assert heartbeats.exists("worker:it-worker") == 0  # removed on shutdown, not left to expire
