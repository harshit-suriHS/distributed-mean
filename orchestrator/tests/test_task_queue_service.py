"""TaskQueue over real gRPC and real Postgres. Needs `docker compose up` (postgres + migrations)."""

import asyncio
import time

import pytest
from conftest import Db, Orchestrator, lease, report_done

from dmean_orchestrator.rpc import task_queue_pb2 as pb

pytestmark = pytest.mark.integration


async def report_failed(
    orch: Orchestrator, task: pb.Task, *, retryable: bool, user_input: bool = False
) -> None:
    await orch.stub.Report(
        pb.ReportRequest(
            worker_id="w1",
            task_id=task.task_id,
            attempt=task.attempt,
            failed=pb.Failed(
                failure_type=pb.USER_INPUT if user_input else pb.INTERNAL,
                retryable=retryable,
                error="boom",
            ),
        )
    )


async def heartbeat(orch: Orchestrator, task: pb.Task, worker: str = "w1") -> int:
    resp = await orch.stub.Heartbeat(
        pb.HeartbeatRequest(worker_id=worker, task_id=task.task_id, attempt=task.attempt)
    )
    return resp.action


# ---- leasing --------------------------------------------------------------------------------


async def test_lease_carries_the_whole_task(orch: Orchestrator, db: Db) -> None:
    job_id, job_uuid = db.job(no_of_files=2, columns=3)
    task_id = db.task(job_id, files=2, final=True)
    await orch.service.load_job(job_id)

    task = await lease(orch, "w1")

    assert task is not None
    assert (task.task_id, task.attempt, task.kind) == (task_id, 1, "map")
    assert len(task.input_keys) == 2 and all(k.startswith("inputs/test-") for k in task.input_keys)
    assert (task.no_of_files, task.no_of_columns, task.is_final) == (2, 3, True)
    assert task.output_key == f"results/{job_uuid}.csv"
    assert db.task_row(task_id) | {"error": None} == {
        "status": "running",
        "attempts": 1,
        "worker_id": "w1",
        "error": None,
        "backing_off": None,
    }


async def test_empty_lease_after_the_wait(orch: Orchestrator) -> None:
    started = time.monotonic()
    assert await lease(orch, "idle", wait=1) is None
    assert time.monotonic() - started >= 0.9


async def test_long_poll_wakes_as_soon_as_work_arrives(orch: Orchestrator, db: Db) -> None:
    waiting = asyncio.create_task(lease(orch, "w1", wait=10))
    await asyncio.sleep(0.3)
    assert not waiting.done()

    job_id, _ = db.job(no_of_files=1)
    task_id = db.task(job_id, files=1, final=True)
    started = time.monotonic()
    await orch.service.load_job(job_id)
    task = await asyncio.wait_for(waiting, timeout=2)

    assert task is not None and task.task_id == task_id
    assert time.monotonic() - started < 1


async def test_cancelled_long_poll_does_not_swallow_a_task(orch: Orchestrator, db: Db) -> None:
    call = orch.stub.Lease(pb.LeaseRequest(worker_id="gone", wait_seconds=10))
    await asyncio.sleep(0.3)
    call.cancel()  # the worker shut down while waiting
    await asyncio.sleep(0.3)

    job_id, _ = db.job(no_of_files=1)
    task_id = db.task(job_id, files=1, final=True)
    await orch.service.load_job(job_id)
    task = await lease(orch, "w2")

    assert task is not None and (task.task_id, task.attempt) == (task_id, 1)


async def test_jobs_share_workers_fairly(orch: Orchestrator, db: Db) -> None:
    job_a, _ = db.job(no_of_files=3)
    job_b, _ = db.job(no_of_files=3)
    a = [db.task(job_a, files=1) for _ in range(3)]
    b = [db.task(job_b, files=1) for _ in range(3)]
    await orch.service.load_job(job_a)
    await orch.service.load_job(job_b)

    leased = [await lease(orch, f"w{i}") for i in range(4)]

    assert [t.task_id for t in leased if t] == [a[0], b[0], a[1], b[1]]


# ---- reports --------------------------------------------------------------------------------


async def test_final_done_records_the_result_and_succeeds_the_job(
    orch: Orchestrator, db: Db
) -> None:
    job_id, job_uuid = db.job(no_of_files=2)
    task_id = db.task(job_id, files=2, final=True)
    await orch.service.load_job(job_id)
    task = await lease(orch, "w1")
    assert task is not None

    await report_done(orch, task)
    await report_done(orch, task)  # a retried report is harmless

    assert db.task_row(task_id)["status"] == "done"
    assert db.output_files(job_id) == [("result", f"results/{job_uuid}.csv", 2)]
    assert db.job_row(job_id)["status"] == "succeeded"


async def test_partial_done_records_a_partial(orch: Orchestrator, db: Db) -> None:
    job_id, job_uuid = db.job(no_of_files=10)
    task_id = db.task(job_id, files=4)
    await orch.service.load_job(job_id)
    task = await lease(orch, "w1")
    assert task is not None and task.output_key == f"partials/{job_uuid}/{task_id}.csv"

    await report_done(orch, task)

    assert db.output_files(job_id) == [("partial", task.output_key, 4)]
    assert db.job_row(job_id)["status"] == "running"


async def test_retryable_failure_backs_off_then_retries(orch: Orchestrator, db: Db) -> None:
    job_id, _ = db.job(no_of_files=1)
    task_id = db.task(job_id, files=1, final=True)
    await orch.service.load_job(job_id)
    first = await lease(orch, "w1")
    assert first is not None

    await report_failed(orch, first, retryable=True)

    row = db.task_row(task_id)
    assert (row["status"], row["backing_off"], row["error"]) == ("pending", True, "boom")
    assert await lease(orch, "w2", wait=0) is None  # still backing off
    second = await lease(orch, "w2", wait=2)
    assert second is not None and (second.task_id, second.attempt) == (task_id, 2)
    assert db.task_row(task_id)["worker_id"] == "w2"


async def test_bad_input_fails_the_job_and_stops_its_other_tasks(
    orch: Orchestrator, db: Db
) -> None:
    job_id, _ = db.job(no_of_files=4)
    bad, other, never = (db.task(job_id, files=1) for _ in range(3))
    await orch.service.load_job(job_id)
    t_bad, t_other = await lease(orch, "w1"), await lease(orch, "w2")
    assert t_bad is not None and t_other is not None and t_bad.task_id == bad

    await report_failed(orch, t_bad, retryable=False, user_input=True)

    assert db.job_row(job_id) == {"status": "failed", "failure_type": "user_input", "error": "boom"}
    assert db.task_row(bad)["status"] == "failed"
    assert db.task_row(other)["status"] == db.task_row(never)["status"] == "skipped"
    assert await heartbeat(orch, t_other, "w2") == pb.HeartbeatResponse.STOP
    await report_done(orch, t_other, "w2")  # late output of a failed job: ignored
    assert db.task_row(other)["status"] == "skipped"
    assert await lease(orch, "w3", wait=0) is None


async def test_poison_task_fails_the_job_after_max_attempts(orch: Orchestrator, db: Db) -> None:
    job_id, _ = db.job(no_of_files=1)
    task_id = db.task(job_id, files=1, final=True)
    await orch.service.load_job(job_id)

    for attempt in range(1, 5):
        task = await lease(orch, "w1", wait=3)
        assert task is not None and task.attempt == attempt
        await report_failed(orch, task, retryable=True)

    job = db.job_row(job_id)
    assert (job["status"], job["failure_type"]) == ("failed", "internal")
    assert job["error"] == "out of attempts: boom"
    assert db.task_row(task_id)["status"] == "failed"


# ---- leases ---------------------------------------------------------------------------------


async def test_heartbeats_keep_the_lease(orch: Orchestrator, db: Db) -> None:
    job_id, _ = db.job(no_of_files=1)
    task_id = db.task(job_id, files=1, final=True)
    await orch.service.load_job(job_id)
    task = await lease(orch, "w1")
    assert task is not None

    for _ in range(5):  # 2.5 s, well past the 1 s lease
        await asyncio.sleep(0.5)
        assert await heartbeat(orch, task) == pb.HeartbeatResponse.CONTINUE

    assert await lease(orch, "w2", wait=0) is None
    assert db.task_row(task_id)["attempts"] == 1


async def test_dead_worker_lease_expires_and_another_worker_takes_over(
    orch: Orchestrator, db: Db
) -> None:
    job_id, _ = db.job(no_of_files=1)
    task_id = db.task(job_id, files=1, final=True)
    await orch.service.load_job(job_id)
    dead = await lease(orch, "dead")
    assert dead is not None

    # no heartbeats: lease (1 s) expires, backoff (0.2 s), then w2 gets it
    replacement = await lease(orch, "w2", wait=3)

    assert replacement is not None and (replacement.task_id, replacement.attempt) == (task_id, 2)
    assert db.task_row(task_id)["worker_id"] == "w2"
    assert await heartbeat(orch, dead, "dead") == pb.HeartbeatResponse.STOP  # fenced out
    await report_done(orch, dead, "dead")  # a zombie's done still counts: same output
    assert db.task_row(task_id)["status"] == "done"
    await report_done(orch, replacement, "w2")  # second done: no-op
    assert len(db.output_files(job_id)) == 1


async def test_live_workers_are_the_ones_seen_recently(orch: Orchestrator) -> None:
    await lease(orch, "a", wait=0)
    await lease(orch, "b", wait=0)
    assert orch.service.live_workers() == 2
