"""Restart recovery, the advisory lock, and storage cleanup. Needs postgres + minio."""

import asyncio
from typing import Any

import psycopg
import pytest
from conftest import (
    Db,
    IntegrationSettings,
    Orchestrator,
    lease,
    object_exists,
    report_done,
    start_orchestrator,
    wait_for,
)

from dmean_orchestrator.main import serve
from dmean_orchestrator.rpc import task_queue_pb2 as pb

pytestmark = pytest.mark.integration


async def heartbeat(orch: Orchestrator, task: Any, worker: str) -> int:
    resp = await orch.stub.Heartbeat(
        pb.HeartbeatRequest(worker_id=worker, task_id=task.task_id, attempt=task.attempt)
    )
    return resp.action


async def finish_job(orch: Orchestrator, db: Db, job_id: int, objects: Any = None) -> None:
    """A fake worker: lease and report done until the job ends (optionally writing outputs)."""
    for _ in range(60):
        if db.job_row(job_id)["status"] != "running":
            return
        task = await lease(orch, "w-finish")
        if task is not None:
            if objects is not None:
                objects.put_object(Bucket="dmean", Key=task.output_key, Body=b"1\n2,3\n")
            await report_done(orch, task, "w-finish")


# ---- restart --------------------------------------------------------------------------------


async def test_restart_keeps_leases_pending_tasks_and_unmerged_partials(
    settings: IntegrationSettings, db: Db
) -> None:
    job_id = db.queued_job(no_of_files=5)  # N = 2 → maps of 2, 2, 1
    async with start_orchestrator(settings) as first:
        await first.service.plan(job_id)
        t1, t2 = await lease(first, "w1"), await lease(first, "w2")
        assert t1 is not None and t2 is not None
        await report_done(first, t1, "w1")  # one partial in the pool, waiting for a second
    # the first orchestrator is gone; w2 is still working on t2

    async with start_orchestrator(settings, rebuild=True) as second:
        assert await heartbeat(second, t2, "w2") == pb.HeartbeatResponse.CONTINUE  # grace lease
        t3 = await lease(second, "w3")
        assert t3 is not None and t3.kind == "map"  # the pending map is still queued
        await report_done(second, t2, "w2")  # restored partial + this one → a merge
        merge = await lease(second, "w4")
        assert merge is not None and merge.kind == "merge" and len(merge.input_keys) == 2
        await report_done(second, merge, "w4")
        await report_done(second, t3, "w3")
        await finish_job(second, db, job_id)

    assert db.job_row(job_id)["status"] == "succeeded"
    assert [t["no_of_files"] for t in db.tasks(job_id) if t["is_final"]] == [5]
    assert db.files_used_twice(job_id) == 0


async def test_restart_creates_a_merge_the_crash_missed(
    settings: IntegrationSettings, db: Db
) -> None:
    job_id = db.queued_job(no_of_files=3)  # maps of 2 and 1
    async with start_orchestrator(settings) as first:
        await first.service.plan(job_id)
    a, b = (t["id"] for t in db.tasks(job_id))
    # both maps' done were committed, then the orchestrator died before creating the merge
    db.fake_done(a, f"partials/x/{a}.csv")
    db.fake_done(b, f"partials/x/{b}.csv")

    async with start_orchestrator(settings, rebuild=True) as second:
        final = await lease(second, "w1")
        assert final is not None and final.kind == "merge" and final.is_final
        await report_done(second, final, "w1")

    assert db.job_row(job_id)["status"] == "succeeded"


async def test_a_second_orchestrator_waits_for_the_lock(settings: IntegrationSettings) -> None:
    holder = await psycopg.AsyncConnection.connect(settings.db_dsn, autocommit=True)
    await holder.execute("SELECT pg_advisory_lock(%s)", (settings.advisory_lock_key,))
    stop = asyncio.Event()
    second = asyncio.create_task(serve(settings.model_copy(update={"grpc_port": 0}), stop))
    try:
        await asyncio.sleep(1)
        waiting = await (
            await holder.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
            )
        ).fetchone()
        assert waiting is not None and waiting[0] == 1  # blocked, not scheduling
        assert not second.done()
    finally:
        await holder.close()  # the first orchestrator exits: the second takes over
    await asyncio.sleep(1)
    stop.set()
    assert await asyncio.wait_for(second, timeout=10) == 0


# ---- cleanup --------------------------------------------------------------------------------


async def test_finished_job_inputs_and_partials_are_deleted_result_kept(
    orch: Orchestrator, db: Db, objects: Any, settings: IntegrationSettings
) -> None:
    job_id = db.queued_job(no_of_files=3)
    inputs = [f["key"] for f in db.files(job_id)]
    for key in inputs:
        objects.put_object(Bucket=settings.s3_bucket, Key=key, Body=b"1,2,3\n")
    await orch.service.plan(job_id)

    await finish_job(orch, db, job_id, objects)
    assert db.job_row(job_id)["status"] == "succeeded"

    def cleaned() -> bool:
        return all(f["deleted"] for f in db.files(job_id) if f["type"] != "result")

    await wait_for(cleaned)
    files = db.files(job_id)
    for f in files:
        exists = object_exists(objects, settings.s3_bucket, f["key"])
        assert exists == (f["type"] == "result"), f
    [result] = [f for f in files if f["type"] == "result"]
    assert not result["deleted"]
    objects.delete_object(Bucket=settings.s3_bucket, Key=result["key"])


async def test_failed_job_inputs_are_deleted(
    orch: Orchestrator, db: Db, objects: Any, settings: IntegrationSettings
) -> None:
    job_id = db.queued_job(no_of_files=4)
    inputs = [f["key"] for f in db.files(job_id)]
    for key in inputs:
        objects.put_object(Bucket=settings.s3_bucket, Key=key, Body=b"1,2\n")
    await orch.service.plan(job_id)
    task = await lease(orch, "w1")
    assert task is not None
    await orch.stub.Report(
        pb.ReportRequest(
            worker_id="w1",
            task_id=task.task_id,
            attempt=task.attempt,
            failed=pb.Failed(failure_type=pb.USER_INPUT, retryable=False, error="bad csv"),
        )
    )

    await wait_for(lambda: all(f["deleted"] for f in db.files(job_id)))
    assert not any(object_exists(objects, settings.s3_bucket, k) for k in inputs)


async def test_sweep_expires_abandoned_uploads_and_retries_cleanups(
    orch: Orchestrator, db: Db, objects: Any, settings: IntegrationSettings
) -> None:
    abandoned = db.upload(expired=True)
    fresh = db.upload(expired=False)
    for upload in (abandoned, fresh):
        for name in ("a.csv", "b.csv"):
            objects.put_object(Bucket=settings.s3_bucket, Key=f"inputs/{upload}/{name}", Body=b"1")
    # a finished job whose cleanup never ran (e.g. the orchestrator died right after)
    job_id = db.queued_job(no_of_files=2)
    leftover = db.files(job_id)[0]["key"]
    objects.put_object(Bucket=settings.s3_bucket, Key=leftover, Body=b"1,2,3\n")
    db.admin.execute("UPDATE jobs SET status = 'succeeded' WHERE id = %s", (job_id,))

    cleaner = orch.service._cleaner  # pyright: ignore[reportPrivateUsage]
    assert cleaner is not None
    await cleaner.sweep()

    assert db.upload_status(abandoned) == "expired"
    assert db.upload_status(fresh) == "open"
    assert not object_exists(objects, settings.s3_bucket, f"inputs/{abandoned}/a.csv")
    assert object_exists(objects, settings.s3_bucket, f"inputs/{fresh}/a.csv")
    assert not object_exists(objects, settings.s3_bucket, leftover)
    assert all(f["deleted"] for f in db.files(job_id))
    for name in ("a.csv", "b.csv"):
        objects.delete_object(Bucket=settings.s3_bucket, Key=f"inputs/{fresh}/{name}")
