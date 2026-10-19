"""Planner + merge pool over real gRPC and Postgres (N = 2 via the test settings)."""

import pytest
from conftest import Db, Orchestrator, lease, report_done, wait_for

pytestmark = pytest.mark.integration


def is_running(db: Db, job_id: int) -> bool:
    return db.job_row(job_id)["status"] == "running"


async def test_notify_plans_the_job_into_map_tasks(orch: Orchestrator, db: Db) -> None:
    job_id = db.queued_job(no_of_files=5)
    db.notify_new_job(job_id)
    await wait_for(lambda: is_running(db, job_id))

    tasks = db.tasks(job_id)
    assert [(t["kind"], t["no_of_files"], t["inputs"], t["is_final"]) for t in tasks] == [
        ("map", 2, 2, False),
        ("map", 2, 2, False),
        ("map", 1, 1, False),
    ]
    assert db.files_used_twice(job_id) == 0
    task = await lease(orch, "w1")
    assert task is not None and task.task_id == tasks[0]["id"]


async def test_job_that_fits_in_one_task_writes_the_result_directly(
    orch: Orchestrator, db: Db
) -> None:
    job_id = db.queued_job(no_of_files=2)
    await orch.service.plan(job_id)

    [task] = db.tasks(job_id)
    assert (task["no_of_files"], task["is_final"]) == (2, True)


async def test_duplicate_notify_is_harmless(orch: Orchestrator, db: Db) -> None:
    job_id = db.queued_job(no_of_files=4)
    await orch.service.plan(job_id)
    await orch.service.plan(job_id)
    db.notify_new_job(job_id)

    assert len(db.tasks(job_id)) == 2


async def test_jobs_queued_while_down_are_planned_on_startup(orch: Orchestrator, db: Db) -> None:
    job_id = db.queued_job(no_of_files=3)  # no NOTIFY: as if the orchestrator missed it
    await orch.service.plan_queued()

    assert is_running(db, job_id) and len(db.tasks(job_id)) == 2


async def test_missing_input_files_fail_the_job(orch: Orchestrator, db: Db) -> None:
    job_id = db.queued_job(no_of_files=5, inputs=4)
    await orch.service.plan(job_id)

    job = db.job_row(job_id)
    assert (job["status"], job["failure_type"]) == ("failed", "internal")
    assert job["error"] == "expected 5 input files, found 4"


async def test_job_runs_to_completion_through_merges(orch: Orchestrator, db: Db) -> None:
    """F = 7, N = 2: maps cover 2, 2, 2, 1 files; merges of 2 partials until one covers 7."""
    job_id = db.queued_job(no_of_files=7)
    db.notify_new_job(job_id)
    await wait_for(lambda: is_running(db, job_id))

    leased_kinds: list[str] = []
    for _ in range(50):  # a fake worker: lease, report done, repeat
        if not is_running(db, job_id):
            break
        task = await lease(orch, "w1")
        if task is not None:
            leased_kinds.append(task.kind)
            await report_done(orch, task)

    assert db.job_row(job_id)["status"] == "succeeded"
    tasks = db.tasks(job_id)
    maps = [t for t in tasks if t["kind"] == "map"]
    merges = [t for t in tasks if t["kind"] == "merge"]
    assert [m["no_of_files"] for m in maps] == [2, 2, 2, 1]
    assert all(t["status"] == "done" for t in tasks)
    assert [t["no_of_files"] for t in tasks if t["is_final"]] == [7]  # exactly one final task
    assert len(merges) == 3  # 4 partials → 2 → 1 with fan-in 2
    assert all(m["inputs"] == 2 for m in merges)
    assert db.files_used_twice(job_id) == 0
    outputs = db.output_files(job_id)
    assert [o for o in outputs if o[0] == "result"] == [("result", outputs[-1][1], 7)]
    # merges are handed out first: right after the first two maps finish, a merge comes next
    assert leased_kinds[:3] == ["map", "map", "merge"]


async def test_two_jobs_complete_side_by_side(orch: Orchestrator, db: Db) -> None:
    a = db.queued_job(no_of_files=5)
    b = db.queued_job(no_of_files=3)
    await orch.service.plan_queued()

    for _ in range(60):
        if not (is_running(db, a) or is_running(db, b)):
            break
        task = await lease(orch, "w1")
        if task is not None:
            await report_done(orch, task)

    assert db.job_row(a)["status"] == db.job_row(b)["status"] == "succeeded"
