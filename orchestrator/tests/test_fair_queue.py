import random
from collections import Counter

from dmean_orchestrator.scheduling.fair_queue import (
    CompleteOutcome,
    FailOutcome,
    FairQueue,
    Heartbeat,
    Kind,
    TaskState,
)

T0 = 1000.0


def queue_with(jobs: dict[int, int], *, created: dict[int, float] | None = None) -> FairQueue:
    """jobs: job_id → number of map tasks. Task ids are job_id * 1000 + i."""
    q = FairQueue()
    for job_id, n in jobs.items():
        q.add_job(job_id, (created or {}).get(job_id, float(job_id)))
        for i in range(n):
            q.add_task(job_id * 1000 + i, job_id, Kind.MAP, now=T0)
    return q


def take_id(q: FairQueue, now: float = T0, worker: str = "w") -> int:
    task = q.take(worker, now)
    assert task is not None
    return task.task_id


# ---- fairness -------------------------------------------------------------------------------


def test_fifo_within_a_job() -> None:
    q = queue_with({1: 3})
    assert [take_id(q) for _ in range(3)] == [1000, 1001, 1002]
    assert q.take("w", T0) is None


def test_jobs_alternate_by_fewest_in_flight_oldest_first() -> None:
    q = queue_with({1: 5, 2: 5})
    assert [take_id(q) // 1000 for _ in range(6)] == [1, 2, 1, 2, 1, 2]


def test_new_job_catches_up_to_its_fair_share() -> None:
    q = queue_with({1: 10, 2: 10})
    for _ in range(6):
        take_id(q)  # jobs 1 and 2 have 3 in flight each
    q.add_job(3, 3.0)
    for i in range(10):
        q.add_task(3000 + i, 3, Kind.MAP, now=T0)
    # job 3 gets every worker until it is level with the others
    assert [take_id(q) // 1000 for _ in range(3)] == [3, 3, 3]
    assert {q.in_flight(j) for j in (1, 2, 3)} == {3}


def test_finished_task_frees_a_slot_for_its_job() -> None:
    q = queue_with({1: 5, 2: 5})
    first = take_id(q)  # job 1
    take_id(q)  # job 2
    assert q.complete(first, T0) is CompleteOutcome.ACCEPTED
    assert take_id(q) // 1000 == 1  # job 1 now has fewer in flight


def test_job_without_ready_tasks_gives_its_share_away() -> None:
    q = queue_with({1: 1, 2: 10})
    take_id(q)  # job 1's only task
    assert [take_id(q) // 1000 for _ in range(5)] == [2, 2, 2, 2, 2]


def test_merges_go_first_across_jobs() -> None:
    q = queue_with({1: 3, 2: 3})
    q.add_task(2900, 2, Kind.MERGE, now=T0)
    q.add_task(1900, 1, Kind.MERGE, now=T0)
    assert [take_id(q) for _ in range(3)] == [2900, 1900, 1000]


def test_fair_share_under_random_load() -> None:
    """Three busy jobs and 30 workers finishing at random: in-flight stays within 1 of equal.
    Each job has more tasks than the test hands out, so none runs out (a job that runs out
    correctly gives its share away; see test_job_without_ready_tasks_gives_its_share_away)."""
    rng = random.Random(42)
    q = queue_with({1: 1000, 2: 1000, 3: 1000})
    leased = [take_id(q) for _ in range(30)]
    for step in range(2000):
        done = leased.pop(rng.randrange(len(leased)))
        q.complete(done, T0 + step)
        leased.append(take_id(q, T0 + step))
        counts = [q.in_flight(j) for j in (1, 2, 3)]
        assert max(counts) - min(counts) <= 1
    assert sum(Counter(t // 1000 for t in leased).values()) == 30


# ---- leases ---------------------------------------------------------------------------------


def test_lease_expires_without_heartbeat_and_retries_with_backoff() -> None:
    q = queue_with({1: 1})
    t = take_id(q)
    assert q.expire_due(T0 + 29) == []
    [expired] = q.expire_due(T0 + 30)
    assert (expired.task_id, expired.outcome) == (t, FailOutcome.RETRY)
    assert q.tasks[t].state is TaskState.DELAYED
    assert q.take("w", T0 + 31) is None  # backoff: 2 s after attempt 1
    assert q.release_due(T0 + 32) == [t]
    task = q.take("w2", T0 + 32)
    assert task is not None and (task.task_id, task.attempt) == (t, 2)


def test_heartbeat_keeps_the_lease_alive() -> None:
    q = queue_with({1: 1})
    t = take_id(q)
    for now in (T0 + 10, T0 + 20, T0 + 30, T0 + 40):
        assert q.heartbeat(t, 1, now) is Heartbeat.CONTINUE
        assert q.expire_due(now) == []
    assert [e.task_id for e in q.expire_due(T0 + 70)] == [t]  # 30 s after the last heartbeat


def test_hung_worker_is_stopped_at_the_max_lease_lifetime() -> None:
    q = FairQueue(max_lease_secs=100)
    q.add_job(1, 1.0)
    q.add_task(1000, 1, Kind.MAP, now=T0)
    t = take_id(q)
    now = T0
    while now < T0 + 90:
        now += 10
        assert q.heartbeat(t, 1, now) is Heartbeat.CONTINUE
    assert q.heartbeat(t, 1, T0 + 100) is Heartbeat.STOP
    assert [e.task_id for e in q.expire_due(T0 + 100)] == [t]


def test_stale_attempt_cannot_heartbeat_or_fail_but_its_done_counts() -> None:
    q = queue_with({1: 1})
    t = take_id(q)
    q.expire_due(T0 + 30)
    q.release_due(T0 + 32)
    take_id(q, T0 + 32)  # attempt 2 is now running
    assert q.heartbeat(t, 1, T0 + 33) is Heartbeat.STOP
    assert q.fail(t, 1, retryable=False, now=T0 + 33) is FailOutcome.IGNORED
    assert q.complete(t, T0 + 34) is CompleteOutcome.ACCEPTED  # zombie's output is identical
    assert q.complete(t, T0 + 35) is CompleteOutcome.DUPLICATE  # attempt 2's done: no-op
    assert q.in_flight(1) == 0


def test_poison_task_fails_the_job_after_four_attempts() -> None:
    q = queue_with({1: 1})
    now = T0
    outcomes: list[FailOutcome] = []
    for _ in range(4):
        q.release_due(now)
        task = q.take("w", now)
        assert task is not None
        outcomes.append(q.fail(task.task_id, task.attempt, retryable=True, now=now))
        now += 60
    assert outcomes == [FailOutcome.RETRY] * 3 + [FailOutcome.FAIL_JOB]


def test_backoff_doubles_up_to_the_cap() -> None:
    q = FairQueue(max_attempts=10)
    q.add_job(1, 1.0)
    q.add_task(1000, 1, Kind.MAP, now=T0)
    delays: list[float] = []
    now = T0
    for _ in range(6):
        task = q.take("w", now)
        assert task is not None
        q.fail(task.task_id, task.attempt, retryable=True, now=now)
        delays.append(q.tasks[1000].not_before - now)
        now = q.tasks[1000].not_before
        q.release_due(now)
    assert delays == [2, 4, 8, 16, 30, 30]


def test_not_retryable_failure_fails_the_job_immediately() -> None:
    q = queue_with({1: 2})
    t = take_id(q)
    assert q.fail(t, 1, retryable=False, now=T0) is FailOutcome.FAIL_JOB


def test_retry_goes_to_the_front_of_its_job_line() -> None:
    q = queue_with({1: 5})
    t = take_id(q)  # 1000
    q.fail(t, 1, retryable=True, now=T0)
    q.release_due(T0 + 2)
    assert take_id(q, T0 + 2) == t  # ahead of 1001…1004


def test_release_undoes_a_take_whose_commit_failed() -> None:
    q = queue_with({1: 2})
    task = q.take("w", T0)
    assert task is not None
    q.release(task.task_id)
    again = q.take("w", T0)
    assert again is not None and (again.task_id, again.attempt) == (task.task_id, 1)


# ---- job failure and restart ------------------------------------------------------------------


def test_failed_job_is_never_handed_out_again() -> None:
    q = queue_with({1: 5, 2: 5})
    leased = take_id(q)  # job 1
    unfinished = q.drop_job(1)
    assert set(unfinished) == {1000, 1001, 1002, 1003, 1004}
    assert all(take_id(q) // 1000 == 2 for _ in range(5))
    assert q.take("w", T0) is None
    assert q.heartbeat(leased, 1, T0 + 5) is Heartbeat.STOP
    assert q.complete(leased, T0 + 6) is CompleteOutcome.DUPLICATE  # output ignored


def test_restart_restores_leases_with_a_grace_period() -> None:
    q = FairQueue()
    q.add_job(1, 1.0)
    q.restore_leased(1000, 1, Kind.MAP, attempt=2, worker_id="w", now=T0)
    q.add_task(1001, 1, Kind.MAP, attempt=1, not_before=T0 + 5, now=T0)  # was in backoff
    assert q.in_flight(1) == 1
    assert q.heartbeat(1000, 2, T0 + 10) is Heartbeat.CONTINUE  # the worker carries on
    assert q.take("w", T0) is None
    q.release_due(T0 + 5)
    assert take_id(q, T0 + 5) == 1001


def test_next_wakeup_is_the_earliest_timer() -> None:
    q = queue_with({1: 2})
    assert q.next_wakeup() is None
    t = take_id(q)
    assert q.next_wakeup() == T0 + 30
    q.fail(t, 1, retryable=True, now=T0 + 1)
    assert q.next_wakeup() == T0 + 3  # retry due before the (now stale) lease entry


def test_unfinished_counts_down_to_zero_as_tasks_finish() -> None:
    q = queue_with({1: 3})
    assert q.unfinished(1) == 3
    a, b = take_id(q), take_id(q)
    q.complete(a, T0)
    q.fail(b, 1, retryable=True, now=T0)  # retrying still counts as unfinished
    assert q.unfinished(1) == 2
    q.add_task(1900, 1, Kind.MERGE, now=T0)
    assert q.unfinished(1) == 3
