"""
The fair, lease-based task queue: pure data structures, no I/O, no clock of its own.

Rule: a worker gets a ready merge first (FIFO across jobs); otherwise the next task of the job
with the fewest tasks in flight (oldest job on ties). With equal fair shares that is max-min
fairness: a job with nothing ready drops out and its share goes to the others.

Structures (see design.md §9):
- tasks        dict task_id → QueuedTask            state of every known task
- job.ready    deque per job                        that job's ready map tasks, FIFO
- merges       deque                                ready merge tasks of all jobs, FIFO
- job heap     (in_flight, created_at, job_id, version)   which job is next
- retry heap   (not_before, task_id)                retries waiting out their backoff
- lease heap   (deadline, task_id, attempt)         which lease expires next

Heaps use lazy deletion: changes push a new entry and stale ones are skipped when popped.

The caller (gRPC service) owns persistence: it commits each decision to Postgres and calls
`release()` to undo a `take()` whose commit failed.
"""

import heapq
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, StrEnum


class TaskState(Enum):
    READY = "ready"  # in a job line or the merge line
    DELAYED = "delayed"  # waiting out a retry backoff
    LEASED = "leased"  # held by a worker
    DONE = "done"
    DROPPED = "dropped"  # its job failed


class Kind(StrEnum):
    MAP = "map"
    MERGE = "merge"


@dataclass
class Lease:
    worker_id: str
    deadline: float
    started: float


@dataclass
class QueuedTask:
    task_id: int
    job_id: int
    kind: Kind
    state: TaskState
    attempt: int = 0  # number of times leased; the lease's fencing token
    not_before: float = 0.0
    lease: Lease | None = None


@dataclass
class _Job:
    job_id: int
    created_at: float
    ready: deque[int] = field(default_factory=deque[int])
    in_flight: int = 0
    version: int = 0
    active: bool = True


class Heartbeat(StrEnum):
    CONTINUE = "continue"
    STOP = "stop"


class FailOutcome(StrEnum):
    IGNORED = "ignored"  # stale attempt or task not leased: nothing changes
    RETRY = "retry"  # back in the queue after a backoff
    FAIL_JOB = "fail_job"  # not retryable, or out of attempts: the caller fails the job


class CompleteOutcome(StrEnum):
    ACCEPTED = "accepted"  # first done: the caller records the output
    DUPLICATE = "duplicate"  # already done (or job dropped): ignore


@dataclass(frozen=True)
class Expired:
    task_id: int
    outcome: FailOutcome  # RETRY or FAIL_JOB


class FairQueue:
    def __init__(
        self,
        *,
        lease_secs: float = 30.0,
        max_lease_secs: float = 250.0,
        max_attempts: int = 4,
        min_backoff_secs: float = 2.0,
        max_backoff_secs: float = 30.0,
    ) -> None:
        self.lease_secs = lease_secs
        self.max_lease_secs = max_lease_secs
        self.max_attempts = max_attempts
        self.min_backoff_secs = min_backoff_secs
        self.max_backoff_secs = max_backoff_secs

        self.tasks: dict[int, QueuedTask] = {}
        self._jobs: dict[int, _Job] = {}
        self._merges: deque[int] = deque()
        self._job_heap: list[tuple[int, float, int, int]] = []
        self._retry_heap: list[tuple[float, int]] = []
        self._lease_heap: list[tuple[float, int, int]] = []

    # ---- building the queue -------------------------------------------------------------

    def add_job(self, job_id: int, created_at: float) -> None:
        self._jobs.setdefault(job_id, _Job(job_id, created_at))

    def add_task(
        self,
        task_id: int,
        job_id: int,
        kind: Kind,
        *,
        attempt: int = 0,
        not_before: float = 0.0,
        now: float,
    ) -> None:
        """A pending task (new, or restored on startup). Waits for `not_before` if in the future."""
        task = QueuedTask(task_id, job_id, kind, TaskState.READY, attempt, not_before)
        self.tasks[task_id] = task
        if not_before > now:
            task.state = TaskState.DELAYED
            heapq.heappush(self._retry_heap, (not_before, task_id))
        else:
            self._make_ready(task, front=False)

    def restore_leased(
        self, task_id: int, job_id: int, kind: Kind, *, attempt: int, worker_id: str, now: float
    ) -> None:
        """A task that was running when the orchestrator restarted: give it a fresh grace lease."""
        task = QueuedTask(task_id, job_id, kind, TaskState.LEASED, attempt)
        self.tasks[task_id] = task
        self._lease(task, worker_id, now)

    # ---- leasing --------------------------------------------------------------------------

    def take(self, worker_id: str, now: float) -> QueuedTask | None:
        """Picks the next task fairly and leases it. None if nothing is ready."""
        task_id = self._pick()
        if task_id is None:
            return None
        task = self.tasks[task_id]
        task.attempt += 1
        self._lease(task, worker_id, now)
        return task

    def release(self, task_id: int) -> None:
        """Undoes a take() whose database commit failed: back to the front, attempt not used."""
        task = self.tasks[task_id]
        if task.state is not TaskState.LEASED:
            return
        task.attempt -= 1
        task.lease = None
        self._job(task).in_flight -= 1
        self._make_ready(task, front=True)

    def heartbeat(self, task_id: int, attempt: int, now: float) -> Heartbeat:
        task = self.tasks.get(task_id)
        if (
            task is None
            or task.state is not TaskState.LEASED
            or task.attempt != attempt
            or task.lease is None
            or not self._job(task).active
        ):
            return Heartbeat.STOP  # stale attempt, finished task, or failed job
        hard_limit = task.lease.started + self.max_lease_secs
        if now >= hard_limit:
            return Heartbeat.STOP  # hung: the lease expires at the hard limit
        task.lease.deadline = min(now + self.lease_secs, hard_limit)
        heapq.heappush(self._lease_heap, (task.lease.deadline, task_id, attempt))
        return Heartbeat.CONTINUE

    def complete(self, task_id: int, now: float) -> CompleteOutcome:
        """`done` from any attempt is accepted while the task isn't done: outputs are identical."""
        task = self.tasks.get(task_id)
        if task is None or task.state in (TaskState.DONE, TaskState.DROPPED):
            return CompleteOutcome.DUPLICATE
        if not self._job(task).active:  # the job failed meanwhile: ignore its outputs
            if task.state is TaskState.LEASED:
                self._job(task).in_flight -= 1
            task.state, task.lease = TaskState.DROPPED, None
            return CompleteOutcome.DUPLICATE
        if task.state is TaskState.LEASED:
            self._job(task).in_flight -= 1
            self._bump(self._job(task))
        # READY or DELAYED (its lease had expired): it's done now, so its queued entries go stale
        task.state = TaskState.DONE
        task.lease = None
        return CompleteOutcome.ACCEPTED

    def fail(self, task_id: int, attempt: int, *, retryable: bool, now: float) -> FailOutcome:
        """`failed` counts only from the current attempt: a zombie can't fail a healthy retry."""
        task = self.tasks.get(task_id)
        if task is None or task.state is not TaskState.LEASED or task.attempt != attempt:
            return FailOutcome.IGNORED
        if not self._job(task).active:
            return FailOutcome.IGNORED
        return self._end_lease(task, retryable=retryable, now=now)

    # ---- timers ---------------------------------------------------------------------------

    def expire_due(self, now: float) -> list[Expired]:
        """Leases whose deadline passed without a heartbeat: the worker is presumed dead."""
        expired: list[Expired] = []
        while self._lease_heap and self._lease_heap[0][0] <= now:
            deadline, task_id, attempt = heapq.heappop(self._lease_heap)
            task = self.tasks.get(task_id)
            if (
                task is None
                or task.state is not TaskState.LEASED
                or task.attempt != attempt
                or task.lease is None
                or task.lease.deadline != deadline
            ):
                continue  # renewed, finished or re-leased: stale entry
            if not self._job(task).active:
                self._job(task).in_flight -= 1
                task.state, task.lease = TaskState.DROPPED, None
                continue
            expired.append(Expired(task_id, self._end_lease(task, retryable=True, now=now)))
        return expired

    def release_due(self, now: float) -> list[int]:
        """Retries whose backoff ended go back to the FRONT of their line (they block the job)."""
        released: list[int] = []
        while self._retry_heap and self._retry_heap[0][0] <= now:
            _, task_id = heapq.heappop(self._retry_heap)
            task = self.tasks.get(task_id)
            if task is None or task.state is not TaskState.DELAYED or task.not_before > now:
                continue
            self._make_ready(task, front=True)
            released.append(task_id)
        return released

    def next_wakeup(self) -> float | None:
        """When the earliest retry or lease deadline is due (may be a stale entry: harmless)."""
        times = [h[0][0] for h in (self._retry_heap, self._lease_heap) if h]
        return min(times) if times else None

    # ---- jobs -----------------------------------------------------------------------------

    def drop_job(self, job_id: int) -> list[int]:
        """The job failed: nothing of it is handed out again. Returns its unfinished task ids."""
        job = self._jobs.get(job_id)
        if job is None:
            return []
        job.active = False
        unfinished: list[int] = []
        for task in self.tasks.values():
            if task.job_id != job_id or task.state in (TaskState.DONE, TaskState.DROPPED):
                continue
            unfinished.append(task.task_id)
            if task.state is not TaskState.LEASED:  # leased ones get STOP on their next heartbeat
                task.state = TaskState.DROPPED
        job.ready.clear()
        return unfinished

    def remove_job(self, job_id: int) -> None:
        """The job finished: forget its tasks."""
        self._jobs.pop(job_id, None)
        for task_id in [t.task_id for t in self.tasks.values() if t.job_id == job_id]:
            del self.tasks[task_id]

    def in_flight(self, job_id: int) -> int:
        return self._jobs[job_id].in_flight

    def has_ready(self) -> bool:
        return any(self.tasks[t].state is TaskState.READY for t in self._merges) or any(
            j.active and j.ready for j in self._jobs.values()
        )

    # ---- internals ------------------------------------------------------------------------

    def _job(self, task: QueuedTask) -> _Job:
        return self._jobs[task.job_id]

    def _pick(self) -> int | None:
        while self._merges:  # 1. merges first
            task_id = self._merges.popleft()
            task = self.tasks.get(task_id)
            if task is not None and task.state is TaskState.READY and self._job(task).active:
                return task_id
        while self._job_heap:  # 2. the job with the fewest tasks in flight
            _, _, job_id, version = heapq.heappop(self._job_heap)
            job = self._jobs.get(job_id)
            if job is None or not job.active or version != job.version:
                continue  # stale entry
            while job.ready:  # 3. that job's next ready task
                task_id = job.ready.popleft()
                if self.tasks[task_id].state is TaskState.READY:
                    return task_id
        return None

    def _lease(self, task: QueuedTask, worker_id: str, now: float) -> None:
        task.state = TaskState.LEASED
        task.lease = Lease(worker_id, deadline=now + self.lease_secs, started=now)
        heapq.heappush(self._lease_heap, (task.lease.deadline, task.task_id, task.attempt))
        job = self._job(task)
        job.in_flight += 1
        self._bump(job)

    def _end_lease(self, task: QueuedTask, *, retryable: bool, now: float) -> FailOutcome:
        self._job(task).in_flight -= 1
        task.lease = None
        if not retryable or task.attempt >= self.max_attempts:
            task.state = TaskState.DONE  # terminal for the queue; the caller fails the job
            self._bump(self._job(task))
            return FailOutcome.FAIL_JOB
        backoff = min(self.min_backoff_secs * 2 ** (task.attempt - 1), self.max_backoff_secs)
        task.state = TaskState.DELAYED
        task.not_before = now + backoff
        heapq.heappush(self._retry_heap, (task.not_before, task.task_id))
        self._bump(self._job(task))
        return FailOutcome.RETRY

    def _make_ready(self, task: QueuedTask, *, front: bool) -> None:
        task.state = TaskState.READY
        if task.kind is Kind.MERGE:
            (self._merges.appendleft if front else self._merges.append)(task.task_id)
        else:
            job = self._job(task)
            (job.ready.appendleft if front else job.ready.append)(task.task_id)
        self._bump(self._job(task))

    def _bump(self, job: _Job) -> None:
        """Pushes a fresh heap entry; older ones for this job become stale."""
        job.version += 1
        if job.active and job.ready:
            heapq.heappush(self._job_heap, (job.in_flight, job.created_at, job.job_id, job.version))
