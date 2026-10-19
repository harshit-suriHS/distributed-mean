"""
gRPC TaskQueue service: wraps the in-memory FairQueue and commits every decision to Postgres.

Everything runs on one asyncio loop (grpc.aio), so the queue needs no locks. Rules:
- a reply that changes state (Lease, Report) is sent only after the Postgres commit;
- if the database disagrees with memory, or a write fails after memory already changed,
  the orchestrator stops (crash-only) and rebuilds from Postgres on restart.
"""

import asyncio
from collections import deque
from collections.abc import Callable

import grpc
import structlog

from dmean_orchestrator.database.task_store import LeasedTask, TaskStore
from dmean_orchestrator.rpc import task_queue_pb2 as pb
from dmean_orchestrator.rpc import task_queue_pb2_grpc as pb_grpc
from dmean_orchestrator.scheduling.fair_queue import (
    CompleteOutcome,
    FailOutcome,
    FairQueue,
    Heartbeat,
    Kind,
    QueuedTask,
)
from dmean_orchestrator.settings import Settings
from dmean_orchestrator.storage.keys import partial_key, result_key

log = structlog.get_logger()

FAILURE_TYPES = {pb.INTERNAL: "internal", pb.USER_INPUT: "user_input"}


class TaskQueueService(pb_grpc.TaskQueueServicer):
    def __init__(
        self, settings: Settings, store: TaskStore, on_fatal: Callable[[BaseException], None]
    ) -> None:
        self._settings = settings
        self._store = store
        self._on_fatal = on_fatal
        self.queue = FairQueue(
            lease_secs=settings.lease_secs,
            max_lease_secs=settings.max_lease_secs,
            max_attempts=settings.max_attempts,
            min_backoff_secs=settings.min_backoff_secs,
            max_backoff_secs=settings.max_backoff_secs,
        )
        self._waiters: deque[tuple[str, asyncio.Future[QueuedTask]]] = deque()
        self.workers: dict[str, float] = {}  # worker_id → last seen (loop time)

    # ---- gRPC handlers -----------------------------------------------------------------

    async def Lease(
        self,
        request: pb.LeaseRequest,
        context: grpc.aio.ServicerContext[pb.LeaseRequest, pb.LeaseResponse],
    ) -> pb.LeaseResponse:
        worker_id = request.worker_id
        self._seen(worker_id)
        task = self.queue.take(worker_id, self._now())
        if task is None:
            task = await self._wait_for_task(
                worker_id, min(request.wait_seconds, self._settings.max_wait_secs)
            )
        if task is None:
            return pb.LeaseResponse()
        try:
            leased = await self._store.lease(task.task_id, task.attempt, worker_id)
        except BaseException:
            self.queue.release(task.task_id)  # nothing committed: give it to someone else
            raise
        log.info("leased", task_id=task.task_id, attempt=task.attempt, worker_id=worker_id)
        return pb.LeaseResponse(task=self._to_proto(leased, task.attempt))

    async def Heartbeat(
        self,
        request: pb.HeartbeatRequest,
        context: grpc.aio.ServicerContext[pb.HeartbeatRequest, pb.HeartbeatResponse],
    ) -> pb.HeartbeatResponse:
        self._seen(request.worker_id)
        action = self.queue.heartbeat(request.task_id, request.attempt, self._now())
        if action is Heartbeat.STOP:
            return pb.HeartbeatResponse(action=pb.HeartbeatResponse.STOP)
        return pb.HeartbeatResponse(action=pb.HeartbeatResponse.CONTINUE)

    async def Report(
        self,
        request: pb.ReportRequest,
        context: grpc.aio.ServicerContext[pb.ReportRequest, pb.ReportResponse],
    ) -> pb.ReportResponse:
        self._seen(request.worker_id)
        if request.WhichOneof("outcome") == "done":
            await self._on_done(request.task_id, request.done)
        else:
            await self._on_failed(request.task_id, request.attempt, request.failed)
        return pb.ReportResponse()

    # ---- feeding the queue ---------------------------------------------------------------

    async def load_job(self, job_id: int) -> None:
        """Adds a running job's pending tasks to the queue and wakes waiting workers."""
        created_at, pending = await self._store.load_pending(job_id)
        now = self._now()
        self.queue.add_job(job_id, created_at.timestamp())
        for t in pending:
            self.queue.add_task(
                t.task_id,
                job_id,
                Kind(t.kind),
                attempt=t.attempts,
                not_before=now + t.not_before_in,
                now=now,
            )
        self._dispatch()

    def live_workers(self) -> int:
        cutoff = self._now() - self._settings.worker_alive_secs
        return sum(1 for seen in self.workers.values() if seen >= cutoff)

    async def run_timers(self) -> None:
        """Lease expiry and retry release, woken at the earliest deadline (capped by the tick)."""
        while True:
            wake = self.queue.next_wakeup()
            delay = self._settings.timer_tick_secs
            if wake is not None:
                delay = max(0.0, min(delay, wake - self._now()))
            await asyncio.sleep(delay)
            try:
                await self._on_timers()
            except Exception as err:
                self._on_fatal(err)
                return

    # ---- internals -----------------------------------------------------------------------

    async def _on_done(self, task_id: int, done: pb.Done) -> None:
        recorded = await self._store.record_done(task_id, done.output_key, done.size_bytes)
        outcome = self.queue.complete(task_id, self._now())
        if recorded is None:
            return  # duplicate or late: already done, or the job failed
        if outcome is not CompleteOutcome.ACCEPTED:
            self._fatal(RuntimeError(f"task {task_id}: database accepted done, queue did not"))
            return
        log.info("task done", task_id=task_id, job_id=recorded.job_id, final=recorded.is_final)
        if recorded.is_final:
            log.info("job succeeded", job_id=recorded.job_id)
            self.queue.remove_job(recorded.job_id)
        # merge pool and completion of multi-task jobs: added with the planner (step 2)

    async def _on_failed(self, task_id: int, attempt: int, failed: pb.Failed) -> None:
        task = self.queue.tasks.get(task_id)
        outcome = self.queue.fail(task_id, attempt, retryable=failed.retryable, now=self._now())
        if outcome is FailOutcome.IGNORED or task is None:
            return
        failure_type = FAILURE_TYPES.get(failed.failure_type, "internal")
        log.warning(
            "task failed",
            task_id=task_id,
            attempt=attempt,
            outcome=outcome,
            failure_type=failure_type,
            error=failed.error,
        )
        try:
            if outcome is FailOutcome.RETRY:
                await self._store.retry(
                    task_id, attempt, task.not_before - self._now(), failed.error
                )
            else:
                error = failed.error if not failed.retryable else f"out of attempts: {failed.error}"
                await self._fail_job(task.job_id, task_id, failure_type, error)
        except Exception as err:
            self._fatal(err)  # memory already moved on: rebuild from Postgres

    async def _fail_job(self, job_id: int, task_id: int, failure_type: str, error: str) -> None:
        self.queue.drop_job(job_id)
        await self._store.fail_job(job_id, task_id, failure_type, error)
        log.warning("job failed", job_id=job_id, failure_type=failure_type, error=error)

    async def _on_timers(self) -> None:
        now = self._now()
        for expired in self.queue.expire_due(now):
            task = self.queue.tasks[expired.task_id]
            log.warning("lease expired", task_id=task.task_id, attempt=task.attempt)
            if expired.outcome is FailOutcome.RETRY:
                await self._store.retry(
                    task.task_id, task.attempt, task.not_before - now, "lease expired"
                )
            else:
                await self._fail_job(
                    task.job_id, task.task_id, "internal", "lease expired on every attempt"
                )
        if self.queue.release_due(now):
            self._dispatch()
        cutoff = now - 10 * self._settings.worker_alive_secs
        for worker_id in [w for w, seen in self.workers.items() if seen < cutoff]:
            del self.workers[worker_id]

    async def _wait_for_task(self, worker_id: str, timeout: float) -> QueuedTask | None:
        """Long-poll: FIFO among waiting workers. A cancelled RPC returns its task to the queue."""
        if timeout <= 0:
            return None
        future: asyncio.Future[QueuedTask] = asyncio.get_running_loop().create_future()
        self._waiters.append((worker_id, future))
        try:
            await asyncio.wait({future}, timeout=timeout)
        except asyncio.CancelledError:
            # the worker went away (RPC cancelled); if it was already handed a task, return it
            if future.done() and not future.cancelled():
                self.queue.release(future.result().task_id)
            future.cancel()
            raise
        if not future.done():
            future.cancel()  # timed out: _dispatch skips cancelled futures
            return None
        return future.result()

    def _dispatch(self) -> None:
        """Hands ready tasks to waiting workers, longest-waiting first."""
        while self._waiters:
            worker_id, future = self._waiters[0]
            if future.done():
                self._waiters.popleft()
                continue
            task = self.queue.take(worker_id, self._now())
            if task is None:
                return
            self._waiters.popleft()
            future.set_result(task)

    def _to_proto(self, leased: LeasedTask, attempt: int) -> pb.Task:
        output_key = (
            result_key(leased.job_uuid)
            if leased.is_final
            else partial_key(leased.job_uuid, leased.task_id)
        )
        return pb.Task(
            task_id=leased.task_id,
            attempt=attempt,
            kind=leased.kind,
            input_keys=leased.input_keys,
            no_of_files=leased.no_of_files,
            no_of_columns=leased.no_of_columns,
            is_final=leased.is_final,
            output_key=output_key,
            heartbeat_seconds=self._settings.heartbeat_secs,
        )

    def _seen(self, worker_id: str) -> None:
        self.workers[worker_id] = self._now()

    def _fatal(self, err: BaseException) -> None:
        log.error("database and queue diverged, stopping to rebuild", error=repr(err))
        self._on_fatal(err)

    @staticmethod
    def _now() -> float:
        return asyncio.get_running_loop().time()
