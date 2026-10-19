"""One task execution: read, skip if finished, sum, write, report."""

import time
from collections.abc import Callable

import dramatiq
import redis.exceptions
import structlog
from dramatiq.middleware.time_limit import TimeLimitExceeded

from dmean_worker.database.task_reader import TaskReader
from dmean_worker.processing.aggregation import TaskFailed, render_output, sum_inputs
from dmean_worker.processing.task import Task
from dmean_worker.queue.messages import FailureType, Report, ReportEvent, report_message
from dmean_worker.storage.s3 import Storage

log = structlog.get_logger()


class TaskRunner:
    def __init__(
        self,
        *,
        broker: dramatiq.Broker,
        reader: TaskReader,
        storage: Storage,
        worker_id: str,
        job_check_secs: float,
        report_give_up_secs: float = 300.0,
    ) -> None:
        self._broker = broker
        self._reader = reader
        self._storage = storage
        self.worker_id = worker_id
        self._job_check_secs = job_check_secs
        self._report_give_up_secs = report_give_up_secs

    def run(self, task_id: int, *, last_attempt: bool) -> None:
        with structlog.contextvars.bound_contextvars(task_id=task_id, worker_id=self.worker_id):
            task = self._reader.read_task(task_id)
            if task is None:
                log.warning("task not found, skipping")
                return
            structlog.contextvars.bind_contextvars(job_id=task.job_id)
            if not task.should_run:
                # duplicate delivery or a failed job: ack without computing
                log.info("skipping", task_status=task.status, job_status=task.job_status)
                return

            self._report(
                Report(task_id=task_id, event=ReportEvent.STARTED, worker_id=self.worker_id)
            )
            try:
                self._execute(task)
            except TaskFailed as err:
                log.warning("task failed", failure_type=err.failure_type, error=str(err))
                self._report(self._failed(task_id, err.failure_type, str(err)))
            except (Exception, TimeLimitExceeded) as err:
                if last_attempt:
                    log.error("retries exhausted", error=repr(err))
                    self._report(
                        self._failed(task_id, FailureType.INTERNAL, f"retries exhausted: {err!r}")
                    )
                raise  # Dramatiq retries with backoff, or dead-letters after the last attempt

    def _execute(self, task: Task) -> None:
        # An earlier attempt may have written the output and died before reporting
        size = self._storage.size_if_exists(task.output_key)
        if size is not None:
            log.info("output already exists, reporting done", key=task.output_key)
        else:
            started = time.perf_counter()
            summed = sum_inputs(task, self._storage.get_text, self._job_stopped(task.job_id))
            if summed is None:
                log.info("job is no longer running, stopping early")
                return
            size = self._storage.put_text(task.output_key, render_output(task, *summed))
            log.info(
                "task done",
                key=task.output_key,
                files=task.no_of_files,
                secs=round(time.perf_counter() - started, 3),
            )
        self._report(
            Report(
                task_id=task.id,
                event=ReportEvent.DONE,
                worker_id=self.worker_id,
                output_key=task.output_key,
                size_bytes=size,
            )
        )

    def _job_stopped(self, job_id: int) -> Callable[[], bool]:
        """Re-reads the job status at most every job_check_secs (a failed job stops its tasks)."""
        last_check = time.monotonic()

        def stopped() -> bool:
            nonlocal last_check
            if time.monotonic() - last_check < self._job_check_secs:
                return False
            last_check = time.monotonic()
            return self._reader.job_status(job_id) != "running"

        return stopped

    def _failed(self, task_id: int, failure_type: FailureType, error: str) -> Report:
        return Report(
            task_id=task_id,
            event=ReportEvent.FAILED,
            worker_id=self.worker_id,
            failure_type=failure_type,
            retryable=False,
            error=error,
        )

    def _report(self, report: Report) -> None:
        """Holds the report and retries while Redis is unreachable; never drops it silently."""
        delay, deadline = 1.0, time.monotonic() + self._report_give_up_secs
        while True:
            try:
                self._broker.enqueue(report_message(report))
                return
            except redis.exceptions.ConnectionError as err:
                if time.monotonic() > deadline:
                    raise
                log.warning("redis unavailable, holding report", error=str(err), retry_in=delay)
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
