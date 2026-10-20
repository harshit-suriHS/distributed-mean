"""Runs one leased task: sum its inputs, write the output, say what happened."""

import time
from collections.abc import Callable

import structlog

from dmean_worker.processing.aggregation import TaskFailed, render_output, sum_inputs
from dmean_worker.processing.outcome import Abandoned, Done, Failed, FailureType, Outcome
from dmean_worker.processing.task import Task
from dmean_worker.storage.s3 import Storage

log = structlog.get_logger()


class TaskRunner:
    def __init__(
        self, storage: Storage, slowdown_secs_per_file: float = 0.0, prefetch_files: int = 1
    ) -> None:
        self._storage = storage
        self._slowdown = slowdown_secs_per_file
        self._prefetch = prefetch_files

    def run(self, task: Task, should_stop: Callable[[], bool]) -> Outcome:
        """Never raises: every result, including errors, becomes an Outcome to report."""
        try:
            # An earlier attempt may have written the output and died before reporting
            size = self._storage.size_if_exists(task.output_key)
            if size is not None:
                log.info("output already exists, reporting done", key=task.output_key)
                return Done(task.output_key, size)

            started = time.perf_counter()
            summed = sum_inputs(task, self._load, should_stop, self._prefetch)
            if summed is None:
                log.info("told to stop, abandoning the task")
                return Abandoned()
            size = self._storage.put_text(task.output_key, render_output(task, *summed))
            log.info(
                "task done",
                key=task.output_key,
                files=task.no_of_files,
                secs=round(time.perf_counter() - started, 3),
            )
            return Done(task.output_key, size)
        except TaskFailed as err:  # bad input or a broken invariant: retrying won't help
            log.warning("task failed", failure_type=err.failure_type, error=str(err))
            return Failed(err.failure_type, retryable=False, error=str(err))
        except Exception as err:  # storage errors and the like: the orchestrator retries
            log.warning("task error, will be retried", error=repr(err))
            return Failed(FailureType.INTERNAL, retryable=True, error=repr(err))

    def _load(self, key: str) -> str:
        if self._slowdown:
            time.sleep(self._slowdown)  # simulated slow worker (local demos only)
        return self._storage.get_text(key)
