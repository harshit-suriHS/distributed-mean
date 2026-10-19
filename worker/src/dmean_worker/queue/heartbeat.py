"""Keeps a task's lease alive while it runs, and passes a STOP from the orchestrator on."""

import threading
from types import TracebackType

from dmean_worker.processing.task import Task
from dmean_worker.queue.client import TaskQueueClient


class LeaseKeeper:
    """Heartbeats every task.heartbeat_seconds in a background thread. `stopped` is set when
    the orchestrator answers STOP (the job failed, or the lease was given to someone else)."""

    def __init__(self, client: TaskQueueClient, task: Task) -> None:
        self._client = client
        self._task = task
        self.stopped = threading.Event()
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="lease-keeper", daemon=True)

    def __enter__(self) -> "LeaseKeeper":
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._done.set()
        self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._done.wait(self._task.heartbeat_seconds):
            if not self._client.heartbeat(self._task):
                self.stopped.set()
                return
