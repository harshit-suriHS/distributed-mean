"""
Worker process: Lease → run → Report, one task at a time.

Shutdown follows Celery's warm/cold stages:
- first SIGTERM or SIGINT drains: an idle long-poll is cancelled at once; a running task keeps
  heartbeating, finishes and is reported, then the process exits 0.
- a second signal, or SIGQUIT, stops now: the running task is abandoned and reported as a
  retryable failure, so the orchestrator hands it to another worker at once (no lease wait).
- SIGKILL needs nothing from us: the lease expires and the task is retried.
"""

import os
import signal
import socket
import threading
import time

import grpc
import structlog

from dmean_worker.logs import configure_logging
from dmean_worker.processing.outcome import Abandoned, Failed, FailureType
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.queue.client import TaskQueueClient
from dmean_worker.queue.heartbeat import LeaseKeeper
from dmean_worker.settings import Settings
from dmean_worker.storage.s3 import Storage

log = structlog.get_logger()


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def run_worker(
    settings: Settings,
    client: TaskQueueClient,
    runner: TaskRunner,
    draining: threading.Event,
    stop_now: threading.Event | None = None,
) -> None:
    stop_now = stop_now or threading.Event()
    backoff = 1.0
    while not draining.is_set():
        try:
            task = client.lease(settings.lease_wait_secs, cancel=draining)
            backoff = 1.0
        except grpc.RpcError as err:
            log.warning("orchestrator unreachable", error=str(err.code()), retry_in=backoff)
            draining.wait(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        if task is None:
            continue  # no work within the wait: ask again
        with structlog.contextvars.bound_contextvars(task_id=task.task_id, attempt=task.attempt):
            log.info("leased", kind=task.kind, files=task.no_of_files, final=task.is_final)
            with LeaseKeeper(client, task) as keeper:
                outcome = runner.run(
                    task, should_stop=lambda: keeper.stopped.is_set() or stop_now.is_set()
                )
            if isinstance(outcome, Abandoned) and stop_now.is_set() and not keeper.stopped.is_set():
                log.warning("stopped mid-task, handing it back")
                outcome = Failed(FailureType.INTERNAL, retryable=True, error="worker stopped")
            if not isinstance(outcome, Abandoned):
                client.report(task, outcome, settings.report_give_up_secs)


def main() -> None:
    # Handlers first: a SIGTERM arriving during startup must not be lost
    draining = threading.Event()
    stop_now = threading.Event()

    def on_signal(signum: int, frame: object) -> None:
        if draining.is_set() or signum == signal.SIGQUIT:
            stop_now.set()  # second signal (or QUIT): don't wait for the running task
        draining.set()

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
        signal.signal(sig, on_signal)

    settings = Settings()  # pyright: ignore[reportCallIssue]  # values come from env
    configure_logging(settings.log_level, "worker")
    worker_id = settings.worker_id or make_worker_id()
    structlog.contextvars.bind_contextvars(worker_id=worker_id)

    client = TaskQueueClient(settings.orchestrator_addr, worker_id)
    runner = TaskRunner(Storage(settings), settings.slowdown_secs_per_file, settings.prefetch_files)
    log.info("worker started", orchestrator=settings.orchestrator_addr)
    started = time.monotonic()
    run_worker(settings, client, runner, draining, stop_now)
    client.close()
    log.info("worker stopped", uptime_secs=round(time.monotonic() - started))


if __name__ == "__main__":
    main()
