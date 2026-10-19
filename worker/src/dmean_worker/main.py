"""
Worker process: Lease → run → Report, one task at a time.

SIGTERM drains: an idle long-poll is cancelled at once; a running task finishes and is reported.
"""

import os
import signal
import socket
import threading
import time

import grpc
import structlog

from dmean_worker.logs import configure_logging
from dmean_worker.processing.outcome import Abandoned
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.queue.client import TaskQueueClient
from dmean_worker.queue.heartbeat import LeaseKeeper
from dmean_worker.settings import Settings
from dmean_worker.storage.s3 import Storage

log = structlog.get_logger()


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def run_worker(
    settings: Settings, client: TaskQueueClient, runner: TaskRunner, draining: threading.Event
) -> None:
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
                outcome = runner.run(task, should_stop=keeper.stopped.is_set)
            if not isinstance(outcome, Abandoned):
                client.report(task, outcome, settings.report_give_up_secs)


def main() -> None:
    settings = Settings()  # pyright: ignore[reportCallIssue]  # values come from env
    configure_logging(settings.log_level, "worker")
    worker_id = settings.worker_id or make_worker_id()
    structlog.contextvars.bind_contextvars(worker_id=worker_id)

    draining = threading.Event()

    def on_signal(signum: int, frame: object) -> None:
        draining.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)

    client = TaskQueueClient(settings.orchestrator_addr, worker_id)
    runner = TaskRunner(Storage(settings), settings.slowdown_secs_per_file)
    log.info("worker started", orchestrator=settings.orchestrator_addr)
    started = time.monotonic()
    run_worker(settings, client, runner, draining)
    client.close()
    log.info("worker stopped", uptime_secs=round(time.monotonic() - started))


if __name__ == "__main__":
    main()
