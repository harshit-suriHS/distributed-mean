"""Worker process: 1 process, 1 thread, prefetch 1. SIGTERM drains (finishes the current task)."""

import signal
import threading

import dramatiq
import redis
import structlog
from dramatiq.brokers.redis import RedisBroker

from dmean_worker.database.task_reader import TaskReader
from dmean_worker.logs import configure_logging
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.queue.actor import declare, set_runner
from dmean_worker.queue.heartbeat import Heartbeat, make_worker_id
from dmean_worker.queue.messages import TASKS_QUEUE
from dmean_worker.settings import Settings
from dmean_worker.storage.s3 import Storage

log = structlog.get_logger()


def build_worker(settings: Settings, worker_id: str) -> tuple[dramatiq.Worker, TaskReader]:
    broker = RedisBroker(url=settings.redis_url)
    dramatiq.set_broker(broker)
    broker.add_middleware(
        Heartbeat(
            redis.Redis.from_url(settings.redis_url),
            worker_id,
            settings.heartbeat_ttl_secs,
            settings.heartbeat_every_secs,
        )
    )
    declare(broker, settings.task_time_limit_secs)

    reader = TaskReader(settings.db_dsn)
    set_runner(
        TaskRunner(
            broker=broker,
            reader=reader,
            storage=Storage(settings),
            worker_id=worker_id,
            job_check_secs=settings.job_check_secs,
        )
    )

    worker = dramatiq.Worker(broker, queues={TASKS_QUEUE}, worker_threads=1)
    # Dramatiq's default prefetch is threads × 2: a busy worker would hold a second task
    # that an idle worker should get. One in flight per worker is the admission rule.
    worker.queue_prefetch = 1
    return worker, reader


def main() -> None:
    settings = Settings()  # pyright: ignore[reportCallIssue]  # values come from env
    configure_logging(settings.log_level, "worker")
    worker_id = make_worker_id()
    worker, reader = build_worker(settings, worker_id)

    stop = threading.Event()

    def on_signal(signum: int, frame: object) -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)

    worker.start()
    log.info("worker started", worker_id=worker_id)
    stop.wait()
    log.info("draining: finishing the current task", worker_id=worker_id)
    worker.stop(timeout=settings.task_time_limit_secs * 1000)
    reader.close()
    log.info("worker stopped", worker_id=worker_id)


if __name__ == "__main__":
    main()
