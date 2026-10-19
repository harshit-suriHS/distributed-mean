"""Orchestrator process: one asyncio loop serving the gRPC TaskQueue and running its timers."""

import asyncio
import signal
import sys

import grpc
import structlog
from psycopg_pool import AsyncConnectionPool

from dmean_orchestrator.database.task_store import TaskStore
from dmean_orchestrator.logs import configure_logging
from dmean_orchestrator.rpc import task_queue_pb2_grpc as pb_grpc
from dmean_orchestrator.rpc.service import TaskQueueService
from dmean_orchestrator.settings import Settings

log = structlog.get_logger()


async def serve(settings: Settings, stop: asyncio.Event) -> int:
    """Runs until `stop` is set (exit 0) or the queue and database diverge (exit 1)."""
    fatal: list[BaseException] = []

    def on_fatal(err: BaseException) -> None:
        fatal.append(err)
        stop.set()

    async with AsyncConnectionPool(
        settings.db_dsn, min_size=2, max_size=settings.db_pool_size, open=False
    ) as pool:
        await pool.wait()
        service = TaskQueueService(settings, TaskStore(pool), on_fatal)
        server = grpc.aio.server()
        pb_grpc.add_TaskQueueServicer_to_server(service, server)
        server.add_insecure_port(f"{settings.grpc_host}:{settings.grpc_port}")
        await server.start()
        timers = asyncio.create_task(service.run_timers())
        log.info("orchestrator listening", port=settings.grpc_port)

        await stop.wait()
        timers.cancel()
        await server.stop(grace=5)
    log.info("orchestrator stopped", fatal=bool(fatal))
    return 1 if fatal else 0


def main() -> None:
    settings = Settings()  # pyright: ignore[reportCallIssue]  # values come from env
    configure_logging(settings.log_level, "orchestrator")

    async def run() -> int:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        return await serve(settings, stop)

    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
