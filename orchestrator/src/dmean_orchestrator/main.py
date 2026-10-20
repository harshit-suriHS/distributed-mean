"""
Orchestrator process: one asyncio loop serving the gRPC TaskQueue and running its timers.

Startup: take the advisory lock (a second copy waits here) → rebuild memory from Postgres →
serve gRPC → listen for new jobs (and plan any queued) → timers. Storage cleanup is a separate
scheduled job (`dmean-cleanup`).
"""

import asyncio
import signal
import sys

import grpc
import psycopg
import structlog
from psycopg_pool import AsyncConnectionPool

from dmean_orchestrator.database.job_store import JobStore
from dmean_orchestrator.database.task_store import TaskStore
from dmean_orchestrator.logs import configure_logging
from dmean_orchestrator.rpc import admin_pb2_grpc
from dmean_orchestrator.rpc import task_queue_pb2_grpc as pb_grpc
from dmean_orchestrator.rpc.admin_service import AdminService
from dmean_orchestrator.rpc.service import TaskQueueService
from dmean_orchestrator.settings import Settings

log = structlog.get_logger()


async def hold_lock(
    conn: psycopg.AsyncConnection[tuple[object, ...]], on_lost: asyncio.Event
) -> None:
    """The lock lives as long as this connection. If it drops, another orchestrator may take
    over, so we stop (crash-only) rather than schedule alongside it."""
    try:
        while True:
            await asyncio.sleep(5)
            await conn.execute("SELECT 1")
    except asyncio.CancelledError:
        raise
    except Exception as err:
        log.error("lost the advisory lock connection, stopping", error=repr(err))
        on_lost.set()


async def serve(settings: Settings, stop: asyncio.Event) -> int:
    """Runs until `stop` is set (exit 0) or something fatal happens (exit 1)."""
    fatal: list[BaseException] = []

    def on_fatal(err: BaseException) -> None:
        fatal.append(err)
        stop.set()

    lock_conn = await psycopg.AsyncConnection.connect(settings.db_dsn, autocommit=True)
    try:
        log.info("waiting for the orchestrator lock", key=settings.advisory_lock_key)
        await lock_conn.execute("SELECT pg_advisory_lock(%s)", (settings.advisory_lock_key,))
        log.info("orchestrator lock acquired")

        async with AsyncConnectionPool(
            settings.db_dsn, min_size=2, max_size=settings.db_pool_size, open=False
        ) as pool:
            await pool.wait()
            service = TaskQueueService(settings, TaskStore(pool), JobStore(pool), on_fatal)
            await service.rebuild()  # before serving: no lease is handed out from a partial view

            server = grpc.aio.server()
            pb_grpc.add_TaskQueueServicer_to_server(service, server)
            admin_pb2_grpc.add_AdminServicer_to_server(AdminService(service), server)
            server.add_insecure_port(f"{settings.grpc_host}:{settings.grpc_port}")
            await server.start()
            lock_lost = asyncio.Event()
            background = [
                asyncio.create_task(service.run_timers()),
                asyncio.create_task(service.listen_for_jobs(settings.db_dsn)),
                asyncio.create_task(hold_lock(lock_conn, lock_lost)),
            ]
            log.info("orchestrator listening", port=settings.grpc_port)

            lost = asyncio.create_task(lock_lost.wait())
            await asyncio.wait(
                [asyncio.create_task(stop.wait()), lost], return_when=asyncio.FIRST_COMPLETED
            )
            if lost.done():
                fatal.append(RuntimeError("advisory lock lost"))
            for task in background:
                task.cancel()
            await server.stop(grace=5)
    finally:
        await lock_conn.close()  # releases the lock
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
