"""
`dmean-cleanup`: one storage-cleanup sweep, then exit. Run it on a schedule: hourly in compose
(a small loop service), a Kubernetes CronJob in production.
"""

import asyncio

import structlog
from psycopg_pool import AsyncConnectionPool

from dmean_orchestrator.database.cleanup_store import CleanupStore
from dmean_orchestrator.logs import configure_logging
from dmean_orchestrator.settings import Settings
from dmean_orchestrator.storage.cleanup import Cleaner
from dmean_orchestrator.storage.s3 import ObjectStore

log = structlog.get_logger()


async def run_once(settings: Settings) -> None:
    async with AsyncConnectionPool(settings.db_dsn, min_size=1, max_size=2, open=False) as pool:
        await pool.wait()
        await Cleaner(CleanupStore(pool), ObjectStore(settings)).sweep(settings.cleanup_after_hours)
    log.info("cleanup sweep done", after_hours=settings.cleanup_after_hours)


def main() -> None:
    settings = Settings()  # pyright: ignore[reportCallIssue]  # values come from env
    configure_logging(settings.log_level, "cleanup")
    asyncio.run(run_once(settings))


if __name__ == "__main__":
    main()
