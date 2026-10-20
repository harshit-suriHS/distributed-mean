"""
Deletes what finished jobs and expired uploads leave behind. Results are kept forever.

Runs as a scheduled job (`dmean-cleanup`, hourly), not inside the orchestrator: deleting tens
of thousands of objects competes with running jobs for the object store, so a job's inputs and
partials are deleted only CLEANUP_AFTER_HOURS (24) after it ends.

Order matters: delete the objects first, then mark the rows; if we stop in between, the sweep
deletes again (deleting a missing key is harmless) and marks them.
"""

import structlog

from dmean_orchestrator.database.cleanup_store import CleanupStore
from dmean_orchestrator.storage.s3 import ObjectStore

log = structlog.get_logger()


class Cleaner:
    def __init__(self, store: CleanupStore, objects: ObjectStore) -> None:
        self._store = store
        self._objects = objects

    async def clean_job(self, job_id: int, after_hours: float) -> int:
        """Deletes the inputs and partials of a job that ended more than after_hours ago."""
        leftovers = await self._store.leftovers(job_id, after_hours)
        if not leftovers:
            return 0
        await self._objects.delete([key for _, key in leftovers])
        await self._store.mark_deleted([file_id for file_id, _ in leftovers])
        log.info("job storage cleaned", job_id=job_id, objects=len(leftovers))
        return len(leftovers)

    async def sweep(self, after_hours: float) -> None:
        """Cleans jobs that ended more than after_hours ago, and expires abandoned uploads."""
        for job_id in await self._store.finished_jobs_with_leftovers(after_hours):
            await self.clean_job(job_id, after_hours)
        await self._store.expire_uploads()
        for upload_uuid in await self._store.expired_upload_uuids():
            keys = await self._objects.list_prefix(f"inputs/{upload_uuid}/")
            if keys:
                await self._objects.delete(keys)
                log.info("expired upload deleted", upload=upload_uuid, objects=len(keys))
