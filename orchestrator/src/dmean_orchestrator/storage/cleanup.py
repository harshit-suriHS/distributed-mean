"""
Deletes what finished jobs and expired uploads leave behind. Results are kept forever.

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

    async def clean_job(self, job_id: int) -> int:
        """Deletes a finished job's inputs and partials. Returns how many objects."""
        leftovers = await self._store.leftovers(job_id)
        if not leftovers:
            return 0
        await self._objects.delete([key for _, key in leftovers])
        await self._store.mark_deleted([file_id for file_id, _ in leftovers])
        log.info("job storage cleaned", job_id=job_id, objects=len(leftovers))
        return len(leftovers)

    async def sweep(self) -> None:
        """Retries interrupted cleanups and expires abandoned uploads."""
        for job_id in await self._store.finished_jobs_with_leftovers():
            await self.clean_job(job_id)
        await self._store.expire_uploads()
        for upload_uuid in await self._store.expired_upload_uuids():
            keys = await self._objects.list_prefix(f"inputs/{upload_uuid}/")
            if keys:
                await self._objects.delete(keys)
                log.info("expired upload deleted", upload=upload_uuid, objects=len(keys))
