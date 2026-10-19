"""What storage cleanup must delete, decided by Postgres (design §20), not S3 lifecycle rules."""

from typing import Any

from psycopg_pool import AsyncConnectionPool


class CleanupStore:
    def __init__(self, pool: AsyncConnectionPool[Any]) -> None:
        self._pool = pool

    async def leftovers(self, job_id: int) -> list[tuple[int, str]]:
        """(file id, key) of a finished job's inputs and partials still in storage."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """SELECT f.id, f.key FROM files f JOIN jobs j ON j.id = f.job_id
                        WHERE f.job_id = %s AND f.type IN ('input', 'partial')
                          AND f.deleted_at IS NULL
                          AND j.status IN ('succeeded', 'failed')""",
                    (job_id,),
                )
            ).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]

    async def mark_deleted(self, file_ids: list[int]) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE files SET deleted_at = now() WHERE id = ANY(%s) AND deleted_at IS NULL",
                (file_ids,),
            )

    async def finished_jobs_with_leftovers(self) -> list[int]:
        """Finished jobs whose cleanup didn't happen or didn't finish (the sweep retries them)."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """SELECT DISTINCT f.job_id FROM files f JOIN jobs j ON j.id = f.job_id
                        WHERE j.status IN ('succeeded', 'failed')
                          AND f.type IN ('input', 'partial') AND f.deleted_at IS NULL"""
                )
            ).fetchall()
        return [int(r[0]) for r in rows]

    async def expire_uploads(self) -> list[str]:
        """
        Marks open uploads past their expiry as expired and returns their uuids. The status flips
        BEFORE any object is deleted: a commit racing with expiry loses (it needs status = 'open'),
        so no job can ever reference deleted inputs.
        """
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """UPDATE uploads SET status = 'expired'
                        WHERE status = 'open' AND expires_at <= now()
                    RETURNING uuid::text"""
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    async def expired_upload_uuids(self) -> list[str]:
        """Recently expired uploads: their objects are deleted (again, if a sweep was interrupted).
        Older ones were handled by earlier sweeps, so the list doesn't grow forever."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """SELECT uuid::text FROM uploads
                        WHERE status = 'expired' AND expires_at > now() - interval '2 days'"""
                )
            ).fetchall()
        return [str(r[0]) for r in rows]
