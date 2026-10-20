"""What storage cleanup must delete, decided by Postgres (design §20), not S3 lifecycle rules."""

from typing import Any

from psycopg_pool import AsyncConnectionPool


class CleanupStore:
    def __init__(self, pool: AsyncConnectionPool[Any]) -> None:
        self._pool = pool

    async def leftovers(self, job_id: int, after_hours: float) -> list[tuple[int, str]]:
        """(file id, key) of inputs and partials still in storage, for a job that ended more
        than after_hours ago."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """SELECT f.id, f.key FROM files f JOIN jobs j ON j.id = f.job_id
                        WHERE f.job_id = %s AND f.type IN ('input', 'partial')
                          AND f.deleted_at IS NULL
                          AND j.status IN ('succeeded', 'failed')
                          AND j.completed_at <= now() - make_interval(secs => %s)""",
                    (job_id, after_hours * 3600),
                )
            ).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]

    async def mark_deleted(self, file_ids: list[int]) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE files SET deleted_at = now() WHERE id = ANY(%s) AND deleted_at IS NULL",
                (file_ids,),
            )

    async def finished_jobs_with_leftovers(self, after_hours: float) -> list[int]:
        """Jobs that ended more than after_hours ago and still have inputs or partials."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """SELECT DISTINCT f.job_id FROM files f JOIN jobs j ON j.id = f.job_id
                        WHERE j.status IN ('succeeded', 'failed')
                          AND j.completed_at <= now() - make_interval(secs => %s)
                          AND f.type IN ('input', 'partial') AND f.deleted_at IS NULL""",
                    (after_hours * 3600,),
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

    async def rejected_upload_uuids(self, after_hours: float) -> list[str]:
        """Uploads whose job failed validation more than after_hours ago. Such a job never got
        input `files` rows, so its objects are found by listing the upload's prefix. Recent ones
        only (a 2-day window), like expired uploads."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """SELECT u.uuid::text FROM uploads u JOIN jobs j ON j.id = u.job_id
                        WHERE j.status = 'failed'
                          AND j.completed_at <= now() - make_interval(secs => %s)
                          AND j.completed_at > now() - make_interval(secs => %s)
                          AND NOT EXISTS (SELECT 1 FROM files f WHERE f.job_id = j.id)""",
                    (after_hours * 3600, after_hours * 3600 + 2 * 86400),
                )
            ).fetchall()
        return [str(r[0]) for r in rows]
