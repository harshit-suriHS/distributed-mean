"""
Postgres writes for the task queue (role `orchestrator`). Postgres is the source of truth:
every decision the queue makes is committed here before a worker hears about it.

All updates are conditional (`WHERE status = ...`), so a duplicate or late call changes nothing.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.rows import tuple_row
from psycopg_pool import AsyncConnectionPool


class StoreConflict(RuntimeError):
    """The database disagrees with the in-memory queue: a bug, so the orchestrator restarts."""


@dataclass(frozen=True)
class LeasedTask:
    task_id: int
    kind: str
    no_of_files: int
    is_final: bool
    job_uuid: str
    no_of_columns: int
    input_keys: list[str]


@dataclass(frozen=True)
class PendingTask:
    task_id: int
    kind: str
    attempts: int
    not_before_in: float  # seconds from now (≤ 0: ready now)


@dataclass(frozen=True)
class DoneTask:
    job_id: int
    is_final: bool


LEASE_SQL = """
UPDATE tasks
   SET status = 'running', attempts = %(attempt)s, worker_id = %(worker)s,
       started_at = now(), not_before = NULL
 WHERE id = %(task_id)s AND status = 'pending'
"""

PAYLOAD_SQL = """
SELECT t.id, t.kind, t.no_of_files, t.is_final, j.uuid::text, j.no_of_columns,
       array_agg(f.key ORDER BY f.id)
  FROM tasks t
  JOIN jobs j        ON j.id = t.job_id
  JOIN task_files tf ON tf.task_id = t.id
  JOIN files f       ON f.id = tf.file_id
 WHERE t.id = %s
 GROUP BY t.id, j.id
"""


class TaskStore:
    def __init__(self, pool: AsyncConnectionPool[Any]) -> None:
        self._pool = pool

    async def lease(self, task_id: int, attempt: int, worker_id: str) -> LeasedTask:
        """Marks the task running and returns everything the worker needs (one transaction)."""
        async with self._pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                LEASE_SQL, {"task_id": task_id, "attempt": attempt, "worker": worker_id}
            )
            if cur.rowcount != 1:
                raise StoreConflict(f"task {task_id} is not pending in the database")
            cur = conn.cursor(row_factory=tuple_row)
            row = await (await cur.execute(PAYLOAD_SQL, (task_id,))).fetchone()
        if row is None:
            raise StoreConflict(f"task {task_id} has no input files")
        return LeasedTask(
            task_id=int(row[0]),
            kind=str(row[1]),
            no_of_files=int(row[2]),
            is_final=bool(row[3]),
            job_uuid=str(row[4]),
            no_of_columns=int(row[5]),
            input_keys=[str(k) for k in row[6]],
        )

    async def record_done(self, task_id: int, output_key: str, size_bytes: int) -> DoneTask | None:
        """
        First `done` wins: marks the task done, records its output as a `files` row and, for the
        final task, the job as succeeded. None if the task was already done (or skipped).
        """
        async with self._pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    """UPDATE tasks SET status = 'done', completed_at = now(), error = NULL
                        WHERE id = %s AND status IN ('running', 'pending')
                    RETURNING job_id, is_final, no_of_files""",
                    (task_id,),
                )
            ).fetchone()
            if row is None:
                return None
            job_id, is_final, no_of_files = int(row[0]), bool(row[1]), int(row[2])
            await conn.execute(
                """INSERT INTO files (job_id, type, key, no_of_files, produced_by, size_bytes)
                   VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (key) DO NOTHING""",
                (
                    job_id,
                    "result" if is_final else "partial",
                    output_key,
                    no_of_files,
                    task_id,
                    size_bytes,
                ),
            )
            if is_final:
                await conn.execute(
                    """UPDATE jobs SET status = 'succeeded', completed_at = now()
                        WHERE id = %s AND status = 'running'""",
                    (job_id,),
                )
        return DoneTask(job_id, is_final)

    async def retry(self, task_id: int, attempt: int, delay_secs: float, error: str) -> None:
        """Back to pending; not leasable for delay_secs (backoff)."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """UPDATE tasks
                      SET status = 'pending', worker_id = NULL, error = %s,
                          not_before = now() + make_interval(secs => %s)
                    WHERE id = %s AND status = 'running' AND attempts = %s""",
                (error, delay_secs, task_id, attempt),
            )
            if cur.rowcount != 1:
                raise StoreConflict(f"task {task_id} attempt {attempt} is not running")

    async def fail_job(self, job_id: int, task_id: int, failure_type: str, error: str) -> None:
        """The failing task → failed, the job → failed, its unfinished tasks → skipped."""
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                """UPDATE tasks SET status = 'failed', error = %s, completed_at = now()
                    WHERE id = %s""",
                (error, task_id),
            )
            await conn.execute(
                """UPDATE jobs SET status = 'failed', failure_type = %s, error = %s,
                                   completed_at = now()
                    WHERE id = %s AND status = 'running'""",
                (failure_type, error, job_id),
            )
            await conn.execute(
                """UPDATE tasks SET status = 'skipped'
                    WHERE job_id = %s AND status IN ('pending', 'running')""",
                (job_id,),
            )

    async def load_pending(self, job_id: int) -> tuple[datetime, list[PendingTask]]:
        """A running job's created_at and its pending tasks (for planning and restarts)."""
        async with self._pool.connection() as conn:
            job = await (
                await conn.execute("SELECT created_at FROM jobs WHERE id = %s", (job_id,))
            ).fetchone()
            if job is None:
                raise StoreConflict(f"job {job_id} not found")
            rows = await (
                await conn.execute(
                    """SELECT id, kind, attempts,
                              coalesce(extract(epoch FROM not_before - now()), 0)::float8
                         FROM tasks WHERE job_id = %s AND status = 'pending' ORDER BY id""",
                    (job_id,),
                )
            ).fetchall()
        return job[0], [PendingTask(int(r[0]), str(r[1]), int(r[2]), float(r[3])) for r in rows]
