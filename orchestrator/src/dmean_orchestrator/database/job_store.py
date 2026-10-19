"""Postgres writes for planning jobs and creating merges (role `orchestrator`)."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg_pool import AsyncConnectionPool

from dmean_orchestrator.scheduling.merge_pool import Partial
from dmean_orchestrator.scheduling.planner import chunk

INSERT_TASKS_SQL = """
WITH planned AS (
  SELECT ord, n, final FROM unnest(%(counts)s::int[], %(finals)s::bool[])
         WITH ORDINALITY AS p(n, final, ord)
), inserted AS (
  INSERT INTO tasks (job_id, kind, no_of_files, is_final)
  SELECT %(job_id)s, %(kind)s, n, final FROM planned ORDER BY ord
  RETURNING id
)
SELECT id FROM inserted ORDER BY id
"""


@dataclass(frozen=True)
class PlanResult:
    files_per_task: int
    tasks: int


class JobStore:
    def __init__(self, pool: AsyncConnectionPool[Any]) -> None:
        self._pool = pool

    async def queued_job_ids(self) -> list[int]:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute("SELECT id FROM jobs WHERE status = 'queued' ORDER BY id")
            ).fetchall()
        return [int(r[0]) for r in rows]

    async def plan_job(self, job_id: int, size_for: Callable[[int, int], int]) -> PlanResult | None:
        """
        Splits a queued job's input files into map tasks of N files and sets it running, in one
        transaction. None if the job isn't queued (already planned, e.g. a repeated NOTIFY).
        `size_for(F, C)` returns N.
        """
        async with self._pool.connection() as conn, conn.transaction():
            job = await (
                await conn.execute(
                    """SELECT no_of_files, no_of_columns FROM jobs
                        WHERE id = %s AND status = 'queued' FOR UPDATE""",
                    (job_id,),
                )
            ).fetchone()
            if job is None:
                return None
            no_of_files, no_of_columns = int(job[0]), int(job[1])
            file_ids = [
                int(r[0])
                for r in await (
                    await conn.execute(
                        """SELECT id FROM files WHERE job_id = %s AND type = 'input'
                            ORDER BY id""",
                        (job_id,),
                    )
                ).fetchall()
            ]
            if len(file_ids) != no_of_files:
                await conn.execute(
                    """UPDATE jobs SET status = 'failed', failure_type = 'internal',
                                       error = %s, completed_at = now()
                        WHERE id = %s""",
                    (f"expected {no_of_files} input files, found {len(file_ids)}", job_id),
                )
                return None

            n = size_for(no_of_files, no_of_columns)
            groups = chunk(file_ids, n)
            single = len(groups) == 1  # the whole job fits in one map task: it writes the result
            task_ids = await self._insert_tasks(
                conn, job_id, "map", [len(g) for g in groups], [single] * len(groups)
            )
            await self._link_inputs(conn, task_ids, groups)
            await conn.execute(
                """UPDATE jobs SET status = 'running', files_per_task = %s, started_at = now()
                    WHERE id = %s""",
                (n, job_id),
            )
        return PlanResult(files_per_task=n, tasks=len(task_ids))

    async def create_merge(self, job_id: int, inputs: list[Partial], is_final: bool) -> int:
        """
        One merge task over these partials. UNIQUE (file_id) on task_files makes merging a
        partial twice impossible: the transaction fails instead.
        """
        async with self._pool.connection() as conn, conn.transaction():
            [task_id] = await self._insert_tasks(
                conn, job_id, "merge", [sum(p.no_of_files for p in inputs)], [is_final]
            )
            await self._link_inputs(conn, [task_id], [[p.file_id for p in inputs]])
        return task_id

    @staticmethod
    async def _insert_tasks(
        conn: Any, job_id: int, kind: str, counts: list[int], finals: list[bool]
    ) -> list[int]:
        rows = await (
            await conn.execute(
                INSERT_TASKS_SQL,
                {"job_id": job_id, "kind": kind, "counts": counts, "finals": finals},
            )
        ).fetchall()
        return [int(r[0]) for r in rows]

    @staticmethod
    async def _link_inputs(conn: Any, task_ids: list[int], groups: list[list[int]]) -> None:
        owners = [task_id for task_id, group in zip(task_ids, groups, strict=True) for _ in group]
        files = [file_id for group in groups for file_id in group]
        await conn.execute(
            """INSERT INTO task_files (task_id, file_id)
               SELECT * FROM unnest(%s::bigint[], %s::bigint[])""",
            (owners, files),
        )
