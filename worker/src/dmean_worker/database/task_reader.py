"""Read-only access to Postgres (role worker_ro). Workers never write the database."""

import time
from collections.abc import Callable

import psycopg
import structlog

from dmean_worker.processing.task import Task

log = structlog.get_logger()

READ_TASK_SQL = """
SELECT t.id, t.job_id, j.uuid::text AS job_uuid, t.kind, t.no_of_files, t.is_final, t.status,
       j.no_of_columns, j.status AS job_status,
       array_agg(f.key ORDER BY f.id) AS input_keys
  FROM tasks t
  JOIN jobs j        ON j.id = t.job_id
  JOIN task_files tf ON tf.task_id = t.id
  JOIN files f       ON f.id = tf.file_id
 WHERE t.id = %s
 GROUP BY t.id, j.id
"""


class TaskReader:
    """
    One connection per worker. Connection errors are retried in place with backoff and
    never raised: a Postgres outage must not use up a task's Dramatiq attempts.
    """

    def __init__(self, dsn: str, max_backoff_secs: float = 30.0) -> None:
        self._dsn = dsn
        self._max_backoff = max_backoff_secs
        self._conn: psycopg.Connection[tuple[object, ...]] | None = None

    def read_task(self, task_id: int) -> Task | None:
        row = self._with_backoff(lambda conn: conn.execute(READ_TASK_SQL, (task_id,)).fetchone())
        if row is None:
            return None
        return Task(*row)  # type: ignore[arg-type]  # column order matches the dataclass

    def job_status(self, job_id: int) -> str | None:
        row = self._with_backoff(
            lambda conn: conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()
        )
        return None if row is None else str(row[0])

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _with_backoff[T](self, query: Callable[[psycopg.Connection[tuple[object, ...]]], T]) -> T:
        delay = 1.0
        while True:
            try:
                return query(self._connection())
            except psycopg.OperationalError as err:
                log.warning("postgres unavailable, retrying", error=str(err), retry_in=delay)
                self.close()
                time.sleep(delay)
                delay = min(delay * 2, self._max_backoff)

    def _connection(self) -> psycopg.Connection[tuple[object, ...]]:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=True)
        return self._conn
