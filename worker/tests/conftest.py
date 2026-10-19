"""Integration fixtures: real Postgres, MinIO and Redis (database 15, flushed per test)."""

import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field

import psycopg
import pytest
import redis
from dramatiq.brokers.redis import RedisBroker

from dmean_worker.database.task_reader import TaskReader
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.queue.messages import RESULTS_QUEUE, Report
from dmean_worker.settings import Settings
from dmean_worker.storage.s3 import Storage

TEST_REDIS_DB = 15


class IntegrationSettings(Settings):
    postgres_user: str  # superuser, only for creating fixtures
    postgres_password: str


@pytest.fixture(scope="session")
def settings() -> IntegrationSettings:
    return IntegrationSettings(redis_db=TEST_REDIS_DB)  # pyright: ignore[reportCallIssue]


@pytest.fixture(scope="session")
def admin(settings: IntegrationSettings) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    dsn = settings.postgres_dsn(settings.postgres_user, settings.postgres_password)
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="session")
def storage(settings: IntegrationSettings) -> Storage:
    return Storage(settings)


@pytest.fixture
def broker(settings: IntegrationSettings) -> Iterator[RedisBroker]:
    client = redis.Redis.from_url(settings.redis_url)
    client.flushdb()
    broker = RedisBroker(url=settings.redis_url)
    yield broker
    broker.close()
    client.flushdb()


@pytest.fixture
def runner(
    settings: IntegrationSettings, broker: RedisBroker, storage: Storage
) -> Iterator[TaskRunner]:
    reader = TaskReader(settings.db_dsn)
    yield TaskRunner(
        broker=broker, reader=reader, storage=storage, worker_id="test-worker", job_check_secs=0.0
    )
    reader.close()


@dataclass
class Db:
    """Creates jobs, files and tasks the way the API and orchestrator would, then cleans up."""

    admin: psycopg.Connection[tuple[object, ...]]
    storage: Storage
    job_ids: list[int] = field(default_factory=list[int])
    keys: list[str] = field(default_factory=list[str])

    def job(self, no_of_files: int, columns: int, *, failed: bool = False) -> tuple[int, str]:
        row = self.admin.execute(
            """INSERT INTO jobs (no_of_files, no_of_columns, status, failure_type)
               VALUES (%s, %s, %s, %s) RETURNING id, uuid::text""",
            (
                no_of_files,
                columns,
                "failed" if failed else "running",
                "internal" if failed else None,
            ),
        ).fetchone()
        assert row is not None
        job_id, job_uuid = int(str(row[0])), str(row[1])
        self.job_ids.append(job_id)
        self.keys += [f"partials/{job_uuid}/", f"results/{job_uuid}.csv"]
        return job_id, job_uuid

    def inputs(self, job_id: int, contents: list[str], *, upload: bool = True) -> list[int]:
        prefix = f"inputs/test-{uuid.uuid4()}/"
        self.keys.append(prefix)
        ids: list[int] = []
        for i, text in enumerate(contents):
            key = f"{prefix}file_{i}.csv"
            if upload:
                self.storage.put_text(key, text)
            ids.append(self._file(job_id, "input", key, 1, None))
        return ids

    def partial(self, job_id: int, job_uuid: str, text: str, no_of_files: int) -> int:
        producer = self.task(job_id, "map", [], no_of_files, status="done")
        key = f"partials/{job_uuid}/{producer}.csv"
        self.storage.put_text(key, text)
        return self._file(job_id, "partial", key, no_of_files, producer)

    def task(
        self,
        job_id: int,
        kind: str,
        file_ids: list[int],
        no_of_files: int,
        *,
        final: bool = False,
        status: str = "running",
    ) -> int:
        row = self.admin.execute(
            """INSERT INTO tasks (job_id, kind, no_of_files, is_final, status)
               VALUES (%s, %s, %s, %s, %s) RETURNING id""",
            (job_id, kind, no_of_files, final, status),
        ).fetchone()
        assert row is not None
        task_id = int(str(row[0]))
        for file_id in file_ids:
            self.admin.execute("INSERT INTO task_files VALUES (%s, %s)", (task_id, file_id))
        return task_id

    def _file(
        self, job_id: int, type_: str, key: str, no_of_files: int, producer: int | None
    ) -> int:
        row = self.admin.execute(
            """INSERT INTO files (job_id, type, key, no_of_files, produced_by)
               VALUES (%s, %s, %s, %s, %s) RETURNING id""",
            (job_id, type_, key, no_of_files, producer),
        ).fetchone()
        assert row is not None
        return int(str(row[0]))

    def cleanup(self) -> None:
        ids = self.job_ids
        self.admin.execute(
            "DELETE FROM task_files WHERE task_id IN (SELECT id FROM tasks WHERE job_id = ANY(%s))",
            (ids,),
        )
        self.admin.execute("DELETE FROM files WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute("DELETE FROM tasks WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute("DELETE FROM jobs WHERE id = ANY(%s)", (ids,))
        s3 = self.storage._s3  # pyright: ignore[reportPrivateUsage]
        for prefix in self.keys:
            listed = s3.list_objects_v2(Bucket=self.storage.bucket, Prefix=prefix)
            objects = [{"Key": o["Key"]} for o in listed.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket=self.storage.bucket, Delete={"Objects": objects})


@pytest.fixture
def db(admin: psycopg.Connection[tuple[object, ...]], storage: Storage) -> Iterator[Db]:
    fixtures = Db(admin, storage)
    yield fixtures
    fixtures.cleanup()


def collect_reports(broker: RedisBroker, expected: int, timeout_secs: float = 10.0) -> list[Report]:
    """Reads reports from the `results` queue, as the orchestrator will."""
    consumer = broker.consume(RESULTS_QUEUE, prefetch=50, timeout=200)
    reports: list[Report] = []
    deadline = time.monotonic() + (timeout_secs if expected else 1.0)
    try:
        while time.monotonic() < deadline and (expected == 0 or len(reports) < expected):
            message = next(consumer)
            if message is not None:
                reports.append(Report.model_validate(message.kwargs["report"]))
                consumer.ack(message)
    finally:
        consumer.close()
    return reports
