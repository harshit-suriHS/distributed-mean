"""Integration fixtures: a real TaskQueue gRPC server on a random port, backed by real Postgres."""

import asyncio
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import boto3
import grpc
import psycopg
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from psycopg_pool import AsyncConnectionPool

from dmean_orchestrator.database.cleanup_store import CleanupStore
from dmean_orchestrator.database.job_store import JobStore
from dmean_orchestrator.database.task_store import TaskStore
from dmean_orchestrator.rpc import admin_pb2_grpc
from dmean_orchestrator.rpc import task_queue_pb2_grpc as pb_grpc
from dmean_orchestrator.rpc.admin_service import AdminService
from dmean_orchestrator.rpc.service import TaskQueueService
from dmean_orchestrator.settings import Settings
from dmean_orchestrator.storage.cleanup import Cleaner
from dmean_orchestrator.storage.s3 import ObjectStore


class IntegrationSettings(Settings):
    postgres_user: str  # superuser, only for creating fixtures
    postgres_password: str


# Short timings so lease expiry and backoff happen within a test
FAST = {
    "lease_secs": 1.0,
    "heartbeat_secs": 1,
    "max_lease_secs": 5.0,
    "min_backoff_secs": 0.2,
    "max_backoff_secs": 1.0,
    "timer_tick_secs": 0.05,
    "max_files_per_task": 2,  # small tasks, so planning and merging show up with a few files
}


@pytest.fixture(scope="session")
def settings() -> IntegrationSettings:
    return IntegrationSettings(**FAST)  # pyright: ignore[reportCallIssue, reportArgumentType]


@dataclass
class Orchestrator:
    service: TaskQueueService
    stub: pb_grpc.TaskQueueStub
    fatal: list[BaseException]
    admin: admin_pb2_grpc.AdminStub


@asynccontextmanager
async def start_orchestrator(
    settings: IntegrationSettings, *, rebuild: bool = False
) -> AsyncGenerator[Orchestrator]:
    """One orchestrator instance; tests can start a second one to simulate a restart."""
    fatal: list[BaseException] = []
    async with AsyncConnectionPool(settings.db_dsn, min_size=1, max_size=5, open=False) as pool:
        await pool.wait()
        cleaner = Cleaner(CleanupStore(pool), ObjectStore(settings))
        service = TaskQueueService(
            settings, TaskStore(pool), JobStore(pool), fatal.append, cleaner=cleaner
        )
        if rebuild:
            await service.rebuild()
        server = grpc.aio.server()
        pb_grpc.add_TaskQueueServicer_to_server(service, server)
        admin_pb2_grpc.add_AdminServicer_to_server(AdminService(service), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        background = [
            asyncio.create_task(service.run_timers()),
            asyncio.create_task(service.listen_for_jobs(settings.db_dsn)),
        ]
        await asyncio.sleep(0.1)  # let the listener subscribe
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield Orchestrator(
                service, pb_grpc.TaskQueueStub(channel), fatal, admin_pb2_grpc.AdminStub(channel)
            )
        for task in background:
            task.cancel()
        await server.stop(grace=None)
    assert fatal == [], f"orchestrator hit a fatal error: {fatal}"


@pytest.fixture
async def orch(settings: IntegrationSettings) -> AsyncIterator[Orchestrator]:
    async with start_orchestrator(settings) as orchestrator:
        yield orchestrator


@pytest.fixture(scope="session")
def objects(settings: IntegrationSettings) -> Any:
    """A plain boto3 client to create and inspect objects in MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def object_exists(objects: Any, bucket: str, key: str) -> bool:
    try:
        objects.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


@dataclass
class Db:
    """Creates jobs, input files and pending tasks the way the API and planner would."""

    admin: psycopg.Connection[tuple[Any, ...]]
    job_ids: list[int] = field(default_factory=list[int])
    upload_uuids: list[str] = field(default_factory=list[str])

    def job(self, no_of_files: int, columns: int = 3) -> tuple[int, str]:
        row = self.admin.execute(
            """INSERT INTO jobs (no_of_files, no_of_columns, status, started_at)
               VALUES (%s, %s, 'running', now()) RETURNING id, uuid::text""",
            (no_of_files, columns),
        ).fetchone()
        assert row is not None
        self.job_ids.append(int(row[0]))
        return int(row[0]), str(row[1])

    def queued_job(self, no_of_files: int, *, inputs: int | None = None) -> int:
        """A committed job as the API leaves it: queued, with its input files, no tasks yet."""
        row = self.admin.execute(
            "INSERT INTO jobs (no_of_files, no_of_columns) VALUES (%s, 3) RETURNING id",
            (no_of_files,),
        ).fetchone()
        assert row is not None
        job_id = int(row[0])
        self.job_ids.append(job_id)
        prefix = f"inputs/test-{uuid.uuid4()}/"
        for i in range(no_of_files if inputs is None else inputs):
            self.admin.execute(
                "INSERT INTO files (job_id, type, key, no_of_files) VALUES (%s, 'input', %s, 1)",
                (job_id, f"{prefix}f{i}.csv"),
            )
        return job_id

    def notify_new_job(self, job_id: int) -> None:
        self.admin.execute("SELECT pg_notify('new_job', %s)", (str(job_id),))

    def tasks(self, job_id: int) -> list[dict[str, Any]]:
        rows = self.admin.execute(
            """SELECT t.id, t.kind, t.no_of_files, t.is_final, t.status, count(tf.file_id)
                 FROM tasks t LEFT JOIN task_files tf ON tf.task_id = t.id
                WHERE t.job_id = %s GROUP BY t.id ORDER BY t.id""",
            (job_id,),
        ).fetchall()
        keys = ["id", "kind", "no_of_files", "is_final", "status", "inputs"]
        return [dict(zip(keys, r, strict=True)) for r in rows]

    def files(self, job_id: int) -> list[dict[str, Any]]:
        rows = self.admin.execute(
            """SELECT id, type, key, deleted_at IS NOT NULL FROM files
                WHERE job_id = %s ORDER BY id""",
            (job_id,),
        ).fetchall()
        return [dict(zip(["id", "type", "key", "deleted"], r, strict=True)) for r in rows]

    def fake_done(self, task_id: int, key: str) -> None:
        """As if the task's done was committed and the orchestrator then crashed."""
        row = self.admin.execute(
            "UPDATE tasks SET status = 'done' WHERE id = %s RETURNING job_id, no_of_files",
            (task_id,),
        ).fetchone()
        assert row is not None
        self.admin.execute(
            """INSERT INTO files (job_id, type, key, no_of_files, produced_by)
               VALUES (%s, 'partial', %s, %s, %s)""",
            (row[0], key, row[1], task_id),
        )

    def upload(self, *, expired: bool, committed_job: int | None = None) -> str:
        row = self.admin.execute(
            """INSERT INTO uploads (status, job_id, expires_at)
               VALUES (%s, %s, now() + %s * interval '1 hour') RETURNING uuid::text""",
            ("committed" if committed_job else "open", committed_job, -1 if expired else 24),
        ).fetchone()
        assert row is not None
        self.upload_uuids.append(str(row[0]))
        return str(row[0])

    def upload_status(self, upload_uuid: str) -> str:
        row = self.admin.execute(
            "SELECT status FROM uploads WHERE uuid = %s", (upload_uuid,)
        ).fetchone()
        assert row is not None
        return str(row[0])

    def files_used_twice(self, job_id: int) -> int:
        row = self.admin.execute(
            """SELECT count(*) FROM files f
                WHERE f.job_id = %s
                  AND (SELECT count(*) FROM task_files tf WHERE tf.file_id = f.id) > 1""",
            (job_id,),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def task(self, job_id: int, files: int, *, final: bool = False, kind: str = "map") -> int:
        prefix = f"inputs/test-{uuid.uuid4()}/"
        row = self.admin.execute(
            """INSERT INTO tasks (job_id, kind, no_of_files, is_final)
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (job_id, kind, files, final),
        ).fetchone()
        assert row is not None
        task_id = int(row[0])
        for i in range(files):
            file_row = self.admin.execute(
                """INSERT INTO files (job_id, type, key, no_of_files)
                   VALUES (%s, 'input', %s, 1) RETURNING id""",
                (job_id, f"{prefix}f{i}.csv"),
            ).fetchone()
            assert file_row is not None
            self.admin.execute("INSERT INTO task_files VALUES (%s, %s)", (task_id, file_row[0]))
        return task_id

    def task_row(self, task_id: int) -> dict[str, Any]:
        row = self.admin.execute(
            """SELECT status, attempts, worker_id, error, not_before > now() AS backing_off
                 FROM tasks WHERE id = %s""",
            (task_id,),
        ).fetchone()
        assert row is not None
        return dict(
            zip(["status", "attempts", "worker_id", "error", "backing_off"], row, strict=True)
        )

    def job_row(self, job_id: int) -> dict[str, Any]:
        row = self.admin.execute(
            "SELECT status, failure_type, error FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()
        assert row is not None
        return dict(zip(["status", "failure_type", "error"], row, strict=True))

    def output_files(self, job_id: int) -> list[tuple[str, str, int]]:
        return [
            (str(r[0]), str(r[1]), int(r[2]))
            for r in self.admin.execute(
                """SELECT type, key, no_of_files FROM files
                    WHERE job_id = %s AND type <> 'input' ORDER BY id""",
                (job_id,),
            ).fetchall()
        ]

    def cleanup(self) -> None:
        self.admin.execute("DELETE FROM uploads WHERE uuid = ANY(%s::uuid[])", (self.upload_uuids,))
        ids = self.job_ids
        self.admin.execute(
            "DELETE FROM task_files WHERE task_id IN (SELECT id FROM tasks WHERE job_id = ANY(%s))",
            (ids,),
        )
        self.admin.execute("DELETE FROM files WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute("DELETE FROM tasks WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute("DELETE FROM jobs WHERE id = ANY(%s)", (ids,))


@pytest.fixture
def db(settings: IntegrationSettings) -> Iterator[Db]:
    dsn = (
        f"host={settings.postgres_host} port={settings.postgres_port} "
        f"dbname={settings.postgres_db} user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        fixtures = Db(conn)
        yield fixtures
        fixtures.cleanup()


async def lease(orch: Orchestrator, worker: str, wait: int = 1) -> Any:
    from dmean_orchestrator.rpc import task_queue_pb2 as pb

    resp = await orch.stub.Lease(pb.LeaseRequest(worker_id=worker, wait_seconds=wait))
    return resp.task if resp.HasField("task") else None


async def report_done(orch: Orchestrator, task: Any, worker: str = "w1") -> None:
    from dmean_orchestrator.rpc import task_queue_pb2 as pb

    await orch.stub.Report(
        pb.ReportRequest(
            worker_id=worker,
            task_id=task.task_id,
            attempt=task.attempt,
            done=pb.Done(output_key=task.output_key, size_bytes=42),
        )
    )


async def wait_for(check: Any, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        assert asyncio.get_running_loop().time() < deadline, "condition not met in time"
        await asyncio.sleep(0.05)
