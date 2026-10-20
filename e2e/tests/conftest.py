"""
Starts the real services as processes (needs `docker compose up` for postgres + minio, and
`uv sync` in orchestrator/ and worker/, `npm install` in api/):

- API (node, built from api/)           on API_PORT 3010
- orchestrator (its venv entry point)   on GRPC_PORT 50061, small tasks so merges happen
- workers (their venv entry point), started and killed per test

The client side talks only to the public API and MinIO, like a real user.
"""

import os
import signal
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
import psycopg
import pytest
import requests
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
API_PORT = 3010
GRPC_PORT = 50061
API = f"http://127.0.0.1:{API_PORT}"


def read_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key] = value
    return env


ENV = read_env()

# Fast, small-scale settings for the orchestrator under test
ORCHESTRATOR_ENV = {
    "GRPC_PORT": str(GRPC_PORT),
    "MAX_FILES_PER_TASK": "3",  # 3 files per map task → several maps and merges
    "LEASE_SECS": "3",
    "HEARTBEAT_SECS": "1",
    "MIN_BACKOFF_SECS": "0.5",
    "MAX_BACKOFF_SECS": "2",
    "TIMER_TICK_SECS": "0.1",
    "ADVISORY_LOCK_KEY": "4343",  # its own lock: a dev orchestrator on 4242 doesn't block it
}


def wait_until(check: Any, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            if check():
                return
        except Exception:
            pass
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.1)


class Proc:
    """A service process; output goes to a log file next to the test run."""

    def __init__(self, name: str, cmd: list[str], cwd: Path, env: dict[str, str]) -> None:
        self.name = name
        self.log_path = ROOT / "e2e" / ".logs" / f"{name}.log"
        self.log_path.parent.mkdir(exist_ok=True)
        self._log = self.log_path.open("w")
        self.popen = subprocess.Popen(
            cmd,
            cwd=cwd,
            env={**os.environ, **ENV, **env},
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

    def stop(self, sig: int = signal.SIGTERM, timeout: float = 15) -> int:
        if self.popen.poll() is None:
            self.popen.send_signal(sig)
            try:
                self.popen.wait(timeout)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait()
        self._log.close()
        return self.popen.returncode

    def kill(self) -> None:
        self.stop(signal.SIGKILL)

    def log(self) -> str:
        return self.log_path.read_text()


def start_orchestrator(name: str = "orchestrator") -> Proc:
    proc = Proc(
        name,
        [str(ROOT / "orchestrator/.venv/bin/dmean-orchestrator")],
        ROOT / "orchestrator",
        ORCHESTRATOR_ENV,
    )
    wait_until(lambda: "orchestrator listening" in proc.log(), 20, "orchestrator to listen")
    return proc


def start_worker(name: str, slowdown: float = 0.0) -> Proc:
    return Proc(
        name,
        [str(ROOT / "worker/.venv/bin/dmean-worker")],
        ROOT / "worker",
        {
            "WORKER_ID": name,
            "ORCHESTRATOR_ADDR": f"127.0.0.1:{GRPC_PORT}",
            "LEASE_WAIT_SECS": "2",
            "SLOWDOWN_SECS_PER_FILE": str(slowdown),
        },
    )


@pytest.fixture(scope="session")
def api() -> Iterator[None]:
    subprocess.run(["npm", "run", "-s", "build"], cwd=ROOT / "api", check=True)
    proc = Proc("api", ["node", "dist/index.js"], ROOT / "api", {"API_PORT": str(API_PORT)})
    wait_until(lambda: requests.get(f"{API}/health", timeout=1).ok, 20, "the API")
    yield
    proc.stop()


@pytest.fixture
def orchestrator(api: None) -> Iterator[Proc]:
    proc = start_orchestrator()
    yield proc
    assert proc.stop() == 0, proc.log()[-2000:]


@pytest.fixture
def workers() -> Iterator[list[Proc]]:
    procs: list[Proc] = []
    yield procs
    for p in procs:
        p.stop()


@pytest.fixture(scope="session")
def s3() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=ENV["S3_ENDPOINT"],
        region_name=ENV.get("S3_REGION", "us-east-1"),
        aws_access_key_id=ENV["S3_ACCESS_KEY"],
        aws_secret_access_key=ENV["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture(scope="session")
def admin() -> Iterator[psycopg.Connection[tuple[Any, ...]]]:
    dsn = (
        f"host=localhost port={ENV['POSTGRES_PORT']} dbname={ENV['POSTGRES_DB']} "
        f"user={ENV['POSTGRES_USER']} password={ENV['POSTGRES_PASSWORD']}"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn


@dataclass
class Client:
    """What a user does: upload CSVs through the API's presigned policy, submit, poll."""

    s3: Any
    admin: psycopg.Connection[tuple[Any, ...]]
    jobs: list[str] = field(default_factory=list[str])

    def submit(self, rows: list[list[float]]) -> str:
        upload = requests.post(f"{API}/uploads", timeout=5).json()
        for i, row in enumerate(rows):
            body = ",".join(map(repr, row)) + "\n"
            fields = {**upload["fields"], "key": f"{upload['prefix']}file_{i:05d}.csv"}
            resp = requests.post(upload["url"], data=fields, files={"file": body}, timeout=10)
            assert resp.status_code == 204, resp.text
        job = requests.post(
            f"{API}/jobs",
            json={
                "upload_id": upload["upload_id"],
                "no_of_files": len(rows),
                "no_of_columns": len(rows[0]),
            },
            timeout=30,
        )
        assert job.status_code == 202, job.text
        self.jobs.append(job.json()["id"])
        return job.json()["id"]

    def status(self, job_id: str) -> dict[str, Any]:
        return requests.get(f"{API}/jobs/{job_id}", timeout=5).json()

    def wait(self, job_id: str, timeout: float = 60) -> dict[str, Any]:
        wait_until(
            lambda: self.status(job_id)["status"] in ("succeeded", "failed"), timeout, job_id
        )
        return self.status(job_id)

    def result(self, job_id: str) -> list[float]:
        body = self.s3.get_object(Bucket=ENV["S3_BUCKET"], Key=f"results/{job_id}.csv")
        return [float(v) for v in body["Body"].read().decode().strip().split(",")]

    def tasks_by_worker(self, job_id: str) -> dict[str, int]:
        rows = self.admin.execute(
            """SELECT t.worker_id, count(*) FROM tasks t JOIN jobs j ON j.id = t.job_id
                WHERE j.uuid = %s AND t.status = 'done' GROUP BY t.worker_id""",
            (job_id,),
        ).fetchall()
        return {str(r[0]): int(r[1]) for r in rows}

    def cleanup(self) -> None:
        for job_id in self.jobs:
            self.s3.delete_object(Bucket=ENV["S3_BUCKET"], Key=f"results/{job_id}.csv")
        ids = [
            r[0]
            for r in self.admin.execute(
                "SELECT id FROM jobs WHERE uuid = ANY(%s::uuid[])", (self.jobs,)
            ).fetchall()
        ]
        self.admin.execute("DELETE FROM uploads WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute(
            "DELETE FROM task_files WHERE task_id IN (SELECT id FROM tasks WHERE job_id = ANY(%s))",
            (ids,),
        )
        self.admin.execute("DELETE FROM files WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute("DELETE FROM tasks WHERE job_id = ANY(%s)", (ids,))
        self.admin.execute("DELETE FROM jobs WHERE id = ANY(%s)", (ids,))


@pytest.fixture
def client(s3: Any, admin: psycopg.Connection[tuple[Any, ...]]) -> Iterator[Client]:
    c = Client(s3, admin)
    yield c
    c.cleanup()


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"
