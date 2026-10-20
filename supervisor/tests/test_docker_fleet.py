"""The Docker runtime against the real Docker daemon, with a tiny stand-in worker image."""

import time
from collections.abc import Iterator
from typing import Any

import pytest

from dmean_supervisor.docker_fleet import DockerFleet, container_env

pytestmark = pytest.mark.integration

IMAGE = "python:3.13-slim"  # every service image builds FROM it, so it's already local
# Runs until SIGTERM, then exits 0 (a drain); like tests/fake_worker.py
FAKE_WORKER = [
    "python",
    "-c",
    "import signal, sys, time; signal.signal(signal.SIGTERM, lambda *a: sys.exit(0)); "
    "time.sleep(600)",
]


def wait_until(check: Any, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.2)


@pytest.fixture
def fleet() -> Iterator[DockerFleet]:
    f = DockerFleet(IMAGE, "dmean-test", command=FAKE_WORKER)
    yield f
    f.stop_all(10)


def test_workers_outlive_the_supervisor_and_are_adopted(fleet: DockerFleet) -> None:
    a, b = fleet.spawn(), fleet.spawn()
    fleet.shutdown(10)  # the supervisor stops: its containers keep running

    restarted = DockerFleet(IMAGE, "dmean-test", command=FAKE_WORKER)  # a new supervisor
    assert sorted(c.worker_id for c in restarted.children()) == sorted([a, b])
    assert all(time.monotonic() - c.started_at < 60 for c in restarted.children())


def test_drain_and_kill(fleet: DockerFleet) -> None:
    a, b = fleet.spawn(), fleet.spawn()
    fleet.terminate(a)  # SIGTERM → exits 0
    fleet.kill(b)  # SIGKILL → 137
    codes: dict[str, int] = {}

    def gone() -> bool:
        codes.update(fleet.reap())
        return not fleet.children()

    wait_until(gone)
    assert codes == {a: 0, b: 137}


def test_containers_get_worker_settings_but_no_db_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPERVISOR_DB_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("ORCHESTRATOR_ADDR", "orchestrator:50051")
    env = container_env("w-1")
    assert env["WORKER_ID"] == "w-1"
    assert env["S3_ENDPOINT"] == "http://minio:9000"
    assert env["ORCHESTRATOR_ADDR"] == "orchestrator:50051"
    assert not any("POSTGRES" in k or "DB_PASSWORD" in k for k in env)
    assert "PATH" not in env  # nothing of the supervisor's own environment
