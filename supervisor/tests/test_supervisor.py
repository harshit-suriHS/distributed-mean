"""
The supervisor with real child processes (a fake worker program), a fake orchestrator Admin
server, and real Postgres for W and NOTIFY settings_changed.
"""

import sys
import threading
import time
from collections.abc import Iterator
from concurrent import futures
from pathlib import Path
from typing import Any

import grpc
import psycopg
import pytest

from dmean_supervisor.fleet import Fleet
from dmean_supervisor.main import Orchestrator, reconcile_once, run
from dmean_supervisor.rpc import admin_pb2 as pb
from dmean_supervisor.rpc import admin_pb2_grpc as pb_grpc
from dmean_supervisor.settings import SupervisorSettings

pytestmark = pytest.mark.integration

FAKE_WORKER = [sys.executable, str(Path(__file__).with_name("fake_worker.py"))]


class FakeAdmin(pb_grpc.AdminServicer):
    """Reports every worker as seen just now, except those listed in `hidden`."""

    def __init__(self) -> None:
        self.ids: set[str] = set()
        self.hidden: set[str] = set()

    def ListWorkers(self, request: Any, context: Any) -> pb.ListWorkersResponse:
        return pb.ListWorkersResponse(
            workers=[
                pb.WorkerInfo(worker_id=w, seconds_since_seen=0.5)
                for w in sorted(self.ids - self.hidden)
            ]
        )


class IntegrationSettings(SupervisorSettings):
    postgres_user: str
    postgres_password: str


@pytest.fixture
def admin_server() -> Iterator[tuple[FakeAdmin, str]]:
    fake = FakeAdmin()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_AdminServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield fake, f"127.0.0.1:{port}"
    server.stop(grace=None)


@pytest.fixture
def settings(admin_server: tuple[FakeAdmin, str]) -> IntegrationSettings:
    return IntegrationSettings(  # pyright: ignore[reportCallIssue]
        orchestrator_addr=admin_server[1],
        worker_command=FAKE_WORKER,
        worker_id_prefix="t",
        reconcile_secs=0.3,
        startup_grace_secs=0,
        drain_timeout_secs=5,
    )


@pytest.fixture
def fleet() -> Iterator[Fleet]:
    f = Fleet(FAKE_WORKER, "t")
    yield f
    f.stop_all(5)


def wait_until(check: Any, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)


def running(fleet: Fleet) -> list[str]:
    fleet.reap()
    return sorted(c.worker_id for c in fleet.children() if not c.draining)


def test_reconcile_scales_up_down_and_replaces_crashes(
    settings: IntegrationSettings, admin_server: tuple[FakeAdmin, str], fleet: Fleet
) -> None:
    fake, address = admin_server
    orchestrator = Orchestrator(address)

    reconcile_once(settings, fleet, orchestrator, desired=3)
    assert running(fleet) == ["t-1", "t-2", "t-3"]

    fake.ids = {"t-1", "t-2", "t-3"}
    reconcile_once(settings, fleet, orchestrator, desired=1)  # drain two
    # running() reaps exited workers: wait until the two drained ones are gone
    wait_until(lambda: len(running(fleet)) == 1 and len(fleet.children()) == 1)

    survivor = running(fleet)[0]
    fleet._procs[survivor].kill()  # pyright: ignore[reportPrivateUsage]  # a crash
    wait_until(lambda: fleet._procs[survivor].poll() is not None)  # pyright: ignore[reportPrivateUsage]
    reconcile_once(settings, fleet, orchestrator, desired=1)
    assert running(fleet) == ["t-4"]  # replaced
    orchestrator.close()


def test_hung_worker_is_killed_and_replaced(
    settings: IntegrationSettings, admin_server: tuple[FakeAdmin, str], fleet: Fleet
) -> None:
    fake, address = admin_server
    orchestrator = Orchestrator(address)
    reconcile_once(settings, fleet, orchestrator, desired=2)
    fake.ids = {"t-1", "t-2"}
    fake.hidden = {"t-2"}  # alive as a process, but the orchestrator doesn't see it

    reconcile_once(settings, fleet, orchestrator, desired=2)

    wait_until(lambda: running(fleet) == ["t-1", "t-3"] and len(fleet.children()) == 2)
    orchestrator.close()


def test_orchestrator_unreachable_kills_nobody(settings: IntegrationSettings, fleet: Fleet) -> None:
    orchestrator = Orchestrator("127.0.0.1:1")  # nothing listens there
    reconcile_once(settings, fleet, orchestrator, desired=2)
    reconcile_once(settings, fleet, orchestrator, desired=2)
    assert running(fleet) == ["t-1", "t-2"]
    orchestrator.close()


def test_follows_put_workers_through_notify(settings: IntegrationSettings) -> None:
    """The full loop: W changes in Postgres (as PUT /workers does), the fleet follows."""
    dsn = (
        f"host={settings.postgres_host} port={settings.postgres_port} "
        f"dbname={settings.postgres_db} user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute("SELECT desired_workers FROM settings").fetchone()
        assert row is not None
        before = int(row[0])

        def set_w(n: int) -> None:
            conn.execute("UPDATE settings SET desired_workers = %s", (n,))
            conn.execute("SELECT pg_notify('settings_changed', %s)", (str(n),))

        set_w(2)
        stop = threading.Event()
        captured: dict[str, Fleet] = {}
        original_init = Fleet.__init__

        def spying_init(self: Fleet, command: list[str], id_prefix: str) -> None:
            original_init(self, command, id_prefix)
            captured["fleet"] = self

        Fleet.__init__ = spying_init  # type: ignore[method-assign]
        thread = threading.Thread(target=run, args=(settings, stop), daemon=True)
        try:
            thread.start()
            wait_until(lambda: "fleet" in captured and len(running(captured["fleet"])) == 2)
            set_w(4)
            wait_until(lambda: len(running(captured["fleet"])) == 4)
            set_w(1)
            wait_until(lambda: len(running(captured["fleet"])) == 1)
        finally:
            Fleet.__init__ = original_init  # type: ignore[method-assign]
            stop.set()
            thread.join(timeout=15)
            set_w(before)
        assert not thread.is_alive()
        assert captured["fleet"].children() == []  # shutdown drained everyone
