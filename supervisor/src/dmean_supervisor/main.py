"""
Supervisor process: keeps `settings.desired_workers` worker processes running.

Every reconcile_secs, and at once on NOTIFY settings_changed (PUT /workers): read W from
Postgres, ask the orchestrator what it sees (Admin.ListWorkers), then spawn, drain or kill.
On Kubernetes this job belongs to a Deployment (replicas = W) instead.
"""

import signal
import threading
import time

import grpc
import psycopg
import structlog

from dmean_supervisor.fleet import Fleet
from dmean_supervisor.logs import configure_logging
from dmean_supervisor.reconcile import Seen, plan
from dmean_supervisor.rpc import admin_pb2, admin_pb2_grpc
from dmean_supervisor.settings import SupervisorSettings

log = structlog.get_logger()


class Orchestrator:
    def __init__(self, address: str) -> None:
        self._channel = grpc.insecure_channel(address)
        self._stub = admin_pb2_grpc.AdminStub(self._channel)

    def seen(self) -> dict[str, Seen] | None:
        """What the orchestrator sees, or None if it can't be reached (then nobody is judged)."""
        try:
            resp = self._stub.ListWorkers(admin_pb2.ListWorkersRequest(), timeout=3)
        except grpc.RpcError:
            return None
        return {w.worker_id: Seen(w.busy, w.seconds_since_seen) for w in resp.workers}

    def close(self) -> None:
        self._channel.close()


def reconcile_once(
    settings: SupervisorSettings, fleet: Fleet, orchestrator: Orchestrator, desired: int
) -> None:
    for worker_id, code in fleet.reap():
        log.info("worker exited", worker=worker_id, code=code)
    seen = orchestrator.seen()
    if seen is None:
        log.warning("orchestrator unreachable: not judging liveness this round")
    decision = plan(
        desired,
        fleet.children(),
        seen,
        time.monotonic(),
        startup_grace_secs=settings.startup_grace_secs,
        hang_after_secs=settings.hang_after_secs,
    )
    for worker_id in decision.kill:
        fleet.kill(worker_id)
    for worker_id in decision.terminate:
        fleet.terminate(worker_id)
    for _ in range(decision.spawn):
        fleet.spawn()


def run(settings: SupervisorSettings, stop: threading.Event) -> None:
    fleet = Fleet(settings.worker_command, settings.worker_id_prefix)
    orchestrator = Orchestrator(settings.orchestrator_addr)
    try:
        with psycopg.connect(settings.db_dsn, autocommit=True) as conn:
            conn.execute("LISTEN settings_changed")
            last_desired = -1
            while not stop.is_set():
                row = conn.execute("SELECT desired_workers FROM settings WHERE id = 1").fetchone()
                desired = int(row[0]) if row else 0
                if desired != last_desired:
                    log.info("desired workers", desired=desired)
                    last_desired = desired
                reconcile_once(settings, fleet, orchestrator, desired)
                # sleep until the next round, or until PUT /workers changes W
                for _ in conn.notifies(timeout=settings.reconcile_secs, stop_after=1):
                    pass
    finally:
        log.info("supervisor stopping: draining workers")
        fleet.stop_all(settings.drain_timeout_secs)
        orchestrator.close()
        log.info("supervisor stopped")


def main() -> None:
    stop = threading.Event()

    def on_signal(signum: int, frame: object) -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)

    settings = SupervisorSettings()  # pyright: ignore[reportCallIssue]  # values come from env
    configure_logging(settings.log_level, "supervisor")
    run(settings, stop)


if __name__ == "__main__":
    main()
