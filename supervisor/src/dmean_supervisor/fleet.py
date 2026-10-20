"""The worker processes this supervisor runs."""

import os
import signal
import subprocess
import time

import structlog

from dmean_supervisor.reconcile import Child

log = structlog.get_logger()


def worker_env(worker_id: str) -> dict[str, str]:
    """The supervisor's env minus database credentials: workers never touch Postgres."""
    env = {
        k: v
        for k, v in os.environ.items()
        if not (k.startswith("POSTGRES_") or k.endswith("_DB_PASSWORD"))
    }
    return {**env, "WORKER_ID": worker_id}


class Fleet:
    def __init__(self, command: list[str], id_prefix: str) -> None:
        self._command = command
        self._prefix = id_prefix
        self._counter = 0
        self._procs: dict[str, subprocess.Popen[bytes]] = {}
        self._children: dict[str, Child] = {}

    def children(self) -> list[Child]:
        return list(self._children.values())

    def spawn(self) -> str:
        self._counter += 1
        worker_id = f"{self._prefix}-{self._counter}"
        proc = subprocess.Popen(self._command, env=worker_env(worker_id))
        self._procs[worker_id] = proc
        self._children[worker_id] = Child(worker_id, started_at=time.monotonic())
        log.info("worker spawned", worker=worker_id, pid=proc.pid)
        return worker_id

    def terminate(self, worker_id: str) -> None:
        """SIGTERM: the worker finishes and reports its current task, then exits."""
        proc = self._procs.get(worker_id)
        child = self._children.get(worker_id)
        if proc is None or child is None or child.draining:
            return
        proc.send_signal(signal.SIGTERM)
        self._children[worker_id] = Child(worker_id, child.started_at, draining=True)
        log.info("worker draining", worker=worker_id)

    def kill(self, worker_id: str) -> None:
        proc = self._procs.get(worker_id)
        if proc is not None:
            proc.kill()
            log.warning("hung worker killed", worker=worker_id)

    def reap(self) -> list[tuple[str, int]]:
        """Forgets exited workers; returns (worker_id, exit code)."""
        exited: list[tuple[str, int]] = []
        for worker_id, proc in list(self._procs.items()):
            code = proc.poll()
            if code is not None:
                exited.append((worker_id, code))
                del self._procs[worker_id]
                del self._children[worker_id]
        return exited

    def stop_all(self, timeout_secs: float) -> None:
        """Drain everyone; SIGKILL whoever is still running after the timeout."""
        for worker_id in list(self._procs):
            self.terminate(worker_id)
        deadline = time.monotonic() + timeout_secs
        while self._procs and time.monotonic() < deadline:
            self.reap()
            time.sleep(0.2)
        for worker_id in list(self._procs):
            self.kill(worker_id)
        for proc in self._procs.values():
            proc.wait()
        self.reap()
