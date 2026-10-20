"""
Worker containers this supervisor runs through the Docker API (RUNTIME=docker, the compose setup).

Unlike child processes, containers belong to the Docker daemon: if the supervisor stops or
crashes, its workers keep running, and on restart it adopts them again by label. The cost is
access to the Docker socket, which is root on the host; accepted locally. In production this
whole class is replaced by the platform (Kubernetes Deployment / ECS service, desired count = W).

Same interface as Fleet (child processes), so the reconcile loop doesn't know which it drives.
"""

import re
import socket
import threading
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import docker
import structlog
from dmean_worker.settings import Settings as WorkerSettings
from docker.errors import APIError, NotFound

from dmean_supervisor.fleet import worker_env
from dmean_supervisor.reconcile import Child

log = structlog.get_logger()

ROLE_LABEL = "dmean.role"
OWNER_LABEL = "dmean.supervisor"  # which supervisor's fleet: its worker_id_prefix
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
# Worker settings by env name: only these reach a worker container
WORKER_ENV = {name.upper() for name in WorkerSettings.model_fields}


def container_env(worker_id: str) -> dict[str, str]:
    """What a worker container needs: its settings, never the supervisor's DB credentials."""
    return {k: v for k, v in worker_env(worker_id).items() if k in WORKER_ENV}


def created_monotonic(created: str) -> float:
    """Docker's Created timestamp (RFC 3339, nanoseconds) on our monotonic clock."""
    trimmed = re.sub(r"(\.\d{6})\d*", r"\1", created).replace("Z", "+00:00")
    age = (datetime.now(UTC) - datetime.fromisoformat(trimmed)).total_seconds()
    return time.monotonic() - max(age, 0.0)


class DockerFleet:
    def __init__(
        self,
        image: str,
        id_prefix: str,
        network: str | None = None,
        project: str | None = None,
        command: list[str] | None = None,
        client: Any = None,
    ) -> None:
        self._docker: Any = client or docker.from_env()
        self._image = image
        self._prefix = id_prefix
        self._command = command
        here = self._own_container()
        # Defaults from our own container: same network, same compose project (so
        # `docker compose down --remove-orphans` removes the workers too)
        self._network = network or (
            next(iter(here.attrs["NetworkSettings"]["Networks"])) if here else None
        )
        self._project = project or (here.labels.get(COMPOSE_PROJECT_LABEL) if here else None)
        self._children: dict[str, Child] = {}
        self.reap()  # adopt workers left running by a previous supervisor

    def _own_container(self) -> Any:
        try:
            return self._docker.containers.get(socket.gethostname())
        except NotFound:
            return None  # not running in a container (tests on the host)

    def _labels(self) -> dict[str, str]:
        labels = {ROLE_LABEL: "worker", OWNER_LABEL: self._prefix}
        if self._project:
            labels[COMPOSE_PROJECT_LABEL] = self._project
        return labels

    def _containers(self) -> list[Any]:
        return self._docker.containers.list(
            all=True,
            filters={"label": [f"{ROLE_LABEL}=worker", f"{OWNER_LABEL}={self._prefix}"]},
        )

    def children(self) -> list[Child]:
        return list(self._children.values())

    def spawn(self) -> str:
        worker_id = f"{self._prefix}-{uuid.uuid4().hex[:6]}"
        container = self._docker.containers.run(
            self._image,
            self._command,
            name=worker_id,
            hostname=worker_id,
            environment=container_env(worker_id),
            labels=self._labels(),
            network=self._network,
            init=True,  # PID 1 forwards SIGTERM to the worker
            stop_signal="SIGTERM",
            restart_policy={"Name": "no"},  # the supervisor replaces exited workers
            detach=True,
        )
        self._children[worker_id] = Child(worker_id, started_at=time.monotonic())
        log.info("worker container started", worker=worker_id, container=container.short_id)
        return worker_id

    def terminate(self, worker_id: str) -> None:
        """SIGTERM: the worker finishes and reports its current task, then exits."""
        child = self._children.get(worker_id)
        if child is None or child.draining:
            return
        try:
            self._docker.containers.get(worker_id).kill(signal="SIGTERM")
        except APIError as err:
            log.warning("could not signal worker", worker=worker_id, error=str(err))
            return
        self._children[worker_id] = Child(worker_id, child.started_at, draining=True)
        log.info("worker draining", worker=worker_id)

    def kill(self, worker_id: str) -> None:
        try:
            self._docker.containers.get(worker_id).kill()
            log.warning("hung worker killed", worker=worker_id)
        except APIError:
            pass  # already gone

    def reap(self) -> list[tuple[str, int]]:
        """Syncs with Docker: removes exited worker containers (returns their exit codes) and
        adopts running ones we don't know yet (left by a previous supervisor)."""
        exited: list[tuple[str, int]] = []
        running: set[str] = set()
        for c in self._containers():
            if c.status in ("exited", "dead"):
                exited.append((c.name, int(c.attrs["State"]["ExitCode"])))
                c.remove(force=True)
                continue
            running.add(c.name)
            if c.name not in self._children:
                self._children[c.name] = Child(c.name, created_monotonic(c.attrs["Created"]))
                log.info("worker container adopted", worker=c.name)
        for worker_id in list(self._children):
            if worker_id not in running:
                del self._children[worker_id]
        return exited

    def shutdown(self, timeout_secs: float, hurry: threading.Event | None = None) -> None:
        """The supervisor is stopping: leave the workers running (that's the point of
        containers); the next supervisor adopts them."""
        log.info("supervisor stopping: leaving workers running", workers=len(self._children))

    def stop_all(self, timeout_secs: float, hurry: threading.Event | None = None) -> None:
        """Drain every worker container; SIGKILL whoever is left after the timeout."""
        for worker_id in list(self._children):
            self.terminate(worker_id)
        deadline = time.monotonic() + timeout_secs
        while self._children and time.monotonic() < deadline and not (hurry and hurry.is_set()):
            self.reap()
            time.sleep(0.5)
        for worker_id in list(self._children):
            self.kill(worker_id)
        for c in self._containers():
            c.remove(force=True)
        self._children.clear()
