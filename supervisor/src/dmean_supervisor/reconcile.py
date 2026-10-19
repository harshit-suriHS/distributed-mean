"""
The supervisor's decision, as a pure function: given the desired number of workers, the worker
processes we run, and what the orchestrator sees, what to spawn, stop and kill.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Child:
    worker_id: str
    started_at: float
    draining: bool = False  # SIGTERM sent: finishing its current task, no longer counted


@dataclass(frozen=True)
class Seen:
    """A worker as the orchestrator sees it (Admin.ListWorkers)."""

    busy: bool
    seconds_since_seen: float


@dataclass(frozen=True)
class Plan:
    spawn: int = 0
    terminate: list[str] = field(default_factory=list[str])  # SIGTERM: drain and exit
    kill: list[str] = field(default_factory=list[str])  # SIGKILL: hung, replaced by a new one


def plan(
    desired: int,
    children: list[Child],
    seen: dict[str, Seen] | None,
    now: float,
    *,
    startup_grace_secs: float,
    hang_after_secs: float,
) -> Plan:
    """
    - Hung: running past its startup grace but not seen by the orchestrator for hang_after_secs
      → kill (and replace). Skipped when `seen` is None (orchestrator unreachable): then
      "not seen" means nothing.
    - Too few healthy workers → spawn the difference.
    - Too many → SIGTERM the surplus, idle ones first, then the newest. A stopping worker
      finishes and reports its current task, so scaling down never loses work.
    """
    kill: list[str] = []
    if seen is not None:
        for c in children:
            if now - c.started_at < startup_grace_secs:
                continue
            info = seen.get(c.worker_id)
            if info is None or info.seconds_since_seen > hang_after_secs:
                kill.append(c.worker_id)

    healthy = [c for c in children if not c.draining and c.worker_id not in kill]
    if len(healthy) < desired:
        return Plan(spawn=desired - len(healthy), kill=kill)
    surplus = len(healthy) - desired
    if surplus == 0:
        return Plan(kill=kill)

    def busy(c: Child) -> bool:
        info = (seen or {}).get(c.worker_id)
        return info is not None and info.busy

    # idle first, then newest first
    order = sorted(healthy, key=lambda c: (busy(c), -c.started_at))
    return Plan(terminate=[c.worker_id for c in order[:surplus]], kill=kill)
