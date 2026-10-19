from dmean_supervisor.reconcile import Child, Seen, plan

NOW = 1000.0


def decide(
    desired: int, children: list[Child], seen: dict[str, Seen] | None
) -> tuple[int, list[str], list[str]]:
    p = plan(desired, children, seen, NOW, startup_grace_secs=30, hang_after_secs=60)
    return p.spawn, p.terminate, p.kill


def child(worker_id: str, age: float = 100, draining: bool = False) -> Child:
    return Child(worker_id, started_at=NOW - age, draining=draining)


def ok(busy: bool = False) -> Seen:
    return Seen(busy=busy, seconds_since_seen=1)


def test_spawns_up_to_desired() -> None:
    assert decide(3, [child("a")], {"a": ok()}) == (2, [], [])


def test_nothing_to_do_at_desired() -> None:
    assert decide(2, [child("a"), child("b")], {"a": ok(), "b": ok()}) == (0, [], [])


def test_scale_down_stops_idle_workers_first() -> None:
    children = [child("busy-old", age=300), child("idle-old", age=200), child("busy-new", age=10)]
    seen = {"busy-old": ok(busy=True), "idle-old": ok(), "busy-new": ok(busy=True)}
    _, terminate, _ = decide(1, children, seen)
    # idle first, then the newest of the busy ones
    assert terminate == ["idle-old", "busy-new"]


def test_draining_workers_are_not_counted() -> None:
    children = [child("a"), child("b", draining=True)]
    assert decide(2, children, {"a": ok(), "b": ok()}) == (1, [], [])


def test_hung_worker_is_killed_and_replaced() -> None:
    children = [child("a"), child("stuck")]
    seen = {"a": ok(), "stuck": Seen(busy=True, seconds_since_seen=90)}
    assert decide(2, children, seen) == (1, [], ["stuck"])


def test_worker_the_orchestrator_never_saw_is_killed_after_the_grace() -> None:
    assert decide(1, [child("ghost", age=100)], {}) == (1, [], ["ghost"])
    assert decide(1, [child("new", age=5)], {}) == (0, [], [])  # still starting up


def test_no_killing_while_the_orchestrator_is_unreachable() -> None:
    children = [child("a"), child("b")]
    assert decide(2, children, None) == (0, [], [])  # unseen means nothing during an outage
    assert decide(1, children, None) == (0, ["a"], [])  # scaling still works (newest first)


def test_scale_to_zero() -> None:
    _, terminate, _ = decide(0, [child("a"), child("b")], {"a": ok(), "b": ok()})
    assert sorted(terminate) == ["a", "b"]
