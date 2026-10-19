"""TaskRunner with an in-memory store: every result becomes an Outcome, nothing raises."""

from typing import Any

from dmean_worker.processing.outcome import Abandoned, Done, Failed, FailureType
from dmean_worker.processing.runner import TaskRunner
from dmean_worker.processing.task import Task
from dmean_worker.storage.csv_format import read_partial


class MemoryStorage:
    def __init__(self, objects: dict[str, str] | None = None, *, broken: bool = False) -> None:
        self.objects = dict(objects or {})
        self.broken = broken
        self.reads: list[str] = []

    def get_text(self, key: str) -> str:
        if self.broken:
            raise ConnectionError("storage down")
        self.reads.append(key)
        return self.objects[key]

    def put_text(self, key: str, text: str) -> int:
        self.objects[key] = text
        return len(text)

    def size_if_exists(self, key: str) -> int | None:
        return len(self.objects[key]) if key in self.objects else None


def task(
    keys: list[str], *, final: bool = False, kind: str = "map", files: int | None = None
) -> Task:
    return Task(
        task_id=7,
        attempt=1,
        kind=kind,
        input_keys=keys,
        no_of_files=len(keys) if files is None else files,
        no_of_columns=3,
        is_final=final,
        output_key="out.csv",
    )


def runner(storage: MemoryStorage) -> TaskRunner:
    return TaskRunner(storage)  # pyright: ignore[reportArgumentType]


def never() -> bool:
    return False


def test_map_writes_a_partial() -> None:
    storage = MemoryStorage({"a": "1,2,3\n", "b": "4,5,6\n"})
    outcome = runner(storage).run(task(["a", "b"]), never)
    assert outcome == Done("out.csv", len(storage.objects["out.csv"]))
    sums, count = read_partial(storage.objects["out.csv"], 3)
    assert (sums.tolist(), count) == ([5.0, 7.0, 9.0], 2)


def test_final_task_writes_the_mean() -> None:
    storage = MemoryStorage({"a": "1,2,3\n", "b": "4,5,6\n"})
    runner(storage).run(task(["a", "b"], final=True), never)
    assert storage.objects["out.csv"] == "2.5,3.5,4.5\n"


def test_existing_output_is_reported_without_reading_inputs() -> None:
    storage = MemoryStorage({"out.csv": "2\n1,2,3\n"})
    outcome = runner(storage).run(task(["missing"]), never)
    assert isinstance(outcome, Done) and storage.reads == []


def test_bad_csv_is_a_non_retryable_user_input_failure() -> None:
    storage = MemoryStorage({"a": "1,2,3\n", "bad.csv": "1,x,3\n"})
    outcome = runner(storage).run(task(["a", "bad.csv"]), never)
    assert isinstance(outcome, Failed)
    assert (outcome.failure_type, outcome.retryable) == (FailureType.USER_INPUT, False)
    assert "bad.csv" in outcome.error and "out.csv" not in storage.objects


def test_count_mismatch_is_a_non_retryable_internal_failure() -> None:
    storage = MemoryStorage({"a": "1,2,3\n"})
    outcome = runner(storage).run(task(["a"], files=2), never)
    assert isinstance(outcome, Failed)
    assert (outcome.failure_type, outcome.retryable) == (FailureType.INTERNAL, False)


def test_storage_error_is_retryable() -> None:
    outcome = runner(MemoryStorage({"a": "1,2,3\n"}, broken=True)).run(task(["a"]), never)
    assert isinstance(outcome, Failed)
    assert (outcome.failure_type, outcome.retryable) == (FailureType.INTERNAL, True)


def test_stop_abandons_without_writing() -> None:
    storage = MemoryStorage({"a": "1,2,3\n", "b": "4,5,6\n"})
    calls = iter([False, True])
    outcome = runner(storage).run(task(["a", "b"]), lambda: next(calls))
    assert outcome == Abandoned() and "out.csv" not in storage.objects


def test_task_from_proto_round_trip() -> None:
    from dmean_worker.rpc import task_queue_pb2 as pb

    proto: Any = pb.Task(
        task_id=3,
        attempt=2,
        kind="merge",
        input_keys=["p1", "p2"],
        no_of_files=10,
        no_of_columns=5,
        is_final=True,
        output_key="r.csv",
        heartbeat_seconds=10,
    )
    t = Task.from_proto(proto)
    assert (t.task_id, t.attempt, t.kind, t.input_keys, t.is_final) == (
        3,
        2,
        "merge",
        ["p1", "p2"],
        True,
    )
