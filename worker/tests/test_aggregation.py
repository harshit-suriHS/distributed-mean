import numpy as np
import pytest

from dmean_worker.processing.aggregation import TaskFailed, render_output, sum_inputs
from dmean_worker.processing.outcome import FailureType
from dmean_worker.processing.task import Task
from dmean_worker.storage.csv_format import format_row, parse_input, read_partial, write_partial


def make_task(
    kind: str, keys: list[str], no_of_files: int, columns: int = 3, *, final: bool = False
) -> Task:
    return Task(
        task_id=1,
        attempt=1,
        kind=kind,
        input_keys=keys,
        no_of_files=no_of_files,
        no_of_columns=columns,
        is_final=final,
        output_key="out.csv",
    )


def never_stop() -> bool:
    return False


def test_map_sums_inputs() -> None:
    files = {"a": "1,2,3\n", "b": "4,5,6\n"}
    summed = sum_inputs(make_task("map", ["a", "b"], 2), files.__getitem__, never_stop)
    assert summed is not None
    sums, count = summed
    assert sums.tolist() == [5.0, 7.0, 9.0]
    assert count == 2


def test_problem_statement_example_end_to_end() -> None:
    files = {"file_1": "1,2,3\n", "file_2": "4,5,6\n"}
    task = make_task("map", ["file_1", "file_2"], 2, final=True)
    summed = sum_inputs(task, files.__getitem__, never_stop)
    assert summed is not None
    assert render_output(task, *summed) == "2.5,3.5,4.5\n"


def test_merge_adds_partial_counts() -> None:
    files = {
        "p1": write_partial(np.array([1.0, 2.0, 3.0]), 2),
        "p2": write_partial(np.array([10.0, 20.0, 30.0]), 3),
    }
    task = make_task("merge", ["p1", "p2"], 5)
    summed = sum_inputs(task, files.__getitem__, never_stop)
    assert summed is not None
    sums, count = read_partial(render_output(task, *summed), 3)
    assert sums.tolist() == [11.0, 22.0, 33.0]
    assert count == 5


def test_final_merge_writes_means() -> None:
    files = {
        "p1": write_partial(np.array([1.0, 2.0, 3.0]), 2),
        "p2": write_partial(np.array([3.0, 6.0, 9.0]), 2),
    }
    task = make_task("merge", ["p1", "p2"], 4, final=True)
    summed = sum_inputs(task, files.__getitem__, never_stop)
    assert summed is not None
    assert parse_input(render_output(task, *summed), 3).tolist() == [1.0, 2.0, 3.0]


def test_count_mismatch_is_an_internal_failure() -> None:
    files = {"p1": write_partial(np.array([1.0, 2.0, 3.0]), 2)}
    with pytest.raises(TaskFailed, match="summed 2 files, expected 3") as err:
        sum_inputs(make_task("merge", ["p1"], 3), files.__getitem__, never_stop)
    assert err.value.failure_type is FailureType.INTERNAL


def test_bad_uploaded_file_is_a_user_input_failure() -> None:
    files = {"a": "1,2,3\n", "bad.csv": "1,2\n"}
    with pytest.raises(TaskFailed, match=r"bad\.csv: input file: expected 3 values") as err:
        sum_inputs(make_task("map", ["a", "bad.csv"], 2), files.__getitem__, never_stop)
    assert err.value.failure_type is FailureType.USER_INPUT


def test_corrupt_partial_is_an_internal_failure() -> None:
    files = {"p1": "two\n1,2,3\n"}
    with pytest.raises(TaskFailed) as err:
        sum_inputs(make_task("merge", ["p1"], 2), files.__getitem__, never_stop)
    assert err.value.failure_type is FailureType.INTERNAL


def test_stops_when_the_job_stops() -> None:
    loaded: list[str] = []

    def load(key: str) -> str:
        loaded.append(key)
        return "1,2,3\n"

    checks = iter([False, True])
    summed = sum_inputs(make_task("map", ["a", "b", "c"], 3), load, lambda: next(checks))
    assert summed is None
    assert loaded == ["a", "b"]  # stopped before summing the second file (already fetched)


def test_sum_matches_numpy_for_many_files() -> None:
    rng = np.random.default_rng(3)
    data = rng.random((200, 64))
    files = {f"f{i}": format_row(row) for i, row in enumerate(data)}
    task = make_task("map", list(files), 200, columns=64, final=True)
    summed = sum_inputs(task, files.__getitem__, never_stop)
    assert summed is not None
    means = parse_input(render_output(task, *summed), 64)
    np.testing.assert_allclose(means, data.mean(axis=0), rtol=1e-13)


def test_prefetch_keeps_at_most_four_files_ahead_and_sums_identically() -> None:
    import threading
    import time

    rng = np.random.default_rng(9)
    rows = rng.random((40, 50))
    files = {f"f{i}": format_row(r) for i, r in enumerate(rows)}
    task = make_task("map", list(files), 40, columns=50)
    lock = threading.Lock()
    started = 0
    consumed = 0
    max_ahead = 0

    def load(key: str) -> str:
        nonlocal started, max_ahead
        with lock:
            started += 1
            max_ahead = max(max_ahead, started - consumed)
        time.sleep(0.01)  # an S3 GET
        return files[key]

    def stop() -> bool:  # called once per file, just before it is summed
        nonlocal consumed
        with lock:
            consumed += 1
        return False

    t0 = time.perf_counter()
    parallel = sum_inputs(task, load, stop, prefetch=4)
    parallel_secs = time.perf_counter() - t0
    t0 = time.perf_counter()
    sequential = sum_inputs(task, files.__getitem__, never_stop)
    assert parallel is not None and sequential is not None
    # same files, same order: bit-for-bit identical sums
    assert parallel[0].tobytes() == sequential[0].tobytes() and parallel[1] == 40
    # never more than 4 files fetched ahead (+ the accumulator = 5 in memory)
    assert max_ahead <= 4
    # 40 GETs of 10 ms: ~0.4 s one by one, ~0.1 s four at a time
    assert parallel_secs < 0.25


def test_prefetch_window_is_capped_at_four() -> None:
    from dmean_worker.processing.prefetch import prefetched

    seen = list(prefetched(["a", "b", "c"], lambda k: k.upper(), window=99))
    assert seen == [("a", "A"), ("b", "B"), ("c", "C")]


def test_stopping_drops_queued_fetches() -> None:
    loaded: list[str] = []
    files = {f"f{i}": "1,2,3\n" for i in range(50)}

    def load(key: str) -> str:
        loaded.append(key)
        return files[key]

    checks = iter([False, False, True])
    task = make_task("map", list(files), 50)
    assert sum_inputs(task, load, lambda: next(checks), prefetch=4) is None
    assert len(loaded) <= 7  # 2 summed + at most a window ahead, not all 50
