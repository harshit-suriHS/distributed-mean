import math

import pytest

from dmean_orchestrator.scheduling.merge_pool import Partial, covers, take_merges
from dmean_orchestrator.scheduling.planner import Sizing, chunk, files_per_task, merge_fan_in

SIZING = Sizing(
    target_task_secs=10.0,
    request_secs=0.008,
    bytes_per_value=19.3,
    bandwidth_bytes_per_sec=150e6,
    parse_values_per_sec=5_000_000.0,
    max_files_per_task=5000,
)


@pytest.mark.parametrize(
    ("c", "expected", "why"),
    [
        (1_000_000, 29, "10 s / (8 ms + 129 ms download + 200 ms parse) per file"),
        (100_000, 244, "10 s / (8 ms + 12.9 ms download + 20 ms parse) per file"),
        (2_000, 1_155, "small files: the 8 ms request latency dominates"),
        (10, 1_249, "tiny files: almost pure request latency"),
    ],
)
def test_files_per_task_is_about_ten_seconds_of_work(c: int, expected: int, why: str) -> None:
    assert files_per_task(c, SIZING) == expected, why


def test_n_depends_only_on_c_never_on_f_or_the_number_of_workers() -> None:
    import inspect

    assert list(inspect.signature(files_per_task).parameters) == ["no_of_columns", "s"]


def test_cost_per_file_is_latency_plus_download_plus_parse() -> None:
    c = 1_000_000
    assert SIZING.secs_per_file(c) == pytest.approx(0.008 + c * 19.3 / 150e6 + c / 5e6)


def test_n_is_at_least_one_and_capped() -> None:
    slow = Sizing(0.1, 0.008, 19.3, 150e6, 5e6, 5000)
    assert files_per_task(1_000_000, slow) == 1  # one file takes longer than T
    fast = Sizing(1000.0, 0.008, 19.3, 150e6, 5e6, 5000)
    assert files_per_task(10, fast) == 5000  # the safety cap


def test_merge_fan_in_equals_n_and_is_at_least_two() -> None:
    assert merge_fan_in(1_000_000, SIZING) == files_per_task(1_000_000, SIZING) == 29
    assert merge_fan_in(2_000, SIZING) == 1_155
    tiny_budget = Sizing(0.1, 0.008, 19.3, 150e6, 5e6, 5000)
    assert merge_fan_in(1_000_000, tiny_budget) == 2  # a merge needs two inputs


def test_chunk_keeps_order_and_last_chunk_is_smaller() -> None:
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


def partials(*counts: int) -> list[Partial]:
    return [Partial(i, n) for i, n in enumerate(counts)]


def test_full_groups_are_merged_as_soon_as_they_exist() -> None:
    pool = partials(1, 1, 1, 1, 1)
    groups = take_merges(pool, fan_in=2, unfinished=3)
    assert [[p.file_id for p in g] for g in groups] == [[0, 1], [2, 3]]
    assert [p.file_id for p in pool] == [4]


def test_leftovers_wait_while_work_is_outstanding() -> None:
    pool = partials(1, 1)
    assert take_merges(pool, fan_in=3, unfinished=1) == []
    assert len(pool) == 2


def test_leftovers_become_one_last_merge_when_nothing_else_will_arrive() -> None:
    pool = partials(4, 3)
    [last] = take_merges(pool, fan_in=3, unfinished=0)
    assert covers(last) == 7 and pool == []


def test_single_partial_is_not_merged_alone() -> None:
    pool = partials(4)
    assert take_merges(pool, fan_in=2, unfinished=0) == []


@pytest.mark.parametrize(("f", "n"), [(10, 2), (10, 3), (37, 4), (100, 7), (1000, 125)])
def test_simulated_job_always_ends_in_exactly_one_final_merge(f: int, n: int) -> None:
    """Maps finish in any order; merges are created as the pool allows; the last covers F."""
    fan_in = max(2, n)
    maps = [len(c) for c in chunk(list(range(f)), n)]
    running = list(maps)  # tasks whose outputs are still to come (covered file counts)
    pool: list[Partial] = []
    next_id, merges, final = 0, 0, None
    while running:
        covered = running.pop(0)
        pool.append(Partial(next_id, covered))
        next_id += 1
        for group in take_merges(pool, fan_in, unfinished=len(running)):
            merges += 1
            if covers(group) == f:
                final = group
            else:
                running.append(covers(group))
    assert final is not None and covers(final) == f
    assert pool == []
    assert merges <= math.ceil(len(maps) / (fan_in - 1))  # each merge removes fan_in - 1 partials
