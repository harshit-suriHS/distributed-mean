import math

import pytest

from dmean_orchestrator.scheduling.merge_pool import Partial, covers, take_merges
from dmean_orchestrator.scheduling.planner import Sizing, chunk, files_per_task, merge_fan_in

SIZING = Sizing(
    target_task_secs=25.0,
    min_task_secs=5.0,
    per_file_secs=0.008,
    rate=5_000_000.0,
    min_tasks_per_job=8,
    max_files_per_task=5000,
)


@pytest.mark.parametrize(
    ("f", "c", "expected", "why"),
    [
        (100_000, 1_000_000, 120, "time: 25 s / (8 ms + 0.2 s) per file = 120 files"),
        (2_000, 1_000_000, 120, "time again: still ~25 s tasks"),
        (40, 1_000_000, 24, "floor: 5 s / 0.208 s = 24 files, so 2 tasks rather than 8 tiny ones"),
        (2, 999_999, 24, "N >= F: the whole job is one final map task"),
        (20_000, 2_000, 2_500, "split: at least 8 tasks per job (time would allow 2,976)"),
        (2_000, 100, 623, "floor: 5 s / 8.02 ms = 623 files"),
        (100_000, 10, 3_124, "time: tiny files are dominated by the per-file cost"),
    ],
)
def test_files_per_task_is_sized_by_time(f: int, c: int, expected: int, why: str) -> None:
    assert files_per_task(f, c, SIZING) == expected, why


def test_files_per_task_does_not_depend_on_the_number_of_workers() -> None:
    import inspect

    assert "worker" not in " ".join(inspect.signature(files_per_task).parameters)


def test_per_file_cost_is_what_makes_small_files_cheap_to_batch() -> None:
    # per-file cost dominates small files: 2,000 values parse in 0.4 ms, the GET takes 8 ms
    assert SIZING.secs_per_file(2_000) == 0.008 + 2_000 / 5_000_000


def test_merge_fan_in_is_a_target_time_of_partials() -> None:
    assert merge_fan_in(1_000_000, SIZING) == 120
    assert merge_fan_in(2_000, SIZING) == 2_976
    tiny_budget = Sizing(0.1, 0.01, 0.008, 5e6, 8, 5000)
    assert merge_fan_in(1_000_000, tiny_budget) == 2  # never below 2


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
