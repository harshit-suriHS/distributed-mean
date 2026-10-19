import math

import pytest

from dmean_orchestrator.scheduling.merge_pool import Partial, covers, take_merges
from dmean_orchestrator.scheduling.planner import chunk, files_per_task, merge_fan_in


def n_for(f: int, c: int, w: int) -> int:
    return files_per_task(
        no_of_files=f,
        no_of_columns=c,
        live_workers=w,
        target_task_secs=25.0,
        min_task_secs=5.0,
        rate=5_000_000.0,
        max_files_per_task=5000,
    )


@pytest.mark.parametrize(
    ("f", "c", "w", "expected", "why"),
    [
        (100_000, 1_000_000, 10, 125, "time-bound: 25 s × 5M/s ÷ 1M values = 125 files"),
        (2_000, 1_000_000, 10, 50, "spread-bound: F/4W = 50, between N_min = 25 and 125"),
        (100_000, 1_000_000, 10_000, 25, "floor: F/4W = 2.5 would be tiny tasks; N_min = 25"),
        (100_000, 1_000, 10, 5000, "floor 25,000 files (5 s of work) capped at N_max"),
        (2, 999_999, 4, 25, "N ≥ F: the whole job becomes one final map task"),
    ],
)
def test_files_per_task(f: int, c: int, w: int, expected: int, why: str) -> None:
    assert n_for(f, c, w) == expected, why


def test_no_live_workers_counts_as_one() -> None:
    n = n_for(1000, 1_000_000, 0)
    assert n == 125  # 1000 / (4 × 1) = 250, capped by time at 125


def test_merge_fan_in_is_n_but_at_least_two() -> None:
    assert (merge_fan_in(125), merge_fan_in(1)) == (125, 2)


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
    fan_in = merge_fan_in(n)
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
