"""
Dynamic merge pool (design §8): merge whatever partials finished first, not a planned tree,
so one slow map never stalls a whole subtree.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Partial:
    file_id: int
    no_of_files: int  # original files it covers


def take_merges(pool: list[Partial], fan_in: int, unfinished: int) -> list[list[Partial]]:
    """
    Removes and returns the groups to merge now:
    - every full group of `fan_in` partials (oldest first);
    - otherwise, once nothing else of the job is unfinished, all remaining partials (≥ 2) in one
      last merge. Fewer than `fan_in` remain after the full groups, so it still fits the fan-in.
    """
    groups: list[list[Partial]] = []
    while len(pool) >= fan_in:
        groups.append(pool[:fan_in])
        del pool[:fan_in]
    if not groups and unfinished == 0 and len(pool) >= 2:
        groups.append(pool[:])
        pool.clear()
    return groups


def covers(group: list[Partial]) -> int:
    return sum(p.no_of_files for p in group)
