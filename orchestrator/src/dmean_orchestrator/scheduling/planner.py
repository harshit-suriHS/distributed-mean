"""Task sizing (design §7): how many files a map task sums, and the merge fan-in."""

from collections.abc import Sequence


def files_per_task(
    *,
    no_of_files: int,
    no_of_columns: int,
    live_workers: int,
    target_task_secs: float,
    min_task_secs: float,
    rate: float,
    max_files_per_task: int,
) -> int:
    """
    N = clamp( min( T·rate/C,  max( F/(4W), N_min ) ),  1,  N_max )

    - T·rate/C: files one worker sums in the target task time (short tasks: fair, cheap retries)
    - F/(4W): at least ~4 tasks per worker, so fast workers can take more
    - N_min = T_min·rate/C: never millisecond tasks, even when W is large
    """
    by_time = target_task_secs * rate / no_of_columns
    floor = min_task_secs * rate / no_of_columns
    spread = no_of_files / (4 * max(live_workers, 1))
    n = int(min(by_time, max(spread, floor)))
    return max(1, min(n, max_files_per_task))


def merge_fan_in(files_per_task: int) -> int:
    """A partial is the size of one file, so a merge of M partials costs about a map of N files."""
    return max(2, files_per_task)


def chunk[T](items: Sequence[T], size: int) -> list[list[T]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]
