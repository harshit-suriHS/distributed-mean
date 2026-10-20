"""
Task sizing (design §7): how many files a map task sums, and the merge fan-in.

Tasks are sized by expected *time*, not by the number of workers (like Spark/Hadoop split
sizes): a fleet that grows or shrinks after planning just takes more or fewer of the same
~T-second tasks, so nothing has to be re-planned.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Sizing:
    target_task_secs: float  # T: ~25 s, short enough for fairness and cheap retries
    min_task_secs: float  # T_min: never millisecond tasks
    per_file_secs: float  # L: fixed cost per input file (an S3 GET), measured ~5-10 ms locally
    rate: float  # R: values/s one worker parses (~5M, measured)
    min_tasks_per_job: int  # K: a big job splits into at least this many tasks
    max_files_per_task: int  # N_max: safety cap

    def secs_per_file(self, no_of_columns: int) -> float:
        """Expected cost of one input: fetching it plus parsing its C values."""
        return self.per_file_secs + no_of_columns / self.rate


def files_per_task(no_of_files: int, no_of_columns: int, s: Sizing) -> int:
    """
    N = clamp( min( N_time, max( N_split, N_floor ) ), 1, N_max )

    - N_time  = T / cost   files that take about T seconds (cost = L + C/R per file)
    - N_split = ceil(F/K)  at least K tasks per job, so a big job spreads across workers
    - N_floor = T_min/cost tasks never shorter than T_min
    """
    cost = s.secs_per_file(no_of_columns)
    n_time = s.target_task_secs / cost
    n_floor = s.min_task_secs / cost
    n_split = math.ceil(no_of_files / s.min_tasks_per_job)
    n = int(min(n_time, max(n_split, n_floor)))
    return max(1, min(n, s.max_files_per_task))


def merge_fan_in(no_of_columns: int, s: Sizing) -> int:
    """A partial costs about one file (C values + a count), so a merge of M partials takes
    about T seconds when M = N_time. At least 2: a merge needs two inputs."""
    n_time = int(s.target_task_secs / s.secs_per_file(no_of_columns))
    return max(2, min(n_time, s.max_files_per_task))


def chunk[T](items: Sequence[T], size: int) -> list[list[T]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]
