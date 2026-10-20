"""
Task sizing: how many files a map task sums (N), and the merge fan-in (M).

Every task is about T seconds of work, computed from C alone: no assumption about the number of
workers, so a fleet that grows or shrinks after planning just takes more or fewer of the same
tasks, and nothing is re-planned. F only decides how many tasks there are (F / N).

    cost per file = L + C × (bytes_per_value / B + 1 / R)
                    ▲        ▲                    ▲
                    latency  download (size / bandwidth)   parse
    N = T / cost   (1 .. N_max)
    M = max(2, N)  a partial costs about one input file to read, so a merge also takes ~T

Download and parse are added as if sequential; prefetch overlaps them, so real tasks come out a
little shorter than T, which is the safe direction.
"""

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Sizing:
    target_task_secs: float  # T: ~10 s: short tail behind slow workers, cheap retries
    request_secs: float  # L: per-GET latency (first byte), ~8 ms under load locally
    bytes_per_value: float  # CSV bytes per value: ~19.3 for random floats (measured)
    bandwidth_bytes_per_sec: float  # B: download rate of one stream, ~150 MB/s locally
    parse_values_per_sec: float  # R: CSV parse rate of one worker, ~5M values/s (measured)
    max_files_per_task: int  # N_max: safety cap

    def secs_per_file(self, no_of_columns: int) -> float:
        """Expected cost of one input (or partial): request latency, download, parse."""
        per_value = (
            self.bytes_per_value / self.bandwidth_bytes_per_sec + 1 / self.parse_values_per_sec
        )
        return self.request_secs + no_of_columns * per_value


def files_per_task(no_of_columns: int, s: Sizing) -> int:
    """N: as many files as take about T seconds."""
    n = int(s.target_task_secs / s.secs_per_file(no_of_columns))
    return max(1, min(n, s.max_files_per_task))


def merge_fan_in(no_of_columns: int, s: Sizing) -> int:
    """M = max(2, N): a merge needs two inputs; otherwise it's sized exactly like a map."""
    return max(2, files_per_task(no_of_columns, s))


def chunk[T](items: Sequence[T], size: int) -> list[list[T]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]
