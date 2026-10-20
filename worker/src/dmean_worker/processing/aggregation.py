"""The actual work, free of gRPC and S3 so it can be unit tested."""

from collections.abc import Callable

import numpy as np

from dmean_worker.processing.outcome import FailureType
from dmean_worker.processing.prefetch import prefetched
from dmean_worker.processing.task import Task
from dmean_worker.storage.csv_format import (
    InvalidInput,
    Vector,
    parse_input,
    read_partial,
    write_partial,
    write_result,
)


class TaskFailed(Exception):
    """A failure that retrying can't fix: report it and fail the job."""

    def __init__(self, failure_type: FailureType, message: str) -> None:
        super().__init__(message)
        self.failure_type = failure_type


def sum_inputs(
    task: Task,
    load_text: Callable[[str], str],
    should_stop: Callable[[], bool],
    prefetch: int = 1,
) -> tuple[Vector, int] | None:
    """
    Streams the task's inputs into one accumulator, fetching up to `prefetch` files ahead:
    memory is the accumulator plus at most `prefetch` files (≤ 4 + 1), whatever the number of
    inputs. Returns None if the job stopped meanwhile.
    """
    acc = np.zeros(task.no_of_columns, dtype=np.float64)
    count = 0
    for key, text in prefetched(task.input_keys, load_text, prefetch):
        if should_stop():
            return None
        if task.kind == "map":
            try:
                values = parse_input(text, task.no_of_columns)
            except InvalidInput as err:
                raise TaskFailed(FailureType.USER_INPUT, f"{key}: {err}") from err
            files = 1
        else:
            try:
                values, files = read_partial(text, task.no_of_columns)
            except InvalidInput as err:
                raise TaskFailed(FailureType.INTERNAL, f"{key}: {err}") from err
        acc += values
        count += files

    # Never write an output whose count is wrong: that would be double counting or a lost file
    if count != task.no_of_files:
        raise TaskFailed(FailureType.INTERNAL, f"summed {count} files, expected {task.no_of_files}")
    return acc, count


def render_output(task: Task, sums: Vector, count: int) -> str:
    return write_result(sums / count) if task.is_final else write_partial(sums, count)
