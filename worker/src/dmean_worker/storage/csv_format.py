"""
The only module that knows file formats. CSV everywhere:

- input:   one line of C numbers
- partial: line 1 the count, line 2 the C sums
- result:  one line of C means

Floats are written with repr (shortest string that parses back to the identical float64),
so writing then reading is bit-for-bit lossless.
"""

import io

import numpy as np
from numpy.typing import NDArray

Vector = NDArray[np.float64]


class InvalidInput(ValueError):
    """A file's contents are wrong: bad number, wrong width, NaN/inf."""


def _parse_values(text: str, columns: int, what: str) -> Vector:
    # One value per line would also parse to a flat array; the format is one line
    if "\n" in text.strip():
        raise InvalidInput(f"{what}: expected one line of values, got several lines")
    try:
        # loadtxt raises on a bad value (fromstring may silently stop early)
        values = np.loadtxt(io.StringIO(text), delimiter=",", dtype=np.float64, ndmin=1)
    except ValueError as err:
        raise InvalidInput(f"{what}: not a valid row of numbers ({err})") from err
    if values.shape[0] != columns:
        raise InvalidInput(f"{what}: expected {columns} values, got {values.shape[0]}")
    if not np.isfinite(values).all():
        raise InvalidInput(f"{what}: contains NaN or infinity")
    return values


def format_row(values: Vector) -> str:
    return ",".join(map(repr, values.tolist())) + "\n"


def parse_input(text: str, columns: int) -> Vector:
    return _parse_values(text, columns, "input file")


def write_partial(sums: Vector, count: int) -> str:
    return f"{count}\n{format_row(sums)}"


def read_partial(text: str, columns: int) -> tuple[Vector, int]:
    head, _, body = text.partition("\n")
    try:
        count = int(head)
    except ValueError as err:
        raise InvalidInput(f"partial: bad count line {head!r}") from err
    if count < 1:
        raise InvalidInput(f"partial: count must be positive, got {count}")
    return _parse_values(body, columns, "partial"), count


def write_result(means: Vector) -> str:
    return format_row(means)
