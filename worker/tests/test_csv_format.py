import numpy as np
import pytest

from dmean_worker.storage.csv_format import (
    InvalidInput,
    format_row,
    parse_input,
    read_partial,
    write_partial,
    write_result,
)


def test_round_trip_is_bit_identical() -> None:
    values = np.concatenate(
        [
            np.random.default_rng(7).random(10_000),
            [0.1 + 0.2, 1 / 3, 1e-300, 5e-324, 1e308, -0.0, 123456789.123456789],
        ]
    )
    parsed = parse_input(format_row(values), values.size)
    assert parsed.tobytes() == values.tobytes()


def test_partial_round_trip() -> None:
    sums = np.random.default_rng(1).random(50) * 1000
    back, count = read_partial(write_partial(sums, 2500), 50)
    assert count == 2500
    assert back.tobytes() == sums.tobytes()


def test_result_is_one_csv_line() -> None:
    assert write_result(np.array([2.5, 3.5, 4.5])) == "2.5,3.5,4.5\n"


def test_problem_statement_example_parses() -> None:
    assert parse_input("1,2,3\n", 3).tolist() == [1.0, 2.0, 3.0]
    assert parse_input("4, 5, 6", 3).tolist() == [4.0, 5.0, 6.0]  # spaces, no trailing newline


@pytest.mark.parametrize(
    ("text", "columns", "message"),
    [
        ("1,2\n", 3, "expected 3 values, got 2"),
        ("1,2,3,4\n", 3, "expected 3 values, got 4"),
        ("1,abc,3\n", 3, "not a valid row of numbers"),
        ("1,nan,3\n", 3, "NaN or infinity"),
        ("1,inf,3\n", 3, "NaN or infinity"),
        ("1\n2\n3\n", 3, "several lines"),
        ("1,2,3\n4,5,6\n", 3, "several lines"),
    ],
)
def test_invalid_inputs(text: str, columns: int, message: str) -> None:
    with pytest.raises(InvalidInput, match=message):
        parse_input(text, columns)


@pytest.mark.parametrize("text", ["x\n1,2\n", "0\n1,2\n", "-3\n1,2\n"])
def test_invalid_partial_count(text: str) -> None:
    with pytest.raises(InvalidInput):
        read_partial(text, 2)
