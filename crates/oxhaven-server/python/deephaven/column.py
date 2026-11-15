"""Input columns: the named, typed data that `new_table` builds a table from."""

from typing import Sequence


class InputColumn:
    """A column name, a column type and the values, for `new_table`.

    Behavior: `deephaven.column.InputColumn`
    """

    def __init__(self, name: str, data_type: str, input_data: Sequence):
        self.name = name
        self.data_type = data_type
        self.input_data = input_data


def bool_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of Booleans."""
    return InputColumn(name, "bool", data)


def int_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 32-bit integers."""
    return InputColumn(name, "int", data)


def long_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 64-bit integers."""
    return InputColumn(name, "long", data)


def double_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 64-bit floating point numbers."""
    return InputColumn(name, "double", data)


def string_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of strings."""
    return InputColumn(name, "string", data)
