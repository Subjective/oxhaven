# What a Python function is given for an argument: the Python type and value for each column
# type, for a null cell, for a constant, a row variable and a variable of the scope, and what
# a parameter's annotation changes.
# oracle: results unannotated, constants, annotated, optional_long, optional_int, optional_double, optional_other, optional_null, numpy_given, several
from typing import Any, Optional, Union

import numpy as np

from deephaven import empty_table


def show(x) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_int(x: int) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_float(x: float) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_bool(x: bool) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_str(x: str) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_any(x: Any) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_object(x: object) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_union(x: Union[int, float]) -> str:
    return type(x).__name__ + ":" + repr(x)


def maybe_int(x: Optional[int]) -> str:
    return type(x).__name__ + ":" + repr(x)


def maybe_float(x: Optional[float]) -> str:
    return type(x).__name__ + ":" + repr(x)


def maybe_bool(x: Optional[bool]) -> str:
    return type(x).__name__ + ":" + repr(x)


def maybe_str(x: Optional[str]) -> str:
    return type(x).__name__ + ":" + repr(x)


def show_np_int32(x: np.int32) -> str:
    return type(x).__name__ + ":" + repr(int(x))


def show_np_float64(x: np.float64) -> str:
    return type(x).__name__ + ":" + repr(float(x))


def two(x, y) -> str:
    return type(x).__name__ + ":" + repr(x) + "/" + type(y).__name__ + ":" + repr(y)


def with_default(x, y=10) -> int:
    return x + y


def keyword_only(x, *, y=3) -> int:
    return x + y


def total(first: int, *rest: float) -> float:
    return first + sum(rest)


number = 7
real = 2.5
word = "w"
flag = True

source = empty_table(4).update([
    "B = i == 1 ? null : (byte) i", "S = i == 1 ? null : (short) i",
    "C = i == 1 ? null : (char) (i + 65)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long) i", "F = i == 1 ? null : (float) i",
    "D = i == 1 ? null : (double) i", "Z = i == 1 ? null : (Boolean) (i > 1)",
    "T = i == 1 ? null : `s` + i",
])
unannotated = source.select([
    "Sum = show(I + 1)", "Product = show(D * 2)", "Compared = show(I > 1)",
    "B = show(B)", "S = show(S)", "C = show(C)", "I = show(I)", "L = show(L)", "F = show(F)",
    "D = show(D)", "Z = show(Z)", "T = show(T)",
])
constants = source.select([
    "A = show(1)", "B = show(1L)", "C = show(1.5)", "D = show(1.5f)", "E = show(`abc`)",
    "F = show(true)", "G = show(null)", "H = show(i)", "J = show(ii)", "K = show(k)",
    "Two = two(I, number)", "N = show(number)", "R = show(real)", "W = show(word)",
    "Y = show(flag)",
])
annotated = source.select([
    "Any = show_any(I)", "Object = show_object(T)", "UnionInt = show_union(I)",
    "UnionFloat = show_union(D)", "B = show_int(B)", "S = show_int(S)", "C = show_int(C)", "I = show_int(I)", "L = show_int(L)",
    "F = show_float(F)", "D = show_float(D)", "Z = show_bool(Z)", "T = show_str(T)",
    "One = show_int(1)", "Real = show_float(1.5)",
    "Text = show_str(`x`)", "Yes = show_bool(true)",
])
optional_long = source.select(["Sum = maybe_int(L + 0)", "L = maybe_int(L)"])
optional_int = source.select(["I = maybe_int(I)"])
optional_double = source.select(["D = maybe_float(D)"])
optional_other = source.select([
    "C = maybe_int(C)", "Z = maybe_bool(Z)", "T = maybe_str(T)",
])
optional_null = source.select(["N = maybe_int(null)", "T = maybe_str(null)"])
numpy_given = source.select(["I = show_np_int32(I)", "S = show_np_int32(S)", "D = show_np_float64(D)"])
several = source.select([
    "A = with_default(I)", "W = with_default(I, 1)", "K = keyword_only(I)", "E = total(I, D, D)",
    "G = total(I)",
])
