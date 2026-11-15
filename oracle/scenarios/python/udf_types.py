# The column type a Python function's return annotation gives, the cells its values become,
# and a call as part of a larger expression.
# oracle: results plain, optional, numpy_types, dtype_types, narrowed, expressions, filtered
from typing import Optional

import numpy as np

from deephaven import dtypes, empty_table


def as_int(x) -> int:
    return 2 * x


def as_float(x) -> float:
    return x / 2


def as_whole_float(x) -> float:
    return x


def as_bool(x) -> bool:
    return x < 3


def as_str(x) -> str:
    return "s" + str(x)


def text_annotation(x) -> "int":
    return x + 1


def maybe_int(x) -> Optional[int]:
    return None if x == 2 else x


def maybe_float(x) -> Optional[float]:
    return None if x == 2 else x * 1.5


def maybe_bool(x) -> Optional[bool]:
    return None if x == 2 else x > 2


def maybe_str(x) -> Optional[str]:
    return None if x == 2 else str(x)


def or_none(x) -> int | None:
    return None if x == 3 else x


def none_str(x) -> str:
    return None if x == 1 else "t"


def none_bool(x) -> bool:
    return None if x == 1 else True


def truncated(x) -> Optional[int]:
    return x + 0.75


def np_int8(x) -> np.int8:
    return x


def np_int16(x) -> np.int16:
    return x


def np_int32(x) -> np.int32:
    return x


def np_int64(x) -> np.int64:
    return x


def np_float32(x) -> np.float32:
    return x + 0.1


def np_float64(x) -> np.float64:
    return x + 0.1


def np_bool(x) -> np.bool_:
    return x


def dt_int(x) -> dtypes.int32:
    return x


def dt_short(x) -> dtypes.short:
    return x


def dt_double(x) -> dtypes.double:
    return x


def dt_string(x) -> dtypes.string:
    return str(x)


def dt_bool(x) -> dtypes.bool_:
    return x


def wraps_byte(x) -> np.int8:
    return 300 + x


def wraps_int(x) -> np.int32:
    return 2**40 + x


def null_long(x) -> np.int64:
    return -(2**63) if x == 1 else x


def null_int(x) -> np.int32:
    return -(2**31) if x == 1 else x


def too_big_float(x) -> np.float32:
    return 1e300 if x == 1 else x


def not_a_number(x) -> float:
    return float("nan") if x == 1 else float("inf")


source = empty_table(5).update("X = i")
plain = source.update([
    "A = as_int(X)", "B = as_float(X)", "C = as_whole_float(X)", "D = as_bool(X)", "E = as_str(X)",
    "F = text_annotation(X)",
])
optional = source.update([
    "A = maybe_int(X)", "B = maybe_float(X)", "C = maybe_bool(X)", "D = maybe_str(X)",
    "E = or_none(X)", "F = none_str(X)", "G = none_bool(X)", "H = truncated(X)",
])
numpy_types = source.update([
    "A = np_int8(X)", "B = np_int16(X)", "C = np_int32(X)", "D = np_int64(X)", "E = np_float32(X)",
    "F = np_float64(X)", "G = np_bool(X)",
])
dtype_types = source.update([
    "A = dt_int(X)", "B = dt_short(X)", "C = dt_double(X)", "D = dt_string(X)", "E = dt_bool(X)",
])
narrowed = source.update([
    "A = wraps_byte(X)", "B = wraps_int(X)", "C = null_long(X)", "D = null_int(X)",
    "E = too_big_float(X)", "F = not_a_number(X)",
])
expressions = source.update([
    "A = as_int(X) + 1", "B = as_int(X + 1)", "C = as_int(as_int(X))", "D = as_float(X) * 2",
    "E = as_str(X) + `z`", "F = X > 1 ? as_int(X) : -1", "G = as_int(X) > 2 && as_int(X) < 8",
    "H = as_int(i) + as_int(ii) + as_int(k)", "J = isNull(maybe_int(X))",
]).view(["A", "B", "C", "D", "E", "F", "G", "H", "J", "K = as_int(A)"])
filtered = source.where("as_bool(X)").where("as_int(X) > 0").update_view("Y = as_int(X)")
