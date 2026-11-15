"""The special values of the engine's primitive types: the value that stands for null in
each, and the smallest and largest values that are not null.

A column of this server marks a null in a validity bitmap, but the null values stay
observable as in Java: a value equal to a type's null constant is null in a column
(`PARITY.md`, "Null representation in the Java engine").

Behavior: `io.deephaven.util.QueryConstants`, as jpy hands its fields to Python: a `char`
is an `int`, and a `float` is the nearest double.
"""

import struct as _struct


def _float(bits: int) -> float:
    """The `float` with these bits, as a Python float."""
    return _struct.unpack(">f", _struct.pack(">I", bits))[0]


NULL_BOOLEAN = None

NULL_CHAR = 0xFFFF
MIN_CHAR = 0
MAX_CHAR = 0xFFFE

NULL_BYTE = -(2**7)
MIN_BYTE = -(2**7) + 1
MAX_BYTE = 2**7 - 1

NULL_SHORT = -(2**15)
MIN_SHORT = -(2**15) + 1
MAX_SHORT = 2**15 - 1

NULL_INT = -(2**31)
MIN_INT = -(2**31) + 1
MAX_INT = 2**31 - 1

NULL_LONG = -(2**63)
MIN_LONG = -(2**63) + 1
MAX_LONG = 2**63 - 1

NULL_FLOAT = -_float(0x7F7FFFFF)
NAN_FLOAT = float("nan")
NEG_INFINITY_FLOAT = float("-inf")
POS_INFINITY_FLOAT = float("inf")
MIN_FLOAT = NEG_INFINITY_FLOAT
MAX_FLOAT = POS_INFINITY_FLOAT
MIN_FINITE_FLOAT = -_float(0x7F7FFFFE)
MAX_FINITE_FLOAT = _float(0x7F7FFFFF)
MIN_POS_FLOAT = _float(0x00000001)

NULL_DOUBLE = -1.7976931348623157e308
NAN_DOUBLE = float("nan")
NEG_INFINITY_DOUBLE = float("-inf")
POS_INFINITY_DOUBLE = float("inf")
MIN_DOUBLE = NEG_INFINITY_DOUBLE
MAX_DOUBLE = POS_INFINITY_DOUBLE
MIN_FINITE_DOUBLE = -1.7976931348623155e308
MAX_FINITE_DOUBLE = 1.7976931348623157e308
MIN_POS_DOUBLE = 5e-324
