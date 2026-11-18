"""The data types of the engine, as `DType` objects.

Behavior: `deephaven.dtypes` of the reference package. There a `DType` names a Java class
and `array` makes a Java array through jpy; so it does here, with the arrays of the
module `jpy` this server holds (`python/jpy.py`, task 11.2), which convert the values as
jpy does: an integer type takes anything with `__index__` and keeps the low bits, a
floating point type takes any real number, `None` in a primitive array is zero, a
`Boolean` is the truth of the value.

numpy and pandas are imported when a function is given one of their values, never at
import: the server does not need them.

Types the engine has no column type for (`oxhaven_contracts::schema::ColumnType`) exist as
names, and a column of one is refused: `BigDecimal`, `BigInteger`, `Character`,
`StringSet`, `LocalDate`, `LocalTime`, `ZonedDateTime`, `Duration`, `Period`, `TimeZone`,
`BusinessCalendar`, `PyObject`, `JObject`, `bool_array` (Java's `boolean[]`) and
`zdt_array`.
"""

import datetime
import sys
from typing import Any, Callable, Optional, Sequence

from . import _calendar, _java_time
from .constants import (
    NULL_BYTE,
    NULL_CHAR,
    NULL_DOUBLE,
    NULL_FLOAT,
    NULL_INT,
    NULL_LONG,
    NULL_SHORT,
)
from .dherror import DHError

_j_name_type_map = {}

# The numpy type of a `DType`, by the name it is found under: a class of the numpy module,
# or the text of a dtype.
_NP_OBJECT = "object_"


class DType:
    """A data type of the engine: the name of a Java class, and what goes with it.

    Behavior: `deephaven.dtypes.DType`. `j_type` is the Python class of the type's values
    where this package has one (`None` otherwise), and `qst_type` is always `None`: there
    is no Java to describe the type to.
    """

    def __init__(
        self,
        j_name: str,
        j_type: Optional[type] = None,
        qst_type: Any = None,
        is_primitive: bool = False,
        np_type: Any = _NP_OBJECT,
    ):
        self.j_name = j_name
        self.j_type = j_type
        self.qst_type = qst_type
        self.is_primitive = is_primitive
        self._np_type = np_type
        _j_name_type_map[j_name] = self

    @property
    def np_type(self):
        """The numpy type that holds values of this type."""
        if isinstance(self._np_type, str):
            import numpy

            named = getattr(numpy, self._np_type, None)
            return named if named is not None else numpy.dtype(self._np_type)
        return self._np_type

    def __repr__(self):
        return self.j_name

    def __call__(self, *args, **kwargs):
        if self.is_primitive:
            raise DHError(message=f"primitive type {self.j_name} is not callable.")
        try:
            if self.j_type is not str:
                # the reference finds no constructor either for the types of `java.time`
                raise _java_time.NotPorted(f"{self.j_name}#<init>", "there are no Java objects here")
            return str(*args, **kwargs)
        except Exception as e:
            raise DHError(e, f"failed to create an instance of {self.j_name}") from e


bool_ = DType(j_name="java.lang.Boolean", j_type=bool, np_type="bool_")
byte = DType(j_name="byte", j_type=int, is_primitive=True, np_type="int8")
int8 = byte
short = DType(j_name="short", j_type=int, is_primitive=True, np_type="int16")
int16 = short
char = DType(j_name="char", j_type=int, is_primitive=True, np_type="uint16")
int32 = DType(j_name="int", j_type=int, is_primitive=True, np_type="int32")
int64 = DType(j_name="long", j_type=int, is_primitive=True, np_type="int64")
long = int64
float32 = DType(j_name="float", j_type=float, is_primitive=True, np_type="float32")
single = float32
float64 = DType(j_name="double", j_type=float, is_primitive=True, np_type="float64")
double = float64
string = DType(j_name="java.lang.String", j_type=str, np_type="str_")
Character = DType(j_name="java.lang.Character")
BigInteger = DType(j_name="java.math.BigInteger")
BigDecimal = DType(j_name="java.math.BigDecimal")
StringSet = DType(j_name="io.deephaven.stringset.StringSet")
Instant = DType(j_name="java.time.Instant", j_type=_java_time.Instant, np_type="datetime64[ns]")
LocalDate = DType(j_name="java.time.LocalDate", j_type=_java_time.LocalDate)
LocalTime = DType(j_name="java.time.LocalTime", j_type=_java_time.LocalTime)
ZonedDateTime = DType(j_name="java.time.ZonedDateTime", j_type=_java_time.ZonedDateTime)
Duration = DType(j_name="java.time.Duration", j_type=_java_time.Duration)
Period = DType(j_name="java.time.Period", j_type=_java_time.Period)
TimeZone = DType(j_name="java.time.ZoneId", j_type=_java_time.ZoneId)
BusinessCalendar = DType(j_name="io.deephaven.time.calendar.BusinessCalendar", j_type=_calendar.BusinessCalendar)
PyObject = DType(j_name="org.jpy.PyObject")
JObject = DType(j_name="java.lang.Object")
bool_array = DType(j_name="[Z")
byte_array = DType(j_name="[B")
int8_array = byte_array
short_array = DType(j_name="[S")
int16_array = short_array
char_array = DType(j_name="[C")
int32_array = DType(j_name="[I")
long_array = DType(j_name="[J")
int64_array = long_array
single_array = DType(j_name="[F")
float32_array = single_array
double_array = DType(j_name="[D")
float64_array = double_array
string_array = DType(j_name="[Ljava.lang.String;")
boolean_array = DType(j_name="[Ljava.lang.Boolean;")
instant_array = DType(j_name="[Ljava.time.Instant;")
zdt_array = DType(j_name="[Ljava.time.ZonedDateTime;")

_PRIMITIVE_DTYPE_NULL_MAP = {
    bool_: NULL_BYTE,
    byte: NULL_BYTE,
    char: NULL_CHAR,
    int16: NULL_SHORT,
    int32: NULL_INT,
    int64: NULL_LONG,
    float32: NULL_FLOAT,
    float64: NULL_DOUBLE,
}

_BUILDABLE_ARRAY_DTYPE_MAP = {
    bool_: boolean_array,
    byte: int8_array,
    char: char_array,
    int16: int16_array,
    int32: int32_array,
    int64: int64_array,
    float32: float32_array,
    float64: float64_array,
    string: string_array,
    Instant: instant_array,
}

# The letter of each primitive in the name the JVM gives an array class (`[I`), and the
# Java keyword of the primitive that has no `DType`.
_ARRAY_LETTERS = {"Z": "boolean", "B": "byte", "S": "short", "C": "char", "I": "int", "J": "long", "F": "float", "D": "double"}
_PRIMITIVE_LETTERS = {name: letter for letter, name in _ARRAY_LETTERS.items()}


def _component_name(j_name: str) -> Optional[str]:
    """The class name of the elements of the array class `j_name`, as `Class#getName`
    gives it; `None` for a class that is no array."""
    if not j_name.startswith("["):
        return None
    rest = j_name[1:]
    if rest.startswith("["):
        return rest
    if rest.startswith("L") and rest.endswith(";"):
        return rest[1:-1]
    return _ARRAY_LETTERS[rest]


def _canonical_name(j_name: str) -> str:
    """The name of a class as `Class#getCanonicalName` gives it, which is how the engine
    and the wire name a type: `int[]` for `[I`."""
    component = _component_name(j_name)
    return j_name if component is None else _canonical_name(component) + "[]"


def _from_java_name(name: Optional[str]) -> Optional["DType"]:
    """The `DType` of a Java class, by either of its names (`[I` or `int[]`); a new one for
    a class that has none yet.

    Behavior: `deephaven.dtypes.from_jtype`
    """
    if not name:
        return None
    if name.endswith("[]"):
        element = name[:-2]
        if element.endswith("[]"):
            name = "[" + _from_java_name(element).j_name
        elif element in _PRIMITIVE_LETTERS:
            name = "[" + _PRIMITIVE_LETTERS[element]
        else:
            name = f"[L{element};"
    return _j_name_type_map.get(name) or DType(j_name=name)


def from_jtype(j_class: Any) -> Optional[DType]:
    """The `DType` of a Java class, given by its name, or `None` for no class; a class
    without a `DType` gets a new one. The reference takes the Java class itself."""
    if isinstance(j_class, DType):
        return j_class
    return _from_java_name(j_class)


def _engine_type(dtype: DType) -> str:
    """The name the engine knows the column type of `dtype` by, or `NotPorted` for a type
    it has no column type for."""
    name = dtype.j_name
    while (component := _component_name(name)) is not None:
        name = component
    known = name in ("java.lang.Boolean", "java.lang.String", "java.time.Instant") or (
        name in _PRIMITIVE_LETTERS and name != "boolean"
    )
    if not known or name == "boolean":
        raise _java_time.NotPorted(
            "io.deephaven.engine.table.ColumnDefinition#fromGenericType",
            f"the engine has no column type for {dtype.j_name}",
        )
    return _canonical_name(dtype.j_name)


def null_remap(dtype: DType) -> Callable[[Any], Any]:
    """A function that replaces `None` by the null value of the primitive type `dtype`."""
    null_value = _PRIMITIVE_DTYPE_NULL_MAP.get(dtype)
    if null_value is None:
        raise TypeError("null_remap() must be called with a primitive DType")
    return lambda v: null_value if v is None else v


def _numpy():
    return sys.modules.get("numpy")


def _is_ndarray(value) -> bool:
    numpy = _numpy()
    return numpy is not None and isinstance(value, numpy.ndarray)


def _java_array(dtype: DType, values):
    """A Java array of `dtype` elements that holds `values`, which are values of it."""
    import jpy

    return jpy.array(dtype.j_name, values)


def _exact_nanos(value):
    """The nanoseconds of a time that `pandas.Timestamp(value)` reads without loss: an
    integer is nanoseconds, a date is its midnight, and a date and time without a time zone
    is taken as UTC. `NotImplemented` for anything else.

    Behavior: `pandas.Timestamp`, as `deephaven.dtypes._instant_array` uses it
    """
    numpy = _numpy()
    pandas = sys.modules.get("pandas")
    if value is None or (pandas is not None and value is pandas.NaT):
        return NULL_LONG
    if pandas is not None and isinstance(value, pandas.Timestamp):
        return value.value
    if isinstance(value, bool):
        return NotImplemented
    if isinstance(value, int):
        return value
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        delta = value - datetime.datetime(1970, 1, 1)
        return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000
    if isinstance(value, datetime.date):
        return (value - datetime.date(1970, 1, 1)).days * 86_400 * 1_000_000_000
    if numpy is not None and isinstance(value, numpy.datetime64):
        return int(value.astype("datetime64[ns]").astype(numpy.int64))
    return NotImplemented


def _instant_array(data):
    """The instants of a sequence of strings, integers in nanoseconds, `datetime`s, numpy
    or pandas times or instants; `None` is a null.

    Behavior: `deephaven.dtypes._instant_array`: strings are parsed
    (`DateTimeUtils#parseInstant`); a sequence without strings goes through
    `pandas.Timestamp`, value by value, and only if that fails for a value, through
    `deephaven.time.to_j_instant`, whose arithmetic on a `datetime` is in floating point.
    """
    from .time import to_j_instant

    numpy = _numpy()
    of_nanos = _java_time.instant_of_nanos
    if _is_ndarray(data):
        if data.dtype.kind == "U":
            return _java_array(Instant, [_java_time.parse_instant(str(text)) for text in data])
        if data.dtype.kind == "M":
            nanos = data.astype("datetime64[ns]").astype(numpy.int64).tolist()
            return _java_array(Instant, list(map(of_nanos, nanos)))
        if data.dtype.kind == "i":
            return _java_array(Instant, list(map(of_nanos, data.astype(numpy.int64).tolist())))
    # a numpy array of any other kind is converted value by value
    by_value = _is_ndarray(data)
    data = list(data)
    if not data:
        return _java_array(Instant, [])
    if all(d is None or isinstance(d, str) for d in data):
        return _java_array(Instant, [None if d is None else _java_time.parse_instant(d) for d in data])
    if not by_value and not any(isinstance(d, str) for d in data):
        try:
            return _java_array(Instant, list(map(of_nanos, map(_timestamp_nanos, data))))
        except Exception:
            pass
    return _java_array(Instant, list(map(to_j_instant, data)))


def _timestamp_nanos(value) -> int:
    nanos = _exact_nanos(value)
    if nanos is NotImplemented:
        # whatever else `pandas.Timestamp` reads, if pandas is there to ask
        import numpy
        import pandas

        nanos = int(pandas.Timestamp(value).to_numpy().astype("datetime64[ns]").astype(numpy.int64))
    if not NULL_LONG <= nanos < 2**63:
        raise OverflowError("int too big to convert")
    return nanos


def array(dtype: DType, seq: Optional[Sequence], remap: Optional[Callable[[Any], Any]] = None):
    """Creates an array of `dtype` elements from a sequence, converting each value as jpy
    does: without a check for lost precision or range. `remap` is applied to each value
    first; a string for an array of `char` is its code points.
    """
    if seq is None:
        return None
    if _is_ndarray(seq) and seq.ndim > 1:
        raise ValueError("array() does not support multi-dimensional arrays")
    if not isinstance(dtype, DType):
        raise TypeError(f"array() expects a DType for the first argument but given a {type(dtype).__name__}")
    try:
        if isinstance(seq, str) and dtype == char:
            remap = ord
        if remap:
            if not callable(remap):
                raise ValueError("Not a callable")
            seq = [remap(v) for v in seq]
        if dtype == Instant:
            return _instant_array(seq)
        if dtype == bool_ and _is_ndarray(seq):
            # Behavior: `PrimitiveArrayConversionUtility#translateArrayByteToBoolean`,
            # `BooleanUtils#byteAsBoolean`: a negative byte is a null
            bytes_ = seq.astype(dtype="int8").tolist()
            return _java_array(bool_, [None if b < 0 else b > 0 for b in bytes_])
        return _java_array(dtype, seq)
    except Exception as e:
        raise DHError(e, f"failed to create a Java {dtype.j_name} array.") from e


def from_np_dtype(np_dtype) -> DType:
    """The `DType` of a numpy dtype or of the pandas nullable type that corresponds to
    one; `PyObject` if there is none."""
    import numpy as np
    import pandas as pd

    if isinstance(np_dtype, pd.api.extensions.ExtensionDtype):
        if hasattr(np_dtype, "numpy_dtype"):
            np_dtype = np_dtype.numpy_dtype
        elif isinstance(np_dtype, pd.StringDtype):
            return string
        else:
            return PyObject
    if np_dtype.kind in {"U", "S"}:
        return string
    if np_dtype.kind in {"M"}:
        return Instant
    for dtype in _j_name_type_map.values():
        if dtype._np_type != _NP_OBJECT and np.dtype(dtype.np_type) == np_dtype:
            return dtype
    return PyObject


def _is_py_null(x: Any) -> bool:
    """Whether a value is one of Python's nulls: `None`, a NaN, `pandas.NA`, a not-a-time."""
    if x is None:
        return True
    pandas = sys.modules.get("pandas")
    if pandas is None:
        return isinstance(x, float) and x != x
    try:
        return bool(pandas.isna(x))
    except (TypeError, ValueError):
        return False
