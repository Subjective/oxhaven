"""Column definitions, and input columns: the named, typed data that `new_table` builds a
table from.

Behavior: `deephaven.column` of the reference package.
"""

from enum import Enum
from typing import Any, Optional, Sequence
from warnings import warn

from . import _engine, dtypes
from .dherror import DHError
from .dtypes import DType, _instant_array


class ColumnType(Enum):
    """Whether a column is a regular column or a partitioning column."""

    NORMAL = "Normal"
    PARTITIONING = "Partitioning"

    def __repr__(self):
        return self.name


def _class_text(dtype: Optional[DType]) -> str:
    """Behavior: `String#valueOf(Class)`"""
    if dtype is None:
        return "null"
    return dtype.j_name if dtype.is_primitive else f"class {dtype.j_name}"


class ColumnDefinition:
    """The name, the data type, the component type and the kind of one column. Create one
    with `col_def`.

    Behavior: `deephaven.column.ColumnDefinition`, which wraps the Java object; this one
    holds the four values.
    """

    def __init__(
        self,
        name: str,
        data_type: DType,
        component_type: Optional[DType] = None,
        column_type: ColumnType = ColumnType.NORMAL,
    ):
        self.name = name
        self.data_type = data_type
        self.component_type = component_type
        self.column_type = column_type

    @property
    def j_object(self):
        """The reference's Java column definition; here the definition itself."""
        return self

    @property
    def j_column_definition(self):
        return self

    def __repr__(self):
        return f"ColumnDefinition(name={self.name!r}, data_type={self.data_type!r})"

    def __str__(self):
        # Behavior: `io.deephaven.engine.table.ColumnDefinition#toString`
        return (
            f"ColumnDefinition {{name={self.name}, dataType={_class_text(self.data_type)}, "
            f"componentType={_class_text(self.component_type)}, columnType={self.column_type.value}}}"
        )

    def _key(self):
        return self.name, self.data_type, self.component_type, self.column_type

    def __eq__(self, other):
        return isinstance(other, ColumnDefinition) and self._key() == other._key()

    def __hash__(self):
        return hash(self._key())


def _component_type(data_type: DType, component_type: Optional[DType]) -> Optional[DType]:
    """The component type of a column: the element type of an array, the given one for any
    other type.

    Behavior: `io.deephaven.engine.table.ColumnDefinition#checkAndMaybeInferComponentType`.
    A vector class infers its element type too; a definition of one can only come from a
    table, which names the component itself.
    """
    element = dtypes._component_name(data_type.j_name)
    if element is None:
        return component_type
    inferred = dtypes._from_java_name(element)
    if component_type is None or component_type is inferred:
        return inferred
    if not inferred.is_primitive and not component_type.is_primitive:
        # allowed when the given class is a subclass of the element class, which only
        # Java can tell
        raise dtypes._java_time.NotPorted(
            "io.deephaven.engine.table.ColumnDefinition#checkAndMaybeInferComponentType",
            f"component type {component_type} for {data_type}",
        )
    raise RuntimeError(
        "java.lang.IllegalArgumentException: Invalid component type "
        f"{_class_text(component_type)} for array data type {_class_text(data_type)}"
    )


def col_def(
    name: str,
    data_type: DType,
    component_type: Optional[DType] = None,
    column_type: ColumnType = ColumnType.NORMAL,
) -> ColumnDefinition:
    """Creates a column definition."""
    try:
        # as the reference reads `j_type` of whatever it is given
        data_type.j_type
        if component_type is not None:
            component_type.qst_type
        column_type.value
        return ColumnDefinition(name, data_type, _component_type(data_type, component_type), column_type)
    except Exception as e:
        raise DHError(e, f"failed to create a ColumnDefinition ({name}).") from e


class Column(ColumnDefinition):
    """A column definition. Deprecated in the reference package, which prefers `col_def`."""

    def __init__(
        self,
        name: str,
        data_type: DType,
        component_type: Optional[DType] = None,
        column_type: ColumnType = ColumnType.NORMAL,
    ):
        warn(
            "Column is deprecated for removal next release, prefer col_def",
            DeprecationWarning,
            stacklevel=2,
        )
        definition = col_def(name, data_type, component_type, column_type)
        super().__init__(definition.name, definition.data_type, definition.component_type, definition.column_type)


class InputColumn:
    """A column name, a data type and the values, for `new_table`.

    Behavior: `deephaven.column.InputColumn`. `j_column` is the Java column there; here it
    is the values as `deephaven.dtypes.array` converts them, which `new_table` hands to
    the engine. A column of a type the engine has no column type for is refused.
    """

    def __init__(
        self,
        name: str,
        data_type: DType,
        component_type: Optional[DType] = None,
        column_type: ColumnType = ColumnType.NORMAL,
        input_data: Optional[Any] = None,
    ):
        try:
            self._column_definition = col_def(name, data_type, component_type, column_type)
            self.j_column = self._to_j_column(input_data)
        except Exception as e:
            raise DHError(e, f"failed to create an InputColumn ({name}).") from e

    def _to_j_column(self, input_data: Optional[Any] = None):
        data_type = self._column_definition.data_type
        # the engine's name of the type, or the refusal of a type it cannot hold, before
        # the values: an array of a class this server has no values of is refused too
        self._engine_type = dtypes._engine_type(data_type)
        if input_data is None:
            values = dtypes.array(data_type, [])
        elif data_type.is_primitive:
            values = dtypes.array(data_type, input_data, remap=dtypes.null_remap(data_type))
        else:
            values = dtypes.array(data_type, input_data)
        # Behavior: `io.deephaven.qst.column.header.ColumnHeader#of`, which checks the name
        _engine.native.validate_column_name(self._column_definition.name)
        return values


def bool_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of Booleans; `None` is null."""
    return InputColumn(name=name, data_type=dtypes.bool_, input_data=data)


def byte_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of bytes; `None` and `NULL_BYTE` are null."""
    return InputColumn(name=name, data_type=dtypes.byte, input_data=data)


def char_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of chars from code points or from one string; `None` and
    `NULL_CHAR` are null."""
    return InputColumn(name=name, data_type=dtypes.char, input_data=data)


def short_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 16-bit integers; `None` and `NULL_SHORT` are null."""
    return InputColumn(name=name, data_type=dtypes.short, input_data=data)


def int_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 32-bit integers; `None` and `NULL_INT` are null."""
    return InputColumn(name=name, data_type=dtypes.int32, input_data=data)


def long_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 64-bit integers; `None` and `NULL_LONG` are null."""
    return InputColumn(name=name, data_type=dtypes.long, input_data=data)


def float_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 32-bit floating point numbers; `None` and `NULL_FLOAT`
    are null, a NaN is a NaN."""
    return InputColumn(name=name, data_type=dtypes.float32, input_data=data)


def double_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of 64-bit floating point numbers; `None` and `NULL_DOUBLE`
    are null, a NaN is a NaN."""
    return InputColumn(name=name, data_type=dtypes.double, input_data=data)


def string_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of strings; `None` is null."""
    return InputColumn(name=name, data_type=dtypes.string, input_data=data)


def datetime_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column of instants from instants, integers of nanoseconds since
    the epoch, strings, `datetime`s, `numpy.datetime64`s or `pandas.Timestamp`s; `None` is
    null."""
    data = _instant_array(data)
    return InputColumn(name=name, data_type=dtypes.Instant, input_data=data)


def pyobj_col(name: str, data: Sequence) -> InputColumn:
    """Refused: the engine has no column type for Python objects."""
    return InputColumn(name=name, data_type=dtypes.PyObject, input_data=data)


def jobj_col(name: str, data: Sequence) -> InputColumn:
    """Refused: the engine has no column type for Java objects."""
    return InputColumn(name=name, data_type=dtypes.JObject, input_data=data)
