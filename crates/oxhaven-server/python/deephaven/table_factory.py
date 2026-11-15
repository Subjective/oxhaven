"""Functions that create tables."""

from typing import Mapping, Sequence, Union

from . import _engine, dtypes
from .column import InputColumn
from .dherror import DHError
from .table import Table, TableDefinition


def empty_table(size: int) -> Table:
    """Creates a table with rows but no columns."""
    try:
        return Table(_engine.table_op("empty_table", {"size": int(size)}))
    except Exception as e:
        raise DHError(e, "failed to create an empty table.") from e


def _nanos_of_duration(period) -> int:
    """The nanoseconds of a duration given as an integer, a `datetime.timedelta`, a
    `numpy.timedelta64` or a `pandas.Timedelta`.

    Behavior: `deephaven.time.to_j_duration`, for everything but a string
    """
    import datetime
    import sys

    if isinstance(period, int) and not isinstance(period, bool):
        return period
    pandas, numpy = sys.modules.get("pandas"), sys.modules.get("numpy")
    if pandas is not None and isinstance(period, pandas.Timedelta):
        return period.value
    if isinstance(period, datetime.timedelta):
        return int(period / datetime.timedelta(microseconds=1) * 1000)
    if numpy is not None and isinstance(period, numpy.timedelta64):
        return int(period.astype("timedelta64[ns]").astype(numpy.int64))
    raise TypeError("Unsupported conversion: " + str(type(period)) + " -> Duration")


def _nanos_of_instant(instant) -> int:
    """The nanoseconds since the epoch of an instant given as an integer, a
    `datetime.datetime` (a `pandas.Timestamp` is one) or a `numpy.datetime64`.

    Behavior: `deephaven.time.to_j_instant`, for everything but a string
    """
    import datetime
    import sys

    if isinstance(instant, int) and not isinstance(instant, bool):
        return instant
    numpy = sys.modules.get("numpy")
    if isinstance(instant, datetime.datetime):
        # the reference's arithmetic, in floating point as there
        epoch_time = instant.timestamp()
        seconds = int(epoch_time)
        return seconds * 1000000000 + int((epoch_time - seconds) * 1000000000)
    if numpy is not None and isinstance(instant, numpy.datetime64):
        return int(instant.astype("datetime64[ns]").astype(numpy.int64))
    raise TypeError("Unsupported conversion: " + str(type(instant)) + " -> Instant")


def _is_null(value) -> bool:
    """Whether `value` is what `pandas.isnull` calls null: `None`, a NaN, a not-a-time."""
    import sys

    if value is None:
        return True
    pandas = sys.modules.get("pandas")
    if pandas is not None:
        # a boolean for one value, an array for a collection
        null = pandas.isnull(value)
        return bool(null) if getattr(null, "ndim", 0) == 0 else False
    return isinstance(value, float) and value != value


def time_table(period, start_time=None, blink_table: bool = False) -> Table:
    """Creates a table that adds a new row on a regular interval.

    `period` is an integer in nanoseconds, a string such as `"PT1s"`, a
    `datetime.timedelta`, a `numpy.timedelta64` or a `pandas.Timedelta`. `start_time` is an
    integer in nanoseconds since the epoch, a string such as `"2024-01-01T00:00:00 ET"`, a
    `datetime.datetime`, a `numpy.datetime64` or a `pandas.Timestamp`; without it the table
    starts now.
    """
    try:
        if period is None:
            raise ValueError("period must be specified")
        if _is_null(period):
            # the reference hands Java a null duration, which it refuses
            raise ValueError("period must not be null")
        request = {"blink_table": bool(blink_table)}
        if isinstance(period, str):
            request["period_string"] = period
        else:
            request["period_nanos"] = _nanos_of_duration(period)
        if start_time:
            if isinstance(start_time, str):
                request["start_time_string"] = start_time
            elif not _is_null(start_time):
                request["start_time_nanos"] = _nanos_of_instant(start_time)
        return Table(_engine.table_op("time_table", request))
    except Exception as e:
        raise DHError(e, "failed to create a time table.") from e


def _engine_values(data_type, values) -> list:
    """The values of an input column as the engine takes them: an instant as its
    nanoseconds since the epoch, in an array cell too."""
    name = data_type.j_name
    if name == "java.time.Instant":
        return [None if value is None else value.epoch_nanos() for value in values]
    component = dtypes._component_name(name)
    if component is not None:
        component = dtypes._from_java_name(component)
        return [None if value is None else _engine_values(component, value) for value in values]
    return list(values)


def new_table(cols: Union[Sequence[InputColumn], Mapping[str, Sequence]]) -> Table:
    """Creates an in-memory table from a list of input columns, or from a mapping of
    column names to values, whose column types are those pandas infers.

    Each column has the same number of values. Of two input columns with one name the
    later one replaces the earlier in its place, as in the reference.
    """
    try:
        if isinstance(cols, Mapping):
            import pandas as pd

            from .pandas import to_table

            return to_table(pd.DataFrame(cols).convert_dtypes())
        columns = {}
        for col in cols:
            # the reference reads `j_column` of whatever it is given
            values = col.j_column
            definition = col._column_definition
            columns[definition.name] = (
                definition.name,
                col._engine_type,
                _engine_values(definition.data_type, values),
            )
        return Table(_engine.new_table(list(columns.values())))
    except Exception as e:
        # the text is the reference's, "time" included
        raise DHError(e, "failed to create a new time table.") from e


def merge(tables: Sequence[Table]) -> Table:
    """Creates a table of the rows of the tables, which have the same columns, one table
    after another."""
    try:
        handles = [table.j_table for table in tables]
        request = {"source_ids": [_engine.source(index) for index in range(len(handles))]}
        return Table(_engine.table_op("merge", request, handles))
    except Exception as e:
        raise DHError(e, "merge tables operation failed.") from e


def merge_sorted(tables: Sequence[Table], order_by: str) -> Table:
    """Creates a table of the rows of the tables, each of which is sorted by the column
    `order_by`, in the order of that column."""
    try:
        handles = [table.j_table for table in tables]
        # the table service's `merge` with a key column sorts a merge of refreshing tables
        # and takes at least one table; Java's `TableTools.mergeSorted` does neither
        return Table(_engine.local_op("merge_sorted", {"order_by": order_by}, handles))
    except Exception as e:
        raise DHError(e, "merge sorted operation failed.") from e


def _empty_table_of(col_defs) -> "_engine.native.TableHandle":
    """The handle of a static table without rows that has the columns of `col_defs`."""
    definition = TableDefinition(col_defs)
    return _engine.new_table(
        [(column.name, dtypes._engine_type(column.data_type), []) for column in definition.values()]
    )


# Java's integer types: bits, for the narrowing a Java cast makes.
_INTEGER_BITS = {"byte": 8, "short": 16, "int": 32, "long": 64}


def _class_cast(value, wanted: str) -> RuntimeError:
    # jpy hands Java a Python bool as Boolean, an int as Integer or Long, a float as Double
    # and a str as String
    given = {bool: "java.lang.Boolean", float: "java.lang.Double", str: "java.lang.String"}
    name = given.get(type(value))
    if name is None and isinstance(value, int):
        name = "java.lang.Integer" if -(2**31) <= value < 2**31 else "java.lang.Long"
    return RuntimeError(
        f"java.lang.ClassCastException: class {name or type(value).__name__} cannot be cast "
        f"to class {wanted}"
    )


def _row_value(data_type, value):
    """One value of a row a `DynamicTableWriter` is given, as the engine takes it for a
    column of `data_type`.

    Behavior: the setters of `io.deephaven.engine.table.impl.util.DynamicTableWriter`:
    a numeric column takes any number through `Number#intValue` and the like, a column of
    Booleans, characters or instants casts the value to its class, and a column of strings
    takes any object unchecked, which then reads as the text Java's `toString` gives it
    """
    if value is None:
        return None
    name = data_type.j_name
    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if name in _INTEGER_BITS:
        if not is_number:
            raise _class_cast(value, "java.lang.Number")
        bits = _INTEGER_BITS[name]
        if isinstance(value, float):
            # Java's cast of a double: toward zero, NaN is 0, out of range is the nearest
            # long (for `long`) or int (for the narrower types, which then drop bits)
            limit = 64 if name == "long" else 32
            low, high = -(2 ** (limit - 1)), 2 ** (limit - 1) - 1
            value = 0 if value != value else int(max(low, min(high, value)))
        value &= 2**bits - 1
        return value - 2**bits if value >= 2 ** (bits - 1) else value
    if name in ("float", "double"):
        if not is_number:
            raise _class_cast(value, "java.lang.Number")
        return dtypes.array(data_type, [float(value)])[0]
    if name == "java.lang.Boolean":
        if not isinstance(value, bool):
            raise _class_cast(value, "java.lang.Boolean")
        return value
    if name == "java.lang.String":
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int):
            return str(value)
        # a float, whose text is that of Java's `Double#toString`, or another object
        raise dtypes._java_time.NotPorted(
            "io.deephaven.engine.table.impl.util.DynamicTableWriter#logRowPermissive",
            f"a {type(value).__name__} for a column of strings",
        )
    if name == "char":
        # nothing Python hands over is a Java Character
        raise _class_cast(value, "java.lang.Character")
    if name == "java.time.Instant":
        if not isinstance(value, dtypes._java_time.Instant):
            raise _class_cast(value, "java.time.Instant")
        return value.epoch_nanos()
    raise dtypes._java_time.NotPorted(
        "io.deephaven.engine.table.impl.util.DynamicTableWriter#logRowPermissive",
        f"a value for a column of type {name}",
    )


class DynamicTableWriter:
    """Writes rows to a new in-memory table, which is `table`. A row is in the table after
    the next update cycle, which for a row written by a console command is after the
    command. The table only grows.

    Behavior: `deephaven.table_factory.DynamicTableWriter`
    """

    def __init__(self, col_defs: Mapping):
        try:
            self._data_types = list(col_defs.values())
            source = _empty_table_of(dict(col_defs))
            self._j_table_writer, handle = _engine.native.table_writer(source)
            self.table = Table(handle)
        except Exception as e:
            raise DHError(e, "failed to create a DynamicTableWriter.") from e

    @property
    def j_object(self):
        """The engine's writer. In the reference package this is the Java writer."""
        return self._j_table_writer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        """Closes the writer. The table keeps its rows, and as in the reference rows can
        still be written."""

    def write_row(self, *values) -> None:
        """Writes one row: a value for each column, of a type that converts to the
        column's, with loss if need be. A row with a value that does not convert, or with
        the wrong number of values, is not written."""
        try:
            values = _engine.to_sequence(values)
            if len(values) == len(self._data_types):
                values = [_row_value(t, v) for t, v in zip(self._data_types, values)]
            self._j_table_writer.write_row(list(values))
        except Exception as e:
            raise DHError(e, "failed to write a row.") from e


def _print_async_error(e: Exception) -> None:
    print(f"An error occurred during InputTable async operation: {e}")


class InputTable(Table):
    """A table that rows are added to, and for a keyed one changed in and deleted from, by
    the methods here. Create one with `input_table`.

    Behavior: `deephaven.table_factory.InputTable`
    """

    def __init__(self, j_table):
        super().__init__(j_table)
        try:
            names = _engine.native.input_table_names(self.j_table)
        except Exception as e:
            raise DHError(e, "the provided table input is not suitable for input tables.") from e
        if names is None:
            raise DHError("the provided table input is not suitable for input tables.")
        self._key_names, self._value_names = names
        # In the reference this is the Java `InputTableUpdater`; here the table it writes to.
        self.j_input_table = self.j_table

    def add(self, table: Table) -> None:
        """Adds the rows of `table`, which has the columns of this table, and returns when
        they are in this table. In a keyed input table a row replaces the row with the same
        key."""
        try:
            _engine.native.input_table_change(self.j_input_table, table.j_table, False)
        except Exception as e:
            raise DHError(e, "add to InputTable failed.") from e

    def delete(self, table: Table) -> None:
        """Deletes the rows whose keys are the rows of `table`, which has the key columns
        of this table, and returns when they are gone."""
        try:
            _engine.native.input_table_change(self.j_input_table, table.j_table, True)
        except Exception as e:
            raise DHError(e, "delete data in the InputTable failed.") from e

    def _change_async(self, table: Table, delete: bool, on_success, on_error) -> None:
        report = on_error or _print_async_error

        def failed(text: str) -> None:
            report(RuntimeError(text))

        _engine.native.input_table_change_async(
            self.j_input_table, table.j_table, delete, on_success, failed
        )

    def add_async(self, table: Table, on_success=None, on_error=None) -> None:
        """As `add`, without waiting. The table is checked and its rows are read now; they
        are added later, in the order of the calls made on one thread. Then `on_success` is
        called without arguments, or `on_error` with the exception; without `on_error` the
        exception is printed. An exception a callback raises is logged by the server."""
        try:
            self._change_async(table, False, on_success, on_error)
        except Exception as e:
            raise DHError(e, "async add to InputTable failed.") from e

    def delete_async(self, table: Table, on_success=None, on_error=None) -> None:
        """As `delete`, without waiting; see `add_async`."""
        try:
            self._change_async(table, True, on_success, on_error)
        except Exception as e:
            raise DHError(e, "async delete data in the InputTable failed.") from e

    @property
    def key_names(self) -> list:
        """The names of the key columns; none for an append-only input table."""
        return list(self._key_names)

    @property
    def value_names(self) -> list:
        """The names of the columns that are not key columns."""
        return list(self._value_names)


def input_table(col_defs=None, init_table: Table = None, key_cols=None) -> InputTable:
    """Creates an in-memory input table from column definitions, or from an initial table,
    whose columns and rows it starts with. With key columns it is keyed: a row has a key,
    and rows are added, replaced and deleted by key. Without, it is append-only."""
    try:
        if col_defs is None and init_table is None:
            raise ValueError("either column definitions or init table should be provided.")
        elif col_defs and init_table:
            raise ValueError("both column definitions and init table are provided.")

        source = init_table.j_table if init_table else _empty_table_of(col_defs)
        key_cols = _engine.to_sequence(key_cols)
        if key_cols:
            kind = {"in_memory_key_backed": {"key_columns": list(key_cols)}}
        else:
            kind = {"in_memory_append_only": {}}
        request = {"source_table_id": _engine.source(0), "kind": kind}
        result = InputTable(_engine.table_op("create_input_table", request, [source]))
        if init_table and init_table.size > 0:
            # Java's factory fills the table as it makes it; here the rows are the first
            # change, which is made before the table is handed out
            _engine.native.input_table_change(result.j_table, source, False)
        return result
    except Exception as e:
        raise DHError(e, "failed to create an in-memory InputTable.") from e


def ring_table(parent: Table, capacity: int, initialize: bool = True) -> Table:
    """Not ported: the engine has no ring table. The reference creates a table that keeps
    the latest `capacity` rows of `parent`."""
    try:
        raise dtypes._java_time.NotPorted(
            "io.deephaven.engine.table.impl.sources.ring.RingTableTools#of"
        )
    except Exception as e:
        raise DHError(e, "failed to create a ring table.") from e


def function_generated_table(
    table_generator,
    source_tables=None,
    refresh_interval_ms: int = None,
    exec_ctx=None,
    args: tuple = (),
    kwargs: dict = {},
) -> Table:
    """Not ported: the engine has no function generated table. The reference creates a
    table that `table_generator` makes anew when a source table changes or an interval has
    passed."""
    if refresh_interval_ms is None and source_tables is None:
        raise DHError("Either refresh_interval_ms or source_tables must be provided!")
    if refresh_interval_ms is not None and source_tables is not None:
        raise DHError("Only one of refresh_interval_ms and source_tables must be provided!")
    try:
        raise dtypes._java_time.NotPorted(
            "io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory#create"
        )
    except Exception as e:
        raise DHError(e, "failed to create a function generated table.") from e
