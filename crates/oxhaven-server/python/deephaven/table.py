"""The `Table` class."""

import decimal
import json
import struct
from collections import namedtuple
from collections.abc import Iterable, Mapping
from enum import Enum, auto
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union

import _oxhaven
import _oxhaven_bootstrap

from . import _engine, dtypes, liveness_scope
from .agg import Aggregation
from .column import ColumnDefinition, ColumnType, col_def
from .dherror import DHError
from .update_graph import UpdateGraph


class SortDirection(Enum):
    """The direction of a sort on one column.

    Behavior: `deephaven.table.SortDirection`
    """

    DESCENDING = auto()
    ASCENDING = auto()


class NaturalJoinType(Enum):
    """What a natural join does with a row of the left table that several rows of the
    right table match. The values are the names of the table service's join types.

    Behavior: `deephaven.table.NaturalJoinType`
    """

    ERROR_ON_DUPLICATE = "ERROR_ON_DUPLICATE"
    FIRST_MATCH = "FIRST_MATCH"
    LAST_MATCH = "LAST_MATCH"
    EXACTLY_ONE_MATCH = "EXACTLY_ONE_MATCH"


class NewColumnBehaviorType(Enum):
    """What `keyed_transpose` of a ticking table does when a new column value appears.

    Behavior: `deephaven.table.NewColumnBehaviorType`
    """

    FAIL = "FAIL"
    IGNORE = "IGNORE"


class TableDefinition(Mapping):
    """The columns of a table: a mapping from column name to column definition, in column
    order.

    It is made from another table definition, from a mapping of column names to data
    types, or from column definitions (`deephaven.column.col_def`); a table makes its own
    from its handle, where the reference takes the Java table definition.

    Behavior: `deephaven.table.TableDefinition`
    """

    def __init__(self, table_definition):
        self._handle = None
        if isinstance(table_definition, TableDefinition):
            columns = list(table_definition.values())
        elif isinstance(table_definition, _oxhaven.TableHandle):
            self._handle = table_definition
            columns = [
                ColumnDefinition(
                    name,
                    dtypes._from_java_name(type_name),
                    dtypes._from_java_name(component),
                    ColumnType.PARTITIONING if partitioning else ColumnType.NORMAL,
                )
                for name, type_name, component, partitioning in _engine.native.column_definitions(
                    table_definition
                )
            ]
        elif isinstance(table_definition, Mapping):
            for name in table_definition.keys():
                if not isinstance(name, str):
                    raise DHError(
                        f"Expected TableDefinitionLike Mapping to contain str keys, found type {type(name)}"
                    )
            for data_type in table_definition.values():
                if not isinstance(data_type, dtypes.DType):
                    raise DHError(
                        "Expected TableDefinitionLike Mapping to contain DType values, found type "
                        f"{type(data_type)}"
                    )
            columns = [col_def(name, data_type) for name, data_type in table_definition.items()]
        elif isinstance(table_definition, Iterable):
            columns = list(table_definition)
            for column in columns:
                if not isinstance(column, ColumnDefinition):
                    raise DHError(
                        "Expected TableDefinitionLike Iterable to contain ColumnDefinition values, "
                        f"found type {type(column)}"
                    )
        else:
            raise DHError(f"Unexpected TableDefinitionLike type: {type(table_definition)}")
        names = [column.name for column in columns]
        duplicates = sorted({name for name in names if names.count(name) > 1}, key=names.index)
        if duplicates:
            # Behavior: `io.deephaven.engine.table.TableDefinition#checkDuplicateNames`
            raise RuntimeError(
                "java.lang.IllegalArgumentException: Supplied ColumnDefinitions include "
                f"duplicate names [{', '.join(duplicates)}]"
            )
        self._dict = dict(zip(names, columns))

    @property
    def j_object(self):
        """The reference's Java table definition; here the definition itself."""
        return self

    @property
    def table(self) -> "Table":
        """A table that describes the columns, one row each.

        Behavior: `io.deephaven.engine.util.TableTools#metaTable`
        """
        handle = self._handle
        if handle is None:
            # the meta table of a table without rows that has these columns
            columns = [
                (
                    column.name,
                    dtypes._engine_type(column.data_type),
                    column.column_type is ColumnType.PARTITIONING,
                )
                for column in self.values()
            ]
            handle = _engine.native.defined_table(columns)
        return Table(_engine.table_op("meta_table", _engine.sourced({}), [handle]))

    def keys(self):
        """The column names, as a view of a dict."""
        return self._dict.keys()

    def items(self):
        """The pairs of column name and column definition, as a view of a dict."""
        return self._dict.items()

    def values(self):
        """The column definitions, as a view of a dict."""
        return self._dict.values()

    def __getitem__(self, key) -> ColumnDefinition:
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __contains__(self, item: Any) -> bool:
        return item in self._dict

    def __eq__(self, other):
        return isinstance(other, TableDefinition) and list(self.values()) == list(other.values())

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(tuple(self.values()))

    def __str__(self):
        # Behavior: `io.deephaven.engine.table.TableDefinition#toString`
        return f"TableDefinition {{columns=[{', '.join(map(str, self.values()))}]}}"


# How a numpy array is made of the values the engine hands out, by Java type name.
# Behavior: `deephaven.jcompat._j_array_to_numpy_array` without null conversion
_NUMPY_TYPES = {
    "byte": "int8",
    "short": "int16",
    "int": "int32",
    "long": "int64",
    "float": "float32",
    "double": "float64",
    "char": "uint16",
}


def _numpy_array(type_name: str, values: list):
    import numpy as np

    if type_name in _NUMPY_TYPES:
        return np.array(values, dtype=_NUMPY_TYPES[type_name])
    if type_name == "java.lang.Boolean":
        # the reference reads the Booleans as bytes, a null as -1, and views them as bool
        return np.array([True if v is None else v for v in values], dtype=np.bool_)
    if type_name == "java.lang.String":
        return np.array(values, dtype=np.str_)
    if type_name == "java.time.Instant":
        return np.array(values, dtype=np.int64).view("datetime64[ns]")
    return np.array(values, dtype=np.object_)


def _share(value):
    """A share of a table's rows as the arguments of a local operation carry it: JSON has
    no NaN and no infinity, which go as the text Python prints for them."""
    if isinstance(value, float) and (value != value or abs(value) == float("inf")):
        return repr(value)
    return value


class Table:
    """A Deephaven table: a handle to a table of the engine.

    Every method that returns a table creates a new one; a table never changes because of
    an operation on it.

    Behavior: `deephaven.table.Table`
    """

    def __init__(self, j_table):
        if not isinstance(j_table, _oxhaven.TableHandle):
            raise DHError(message=f"not a table handle: {type(j_table).__name__}")
        self._j_table = j_table
        self._definition = TableDefinition(j_table)
        liveness_scope._created(self)

    @property
    def j_table(self):
        """The engine's handle; none once a liveness scope has released the table."""
        if self._j_table is None:
            raise DHError(message=liveness_scope._RELEASED)
        return self._j_table

    @property
    def j_object(self):
        """The engine's handle. In the reference package this is the Java table."""
        return self.j_table

    def _liveness_release(self) -> bool:
        """Gives up this object's hold of the engine table, unless something else holds the
        table too, which keeps it live; returns whether the table is released
        (`deephaven.liveness_scope`)."""
        if self._j_table is not None:
            if self._j_table.holders() > 1:
                return False
            self._definition = TableDefinition(self._definition)
            self._j_table = None
        return True

    @property
    def update_graph(self) -> UpdateGraph:
        """The update graph of the table: the server's."""
        return UpdateGraph()

    def await_update(self, timeout: Optional[int] = None) -> bool:
        """Waits until this refreshing table changes, or for `timeout` milliseconds;
        returns whether it changed."""
        if not self.is_refreshing:
            raise DHError(message="await_update can only be called on refreshing tables.")
        try:
            return _engine.native.await_update(self.j_table, timeout)
        except Exception as e:
            raise DHError(e, "await_update was interrupted.") from e

    def __repr__(self):
        # the shape of the reference's text; the object reference is this handle's
        types = {col.name: col.data_type for col in self.columns[:10]}
        text = (
            "deephaven.table.Table(io.deephaven.engine.table.Table("
            f"objectRef=0x{id(self.j_table):x}, "
            f"num_rows = {self.size}, columns = {types}"
        )
        return text[:114] + "...}))" if len(text) > 118 else text + "))"

    def __str__(self):
        return repr(self)

    # Properties and inspection

    @property
    def size(self) -> int:
        """The current number of rows in the table."""
        return self.j_table.size()

    @property
    def is_refreshing(self) -> bool:
        """Whether this table is refreshing."""
        return self.j_table.is_refreshing()

    @property
    def is_failed(self) -> bool:
        """Whether this table failed in an update cycle: it does not change any more."""
        return self.j_table.is_failed()

    @property
    def is_blink(self) -> bool:
        """Whether this table is a blink table: its rows last one update cycle."""
        return _engine.native.is_blink(self.j_table)

    @property
    def is_flat(self) -> bool:
        """Whether the row keys of this table are its row positions."""
        return _engine.native.is_flat(self.j_table)

    @property
    def definition(self) -> TableDefinition:
        """The columns of the table, by name."""
        return self._definition

    @property
    def column_names(self) -> List[str]:
        """The column names of the table."""
        return list(self.definition.keys())

    @property
    def columns(self) -> List[ColumnDefinition]:
        """The column definitions of the table."""
        return list(self.definition.values())

    @property
    def meta_table(self) -> "Table":
        """A table that describes the columns of this one, one row each."""
        return Table(_engine.table_op("meta_table", _engine.sourced({}), [self.j_table]))

    def has_columns(self, cols: Union[str, Sequence[str]]) -> bool:
        """Whether the table has every one of the columns."""
        return all(col in self._definition for col in _engine.to_sequence(cols))

    def attributes(self) -> Dict[str, Any]:
        """The attributes of the table by name. An attribute whose value is an object of
        the Java engine has a placeholder for its value."""
        try:
            return _engine.native.attributes(self.j_table)
        except Exception as e:
            raise DHError(e, "failed to get the attributes of the table.") from e

    def to_string(
        self, num_rows: int = 10, cols: Optional[Union[str, Sequence[str]]] = None
    ) -> str:
        """The first `num_rows` rows of the table as text, with `|` between the columns."""
        try:
            return _engine.native.to_string(self.j_table, num_rows, list(_engine.to_sequence(cols)))
        except Exception as e:
            raise DHError(e, "table to_string failed") from e

    def _chunks(self, cols, chunk_size: int, instants: bool) -> Generator[list, None, None]:
        """The values of the table `chunk_size` rows at a time: per chunk, one
        `(name, values)` per column that `cols` names, in the table's column order.

        Behavior: `deephaven._table_reader._table_reader_chunk`. The reference holds the
        update graph's shared lock while the generator lives; here each chunk is read under
        the lock, and a command holds it throughout.
        """
        if chunk_size < 0:
            raise ValueError("chunk_size must not be negative.")
        names = list(_engine.to_sequence(cols)) if cols else []
        invalid = set(names) - set(self._definition)
        if invalid or len(set(names)) != len(names):
            raise ValueError(f"Invalid column names: {invalid}")
        first = 0
        while True:
            try:
                chunk = _engine.native.read_columns(
                    self.j_table, names, first, chunk_size, instants
                )
            except Exception as e:
                raise DHError(e, "failed to read the table.") from e
            read = len(chunk[0][1]) if chunk else 0
            if read == 0:
                return
            yield chunk
            first += read

    def iter_dict(
        self, cols: Optional[Union[str, Sequence[str]]] = None, *, chunk_size: int = 2048
    ) -> Generator[Dict[str, Any], None, None]:
        """A generator of the rows, each a dict from column name to value."""
        for chunk in self._chunks(cols, chunk_size, False):
            names = [name for name, _ in chunk]
            for row in zip(*(values for _, values in chunk)):
                yield dict(zip(names, row))

    def iter_tuple(
        self,
        cols: Optional[Union[str, Sequence[str]]] = None,
        *,
        tuple_name: str = "Deephaven",
        chunk_size: int = 2048,
    ) -> Generator[Tuple[Any, ...], None, None]:
        """A generator of the rows, each a named tuple of the column values."""
        # as in the reference, the fields are named by `cols` as given and filled in the
        # table's column order
        row_type = namedtuple(tuple_name, cols or self.column_names, rename=False)

        def rows():
            for chunk in self._chunks(cols, chunk_size, False):
                yield from map(row_type._make, zip(*(values for _, values in chunk)))

        return rows()

    def _numpy_chunks(self, cols, chunk_size: int) -> Generator[list, None, None]:
        types = {col.name: col.data_type.j_name for col in self.columns}
        for chunk in self._chunks(cols, chunk_size, True):
            yield [(name, _numpy_array(types[name], values)) for name, values in chunk]

    def iter_chunk_dict(
        self, cols: Optional[Union[str, Sequence[str]]] = None, chunk_size: int = 2048
    ) -> Generator[Dict[str, Any], None, None]:
        """A generator of chunks of rows, each a dict from column name to numpy array."""
        for chunk in self._numpy_chunks(cols, chunk_size):
            yield dict(chunk)

    def iter_chunk_tuple(
        self,
        cols: Optional[Union[str, Sequence[str]]] = None,
        tuple_name: str = "Deephaven",
        chunk_size: int = 2048,
    ) -> Generator[Tuple[Any, ...], None, None]:
        """A generator of chunks of rows, each a named tuple of numpy arrays."""
        chunk_type = namedtuple(tuple_name, cols or self.column_names, rename=False)

        def chunks():
            for chunk in self._numpy_chunks(cols, chunk_size):
                yield chunk_type._make([values for _, values in chunk])

        return chunks()

    # Operations

    def _op(self, name: str, request: dict, failure: str, others: Sequence["Table"] = ()) -> "Table":
        """The result of the table service operation `name` on this table, which is the
        operation's first source, and `others`."""
        try:
            sources = [self.j_table] + [other.j_table for other in others]
            return Table(_engine.table_op(name, _engine.sourced(request), sources))
        except Exception as e:
            raise DHError(e, failure) from e

    def _scoped_op(self, name: str, request: dict, failure: str) -> "Table":
        """`_op` for an operation Java makes filters or formulas for whatever its
        arguments are, which it cannot where there is no query scope: in the function of
        a partitioned table's transform (`PartitionedTable.transform`)."""
        try:
            _engine.native.require_query_scope()
        except Exception as e:
            raise DHError(e, failure) from e
        return self._op(name, request, failure)

    def _local_op(self, name: str, args: dict, failure: str) -> "Table":
        """The result of the local operation `name` (`console/python/ops.rs`)."""
        try:
            return Table(_engine.local_op(name, args, [self.j_table]))
        except Exception as e:
            raise DHError(e, failure) from e

    def _positions(self, failure: str, *values) -> None:
        """Raises unless every value is an `int`: the reference hands row counts and
        positions to Java, which takes nothing else for a `long`."""
        for value in values:
            if not isinstance(value, int):
                error = TypeError(f"a row count or position must be an int, not {type(value).__name__}")
                raise DHError(error, failure) from error

    def _select_op(self, name: str, formulas, failure: str) -> "Table":
        return self._op(name, {"column_specs": _engine.to_sequence(formulas)}, failure)

    def coalesce(self) -> "Table":
        """This table with its work done: every table here is coalesced already.

        Behavior: `io.deephaven.engine.table.impl.QueryTable#coalesce`, which returns the
        table itself
        """
        return Table(self.j_table)

    def flatten(self) -> "Table":
        """A new table whose row keys are its row positions."""
        return Table(_engine.table_op("flatten", _engine.sourced({}), [self.j_table]))

    def snapshot(self) -> "Table":
        """A static copy of the table as it is now."""
        try:
            return Table(_engine.table_op("snapshot", _engine.sourced({}), [self.j_table]))
        except Exception as e:
            raise DHError(message="failed to create a snapshot.") from e

    def snapshot_when(
        self,
        trigger_table: "Table",
        stamp_cols: Optional[Union[str, Sequence[str]]] = None,
        initial: bool = False,
        incremental: bool = False,
        history: bool = False,
    ) -> "Table":
        """A table that holds a copy of this one taken each time `trigger_table` changes,
        with the stamp columns of the trigger table added."""
        try:
            request = {
                "base_id": _engine.source(0),
                "trigger_id": _engine.source(1),
                "initial": bool(initial),
                "incremental": bool(incremental),
                "history": bool(history),
                "stamp_columns": _engine.to_sequence(stamp_cols),
            }
            handles = [self.j_table, trigger_table.j_table]
            return Table(_engine.table_op("snapshot_when", request, handles))
        except Exception as e:
            raise DHError(message="failed to create a snapshot_when table.") from e

    def drop_columns(self, cols: Union[str, Sequence[str]]) -> "Table":
        """A new table without the columns."""
        failure = "table drop_columns operation failed."
        cols = _engine.to_sequence(cols)
        if not cols:
            # the reference fails here: its call of `dropColumns` without a column matches
            # two Java overloads
            error = RuntimeError("drop_columns needs at least one column")
            raise DHError(error, failure) from error
        return self._op("drop_columns", {"column_names": cols}, failure)

    def move_columns(self, idx: int, cols: Union[str, Sequence[str]]) -> "Table":
        """A new table with the columns moved to start at position `idx`."""
        return self._local_op(
            "move_columns",
            {"idx": idx, "cols": _engine.to_sequence(cols)},
            "table move_columns operation failed.",
        )

    def move_columns_down(self, cols: Union[str, Sequence[str]]) -> "Table":
        """A new table with the columns moved to the end."""
        return self._local_op(
            "move_columns_down",
            {"cols": _engine.to_sequence(cols)},
            "table move_columns_down operation failed.",
        )

    def move_columns_up(self, cols: Union[str, Sequence[str]]) -> "Table":
        """A new table with the columns moved to the front."""
        return self._local_op(
            "move_columns_up",
            {"cols": _engine.to_sequence(cols)},
            "table move_columns_up operation failed.",
        )

    def rename_columns(self, cols: Union[str, Sequence[str]]) -> "Table":
        """A new table with columns renamed, each by `"NewName = OldName"`."""
        return self._local_op(
            "rename_columns",
            {"cols": _engine.to_sequence(cols)},
            "table rename_columns operation failed.",
        )

    def update(self, formulas: Union[str, Sequence[str]]) -> "Table":
        """A new table with the columns of this one and one column per formula, computed
        once and kept in memory."""
        try:
            formulas = _engine.to_sequence(formulas)
            formulas[0]  # the reference looks at the first formula, so none is an error
        except Exception as e:
            raise DHError(e, "table update operation failed.") from e
        return self._select_op("update", formulas, "table update operation failed.")

    def lazy_update(self, formulas: Union[str, Sequence[str]]) -> "Table":
        """As `update`, with each formula computed on first use and cached."""
        return self._select_op("lazy_update", formulas, "table lazy_update operation failed.")

    def view(self, formulas: Union[str, Sequence[str]]) -> "Table":
        """A new table with one column per formula, computed when read."""
        return self._select_op("view", formulas, "table view operation failed.")

    def update_view(self, formulas: Union[str, Sequence[str]]) -> "Table":
        """A new table with the columns of this one and one column per formula, computed
        when read."""
        return self._select_op("update_view", formulas, "table update_view operation failed.")

    def format_columns(self, formulas: Union[str, Sequence[str]]) -> "Table":
        """This table with the formats `formulas`, each `Column = format`: a colour or a
        style for the cells of the column (`*` for the whole row), `Decimal(pattern)` for
        its numbers or `Date(pattern)` for its instants. A format is a hidden column,
        computed when read, that a client reads the format of each cell from.

        Behavior: `deephaven.table.Table.format_columns`
        """
        failure = "failed to color format columns."
        try:
            formulas = _engine.to_sequence(formulas)
        except Exception as e:
            raise DHError(e, failure) from e
        return self._local_op("format_columns", {"formats": formulas}, failure)

    def format_column_where(self, col: str, cond: str, formula: str) -> "Table":
        """This table with the format `formula` of `col` in the rows where `cond` holds.

        Behavior: `deephaven.table.Table.format_column_where`
        """
        return self._local_op(
            "format_column_where",
            {"col": col, "cond": cond, "formula": formula},
            "failed to color format column conditionally.",
        )

    def format_row_where(self, cond: str, formula: str) -> "Table":
        """This table with the format `formula` of the rows where `cond` holds.

        Behavior: `deephaven.table.Table.format_row_where`
        """
        return self._local_op(
            "format_column_where",
            {"col": "*", "cond": cond, "formula": formula},
            "failed to color format rows conditionally.",
        )

    def select(self, formulas: Union[str, Sequence[str], None] = None) -> "Table":
        """A new in-memory table with one column per formula, or with every column of this
        table if no formula is given."""
        return self._select_op("select", formulas, "table select operation failed.")

    def select_distinct(self, formulas: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the distinct rows of the columns or formulas, or of every
        column if none is given."""
        failure = "table select_distinct operation failed."
        formulas = _engine.to_sequence(formulas)
        source = self
        if any(isinstance(formula, str) and "=" in formula for formula in formulas):
            # Behavior: `QueryTable#selectDistinct`, which is `view(columns).selectDistinct()`
            # unless every column is a column name
            source = self._select_op("view", formulas, failure)
            formulas = ()
        # Behavior: `Table#selectDistinct()`, the distinct rows of every column
        names = formulas or source.column_names
        return source._op("select_distinct", {"column_names": names}, failure)

    def where(self, filters: Union[str, Sequence[str], None] = None) -> "Table":
        """A new table with the rows that pass every filter."""
        filters = _engine.to_sequence(filters)
        op = self._scoped_op if filters else self._op
        return op("unstructured_filter", {"filters": filters}, "table where operation failed.")

    def _where_in(self, filter_table: "Table", cols, inverted: bool, failure: str) -> "Table":
        try:
            handles = [self.j_table, filter_table.j_table]
            if not _engine.to_sequence(cols):
                # the table service refuses a `where_in` without match columns
                return Table(_engine.local_op("where_in", {"inverted": inverted}, handles))
            request = {
                "left_id": _engine.source(0),
                "right_id": _engine.source(1),
                "inverted": inverted,
                "columns_to_match": _engine.to_sequence(cols),
            }
            return Table(_engine.table_op("where_in", request, handles))
        except Exception as e:
            raise DHError(e, failure) from e

    def where_in(self, filter_table: "Table", cols: Union[str, Sequence[str]]) -> "Table":
        """A new table with the rows whose values in `cols` are in `filter_table`."""
        return self._where_in(filter_table, cols, False, "table where_in operation failed.")

    def where_not_in(self, filter_table: "Table", cols: Union[str, Sequence[str]]) -> "Table":
        """A new table with the rows whose values in `cols` are not in `filter_table`."""
        return self._where_in(filter_table, cols, True, "table where_not_in operation failed.")

    def where_one_of(self, filters: Union[str, Sequence[str], None] = None) -> "Table":
        """A new table with the rows that pass at least one of the filters."""
        failure = "table where_one_of operation failed."
        filters = _engine.to_sequence(filters)
        if len(filters) == 1:
            # Behavior: `io.deephaven.api.filter.Filter#or`, which of one filter is that filter
            return self._scoped_op("unstructured_filter", {"filters": filters}, failure)
        if filters:
            try:
                _engine.native.require_query_scope()
            except Exception as e:
                raise DHError(e, failure) from e
        return self._local_op("where_one_of", {"filters": filters}, failure)

    def head(self, num_rows: int) -> "Table":
        """A new table with the first `num_rows` rows."""
        self._positions("table head operation failed.", num_rows)
        return self._op("head", {"num_rows": int(num_rows)}, "table head operation failed.")

    def head_pct(self, pct: float) -> "Table":
        """A new table with the first `pct` of the rows, `pct` from 0 to 1."""
        return self._local_op(
            "head_pct", {"pct": _share(pct)}, "table head_pct operation failed."
        )

    def tail(self, num_rows: int) -> "Table":
        """A new table with the last `num_rows` rows."""
        self._positions("table tail operation failed.", num_rows)
        return self._op("tail", {"num_rows": int(num_rows)}, "table tail operation failed.")

    def tail_pct(self, pct: float) -> "Table":
        """A new table with the last `pct` of the rows, `pct` from 0 to 1."""
        return self._local_op(
            "tail_pct", {"pct": _share(pct)}, "table tail_pct operation failed."
        )

    def slice(self, start: int, stop: int) -> "Table":
        """A new table with the rows from position `start` up to `stop`; a negative
        position counts from the end."""
        self._positions("table slice operation failed.", start, stop)
        return self._op(
            "slice",
            {"first_position_inclusive": int(start), "last_position_exclusive": int(stop)},
            "table slice operation failed.",
        )

    def slice_pct(self, start_pct: float, end_pct: float) -> "Table":
        """A new table with the rows from `start_pct` to `end_pct` of the table."""
        return self._local_op(
            "slice_pct",
            {"start_pct": _share(start_pct), "end_pct": _share(end_pct)},
            "table slice_pct operation failed.",
        )

    def restrict_sort_to(self, cols: Union[str, Sequence[str]]) -> "Table":
        """A new table that may be sorted by these columns only."""
        return self._local_op(
            "restrict_sort_to",
            {"cols": _engine.to_sequence(cols)},
            "table restrict_sort_to operation failed.",
        )

    def _sort(self, sorts: Sequence[Tuple[str, str]], failure: str) -> "Table":
        descriptors = [{"column_name": name, "direction": direction} for name, direction in sorts]
        return self._op("sort", {"sorts": descriptors}, failure)

    def sort_descending(self, order_by: Union[str, Sequence[str]]) -> "Table":
        """A new table sorted by the columns, each from greatest to least."""
        sorts = [(col, "DESCENDING") for col in _engine.to_sequence(order_by)]
        return self._sort(sorts, "table sort_descending operation failed.")

    def reverse(self) -> "Table":
        """A new table with the rows in the opposite order."""
        return self._sort([("", "REVERSE")], "table reverse operation failed.")

    def sort(
        self,
        order_by: Union[str, Sequence[str]],
        order: Optional[Union[SortDirection, Sequence[SortDirection]]] = None,
    ) -> "Table":
        """A new table sorted by the columns, each in its direction, ascending if no
        direction is given."""
        failure = "table sort operation failed."
        try:
            order_by = _engine.to_sequence(order_by)
            if not order:
                order = (SortDirection.ASCENDING,) * len(order_by)
            else:
                order = _engine.to_sequence(order)
                if any(o not in (SortDirection.ASCENDING, SortDirection.DESCENDING) for o in order):
                    raise DHError(
                        message="The sort direction must be either 'ASCENDING' or 'DESCENDING'."
                    )
                if len(order_by) != len(order):
                    raise DHError(
                        message="The number of sort columns must be the same as the number of sort directions."
                    )
        except Exception as e:
            raise DHError(e, failure) from e
        return self._sort([(col, direction.name) for col, direction in zip(order_by, order)], failure)

    def ungroup(self, cols: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the array or vector columns `cols`, or every such column,
        unwrapped: one row per element."""
        return self._op(
            "ungroup",
            {"null_fill": False, "columns_to_ungroup": _engine.to_sequence(cols)},
            "table ungroup operation failed.",
        )

    # Aggregations (task 6.5)

    def _agg_all(self, spec: dict, by, failure: str) -> "Table":
        """One aggregation applied to every column that is not grouped by.

        Behavior: `io.deephaven.api.TableOperationsDefaults#aggAllBy(AggSpec, String...)`
        """
        request = {"spec": spec, "group_by_columns": _engine.to_sequence(by)}
        return self._op("aggregate_all", request, failure)

    def head_by(self, num_rows: int, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the first `num_rows` rows of each group."""
        failure = "table head_by operation failed."
        self._positions(failure, num_rows)
        request = {"num_rows": num_rows, "group_by_column_specs": _engine.to_sequence(by)}
        # Behavior: `QueryTable#headBy`, which makes formulas
        return self._scoped_op("head_by", request, failure)

    def tail_by(self, num_rows: int, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the last `num_rows` rows of each group."""
        failure = "table tail_by operation failed."
        self._positions(failure, num_rows)
        request = {"num_rows": num_rows, "group_by_column_specs": _engine.to_sequence(by)}
        return self._scoped_op("tail_by", request, failure)

    def group_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with one row per group and the other columns as arrays."""
        return self._agg_all({"group": {}}, by, "table group-by operation failed.")

    def first_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the first row of each group."""
        return self._agg_all({"first": {}}, by, "table first_by operation failed.")

    def last_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the last row of each group."""
        return self._agg_all({"last": {}}, by, "table last_by operation failed.")

    def sum_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the sum of every other column for each group."""
        return self._agg_all({"sum": {}}, by, "table sum_by operation failed.")

    def abs_sum_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the sum of the absolute values of every other column for each
        group."""
        # the text is the reference's
        return self._agg_all({"abs_sum": {}}, by, "table asb_sum_by operation failed.")

    def weighted_sum_by(
        self, wcol: str, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "Table":
        """A new table with the sum of every other column, weighted by `wcol`, for each
        group."""
        spec = {"weighted_sum": {"weight_column": wcol}}
        return self._agg_all(spec, by, "table weighted_sum_by operation failed.")

    def avg_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the average of every other column for each group."""
        return self._agg_all({"avg": {}}, by, "table avg_by operation failed.")

    def weighted_avg_by(
        self, wcol: str, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "Table":
        """A new table with the average of every other column, weighted by `wcol`, for each
        group."""
        # the text is the reference's
        spec = {"weighted_avg": {"weight_column": wcol}}
        return self._agg_all(spec, by, "table avg_by operation failed.")

    def std_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the sample standard deviation of every other column for each
        group."""
        return self._agg_all({"std": {}}, by, "table std_by operation failed.")

    def var_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the sample variance of every other column for each group."""
        return self._agg_all({"var": {}}, by, "table var_by operation failed.")

    def median_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the median of every other column for each group."""
        # Behavior: `io.deephaven.api.agg.spec.AggSpec#median()`, which averages the two
        # values in the middle of an even number
        spec = {"median": {"average_evenly_divided": True}}
        return self._agg_all(spec, by, "table median_by operation failed.")

    def min_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the least value of every other column for each group."""
        return self._agg_all({"min": {}}, by, "table min_by operation failed.")

    def max_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the greatest value of every other column for each group."""
        return self._agg_all({"max": {}}, by, "table max_by operation failed.")

    def count_by(self, col: str, by: Optional[Union[str, Sequence[str]]] = None) -> "Table":
        """A new table with the number of rows of each group in the column `col`."""
        # Behavior: `io.deephaven.api.TableOperationsDefaults#countBy`, an `aggBy` of a count
        request = {
            "aggregations": [{"count": {"column_name": col}}],
            "group_by_columns": _engine.to_sequence(by),
        }
        return self._op("aggregate", request, "table count_by operation failed.")

    def agg_by(
        self,
        aggs: Union[Aggregation, Sequence[Aggregation]],
        by: Optional[Union[str, Sequence[str]]] = None,
        preserve_empty: bool = False,
        initial_groups: Optional["Table"] = None,
    ) -> "Table":
        """A new table with one row per group and the columns of the aggregations.

        With `preserve_empty` a group that has no rows keeps its row; `initial_groups` is a
        table whose distinct values of the `by` columns are the first groups.
        """
        try:
            aggs = _engine.to_sequence(aggs)
            by = _engine.to_sequence(by)
            if not by and initial_groups:
                raise ValueError("missing group-by column names when initial_groups is provided.")
            request = {
                "source_id": _engine.source(0),
                "aggregations": _aggregations(aggs),
                "preserve_empty": bool(preserve_empty),
                "group_by_columns": by,
            }
            handles = [self.j_table]
            if initial_groups is not None:
                request["initial_groups_id"] = _engine.source(1)
                handles.append(initial_groups.j_table)
            return Table(_engine.table_op("aggregate", request, handles))
        except Exception as e:
            raise DHError(e, "table agg_by operation failed.") from e

    def agg_all_by(
        self, agg: Aggregation, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "Table":
        """A new table with the aggregation applied to every column that is not grouped
        by. An aggregation that names its own result column cannot be used."""
        try:
            request = {"spec": agg.j_agg_spec, "group_by_columns": _engine.to_sequence(by)}
            return Table(
                _engine.table_op("aggregate_all", _engine.sourced(request), [self.j_table])
            )
        except Exception as e:
            raise DHError(e, "table agg_all_by operation failed.") from e

    # Joins (task 6.7)

    def _join(self, name: str, table: "Table", request, failure: str) -> "Table":
        """The result of the join operation `name` of this table, the left one, with
        `table`; `request` returns the other fields of the operation's message."""
        try:
            request = {"left_id": _engine.source(0), "right_id": _engine.source(1), **request()}
            return Table(_engine.table_op(name, request, [self.j_table, table.j_table]))
        except Exception as e:
            raise DHError(e, failure) from e

    def natural_join(
        self,
        table: "Table",
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
        type: NaturalJoinType = NaturalJoinType.ERROR_ON_DUPLICATE,
    ) -> "Table":
        """A new table with the rows of this one, each with the columns `joins` (every
        column that is not matched, if none is given) of the row of `table` that has the
        same values in the `on` columns, or nulls if there is none. `type` says what
        happens when several rows of `table` match; by default that is an error."""
        return self._join(
            "natural_join",
            table,
            lambda: {
                "columns_to_match": _split(on),
                "columns_to_add": _split(joins),
                "join_type": type.value,
            },
            "table natural_join operation failed.",
        )

    def exact_join(
        self,
        table: "Table",
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "Table":
        """As `natural_join`, but every row of this table must match exactly one row of
        `table`."""
        return self._join(
            "exact_join",
            table,
            lambda: {"columns_to_match": _split(on), "columns_to_add": _split(joins)},
            "table exact_join operation failed.",
        )

    def join(
        self,
        table: "Table",
        on: Optional[Union[str, Sequence[str]]] = None,
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "Table":
        """A new table with one row for every pair of a row of this table and a row of
        `table` that have the same values in the `on` columns; for every pair of rows if
        there is no `on` column."""
        return self._join(
            "cross_join",
            table,
            lambda: {"columns_to_match": _split(on), "columns_to_add": _split(joins)},
            "table join operation failed.",
        )

    def _as_of_join(self, name: str, table: "Table", on, joins, failure: str) -> "Table":
        """Behavior: `io.deephaven.api.TableOperationsDefaults#aj(TABLE, String, String)`
        and `#raj`: the last match is the as-of match, the ones before it are exact."""

        def request():
            matches = _split(on)
            return {
                "exact_match_columns": matches[:-1],
                # Java fails here too when there is no match: `matches.get(-1)`
                "as_of_column": matches[-1],
                "columns_to_add": _split(joins),
            }

        return self._join(name, table, request, failure)

    def aj(
        self,
        table: "Table",
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "Table":
        """A new table with the rows of this one, each with columns of the row of `table`
        that has the same values in all `on` columns but the last and, in the last, the
        closest value that is not greater (`"A >= B"`, the default) or is less
        (`"A > B"`)."""
        return self._as_of_join("aj", table, on, joins, "table as-of join operation failed.")

    def raj(
        self,
        table: "Table",
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "Table":
        """As `aj` with the closest value that is not less (`"A <= B"`, the default) or
        is greater (`"A < B"`)."""
        failure = "table reverse-as-of join operation failed."
        return self._as_of_join("raj", table, on, joins, failure)

    def range_join(
        self,
        table: "Table",
        on: Union[str, Sequence[str]],
        aggs: Union[Aggregation, Sequence[Aggregation]],
    ) -> "Table":
        """A new table with the rows of this one, each with the aggregations of the rows
        of `table` that have the same values in all `on` expressions but the last and
        whose value lies in the range the last one names, such as
        `"LeftStart <= RightValue <= LeftEnd"`."""

        def request():
            matches = _engine.to_sequence(on)
            if not matches:
                # Behavior: `io.deephaven.api.TableOperationsDefaults#rangeJoin`
                raise ValueError(
                    "No match expressions found; must include at least a range match expression"
                )
            return {
                "exact_match_columns": matches[:-1],
                "range_match": matches[-1],
                "aggregations": [agg.j_aggregation for agg in _engine.to_sequence(aggs)],
            }

        try:
            sources = [self.j_table, table.j_table]
            request = {"left_id": _engine.source(0), "right_id": _engine.source(1), **request()}
            if request["aggregations"]:
                return Table(_engine.table_op("range_join", request, sources))
            # no aggregation, which the table service refuses and the engine answers
            return Table(_engine.local_op("range_join", {"request": request}, sources))
        except Exception as e:
            raise DHError(e, "table range_join operation failed.") from e

    # update_by (task 6.6)

    def update_by(
        self,
        ops: Union["UpdateByOperation", Sequence["UpdateByOperation"]],
        by: Optional[Union[str, Sequence[str]]] = None,
    ) -> "Table":
        """A new table with the columns of this one and the columns of the operations
        (`deephaven.updateby`), each computed from a row and the rows around it in its
        group."""
        try:
            request = {
                "operations": [op.j_updateby_op for op in _engine.to_sequence(ops)],
                "group_by_columns": _engine.to_sequence(by),
            }
            return Table(_engine.table_op("update_by", _engine.sourced(request), [self.j_table]))
        except Exception as e:
            raise DHError(e, "table update-by operation failed.") from e

    # Partitioned tables (task 7.6)

    def partition_by(
        self, by: Union[str, Sequence[str]], drop_keys: bool = False
    ) -> "PartitionedTable":
        """A partitioned table with one constituent table for each distinct value of the
        `by` columns, which holds the rows with that value. With `drop_keys` the
        constituents do not have the `by` columns."""
        try:
            if not isinstance(drop_keys, bool):
                raise DHError(message="drop_keys must be a boolean value.")
            by = list(_engine.to_sequence(by))
            return PartitionedTable(_engine.native.partition_by(self.j_table, by, drop_keys))
        except Exception as e:
            raise DHError(e, "failed to create a partitioned table.") from e

    def partitioned_agg_by(
        self,
        aggs: Union[Aggregation, Sequence[Aggregation]],
        by: Optional[Union[str, Sequence[str]]] = None,
        preserve_empty: bool = False,
        initial_groups: Optional["Table"] = None,
    ) -> "PartitionedTable":
        """The result of `agg_by` as a partitioned table. If `aggs` has no partition
        aggregation (`deephaven.agg.partition`), one is added, in the column
        `__CONSTITUENT__`.

        Behavior: `io.deephaven.engine.table.impl.QueryTable#partitionedAggBy`
        """
        try:
            aggs = list(_engine.to_sequence(aggs))
            by = list(_engine.to_sequence(by))
            partitions = [agg.j_aggregation["partition"] for agg in aggs if "partition" in agg.j_aggregation]
            if partitions:
                partition = partitions[0]
            else:
                partition = {"column_name": "__CONSTITUENT__", "include_group_by_columns": True}
                aggs.append(Aggregation(j_aggregation={"partition": partition}))
            aggregated = self.agg_by(aggs, by, preserve_empty, initial_groups)
            return PartitionedTable(
                _engine.native.partitioned_aggregation(
                    self.j_table,
                    aggregated.j_table,
                    by,
                    partition["column_name"],
                    partition["include_group_by_columns"],
                )
            )
        except Exception as e:
            raise DHError(e, "table partitioned_agg_by operation failed.") from e

    # Hierarchical tables (task 7.8)

    def rollup(
        self,
        aggs: Union[Aggregation, Sequence[Aggregation]],
        by: Optional[Union[str, Sequence[str]]] = None,
        include_constituents: bool = False,
    ) -> "RollupTable":
        """A rollup table: this table aggregated by the `by` columns, and by one column
        less on each level above, up to the one row of the whole table. With
        `include_constituents` the rows of this table are shown under the groups of the
        last level.

        Behavior: `io.deephaven.engine.table.impl.QueryTable#rollup`
        """
        try:
            aggs = _engine.to_sequence(aggs)
            by = _engine.to_sequence(by)
            request = {
                "aggregate": {
                    "source_id": _engine.source(0),
                    "aggregations": [agg.j_aggregation for agg in aggs],
                    "group_by_columns": by,
                }
            }
            handle = _oxhaven.rollup(json.dumps(request), self.j_table, bool(include_constituents))
            return RollupTable(handle, aggs=aggs, include_constituents=include_constituents, by=by)
        except Exception as e:
            raise DHError(e, "table rollup operation failed.") from e

    def tree(self, id_col: str, parent_col: str, promote_orphans: bool = False) -> "TreeTable":
        """A tree table: the rows of this table, each under the row whose `id_col` is its
        `parent_col`. The rows without a parent are under the root; a row whose parent
        is no row's identifier is not shown, or with `promote_orphans` is under the root
        too.

        Behavior: `deephaven.table.Table.tree`
        """
        try:
            handle = _oxhaven.tree(self.j_table, id_col, parent_col, bool(promote_orphans))
            return TreeTable(j_tree_table=handle, id_col=id_col, parent_col=parent_col)
        except Exception as e:
            raise DHError(e, "table tree operation failed.") from e


# Aggregations (task 6.5)


def _aggregations(aggs: Sequence[Aggregation]) -> List[dict]:
    """The `Aggregation` messages of `aggs`."""
    messages = [agg.j_aggregation for agg in aggs]
    if any("count_where" in message for message in messages):
        # Java makes the filters of a count_where with the query scope at hand
        _engine.native.require_query_scope()
    if any(agg._reaggregating for agg in aggs):
        # Behavior: `AggregationProcessor#validateFormulaIsNotReaggregating`
        error = "AggFormula does not support reaggregating except in a rollup."
        raise ValueError(error)
    return messages


# Joins (task 6.7)

# What Java's `String#trim` removes.
_JAVA_SPACE = "".join(map(chr, range(33)))


def _split(columns) -> List[str]:
    """The match or addition expressions of a join: the reference joins its arguments
    with commas and Java splits that text at every comma, so `"A, B"` is two.

    Behavior: `io.deephaven.api.TableOperationsDefaults#splitToList`
    """
    parts = ",".join(_engine.to_sequence(columns)).split(",")
    return [part.strip(_JAVA_SPACE) for part in parts if part.strip(_JAVA_SPACE)]


def _checked_pairs(columns) -> List[str]:
    """The matches or additions of a multi join input, each `Name` or `Name = Other`,
    whose names must be able to name columns.

    Behavior: `io.deephaven.api.JoinMatch#parse` and `io.deephaven.api.JoinAddition#parse`,
    as far as they refuse a name; the table service reads the expressions again when the
    join is made
    """
    pairs = [str(item) for item in _engine.to_sequence(columns)]
    for pair in pairs:
        left, equals, right = pair.partition("=")
        _engine.native.column_name(left.strip(_JAVA_SPACE))
        if equals:
            _engine.native.column_name(right.removeprefix("=").strip(_JAVA_SPACE))
    return pairs


class MultiJoinInput:
    """One table of a multi join, with the columns to match and the columns to add (every
    other column, if none is given).

    Behavior: `deephaven.table.MultiJoinInput`
    """

    def __init__(
        self,
        table: Table,
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ):
        try:
            self.table = table
            self._handle = table.j_table
            self._on = _checked_pairs(on)
            self._joins = _checked_pairs(joins)
        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinInput object.") from e


class MultiJoinTable:
    """The result of a multi join; `table` is its table.

    Behavior: `deephaven.table.MultiJoinTable`
    """

    def __init__(
        self,
        input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
        on: Optional[Union[str, Sequence[str]]] = None,
    ):
        try:
            inputs = _engine.to_sequence(input)
            is_sequence = isinstance(input, Sequence)
            if isinstance(input, Table) or (
                is_sequence and all(isinstance(t, Table) for t in input)
            ):
                # Behavior: `io.deephaven.engine.table.MultiJoinInput#from`, which splits
                # one string at its commas and takes an array as it is
                if on is None:
                    # the reference's call of Java matches two overloads, for one string
                    # and for an array of them
                    raise TypeError(
                        "ambiguous Java method call, too many matching method overloads found"
                    )
                if isinstance(on, str):
                    keys = on.split(",")
                    while keys and not keys[-1]:
                        keys.pop()
                else:
                    keys = list(_engine.to_sequence(on))
                handles = [table.j_table for table in inputs]
                matches = [(keys, [])] * len(handles)
            elif isinstance(input, MultiJoinInput) or (
                is_sequence and all(isinstance(ji, MultiJoinInput) for ji in input)
            ):
                if on is not None:
                    raise DHError(
                        message="on parameter is not permitted when MultiJoinInput objects are provided."
                    )
                handles = [ji._handle for ji in inputs]
                matches = [(ji._on, ji._joins) for ji in inputs]
            else:
                raise DHError(
                    message="input must be a Table, a sequence of Tables, a MultiJoinInput, or a sequence of "
                    "MultiJoinInputs."
                )
            request = {
                "multi_join_inputs": [
                    {
                        "source_id": _engine.source(index),
                        "columns_to_match": match,
                        "columns_to_add": add,
                    }
                    for index, (match, add) in enumerate(matches)
                ]
            }
            if all(match for match, _ in matches):
                self._handle = _engine.table_op("multi_join", request, handles)
            else:
                # an input without key columns, which the table service refuses and the
                # engine answers
                self._handle = _engine.local_op("multi_join", {"request": request}, handles)
        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinTable object.") from e

    @property
    def table(self) -> Table:
        """The table of the join: one row per key that any input has."""
        return Table(self._handle)


def multi_join(
    input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
    on: Optional[Union[str, Sequence[str]]] = None,
) -> MultiJoinTable:
    """Joins several tables on their key columns: one row per key that any of them has,
    with the columns of each table's row for that key.

    `input` is tables that are matched on the columns `on`, or `MultiJoinInput` objects,
    each with its own columns to match and to add.
    """
    return MultiJoinTable(input, on)


def keyed_transpose(
    table: Table,
    aggs: Union[Aggregation, Sequence[Aggregation]],
    row_by_cols: Union[str, Sequence[str]],
    col_by_cols: Union[str, Sequence[str]],
    initial_groups: Optional[Table] = None,
    new_column_behavior: NewColumnBehaviorType = NewColumnBehaviorType.FAIL,
) -> Table:
    """A table with one row per value of `row_by_cols` and, for each value of
    `col_by_cols`, one column per aggregation, named after the value. The columns are
    those of the values the table has when the result is made, and of the values of
    `initial_groups`; a value that appears later fails the result or is left out, as
    `new_column_behavior` says.

    Behavior: `deephaven.table.keyed_transpose`
    """
    try:
        handles = [table.j_table]
        if initial_groups is not None:
            handles.append(initial_groups.j_table)
        args = {
            "aggregations": [agg.j_aggregation for agg in _engine.to_sequence(aggs)],
            "row_by_cols": _engine.to_sequence(row_by_cols),
            "col_by_cols": _engine.to_sequence(col_by_cols),
            "new_column_behavior": new_column_behavior.value,
        }
        return Table(_engine.local_op("keyed_transpose", args, handles))
    except Exception as e:
        raise DHError(e, "keyed_transpose operation failed.") from e


# Partitioned tables (task 7.6)


def _java_text(value) -> str:
    """A key value as Java's `Arrays#toString` shows it."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return _java_double_text(value)
    if isinstance(value, Table):
        return "QueryTable"
    return str(value)


def _java_double_text(value: float) -> str:
    """Behavior: `java.lang.Double#toString`: the shortest digits that read back as the
    value, plainly from 10^-3 up to 10^7 and otherwise as `d.dddE<exponent>`."""
    if value != value:
        return "NaN"
    if value in (float("inf"), float("-inf")):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "-0.0" if str(value).startswith("-") else "0.0"
    sign = "-" if value < 0 else ""
    # the shortest digits are those of `repr`
    shortest = decimal.Decimal(repr(abs(value)))
    digits = "".join(map(str, shortest.as_tuple().digits)).rstrip("0") or "0"
    point = shortest.adjusted() + 1
    if 1e-3 <= abs(value) < 1e7:
        if point <= 0:
            return f"{sign}0.{'0' * -point}{digits}"
        whole, fraction = digits[:point].ljust(point, "0"), digits[point:]
        return f"{sign}{whole}.{fraction or '0'}"
    return f"{sign}{digits[0]}.{digits[1:] or '0'}E{point - 1}"


# The Java classes jpy gives a Python value as, when a method takes an `Object`.
_JAVA_BASE = "is in module java.base of loader 'bootstrap'"


def _java_class_of(value) -> str:
    """Behavior: jpy's conversion of a Python value to `java.lang.Object`: a bool is a
    `Boolean`, an int the narrowest of `Byte`, `Short`, `Integer` and `Long` that holds it,
    a float a `Double`, a str a `String`"""
    if isinstance(value, bool):
        return "java.lang.Boolean"
    if isinstance(value, int):
        for bits, name in ((8, "Byte"), (16, "Short"), (32, "Integer")):
            if -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                return f"java.lang.{name}"
        return "java.lang.Long"
    if isinstance(value, float):
        return "java.lang.Double"
    if isinstance(value, str):
        return "java.lang.String"
    if isinstance(value, Table):
        return "io.deephaven.engine.table.impl.QueryTable"
    raise dtypes._java_time.NotPorted(
        "io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#constituentFor",
        f"a key value of type {type(value).__name__}",
    )


def _check_java_object(value) -> None:
    """Fails as jpy does for a value it cannot hand to Java as an `Object`.

    Behavior: jpy's conversion of a Python value to `java.lang.Object`"""
    if value is None or isinstance(value, (bool, int, float, str, Table)):
        if isinstance(value, int) and not -(2**63) <= value < 2**63:
            raise OverflowError("int too big to convert")
        return
    if isinstance(value, dtypes._java_time._Value):
        # the stand-ins of this package for Java's time values
        return
    raise ValueError(
        f"cannot convert a Python '{_python_type_name(value)}' to a Java 'java.lang.Object'"
    )


def _python_type_name(value) -> str:
    kind = type(value)
    module = kind.__module__
    return kind.__qualname__ if module == "builtins" else f"{module}.{kind.__qualname__}"


class _ClassCast(Exception):
    """Java's `ClassCastException` of a key value cast to the class a column needs."""

    def __init__(self, value, wanted: str):
        given = _java_class_of(value)
        if given.startswith("java."):
            where = f"({given} and {wanted} are in module java.base of loader 'bootstrap')"
        else:
            where = f"({given} is in unnamed module of loader 'app'; {wanted} {_JAVA_BASE})"
        super().__init__(
            f"java.lang.ClassCastException: class {given} cannot be cast to class {wanted} "
            + where
        )


_NO_MATCH = object()
_INTEGER_BITS = {"byte": 8, "short": 16, "int": 32, "long": 64}


def _match_value(data_type, value):
    """The value of a key column of `data_type` that a match filter given the Python
    value `value` looks for, or `_NO_MATCH` when it can match no row. Raises `_ClassCast`
    where Java's filter fails.

    Behavior: `io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory#getChunkFilter`
    with `ArrayTypeUtils#getUnboxedLongArray` and the like: a primitive column takes any
    `Number` through `Number#longValue` and the like, and casts anything else; a `char`
    column casts to `Character`, which no value jpy makes is; every other column compares
    with `Objects#equals`, under which a value of another class is never equal. A NaN
    matches nothing (`DoubleChunkMatchFilterFactory#makeFilter` without `nanMatch`).
    """
    if value is None:
        return None
    name = data_type.j_name
    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if name in _INTEGER_BITS:
        if not is_number:
            raise _ClassCast(value, "java.lang.Number")
        if isinstance(value, float):
            # Java's cast of a double: toward zero, NaN is 0, out of range the nearest
            # long (for `long`) or int (for the narrower types, which then drop bits)
            limit = 64 if name == "long" else 32
            low, high = -(2 ** (limit - 1)), 2 ** (limit - 1) - 1
            value = 0 if value != value else int(max(low, min(high, value)))
        bits = _INTEGER_BITS[name]
        value &= 2**bits - 1
        return value - 2**bits if value >= 2 ** (bits - 1) else value
    if name in ("float", "double"):
        if not is_number:
            raise _ClassCast(value, "java.lang.Number")
        value = _to_float32(value) if name == "float" else float(value)
        return _NO_MATCH if value != value else value
    if name == "char":
        raise _ClassCast(value, "java.lang.Character")
    if name == "java.lang.String":
        return value if isinstance(value, str) else _NO_MATCH
    if name == "java.lang.Boolean":
        return value if isinstance(value, bool) else _NO_MATCH
    if isinstance(value, (bool, int, float, str, Table)):
        # no column of this engine holds Java's Boolean, Byte to Long, Double, String
        # or a table besides those above
        return _NO_MATCH
    return value


def _to_float32(value) -> float:
    """Behavior: `Number#floatValue` of a `Byte` to `Long` or a `Double`: one rounding to
    the nearest float, ties to even."""
    if isinstance(value, int) and abs(value) >= 2**53:
        # through a double would round twice
        sign, value = (-1 if value < 0 else 1), abs(value)
        drop = value.bit_length() - 24
        kept, rest = value >> drop, value & ((1 << drop) - 1)
        half = 1 << (drop - 1)
        if rest > half or (rest == half and kept & 1):
            kept += 1
        value = sign * float(kept << drop)
    try:
        return struct.unpack("f", struct.pack("f", float(value)))[0]
    except OverflowError:
        return float("inf") if value > 0 else float("-inf")


class NodeType(Enum):
    """The types of the nodes of a rollup.

    Behavior: `deephaven.table.NodeType`; the values are the names of the constants of
    `io.deephaven.engine.table.hierarchical.RollupTable.NodeType`
    """

    AGGREGATED = "Aggregated"
    """The nodes of the levels that aggregate: their columns are the group-by columns
    and the results of the aggregations."""
    CONSTITUENT = "Constituent"
    """The nodes of the rows of the source, in a rollup made with
    `include_constituents`."""


class RollupNodeOperationsRecorder:
    """Records operations to apply to the nodes of a rollup when a client reads it:
    sorts. `RollupTable.node_operation_recorder` makes one, and every operation returns
    a new one.

    Behavior: `deephaven.table.RollupNodeOperationsRecorder`
    """

    def __init__(self, j_node_ops_recorder):
        self.j_node_ops_recorder = j_node_ops_recorder

    @property
    def j_object(self):
        """The engine's recorder. In the reference package this is the Java recorder."""
        return self.j_node_ops_recorder

    def sort(self, order_by: Union[str, Sequence[str]]):
        """A recorder that also sorts the rows of a node by the columns, ascending."""
        order_by = _engine.to_sequence(order_by)
        return self.__class__(self.j_node_ops_recorder.sort(order_by, False))

    def sort_descending(self, order_by: Union[str, Sequence[str]]):
        """A recorder that also sorts the rows of a node by the columns, descending."""
        order_by = _engine.to_sequence(order_by)
        return self.__class__(self.j_node_ops_recorder.sort(order_by, True))

    def format_column(self, formulas: Union[str, Sequence[str]]):
        """A recorder that also formats columns of a node, as `Table.format_columns`
        formats those of a table."""
        formulas = _engine.to_sequence(formulas)
        return self.__class__(self.j_node_ops_recorder.format_columns(formulas))

    def format_row_where(self, cond: str, formula: str):
        """A recorder that also formats the rows of a node where `cond` holds."""
        return self.__class__(self.j_node_ops_recorder.format_where("*", cond, formula))

    def format_column_where(self, col: str, cond: str, formula: str):
        """A recorder that also formats `col` in the rows of a node where `cond` holds."""
        return self.__class__(self.j_node_ops_recorder.format_where(col, cond, formula))

    def update_view(self, formulas: Union[str, Sequence[str]]):
        """A recorder that also adds a column per formula to a node, computed when read.
        A format recorded before can read it."""
        formulas = _engine.to_sequence(formulas)
        return self.__class__(self.j_node_ops_recorder.update_view(formulas))


class RollupTable:
    """The result of `Table.rollup`: a hierarchical table whose levels aggregate by one
    group-by column less each. A client reads it through a view; a script only makes it.

    Behavior: `deephaven.table.RollupTable`
    """

    def __init__(self, j_rollup_table, aggs, include_constituents: bool, by):
        if not isinstance(j_rollup_table, _oxhaven.HierarchicalHandle):
            raise DHError(
                message=f"not a hierarchical table handle: {type(j_rollup_table).__name__}"
            )
        self.j_rollup_table = j_rollup_table
        self.aggs = aggs
        self.include_constituents = include_constituents
        self.by = by

    @property
    def j_object(self):
        """The engine's handle. In the reference package this is the Java rollup table."""
        return self.j_rollup_table

    def node_operation_recorder(self, node_type: "NodeType") -> "RollupNodeOperationsRecorder":
        """A recorder of operations to apply to the nodes of `node_type` when a client
        reads this rollup, with nothing recorded.

        Behavior: `deephaven.table.RollupTable.node_operation_recorder`
        """
        try:
            return RollupNodeOperationsRecorder(
                j_node_ops_recorder=self.j_rollup_table.recorder(node_type.value)
            )
        except Exception as e:
            raise DHError(e, "failed to create a RollupNodeOperationsRecorder.") from e

    def with_node_operations(
        self, recorders: Sequence["RollupNodeOperationsRecorder"]
    ) -> "RollupTable":
        """This rollup with the recorded operations applied to its nodes.

        Behavior: `deephaven.table.RollupTable.with_node_operations`
        """
        try:
            return RollupTable(
                j_rollup_table=self.j_rollup_table.with_node_operations(
                    [op.j_node_ops_recorder for op in recorders]
                ),
                include_constituents=self.include_constituents,
                aggs=self.aggs,
                by=self.by,
            )
        except Exception as e:
            raise DHError(e, "with_node_operations on RollupTable failed.") from e

    def with_filters(self, filters: Union[str, Sequence[str]]) -> "RollupTable":
        """This rollup of the rows of its source that pass every filter. A filter may
        read the columns that no aggregation reads.

        Behavior: `deephaven.table.RollupTable.with_filters`
        """
        try:
            filters = _engine.to_sequence(filters)
            if filters:
                _engine.native.require_query_scope()
            return RollupTable(
                j_rollup_table=self.j_rollup_table.with_filters(filters),
                include_constituents=self.include_constituents,
                aggs=self.aggs,
                by=self.by,
            )
        except Exception as e:
            raise DHError(e, "with_filters operation on RollupTable failed.") from e

    def with_update_view(self, formulas: Union[str, Sequence[str]]) -> "RollupTable":
        """This rollup with a column per formula added to its aggregated nodes, computed
        when read.

        Behavior: `deephaven.table.RollupTable.with_update_view`, which is
        `io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#withUpdateView`: the
        update view of a recorder for aggregated nodes, applied to the rollup
        """
        try:
            formulas = _engine.to_sequence(formulas)
            recorder = self.j_rollup_table.recorder(NodeType.AGGREGATED.value)
            return RollupTable(
                j_rollup_table=self.j_rollup_table.with_node_operations(
                    [recorder.update_view(formulas)]
                ),
                include_constituents=self.include_constituents,
                aggs=self.aggs,
                by=self.by,
            )
        except Exception as e:
            raise DHError(e, "with_update_view operation on RollupTable failed.") from e


class TreeNodeOperationsRecorder:
    """Records operations to apply to the nodes of a tree when a client reads it:
    sorts, formats and filters. `TreeTable.node_operation_recorder` makes one, and every
    operation returns a new one.

    Behavior: `deephaven.table.TreeNodeOperationsRecorder`
    """

    def __init__(self, j_node_ops_recorder):
        self.j_node_ops_recorder = j_node_ops_recorder

    @property
    def j_object(self):
        """The engine's recorder. In the reference package this is the Java recorder."""
        return self.j_node_ops_recorder

    def sort(self, order_by: Union[str, Sequence[str]]):
        """A recorder that also sorts the rows of a node by the columns, ascending."""
        order_by = _engine.to_sequence(order_by)
        return self.__class__(self.j_node_ops_recorder.sort(order_by, False))

    def sort_descending(self, order_by: Union[str, Sequence[str]]):
        """A recorder that also sorts the rows of a node by the columns, descending."""
        order_by = _engine.to_sequence(order_by)
        return self.__class__(self.j_node_ops_recorder.sort(order_by, True))

    def where(self, filters: Union[str, Sequence[str]]):
        """A recorder that also keeps, of the rows of a node, those that pass every
        filter. A filter may read a formatting column recorded before it. A row whose
        children no filter keeps cannot be expanded.

        Behavior: `deephaven.table.TreeNodeOperationsRecorder.where`, which is
        `io.deephaven.engine.table.impl.hierarchical.TreeNodeOperationsRecorder#where`
        """
        filters = _engine.to_sequence(filters)
        if not all(isinstance(f, str) for f in filters):
            # a `deephaven.filters.Filter`, which this package does not have
            raise dtypes._java_time.NotPorted(
                "io.deephaven.engine.table.impl.hierarchical.TreeNodeOperationsRecorder#where(Filter)"
            )
        return self.__class__(self.j_node_ops_recorder.where_filters(filters))

    def format_column(self, formulas: Union[str, Sequence[str]]):
        """A recorder that also formats columns of a node, as `Table.format_columns`
        formats those of a table."""
        formulas = _engine.to_sequence(formulas)
        return self.__class__(self.j_node_ops_recorder.format_columns(formulas))

    def format_row_where(self, cond: str, formula: str):
        """A recorder that also formats the rows of a node where `cond` holds."""
        return self.__class__(self.j_node_ops_recorder.format_where("*", cond, formula))

    def format_column_where(self, col: str, cond: str, formula: str):
        """A recorder that also formats `col` in the rows of a node where `cond` holds."""
        return self.__class__(self.j_node_ops_recorder.format_where(col, cond, formula))


class TreeTable:
    """The result of `Table.tree`: a hierarchical table of the rows of a table, each
    under its parent. A client reads it through a view; a script only makes it.

    Behavior: `deephaven.table.TreeTable`
    """

    def __init__(self, j_tree_table, id_col: str, parent_col: str):
        if not isinstance(j_tree_table, _oxhaven.HierarchicalHandle):
            raise DHError(
                message=f"not a hierarchical table handle: {type(j_tree_table).__name__}"
            )
        self.j_tree_table = j_tree_table
        self.id_col = id_col
        self.parent_col = parent_col

    @property
    def j_object(self):
        """The engine's handle. In the reference package this is the Java tree table."""
        return self.j_tree_table

    def node_operation_recorder(self) -> "TreeNodeOperationsRecorder":
        """A recorder of operations to apply to the nodes when a client reads this
        tree, with nothing recorded.

        Behavior: `deephaven.table.TreeTable.node_operation_recorder`
        """
        return TreeNodeOperationsRecorder(j_node_ops_recorder=self.j_tree_table.tree_recorder())

    def with_node_operations(self, recorder: "TreeNodeOperationsRecorder") -> "TreeTable":
        """This tree with the recorded operations applied to its nodes.

        Behavior: `deephaven.table.TreeTable.with_node_operations`
        """
        try:
            return TreeTable(
                j_tree_table=self.j_tree_table.with_node_operations(
                    [recorder.j_node_ops_recorder]
                ),
                id_col=self.id_col,
                parent_col=self.parent_col,
            )
        except Exception as e:
            raise DHError(e, "with_node_operations on TreeTable failed.") from e

    def with_filters(self, filters: Union[str, Sequence[str]]) -> "TreeTable":
        """This tree of the rows of its source that pass every filter, and of their
        ancestors.

        Behavior: `deephaven.table.TreeTable.with_filters`
        """
        try:
            filters = _engine.to_sequence(filters)
            if filters:
                _engine.native.require_query_scope()
            return TreeTable(
                j_tree_table=self.j_tree_table.with_filters(filters),
                id_col=self.id_col,
                parent_col=self.parent_col,
            )
        except Exception as e:
            raise DHError(e, "with_filters operation on TreeTable failed.") from e


def _wrap_hierarchical(handle) -> Union[RollupTable, TreeTable]:
    """The Python object of a hierarchical table that was not made by this code: a
    variable read back from the server's scope. What made a rollup is not kept with
    it."""
    columns = handle.tree_columns()
    if columns is not None:
        return TreeTable(handle, id_col=columns[0], parent_col=columns[1])
    return RollupTable(handle, aggs=(), include_constituents=False, by=())


def _transformer(func):
    """`func`, a function of tables that answers a table, as the engine calls it: with
    table handles, and where there is no query scope.

    Behavior: `deephaven.jcompat.j_unary_operator` and `j_binary_operator` of a function
    whose answer is a `Table`, and the execution context that
    `PartitionedTableImpl#transform` applies the function in, which has no query scope
    """

    def apply(*handles):
        before = _engine.native.without_query_scope(True)
        try:
            made = func(*[Table(handle) for handle in handles])
        finally:
            _engine.native.without_query_scope(before)
        if made is None:
            return None
        if not isinstance(made, Table):
            raise TypeError(f"the transform function returned a {type(made).__name__}, not a Table")
        return made.j_table

    return apply


def _refreshing_dependencies(dependencies) -> list:
    """The handles of the refreshing tables among `dependencies`, and of the underlying
    tables of the refreshing partitioned tables among them. Anything else is passed over.

    Behavior: the `j_dependencies` of `deephaven.table.PartitionedTable.transform`
    """
    dependencies = _engine.to_sequence(dependencies)
    tables = [d.j_table for d in dependencies if isinstance(d, Table) and d.is_refreshing]
    tables.extend(
        d.table.j_table
        for d in dependencies
        if isinstance(d, PartitionedTable) and d.is_refreshing
    )
    return tables


def _without_rows(definition) -> "_oxhaven.TableHandle":
    """The handle of a table without rows that has the columns of `definition`, a
    `TableDefinitionLike`: how a definition is handed to the engine."""
    columns = [
        (
            column.name,
            dtypes._engine_type(column.data_type),
            column.column_type is ColumnType.PARTITIONING,
        )
        for column in TableDefinition(definition).values()
    ]
    return _engine.native.defined_table(columns)


class PartitionedTable:
    """A table of tables. Its underlying table has one row for each constituent table: the
    key columns, which say which constituent it is, and one column that holds the
    constituent. Every constituent has the same columns.

    `Table.partition_by` and `Table.partitioned_agg_by` make one.

    Behavior: `deephaven.table.PartitionedTable`
    """

    def __init__(self, j_partitioned_table):
        if not isinstance(j_partitioned_table, _oxhaven.PartitionedHandle):
            raise DHError(
                message=f"not a partitioned table handle: {type(j_partitioned_table).__name__}"
            )
        self.j_partitioned_table = j_partitioned_table
        self._table = None
        self._definition = None

    @property
    def j_object(self):
        """The engine's handle. In the reference package this is the Java partitioned
        table."""
        return self.j_partitioned_table

    @classmethod
    def from_partitioned_table(
        cls,
        table: Table,
        key_cols=None,
        unique_keys=None,
        constituent_column=None,
        constituent_table_columns=None,
        constituent_changes_permitted=None,
    ) -> "PartitionedTable":
        """A partitioned table of `table`, whose column `constituent_column` holds the
        constituents. The other arguments are all given or all `None`: with none, the key
        columns are the columns that do not hold tables, the keys are not unique, the
        constituent column is the one column of tables, the constituents have the columns
        of the first one, and they can change when `table` refreshes.

        Behavior: `deephaven.table.PartitionedTable.from_partitioned_table`,
        `io.deephaven.engine.table.PartitionedTableFactory#of`
        """
        none_args = [
            key_cols,
            unique_keys,
            constituent_column,
            constituent_table_columns,
            constituent_changes_permitted,
        ]
        try:
            if all(arg is None for arg in none_args):
                return PartitionedTable(_oxhaven.partitioned_of_table(table.j_table))
            if all(arg is not None for arg in none_args):
                handle = _oxhaven.partitioned_of(
                    table.j_table,
                    list(_engine.to_sequence(key_cols)),
                    unique_keys,
                    constituent_column,
                    _without_rows(constituent_table_columns),
                    constituent_changes_permitted,
                )
                return PartitionedTable(handle)
        except Exception as e:
            raise DHError(e, "failed to build a PartitionedTable.") from e
        missing_value_args = [arg for arg in none_args if arg is None]
        raise DHError(
            message=f"invalid argument values, must specify non-None values for {missing_value_args}."
        )

    @classmethod
    def from_constituent_tables(
        cls, tables: List[Table], constituent_table_columns=None
    ) -> "PartitionedTable":
        """A partitioned table without key columns whose constituents are `tables`, in
        the column `__CONSTITUENT__`; they have the columns of `constituent_table_columns`
        or, without it, of the first table.

        Behavior: `deephaven.table.PartitionedTable.from_constituent_tables`,
        `io.deephaven.engine.table.PartitionedTableFactory#ofTables`
        """
        try:
            handles = [
                None if table is None else table.j_table
                for table in _engine.to_sequence(tables)
            ]
            if not constituent_table_columns:
                return PartitionedTable(_oxhaven.partitioned_of_tables(handles))
            definition = _without_rows(constituent_table_columns)
            return PartitionedTable(_oxhaven.partitioned_of_tables(handles, definition))
        except Exception as e:
            raise DHError(e, "failed to create a PartitionedTable from constituent tables.") from e

    @property
    def table(self) -> Table:
        """The underlying table: the key columns and the column of constituents."""
        if self._table is None:
            self._table = Table(self.j_partitioned_table.table())
        return self._table

    @property
    def update_graph(self):
        """Not ported: the package has no update graph objects."""
        raise dtypes._java_time.NotPorted("io.deephaven.engine.table.Table#getUpdateGraph")

    @property
    def is_refreshing(self) -> bool:
        """Whether the underlying table is refreshing."""
        return self.table.is_refreshing

    def _liveness_release(self) -> bool:
        """A liveness scope may be given a partitioned table (`manage`, `preserve`,
        `unmanage`) and never releases it: the answer is that something else holds it.
        On Java 42.9 a partitioned table, static or refreshing, was usable after every
        scope that had managed it was released, also one that was made in the scope
        (`deephaven.liveness_scope`)."""
        return False

    @property
    def key_columns(self) -> List[str]:
        """The names of the key columns."""
        return self.j_partitioned_table.key_columns()

    def keys(self) -> Table:
        """A table of the keys, one row each: the key columns of the underlying table,
        without duplicates."""
        if self.unique_keys:
            return self.table.view(self.key_columns)
        return self.table.select_distinct(self.key_columns)

    @property
    def unique_keys(self) -> bool:
        """Whether no two rows of the underlying table have the same key."""
        return self.j_partitioned_table.unique_keys()

    @property
    def constituent_column(self) -> str:
        """The name of the column that holds the constituents."""
        return self.j_partitioned_table.constituent_column()

    @property
    def constituent_table_definition(self) -> TableDefinition:
        """The columns every constituent has."""
        if self._definition is None:
            self._definition = Table(self.j_partitioned_table.empty_constituent()).definition
        return self._definition

    @property
    def constituent_table_columns(self) -> List[ColumnDefinition]:
        """The column definitions every constituent has."""
        return list(self.constituent_table_definition.values())

    @property
    def constituent_changes_permitted(self) -> bool:
        """Whether rows of the underlying table may be added, removed or moved, or a cell
        of the constituent column may change."""
        return self.j_partitioned_table.constituent_changes_permitted()

    def merge(self) -> Table:
        """One table with the rows of every constituent: the constituents one after the
        other, in the order of the rows of the underlying table."""
        try:
            return Table(self.j_partitioned_table.merge())
        except Exception as e:
            raise DHError(e, "failed to merge all the constituent tables.") from e

    def filter(self, filters: Union[str, Sequence[str]]) -> "PartitionedTable":
        """A partitioned table with the rows of the underlying table that pass the
        filters. A filter cannot use the constituent column."""
        try:
            filtered = self.table.where(filters)
            return PartitionedTable(self.j_partitioned_table.with_table(filtered.j_table))
        except Exception as e:
            raise DHError(e, "failed to apply filters to the partitioned table.") from e

    def sort(
        self,
        order_by: Union[str, Sequence[str]],
        order: Optional[Union[SortDirection, Sequence[SortDirection]]] = None,
    ) -> "PartitionedTable":
        """A partitioned table with the rows of the underlying table sorted by the
        columns, which cannot include the constituent column."""
        try:
            order_by = _engine.to_sequence(order_by)
            if not order:
                order = (SortDirection.ASCENDING,) * len(order_by)
            order = _engine.to_sequence(order)
            if len(order_by) != len(order):
                raise DHError(
                    message="The number of sort columns must be the same as the number of sort directions."
                )
            if self.constituent_column in order_by:
                # Behavior: `PartitionedTableImpl#sort`
                raise RuntimeError(
                    "java.lang.IllegalArgumentException: Unsupported sort on constituent column "
                    f"{self.constituent_column}"
                )
            ordered = self.table.sort(order_by, order)
            return PartitionedTable(
                self.j_partitioned_table.with_table(ordered.j_table, sorted=True)
            )
        except Exception as e:
            raise DHError(e, "failed to sort the partitioned table.") from e

    def get_constituent(self, key_values: Union[Any, Sequence[Any]]) -> Optional[Table]:
        """The constituent with the key values, one for each key column in their order,
        or `None` if there is none. More than one is an error."""
        try:
            key_values = _engine.to_sequence(key_values)
            for value in key_values:
                _check_java_object(value)
            names = self.key_columns
            if len(key_values) != len(names):
                # Behavior: `PartitionedTableImpl#constituentFor`
                raise RuntimeError(
                    "java.lang.IllegalArgumentException: Key count mismatch: expected one key "
                    f"column value for each key column name in [{', '.join(names)}], instead "
                    f"received [{', '.join(map(_java_text, key_values))}]"
                )
            if not names:
                handle = self.j_partitioned_table.constituent_for(None)
                return Table(handle) if handle is not None else None
            definition = self.table.definition
            types = [definition[name].data_type for name in names]
            # Java filters the underlying table by one match filter per key column, in
            # order, and skips the filters after one that left no row: a value that
            # fails to convert fails only when rows are left for it.
            matched = []
            for i, (data_type, value) in enumerate(zip(types, key_values)):
                try:
                    match = _match_value(data_type, value)
                except _ClassCast as cast:
                    if self._rows_for(names[:i], types[:i], matched) == 0:
                        return None
                    filters = ", ".join(
                        f"{name} in [{_java_text(value)}]"
                        for name, value in zip(names, key_values)
                    )
                    raise RuntimeError(
                        "io.deephaven.engine.exceptions.TableInitializationException: Error "
                        f"while initializing where([{filters}]): an exception occurred while "
                        f"performing the initial filter\ncaused by {cast}"
                    ) from None
                if match is _NO_MATCH:
                    return None
                matched.append(match)
            keys = self._key_table(names, types, matched)
            handle = self.j_partitioned_table.constituent_for(keys)
            return Table(handle) if handle is not None else None
        except Exception as e:
            raise DHError(e, "unable to get constituent table.") from e

    @staticmethod
    def _key_table(names, types, values):
        try:
            return _engine.new_table(
                [
                    (name, dtypes._engine_type(data_type), [value])
                    for name, data_type, value in zip(names, types, values)
                ]
            )
        except Exception as e:
            raise dtypes._java_time.NotPorted(
                "io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#constituentFor",
                f"a key value that is not a value of its key column's type ({e})",
            ) from e

    def _rows_for(self, names, types, values) -> int:
        """The number of rows of the underlying table with these values in these key
        columns."""
        if not names:
            return self.table.size
        keys = Table(self._key_table(names, types, values))
        return self.table.where_in(keys, list(names)).size

    @property
    def constituent_tables(self) -> List[Table]:
        """The constituents now, in the order of the rows of the underlying table."""
        return [Table(handle) for handle in self.j_partitioned_table.constituents()]

    def transform(self, func, dependencies=None) -> "PartitionedTable":
        """A partitioned table whose constituents are what `func` makes of the
        constituents of this one. The other columns of the underlying table are kept.

        `func` is called for every constituent now and, when this partitioned table is
        refreshing, in the update cycle for every constituent that appears. The columns
        of the result's constituents are those `func` gives a table without rows.
        `dependencies` are the refreshing tables and partitioned tables that `func` uses
        besides the constituent: it is not called in a cycle before they have finished.

        As in the reference, `func` cannot use a formula or a filter: they fail there for
        want of a query scope.

        Behavior: `deephaven.table.PartitionedTable.transform`
        """
        try:
            return PartitionedTable(
                self.j_partitioned_table.transform(
                    _transformer(func), _refreshing_dependencies(dependencies)
                )
            )
        except Exception as e:
            raise DHError(e, "failed to transform the PartitionedTable.") from e

    def partitioned_transform(
        self, other: "PartitionedTable", func, dependencies=None
    ) -> "PartitionedTable":
        """A partitioned table with a row for each row of this one whose key `other` has
        too. Its constituents are what `func` makes of the constituent of this one and
        the constituent of `other` with that key; the other columns are those of this
        one's underlying table. `func` is called as the function of `transform` is, for
        every such pair now and for every pair that appears.

        Behavior: `deephaven.table.PartitionedTable.partitioned_transform`
        """
        try:
            return PartitionedTable(
                self.j_partitioned_table.partitioned_transform(
                    other.j_partitioned_table,
                    _transformer(func),
                    _refreshing_dependencies(dependencies),
                )
            )
        except Exception as e:
            raise DHError(
                e, "failed to transform the PartitionedTable with another PartitionedTable."
            ) from e

    def proxy(
        self, require_matching_keys: bool = True, sanity_check_joins: bool = True
    ) -> "PartitionedTableProxy":
        """A proxy that applies a table operation to every constituent and answers a proxy
        of the partitioned table of the results.

        `require_matching_keys` says that an operation with another proxy fails unless
        both partitioned tables have the same keys, now and after every update cycle.
        `sanity_check_joins` says that a join with another proxy fails if a join key is
        in more than one constituent of either.

        Behavior: `deephaven.table.PartitionedTable.proxy`
        """
        return PartitionedTableProxy(self, bool(require_matching_keys), bool(sanity_check_joins))


def _column_names(formulas) -> set:
    """The formulas that are the name of a column and nothing else.

    Behavior: the select columns for which `SelectColumn#isRetain` holds
    """
    return {str(f).strip(_JAVA_SPACE) for f in formulas if "=" not in str(f)}


def _assigned_names(formulas) -> set:
    """The names of the columns that the formulas `Name = expression` assign."""
    pairs = (str(f).partition("=") for f in formulas)
    return {name.strip(_JAVA_SPACE) for name, equals, _ in pairs if equals}


def _exact_matches(on) -> Tuple[List[str], List[str]]:
    """The names of the columns of the left and of the right table that the matches
    `on`, each `Name` or `Left = Right`, compare.

    Behavior: `io.deephaven.api.JoinMatch#parse`
    """
    left, right = [], []
    for match in on:
        first, equals, second = match.partition("=")
        first = first.strip(_JAVA_SPACE)
        left.append(first)
        right.append(second.removeprefix("=").strip(_JAVA_SPACE) if equals else first)
    return left, right


class PartitionedTableProxy:
    """A partitioned table as the target of table operations: each operation is applied
    to every constituent of `target`, now and to every constituent that appears later,
    and answers a proxy of the partitioned table of the results. With another proxy as
    its argument an operation is applied to the pairs of constituents that have the
    same key.

    A formula or a filter of an operation may name the variables of the code that calls
    it. Their values are those of the call, also for a constituent that appears later.

    Behavior: `deephaven.table.PartitionedTableProxy`, and
    `io.deephaven.engine.table.impl.partitioned.PartitionedTableProxyImpl` under it
    """

    def __init__(
        self,
        target: PartitionedTable,
        require_matching_keys: bool = True,
        sanity_check_joins: bool = True,
    ):
        self.target = target
        self.require_matching_keys = require_matching_keys
        self.sanity_check_joins = sanity_check_joins

    @property
    def j_object(self):
        """In the reference this is the Java proxy; here the proxy stands for itself."""
        return self

    @property
    def is_refreshing(self) -> bool:
        return self.target.is_refreshing

    @property
    def update_graph(self):
        return self.target.update_graph

    def __eq__(self, other):
        # Behavior: `PartitionedTableProxyImpl#equals`
        return (
            isinstance(other, PartitionedTableProxy)
            and self.require_matching_keys == other.require_matching_keys
            and self.sanity_check_joins == other.sanity_check_joins
            and self.target.j_partitioned_table.is_same(other.target.j_partitioned_table)
        )

    def __hash__(self):
        return hash((self.require_matching_keys, self.sanity_check_joins))

    def __repr__(self):
        return f"deephaven.table.PartitionedTableProxy({self.target.table!r})"

    def _of(self, j_partitioned_table) -> "PartitionedTableProxy":
        return PartitionedTableProxy(
            PartitionedTable(j_partitioned_table),
            self.require_matching_keys,
            self.sanity_check_joins,
        )

    def _consistent(self) -> List[str]:
        return self.target.j_partitioned_table.consistent_key_columns()

    def _transform(self, operation, consistent=None, other=None, on=None) -> "PartitionedTableProxy":
        """The proxy of what `operation`, a function of a `Table` that calls one of its
        methods, makes of every constituent; with `other`, a table or a proxy, what the
        function of two tables makes of a constituent and that table, or of the two
        constituents of each key.

        `consistent` are the key columns that still are in every constituent as the
        underlying table has them: all that were, if not given. `on` are the matches of
        a join, whose keys are checked when `other` is a proxy.

        Behavior: `PartitionedTableProxyImpl#basicTransform` and `#complexTransform`
        """
        scope = _oxhaven_bootstrap.scope_snapshot()
        handle = self.target.j_partitioned_table
        kept = self._consistent()
        if consistent is not None:
            kept = [name for name in kept if name in consistent]
        inputs = [handle.empty_constituent()]
        other_handle, require, join_keys = None, False, None
        if other is None:
            arguments = [Table(inputs[0])]
        elif isinstance(other, Table):
            arguments = [Table(inputs[0]), other]
        elif isinstance(other, PartitionedTableProxy):
            other_handle = other.target.j_partitioned_table
            inputs.append(other_handle.empty_constituent())
            arguments = [Table(inputs[0]), Table(inputs[1])]
            require = self.require_matching_keys
            if self.sanity_check_joins and on is not None:
                join_keys = _exact_matches(on)
        else:
            raise RuntimeError(
                "java.lang.IllegalArgumentException: Unexpected TableOperations input "
                f"{type(other).__name__}, expected Table or PartitionedTable.Proxy"
            )
        with _engine.Recording(inputs) as recording:
            made = operation(*arguments)
        result = recording.source(made.j_table)
        return self._of(
            _oxhaven.proxy_transform(
                handle,
                other_handle,
                recording.steps,
                result,
                recording.externals,
                scope,
                kept,
                require,
                join_keys,
            )
        )

    def head(self, num_rows: int) -> "PartitionedTableProxy":
        """The first `num_rows` rows of every constituent."""
        try:
            return self._transform(lambda t: t.head(num_rows))
        except Exception as e:
            raise DHError(e, "head operation on the PartitionedTableProxy failed.") from e

    def tail(self, num_rows: int) -> "PartitionedTableProxy":
        """The last `num_rows` rows of every constituent."""
        try:
            return self._transform(lambda t: t.tail(num_rows))
        except Exception as e:
            raise DHError(e, "tail operation on the PartitionedTableProxy failed.") from e

    def reverse(self) -> "PartitionedTableProxy":
        """Every constituent with its rows in the opposite order."""
        try:
            return self._transform(lambda t: t.reverse())
        except Exception as e:
            raise DHError(e, "reverse operation on the PartitionedTableProxy failed.") from e

    def snapshot(self) -> "PartitionedTableProxy":
        """A static copy of every constituent as it is now, or as it is when it
        appears."""
        try:
            return self._transform(lambda t: t.snapshot())
        except Exception as e:
            raise DHError(e, "snapshot operation on the PartitionedTableProxy failed.") from e

    def snapshot_when(
        self,
        trigger_table: Union[Table, "PartitionedTableProxy"],
        stamp_cols: Optional[Union[str, Sequence[str]]] = None,
        initial: bool = False,
        incremental: bool = False,
        history: bool = False,
    ) -> "PartitionedTableProxy":
        """`Table.snapshot_when` of every constituent, with `trigger_table` or, for a
        proxy, with its constituent of the same key as the trigger."""
        try:
            trigger_table.j_object  # the reference reads it, whatever the argument is
            return self._transform(
                lambda t, trigger: t.snapshot_when(
                    trigger, stamp_cols, initial, incremental, history
                ),
                other=trigger_table,
            )
        except Exception as e:
            raise DHError(e, "snapshot_when operation on the PartitionedTableProxy failed.") from e

    def sort(
        self,
        order_by: Union[str, Sequence[str]],
        order: Optional[Union[SortDirection, Sequence[SortDirection]]] = None,
    ) -> "PartitionedTableProxy":
        """Every constituent sorted by the columns, each in its direction, ascending if
        no direction is given.

        As in the reference, `order_by` is not made a sequence: a string names one
        column per character, so only a column whose name is one character can be given
        as a string.
        """
        try:
            if not order:
                order = (SortDirection.ASCENDING,) * len(order_by)
            else:
                order = _engine.to_sequence(order)
                if any(o not in (SortDirection.ASCENDING, SortDirection.DESCENDING) for o in order):
                    raise DHError(
                        message="The sort direction must be either 'ASCENDING' or 'DESCENDING'."
                    )
                if len(order_by) != len(order):
                    raise DHError(
                        message="The number of sort columns must be the same as the number of sort directions."
                    )
            columns, directions = [], []
            for column, direction in zip(order_by, order):
                columns.append(column)
                directions.append(direction)
            return self._transform(lambda t: t.sort(columns, directions))
        except Exception as e:
            raise DHError(e, "sort operation on the PartitionedTableProxy failed.") from e

    def sort_descending(self, order_by: Union[str, Sequence[str]]) -> "PartitionedTableProxy":
        """Every constituent sorted by the columns, each from greatest to least."""
        try:
            order_by = _engine.to_sequence(order_by)
            return self._transform(lambda t: t.sort_descending(order_by))
        except Exception as e:
            raise DHError(e, "sort_descending operation on the PartitionedTableProxy failed.") from e

    def where(
        self, filters: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """The rows of every constituent that pass every filter.

        A filter that reads nothing but key columns which every constituent has, with
        the value of its key, is applied to the underlying table: the constituents of
        the keys that do not pass are not in the result. Every other filter is applied
        to each constituent, which stays in the result when no row of it passes.

        As in the reference, a filter that reads a column at other rows (`Col_[i - 1]`)
        fails for want of a query scope, and a filter that reads only such key columns
        and `i`, `ii` or `k` is applied to the underlying table, where those are its
        row's position and key.

        Behavior: `PartitionedTableProxyImpl#where`
        """
        try:
            filters = [str(f) for f in _engine.to_sequence(filters)]
            if not filters:
                return self._transform(lambda t: t.where())
            scope = _oxhaven_bootstrap.scope_snapshot()
            handle = self.target.j_partitioned_table
            # Java makes every filter for the columns of a constituent first
            Table(handle.empty_constituent()).where(filters)
            reads = _oxhaven.proxy_filter_reads(handle, filters, scope)
            if any(arrays for _, arrays in reads):
                # Observed on Java 42.9 for every filter with a column array
                raise RuntimeError(
                    "io.deephaven.util.ExecutionContextRegistrationException: No "
                    "ExecutionContext registered, or current ExecutionContext has no QueryScope"
                )
            consistent = set(self._consistent())
            on_keys = [set(columns) <= consistent for columns, _ in reads]
            key_filters = [f for f, on_key in zip(filters, on_keys) if on_key]
            others = [f for f, on_key in zip(filters, on_keys) if not on_key]
            if not key_filters:
                return self._transform(lambda t: t.where(others))
            filtered = self._of(self.target.filter(key_filters).j_partitioned_table)
            if not others:
                return filtered
            return filtered._transform(lambda t: t.where(others))
        except Exception as e:
            raise DHError(e, "where operation on the PartitionedTableProxy failed.") from e

    def _where_in(self, filter_table: Table, cols, inverted: bool) -> "PartitionedTableProxy":
        """Behavior: `PartitionedTableProxyImpl#whereIn` and `#whereNotIn`: matches of
        consistent key columns alone are applied to the underlying table."""
        cols = _engine.to_sequence(cols)
        filter_table.j_table  # the reference takes a table only
        handle = self.target.j_partitioned_table
        left, _ = _exact_matches(_split(cols))
        if set(left) <= set(self._consistent()):
            table = self.target.table
            if inverted:
                kept = table.where_not_in(filter_table, cols)
            else:
                kept = table.where_in(filter_table, cols)
            return self._of(handle.with_table(kept.j_table))
        if inverted:
            return self._transform(lambda t, o: t.where_not_in(o, cols), other=filter_table)
        return self._transform(lambda t, o: t.where_in(o, cols), other=filter_table)

    def where_in(
        self, filter_table: Table, cols: Union[str, Sequence[str]]
    ) -> "PartitionedTableProxy":
        """The rows of every constituent whose values in `cols` are in `filter_table`;
        for key columns, the constituents whose keys are."""
        try:
            return self._where_in(filter_table, cols, False)
        except Exception as e:
            raise DHError(e, "where_in operation on the PartitionedTableProxy failed.") from e

    def where_not_in(
        self, filter_table: Table, cols: Union[str, Sequence[str]]
    ) -> "PartitionedTableProxy":
        """The rows of every constituent whose values in `cols` are not in
        `filter_table`; for key columns, the constituents whose keys are not."""
        try:
            return self._where_in(filter_table, cols, True)
        except Exception as e:
            raise DHError(e, "where_not_in operation on the PartitionedTableProxy failed.") from e

    def _all_columns(self, formulas) -> tuple:
        """`formulas`, or the names of every column of a constituent if there is none.

        Behavior: `PartitionedTableProxyImpl#toSelectColumns`
        """
        formulas = _engine.to_sequence(formulas)
        if formulas:
            return formulas
        return tuple(column.name for column in self.target.constituent_table_columns)

    def view(self, formulas: Union[str, Sequence[str]]) -> "PartitionedTableProxy":
        """`Table.view` of every constituent."""
        try:
            formulas = self._all_columns(formulas)
            return self._transform(lambda t: t.view(formulas), _column_names(formulas))
        except Exception as e:
            raise DHError(e, "view operation on the PartitionedTableProxy failed.") from e

    def update_view(self, formulas: Union[str, Sequence[str]]) -> "PartitionedTableProxy":
        """`Table.update_view` of every constituent."""
        try:
            formulas = self._all_columns(formulas)
            kept = set(self._consistent()) - _assigned_names(formulas)
            return self._transform(lambda t: t.update_view(formulas), kept)
        except Exception as e:
            raise DHError(e, "update_view operation on the PartitionedTableProxy failed.") from e

    def update(self, formulas: Union[str, Sequence[str]]) -> "PartitionedTableProxy":
        """`Table.update` of every constituent."""
        try:
            formulas = _engine.to_sequence(formulas)
            formulas[0]  # the reference looks at the first formula, so none is an error
            kept = set(self._consistent()) - _assigned_names(formulas)
            return self._transform(lambda t: t.update(formulas), kept)
        except Exception as e:
            raise DHError(e, "update operation on the PartitionedTableProxy failed.") from e

    def select(
        self, formulas: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """`Table.select` of every constituent. As in the reference, without a formula
        it fails."""
        try:
            formulas = _engine.to_sequence(formulas)
            formulas[0]  # the reference looks at the first formula, so none is an error
            return self._transform(lambda t: t.select(formulas), _column_names(formulas))
        except Exception as e:
            raise DHError(e, "select operation on the PartitionedTableProxy failed.") from e

    def select_distinct(
        self, formulas: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """The distinct rows of the columns or formulas, or of every column if none is
        given, of every constituent."""
        try:
            formulas = _engine.to_sequence(formulas)
            kept = _column_names(formulas) if formulas else None
            return self._transform(lambda t: t.select_distinct(formulas), kept)
        except Exception as e:
            raise DHError(e, "select_distinct operation on the PartitionedTableProxy failed.") from e

    def _join(self, method: str, table, on, joins, exact=None) -> "PartitionedTableProxy":
        """The join `method` of every constituent with `table`, or with the constituent
        of the same key of a proxy. `exact` are the matches that compare for equality,
        all of them if not given."""
        on = _engine.to_sequence(on)
        joins = _engine.to_sequence(joins)
        table.j_object  # the reference reads it, whatever the argument is
        matches = _split(on)
        return self._transform(
            lambda t, o: getattr(t, method)(o, on, joins),
            other=table,
            on=matches if exact is None else exact(matches),
        )

    def natural_join(
        self,
        table: Union[Table, "PartitionedTableProxy"],
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.natural_join` of every constituent."""
        try:
            return self._join("natural_join", table, on, joins)
        except Exception as e:
            raise DHError(e, "natural_join operation on the PartitionedTableProxy failed.") from e

    def exact_join(
        self,
        table: Union[Table, "PartitionedTableProxy"],
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.exact_join` of every constituent."""
        try:
            return self._join("exact_join", table, on, joins)
        except Exception as e:
            raise DHError(e, "exact_join operation on the PartitionedTableProxy failed.") from e

    def join(
        self,
        table: Union[Table, "PartitionedTableProxy"],
        on: Optional[Union[str, Sequence[str]]] = None,
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.join` of every constituent."""
        try:
            return self._join("join", table, on, joins)
        except Exception as e:
            raise DHError(e, "join operation on the PartitionedTableProxy failed.") from e

    def aj(
        self,
        table: Union[Table, "PartitionedTableProxy"],
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.aj` of every constituent. The matches before the last are the join
        key that `sanity_check_joins` checks; without one, only one constituent of each
        partitioned table may have rows."""
        try:
            return self._join("aj", table, on, joins, lambda matches: matches[:-1])
        except Exception as e:
            raise DHError(e, "as-of join operation on the PartitionedTableProxy failed.") from e

    def raj(
        self,
        table: Union[Table, "PartitionedTableProxy"],
        on: Union[str, Sequence[str]],
        joins: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.raj` of every constituent; see `aj`."""
        try:
            return self._join("raj", table, on, joins, lambda matches: matches[:-1])
        except Exception as e:
            raise DHError(
                e, "reverse as-of join operation on the PartitionedTableProxy failed."
            ) from e

    def _aggregated(self, operation) -> "PartitionedTableProxy":
        """An aggregation of every constituent: no key column is as it was.

        Behavior: `PartitionedTableProxyImpl#aggBy` and `#aggAllBy`
        """
        return self._transform(operation, ())

    def group_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.group_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.group_by(by))
        except Exception as e:
            raise DHError(e, "group-by operation on the PartitionedTableProxy failed.") from e

    def agg_by(
        self,
        aggs: Union[Aggregation, Sequence[Aggregation]],
        by: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.agg_by` of every constituent."""
        try:
            aggs = _engine.to_sequence(aggs)
            return self._aggregated(lambda t: t.agg_by(aggs, by))
        except Exception as e:
            raise DHError(e, "agg_by operation on the PartitionedTableProxy failed.") from e

    def agg_all_by(
        self, agg: Aggregation, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """`Table.agg_all_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.agg_all_by(agg, by))
        except Exception as e:
            raise DHError(e, "agg_all_by operation on the PartitionedTableProxy failed.") from e

    def count_by(
        self, col: str, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """`Table.count_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.count_by(col, by))
        except Exception as e:
            raise DHError(e, "count_by operation on the PartitionedTableProxy failed.") from e

    def first_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.first_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.first_by(by))
        except Exception as e:
            raise DHError(e, "first_by operation on the PartitionedTableProxy failed.") from e

    def last_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.last_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.last_by(by))
        except Exception as e:
            raise DHError(e, "last_by operation on the PartitionedTableProxy failed.") from e

    def min_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.min_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.min_by(by))
        except Exception as e:
            raise DHError(e, "min_by operation on the PartitionedTableProxy failed.") from e

    def max_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.max_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.max_by(by))
        except Exception as e:
            raise DHError(e, "max_by operation on the PartitionedTableProxy failed.") from e

    def sum_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.sum_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.sum_by(by))
        except Exception as e:
            raise DHError(e, "sum_by operation on the PartitionedTableProxy failed.") from e

    def abs_sum_by(
        self, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """`Table.abs_sum_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.abs_sum_by(by))
        except Exception as e:
            raise DHError(e, "sum_by operation on the PartitionedTableProxy failed.") from e

    def weighted_sum_by(
        self, wcol: str, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """`Table.weighted_sum_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.weighted_sum_by(wcol, by))
        except Exception as e:
            raise DHError(e, "sum_by operation on the PartitionedTableProxy failed.") from e

    def avg_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.avg_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.avg_by(by))
        except Exception as e:
            raise DHError(e, "avg_by operation on the PartitionedTableProxy failed.") from e

    def weighted_avg_by(
        self, wcol: str, by: Optional[Union[str, Sequence[str]]] = None
    ) -> "PartitionedTableProxy":
        """`Table.weighted_avg_by` of every constituent, when `by` names a column.

        As in the reference, without `by` it is `avg_by(wcol)`: the plain average of
        every other column, grouped by the weight column.
        """
        try:
            if _engine.to_sequence(by):
                return self._aggregated(lambda t: t.weighted_avg_by(wcol, by))
            return self._aggregated(lambda t: t.avg_by(wcol))
        except Exception as e:
            raise DHError(e, "avg_by operation on the PartitionedTableProxy failed.") from e

    def median_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.median_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.median_by(by))
        except Exception as e:
            raise DHError(e, "median_by operation on the PartitionedTableProxy failed.") from e

    def std_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.std_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.std_by(by))
        except Exception as e:
            raise DHError(e, "std_by operation on the PartitionedTableProxy failed.") from e

    def var_by(self, by: Optional[Union[str, Sequence[str]]] = None) -> "PartitionedTableProxy":
        """`Table.var_by` of every constituent."""
        try:
            return self._aggregated(lambda t: t.var_by(by))
        except Exception as e:
            raise DHError(e, "var_by operation on the PartitionedTableProxy failed.") from e

    def update_by(
        self,
        ops: Union["UpdateByOperation", Sequence["UpdateByOperation"]],
        by: Optional[Union[str, Sequence[str]]] = None,
    ) -> "PartitionedTableProxy":
        """`Table.update_by` of every constituent."""
        try:
            ops = _engine.to_sequence(ops)
            by = _engine.to_sequence(by)
            # Behavior: `UpdateBy.UpdateByOperatorCollection#getPreservedColumnNames`:
            # the columns that no operation writes
            kept = set(self._consistent())
            for op in ops:
                pairs = op.j_updateby_op.get("column", {}).get("match_pairs", ())
                if pairs:
                    kept -= {str(pair).partition("=")[0].strip(_JAVA_SPACE) for pair in pairs}
                else:
                    kept &= set(by)
            return self._transform(lambda t: t.update_by(ops, by), kept)
        except Exception as e:
            raise DHError(e, "update-by operation on the PartitionedTableProxy failed.") from e
