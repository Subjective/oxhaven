"""The `Table` class."""

from collections import namedtuple
from collections.abc import Iterable, Mapping
from enum import Enum, auto
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union

import _oxhaven

from . import _engine, dtypes
from .column import ColumnDefinition, col_def
from .dherror import DHError


class SortDirection(Enum):
    """The direction of a sort on one column.

    Behavior: `deephaven.table.SortDirection`
    """

    DESCENDING = auto()
    ASCENDING = auto()


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
                    name, dtypes._from_java_name(type_name), dtypes._from_java_name(component)
                )
                for name, type_name, component in _engine.native.column_definitions(table_definition)
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
            handle = _engine.new_table(
                [(column.name, dtypes._engine_type(column.data_type), []) for column in self.values()]
            )
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


class Table:
    """A Deephaven table: a handle to a table of the engine.

    Every method that returns a table creates a new one; a table never changes because of
    an operation on it.

    Behavior: `deephaven.table.Table`
    """

    def __init__(self, j_table):
        if not isinstance(j_table, _oxhaven.TableHandle):
            raise DHError(message=f"not a table handle: {type(j_table).__name__}")
        self.j_table = j_table
        self._definition = TableDefinition(j_table)

    @property
    def j_object(self):
        """The engine's handle. In the reference package this is the Java table."""
        return self.j_table

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
        """The attributes of the table. Not available yet: the engine keeps no attribute
        map (`io.deephaven.engine.table.AttributeMap#getAttributes`)."""
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
        return self._op(
            "unstructured_filter",
            {"filters": _engine.to_sequence(filters)},
            "table where operation failed.",
        )

    def _where_in(self, filter_table: "Table", cols, inverted: bool, failure: str) -> "Table":
        try:
            request = {
                "left_id": _engine.source(0),
                "right_id": _engine.source(1),
                "inverted": inverted,
                "columns_to_match": _engine.to_sequence(cols),
            }
            handles = [self.j_table, filter_table.j_table]
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
            return self._op("unstructured_filter", {"filters": filters}, failure)
        return self._local_op("where_one_of", {"filters": filters}, failure)

    def head(self, num_rows: int) -> "Table":
        """A new table with the first `num_rows` rows."""
        self._positions("table head operation failed.", num_rows)
        return self._op("head", {"num_rows": int(num_rows)}, "table head operation failed.")

    def head_pct(self, pct: float) -> "Table":
        """A new table with the first `pct` of the rows, `pct` from 0 to 1."""
        return self._local_op("head_pct", {"pct": pct}, "table head_pct operation failed.")

    def tail(self, num_rows: int) -> "Table":
        """A new table with the last `num_rows` rows."""
        self._positions("table tail operation failed.", num_rows)
        return self._op("tail", {"num_rows": int(num_rows)}, "table tail operation failed.")

    def tail_pct(self, pct: float) -> "Table":
        """A new table with the last `pct` of the rows, `pct` from 0 to 1."""
        return self._local_op("tail_pct", {"pct": pct}, "table tail_pct operation failed.")

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
            {"start_pct": start_pct, "end_pct": end_pct},
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
