"""The `Table` class."""

from typing import List, Sequence, Union

import _oxhaven

from . import _engine
from .dherror import DHError


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

    @property
    def j_object(self):
        """The engine's handle. In the reference package this is the Java table."""
        return self.j_table

    def __repr__(self):
        return f"deephaven.table.Table(size={self.size}, columns={self.column_names})"

    @property
    def size(self) -> int:
        """The current number of rows in the table."""
        return self.j_table.size()

    @property
    def is_refreshing(self) -> bool:
        """Whether this table is refreshing."""
        return self.j_table.is_refreshing()

    @property
    def column_names(self) -> List[str]:
        """The column names of the table."""
        return [name for name, _ in self.j_table.columns()]

    def _op(self, name: str, request: dict, failure: str) -> "Table":
        try:
            request = {"source_id": _engine.source(0), **request}
            return Table(_engine.table_op(name, request, [self.j_table]))
        except Exception as e:
            raise DHError(e, failure) from e

    def _select_op(self, name: str, formulas, failure: str) -> "Table":
        return self._op(name, {"column_specs": _engine.to_sequence(formulas)}, failure)

    def update(self, formulas: Union[str, Sequence[str]]) -> "Table":
        """A new table with the columns of this one and one column per formula, computed
        once and kept in memory."""
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

    def where(self, filters: Union[str, Sequence[str], None] = None) -> "Table":
        """A new table with the rows that pass every filter."""
        return self._op(
            "unstructured_filter",
            {"filters": _engine.to_sequence(filters)},
            "table where operation failed.",
        )

    def head(self, num_rows: int) -> "Table":
        """A new table with the first `num_rows` rows."""
        return self._op("head", {"num_rows": int(num_rows)}, "table head operation failed.")

    def tail(self, num_rows: int) -> "Table":
        """A new table with the last `num_rows` rows."""
        return self._op("tail", {"num_rows": int(num_rows)}, "table tail operation failed.")
