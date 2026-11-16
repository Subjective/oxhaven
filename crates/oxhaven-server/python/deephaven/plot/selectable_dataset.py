"""Data sets a user of the figure chooses a part of, with the one-click filter of the IDE.

Not ported.

Behavior: `io.deephaven.plot.filters.Selectables`
"""

from typing import List

from deephaven.table import PartitionedTable, Table


class SelectableDataSet:
    """A table together with the columns a user filters it by."""

    def __init__(self, j_sds=None):
        raise NotImplementedError("not ported: io.deephaven.plot.filters.Selectables#oneClick")


def one_click(t: Table, by: List[str] = None, require_all_filters: bool = False) -> SelectableDataSet:
    """A data set of a table, filtered by what the user chooses for the `by` columns."""
    raise NotImplementedError("not ported: io.deephaven.plot.filters.Selectables#oneClick")


def one_click_partitioned_table(pt: PartitionedTable, require_all_filters: bool = False) -> SelectableDataSet:
    """A data set of a partitioned table, of which the user chooses a constituent."""
    raise NotImplementedError("not ported: io.deephaven.plot.filters.Selectables#oneClick")
