"""The aggregations of `Table.agg_by`, `Table.agg_all_by` and `Table.range_join`.

Each function returns an `Aggregation` that renders itself as the `Aggregation` message
of the table service, or as its `AggSpec` for `agg_all_by`, in protobuf JSON form.
"""

from typing import Any, Optional, Sequence, Union

from . import _engine
from .dherror import DHError


class Aggregation:
    """One aggregation: a specification applied to columns, or an aggregation that names
    its own result column (a count, a count where, a partition, a formula).

    Behavior: `deephaven.agg.Aggregation`. `j_aggregation` and `j_agg_spec` are the
    messages of the table service where the reference has Java objects.
    """

    def __init__(
        self,
        j_agg_spec: Optional[dict] = None,
        j_aggregation: Optional[dict] = None,
        cols: Optional[Union[str, Sequence[str]]] = None,
    ):
        self._j_agg_spec = j_agg_spec
        self._j_aggregation = j_aggregation
        self._cols = _engine.to_sequence(cols)
        # set by `formula`; Java refuses such a formula everywhere but in a rollup
        self._reaggregating = False

    @property
    def j_aggregation(self) -> dict:
        """The `Aggregation` message."""
        if self._j_aggregation:
            return self._j_aggregation
        if not self._cols:
            raise DHError(message="No columns specified for the aggregation operation.")
        return {"columns": {"spec": self._j_agg_spec, "match_pairs": list(self._cols)}}

    @property
    def j_agg_spec(self) -> dict:
        """The `AggSpec` message, which an aggregation that names its result does not have."""
        if self._j_aggregation:
            raise DHError(message="unsupported aggregation operation.")
        return self._j_agg_spec

    @property
    def is_formula(self) -> bool:
        """Whether this is a formula aggregation, in either form."""
        if self._j_agg_spec is not None:
            return "formula" in self._j_agg_spec
        if self._j_aggregation is not None:
            return "formula" in self._j_aggregation
        raise DHError(
            message="Aggregation object is not initialized with a valid aggregation specification or "
            "aggregation object."
        )


def _spec(name: str, cols, **fields) -> Aggregation:
    return Aggregation(j_agg_spec={name: fields}, cols=cols)


def _column(name: str) -> str:
    """`name`, which must be able to name a column as it is: the reference makes the Java
    object of a column that an aggregation names itself in the function, and Java refuses
    the name there.

    Behavior: `io.deephaven.api.ColumnName#of`
    """
    _engine.native.column_name(name)
    return name


def _result_column(col, function: str) -> str:
    if not isinstance(col, str):
        raise DHError(
            message=f"{function} aggregation requires a string value for the 'col' argument."
        )
    return _column(col)


def _filters(filters) -> list:
    """The filters of a count where, which are all to pass. Only strings: the reference
    also takes `deephaven.filters.Filter` objects."""
    filters = _engine.to_sequence(filters)
    for item in filters:
        if not isinstance(item, str):
            raise TypeError(f"a filter must be a string, not {type(item).__name__}")
    return list(filters)


def _sentinel(value) -> dict:
    """The value a `unique` aggregation gives a group with several values, typed as the
    reference types a Python value it hands to Java: an integer is the smallest of `byte`,
    `short`, `int` and `long` that holds it, a float a `double`.

    Behavior: jpy's conversion of a Python object to `java.lang.Object`, and
    `io.deephaven.api.object.UnionObject#of`
    """
    if value is None:
        return {"null_value": "NULL_VALUE"}
    if isinstance(value, bool):
        return {"bool_value": value}
    if isinstance(value, int):
        for field, bits in (("byte_value", 8), ("short_value", 16), ("int_value", 32)):
            if -(1 << (bits - 1)) <= value < 1 << (bits - 1):
                return {field: value}
        if -(1 << 63) <= value < 1 << 63:
            return {"long_value": str(value)}
        raise OverflowError("int too big to convert")
    if isinstance(value, float):
        return {"double_value": value}
    if isinstance(value, str):
        return {"string_value": value}
    raise TypeError(f"unsupported non-unique sentinel: {type(value).__name__}")


def sum_(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The sum of each column."""
    return _spec("sum", cols)


def abs_sum(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The sum of the absolute values of each column."""
    return _spec("abs_sum", cols)


def group(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The values of each column as one array per group."""
    return _spec("group", cols)


def avg(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The average of each column."""
    return _spec("avg", cols)


def count_(col: str) -> Aggregation:
    """The number of rows of each group, in the column `col`."""
    col = _result_column(col, "count_")
    return Aggregation(j_aggregation={"count": {"column_name": col}})


def count_where(col: str, filters: Union[str, Sequence[str]]) -> Aggregation:
    """The number of rows of each group that pass every filter, in the column `col`."""
    col = _result_column(col, "count_where")
    return Aggregation(
        j_aggregation={"count_where": {"column_name": col, "filters": _filters(filters)}}
    )


def partition(col: str, include_by_columns: bool = True) -> Aggregation:
    """The rows of each group as a table, in the column `col`."""
    col = _result_column(col, "partition")
    return Aggregation(
        j_aggregation={
            "partition": {
                "column_name": col,
                "include_group_by_columns": bool(include_by_columns),
            }
        }
    )


def count_distinct(
    cols: Optional[Union[str, Sequence[str]]] = None, count_nulls: bool = False
) -> Aggregation:
    """The number of distinct values of each column."""
    return _spec("count_distinct", cols, count_nulls=bool(count_nulls))


def first(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The first value of each column."""
    return _spec("first", cols)


def formula(
    formula: str,
    formula_param: Optional[str] = None,
    cols: Optional[Union[str, Sequence[str]]] = None,
    reaggregating: bool = False,
) -> Aggregation:
    """A formula over the values of a group: `"Out = sum(A) + max(B)"` over the columns as
    arrays, or with `formula_param` one formula applied to each of `cols`, which the
    parameter stands for."""
    if formula_param:
        return _spec("formula", cols, formula=formula, param_token=formula_param)
    if cols:
        raise DHError(
            message="The 'cols' argument is only valid when 'formula_param' is provided."
        )
    aggregation = Aggregation(j_aggregation={"formula": {"selectable": {"raw": formula}}})
    aggregation._reaggregating = bool(reaggregating)
    return aggregation


def last(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The last value of each column."""
    return _spec("last", cols)


def min_(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The least value of each column."""
    return _spec("min", cols)


def max_(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The greatest value of each column."""
    return _spec("max", cols)


def median(
    cols: Optional[Union[str, Sequence[str]]] = None,
    average_evenly_divided: bool = True,
) -> Aggregation:
    """The median of each column; of an even number of values the average of the two in
    the middle, or the lower one without `average_evenly_divided`."""
    return _spec("median", cols, average_evenly_divided=bool(average_evenly_divided))


def pct(
    percentile: float,
    cols: Optional[Union[str, Sequence[str]]] = None,
    average_evenly_divided: bool = False,
) -> Aggregation:
    """The value at `percentile`, from 0 to 1, of each column."""
    return _spec(
        "percentile",
        cols,
        percentile=float(percentile),
        average_evenly_divided=bool(average_evenly_divided),
    )


def _sorted(order_by) -> list:
    return [{"column_name": _column(name)} for name in _engine.to_sequence(order_by)]


def sorted_first(order_by: str, cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The value of each column in the row that comes first when sorted by `order_by`."""
    return _spec("sorted_first", cols, columns=_sorted(order_by))


def sorted_last(order_by: str, cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The value of each column in the row that comes last when sorted by `order_by`."""
    return _spec("sorted_last", cols, columns=_sorted(order_by))


def std(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The sample standard deviation of each column."""
    return _spec("std", cols)


def unique(
    cols: Optional[Union[str, Sequence[str]]] = None,
    include_nulls: bool = False,
    non_unique_sentinel: Optional[Any] = None,
) -> Aggregation:
    """The one value of each column if a group has only one, else null or
    `non_unique_sentinel`; with `include_nulls` a null counts as a value."""
    if non_unique_sentinel is None and include_nulls:
        raise DHError(
            message="when include_nulls is True, a non-null sentinel value must be provided."
        )
    return _spec(
        "unique",
        cols,
        include_nulls=bool(include_nulls),
        non_unique_sentinel=_sentinel(non_unique_sentinel),
    )


def var(cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The sample variance of each column."""
    return _spec("var", cols)


def weighted_avg(wcol: str, cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The average of each column weighted by the column `wcol`."""
    return _spec("weighted_avg", cols, weight_column=_column(wcol))


def weighted_sum(wcol: str, cols: Optional[Union[str, Sequence[str]]] = None) -> Aggregation:
    """The sum of each column weighted by the column `wcol`."""
    return _spec("weighted_sum", cols, weight_column=_column(wcol))


def distinct(
    cols: Optional[Union[str, Sequence[str]]] = None, include_nulls: bool = False
) -> Aggregation:
    """The distinct values of each column as one array per group."""
    return _spec("distinct", cols, include_nulls=bool(include_nulls))
