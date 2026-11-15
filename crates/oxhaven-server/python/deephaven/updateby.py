"""The operations of `Table.update_by`.

Each function returns an `UpdateByOperation` that holds the `UpdateByOperation` message of
the table service's `UpdateByRequest` in protobuf JSON form. The message is checked when
it is made, with the checks the table service makes of a request, so that a window that
cannot be, or a column pair that cannot be read, fails where the reference fails: in the
function, not in `update_by`.
"""

import json
from enum import Enum
from typing import Optional, Sequence, Union

from . import _engine
from .agg import _filters
from .dherror import DHError


class MathContext(Enum):
    """The precision and rounding of an operation on `BigDecimal` or `BigInteger` values.
    The values are the `MathContext` messages of the table service.

    Behavior: `deephaven.updateby.MathContext`, the constants of `java.math.MathContext`
    """

    UNLIMITED = (0, "HALF_UP")
    DECIMAL32 = (7, "HALF_EVEN")
    DECIMAL64 = (16, "HALF_EVEN")
    DECIMAL128 = (34, "HALF_EVEN")


class BadDataBehavior(Enum):
    """What an exponential moving operation does with a null or a NaN: restart, skip the
    value, fail, or give NaN from then on.

    Behavior: `deephaven.updateby.BadDataBehavior`
    """

    RESET = "RESET"
    SKIP = "SKIP"
    THROW = "THROW"
    POISON = "POISON"


class DeltaControl(Enum):
    """What `delta` gives for a value that follows a null: null, the value, or the value
    as if the one before were zero.

    Behavior: `deephaven.updateby.DeltaControl`
    """

    NULL_DOMINATES = "NULL_DOMINATES"
    VALUE_DOMINATES = "VALUE_DOMINATES"
    ZERO_DOMINATES = "ZERO_DOMINATES"


class OperationControl:
    """How an exponential moving operation treats nulls and NaNs, and its math context.

    Behavior: `deephaven.updateby.OperationControl`. `j_op_control` is the
    `UpdateByEmOptions` message where the reference has a Java object.
    """

    def __init__(
        self,
        on_null: BadDataBehavior = BadDataBehavior.SKIP,
        on_nan: BadDataBehavior = BadDataBehavior.SKIP,
        big_value_context: MathContext = MathContext.DECIMAL128,
    ):
        try:
            precision, rounding_mode = big_value_context.value
            self.j_op_control = {
                "on_null_value": on_null.value,
                "on_nan_value": on_nan.value,
                "big_value_context": {"precision": precision, "rounding_mode": rounding_mode},
            }
        except Exception as e:
            raise DHError(e, "failed to build an OperationControl object.") from e

    @property
    def j_object(self) -> dict:
        return self.j_op_control


class UpdateByOperation:
    """One operation of `Table.update_by`.

    Behavior: `deephaven.updateby.UpdateByOperation`. `j_updateby_op` is the
    `UpdateByOperation` message where the reference has a Java object.
    """

    def __init__(self, j_updateby_op: dict):
        self.j_updateby_op = j_updateby_op

    @property
    def j_object(self) -> dict:
        return self.j_updateby_op


def _operation(spec: str, fields: dict, cols=None) -> UpdateByOperation:
    """The operation of the spec `spec` on the column pairs `cols`, checked as Java checks
    one when it is made.

    Behavior: the `clause` method and the `@Check` methods of the spec classes in
    `io.deephaven.api.updateby.spec`
    """
    pairs = list(_engine.to_sequence(cols))
    message = {"column": {"spec": {spec: fields}, "match_pairs": pairs}}
    _engine.native.check_update_by_operation(json.dumps(message))
    return UpdateByOperation(j_updateby_op=message)


def _ticks(ticks, whole: bool) -> dict:
    """A window or a decay in rows. Java takes a `double` for a decay and a `long` for a
    side of a rolling window."""
    kinds = (int,) if whole else (int, float)
    if isinstance(ticks, bool) or not isinstance(ticks, kinds):
        expected = "an int" if whole else "a number"
        raise TypeError(f"a number of ticks must be {expected}, not {type(ticks).__name__}")
    return {"ticks": {"ticks": float(ticks)}}


def _time(ts_col: str, time) -> dict:
    """A window or a decay in time on the column `ts_col`: an integer in nanoseconds, or a
    duration string such as `"PT5m"` or `"PT00:05:00"`.

    Behavior: `io.deephaven.time.DateTimeUtils#parseDurationNanos` for a string
    """
    if isinstance(time, str):
        nanos = _engine.native.parse_duration_nanos(time)
    elif isinstance(time, int) and not isinstance(time, bool):
        nanos = time
    else:
        raise TypeError(f"a time must be an int or a str, not {type(time).__name__}")
    if not isinstance(ts_col, str):
        raise TypeError(f"a timestamp column must be a str, not {type(ts_col).__name__}")
    return {"time": {"column": ts_col, "nanos": str(nanos)}}


def _em(spec: str, scale, cols, op_control: Optional[OperationControl]) -> UpdateByOperation:
    fields = {"window_scale": scale()}
    if op_control is not None:
        fields["options"] = op_control.j_op_control
    return _operation(spec, fields, cols)


def _tick_window(rev_ticks, fwd_ticks) -> dict:
    return {
        "reverse_window_scale": _ticks(rev_ticks, True),
        "forward_window_scale": _ticks(fwd_ticks, True),
    }


def _time_window(ts_col, rev_time, fwd_time) -> dict:
    return {
        "reverse_window_scale": _time(ts_col, rev_time),
        "forward_window_scale": _time(ts_col, fwd_time),
    }


def _result_column(col) -> None:
    if not isinstance(col, str):
        raise DHError(
            message="count_where aggregation requires a string value for the 'col' argument."
        )


def ema_tick(
    decay_ticks: float,
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving average with a decay in rows."""
    try:
        return _em("ema", lambda: _ticks(decay_ticks, False), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EMA UpdateByOperation.") from e


def ema_time(
    ts_col: str,
    decay_time: Union[int, str],
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving average with a decay in time on the column `ts_col`."""
    try:
        return _em("ema", lambda: _time(ts_col, decay_time), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EMA UpdateByOperation.") from e


def ems_tick(
    decay_ticks: float,
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving sum with a decay in rows."""
    try:
        return _em("ems", lambda: _ticks(decay_ticks, False), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EMS UpdateByOperation.") from e


def ems_time(
    ts_col: str,
    decay_time: Union[int, str],
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving sum with a decay in time on the column `ts_col`."""
    try:
        return _em("ems", lambda: _time(ts_col, decay_time), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EMS UpdateByOperation.") from e


def emmin_tick(
    decay_ticks: float,
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving minimum with a decay in rows."""
    try:
        return _em("em_min", lambda: _ticks(decay_ticks, False), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EM Min UpdateByOperation.") from e


def emmin_time(
    ts_col: str,
    decay_time: Union[int, str],
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving minimum with a decay in time on the column `ts_col`."""
    try:
        return _em("em_min", lambda: _time(ts_col, decay_time), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EM Min UpdateByOperation.") from e


def emmax_tick(
    decay_ticks: float,
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving maximum with a decay in rows."""
    try:
        return _em("em_max", lambda: _ticks(decay_ticks, False), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EM Max UpdateByOperation.") from e


def emmax_time(
    ts_col: str,
    decay_time: Union[int, str],
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving maximum with a decay in time on the column `ts_col`."""
    try:
        return _em("em_max", lambda: _time(ts_col, decay_time), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EM Max UpdateByOperation.") from e


def emstd_tick(
    decay_ticks: float,
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving standard deviation with a decay in rows."""
    try:
        return _em("em_std", lambda: _ticks(decay_ticks, False), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EM Std UpdateByOperation.") from e


def emstd_time(
    ts_col: str,
    decay_time: Union[int, str],
    cols: Union[str, Sequence[str]],
    op_control: Optional[OperationControl] = None,
) -> UpdateByOperation:
    """An exponential moving standard deviation with a decay in time on the column
    `ts_col`."""
    try:
        return _em("em_std", lambda: _time(ts_col, decay_time), cols, op_control)
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EM Std UpdateByOperation.") from e


def cum_sum(cols: Union[str, Sequence[str]]) -> UpdateByOperation:
    """The sum of the values up to and including each row."""
    try:
        return _operation("sum", {}, cols)
    except Exception as e:
        raise DHError(e, "failed to create a cumulative sum UpdateByOperation.") from e


def cum_prod(cols: Union[str, Sequence[str]]) -> UpdateByOperation:
    """The product of the values up to and including each row."""
    try:
        return _operation("product", {}, cols)
    except Exception as e:
        raise DHError(e, "failed to create a cumulative product UpdateByOperation.") from e


def cum_min(cols: Union[str, Sequence[str]]) -> UpdateByOperation:
    """The least of the values up to and including each row."""
    try:
        return _operation("min", {}, cols)
    except Exception as e:
        raise DHError(e, "failed to create a cumulative minimum UpdateByOperation.") from e


def cum_max(cols: Union[str, Sequence[str]]) -> UpdateByOperation:
    """The greatest of the values up to and including each row."""
    try:
        return _operation("max", {}, cols)
    except Exception as e:
        raise DHError(e, "failed to create a cumulative maximum UpdateByOperation.") from e


def cum_count_where(col: str, filters: Union[str, Sequence[str]]) -> UpdateByOperation:
    """The number of rows up to and including each row that pass every filter, in the
    column `col`."""
    _result_column(col)
    filters = _engine.to_sequence(filters)
    try:
        fields = {"result_column": col, "filters": _filters(filters)}
        return _operation("count_where", fields)
    except Exception as e:
        raise DHError(e, "failed to create a cumulative count_where UpdateByOperation.") from e


def forward_fill(cols: Union[str, Sequence[str]]) -> UpdateByOperation:
    """Each null replaced by the last value before it that is not null."""
    try:
        return _operation("fill", {}, cols)
    except Exception as e:
        raise DHError(e, "failed to create a forward fill UpdateByOperation.") from e


def delta(
    cols: Union[str, Sequence[str]],
    delta_control: DeltaControl = DeltaControl.NULL_DOMINATES,
) -> UpdateByOperation:
    """The difference between each value and the one before it."""
    try:
        options = {"null_behavior": delta_control.value}
        return _operation("delta", {"options": options}, cols)
    except Exception as e:
        raise DHError(e, "failed to create a delta UpdateByOperation.") from e


def rolling_sum_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The sum over a window of rows: `rev_ticks` rows back, the current row among them,
    and `fwd_ticks` rows ahead."""
    try:
        return _operation("rolling_sum", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling sum (tick) UpdateByOperation.") from e


def rolling_sum_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The sum over a window of time on the column `ts_col`: `rev_time` back and
    `fwd_time` ahead."""
    try:
        return _operation("rolling_sum", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling sum (time) UpdateByOperation.") from e


def rolling_group_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The values of a window of rows as an array."""
    try:
        return _operation("rolling_group", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling group (tick) UpdateByOperation.") from e


def rolling_group_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The values of a window of time as an array."""
    try:
        return _operation("rolling_group", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling group (time) UpdateByOperation.") from e


def rolling_avg_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The average over a window of rows."""
    try:
        return _operation("rolling_avg", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling average (tick) UpdateByOperation.") from e


def rolling_avg_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The average over a window of time."""
    try:
        return _operation("rolling_avg", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling average (time) UpdateByOperation.") from e


def rolling_min_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The least value of a window of rows."""
    try:
        return _operation("rolling_min", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling minimum (tick) UpdateByOperation.") from e


def rolling_min_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The least value of a window of time."""
    try:
        return _operation("rolling_min", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling minimum (time) UpdateByOperation.") from e


def rolling_max_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The greatest value of a window of rows."""
    try:
        return _operation("rolling_max", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling maximum (tick) UpdateByOperation.") from e


def rolling_max_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The greatest value of a window of time."""
    try:
        return _operation("rolling_max", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling maximum (time) UpdateByOperation.") from e


def rolling_prod_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The product over a window of rows."""
    try:
        return _operation("rolling_product", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling product (tick) UpdateByOperation.") from e


def rolling_prod_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The product over a window of time."""
    try:
        return _operation("rolling_product", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling product (time) UpdateByOperation.") from e


def rolling_count_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The number of values that are not null in a window of rows."""
    try:
        return _operation("rolling_count", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling count (tick) UpdateByOperation.") from e


def rolling_count_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The number of values that are not null in a window of time."""
    try:
        return _operation("rolling_count", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling count (time) UpdateByOperation.") from e


def rolling_std_tick(
    cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The sample standard deviation over a window of rows."""
    try:
        return _operation("rolling_std", _tick_window(rev_ticks, fwd_ticks), cols)
    except Exception as e:
        raise DHError(
            e, "failed to create a rolling standard deviation (tick) UpdateByOperation."
        ) from e


def rolling_std_time(
    ts_col: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The sample standard deviation over a window of time."""
    try:
        return _operation("rolling_std", _time_window(ts_col, rev_time, fwd_time), cols)
    except Exception as e:
        raise DHError(
            e, "failed to create a rolling standard deviation (time) UpdateByOperation."
        ) from e


def rolling_wavg_tick(
    wcol: str, cols: Union[str, Sequence[str]], rev_ticks: int, fwd_ticks: int = 0
) -> UpdateByOperation:
    """The average, weighted by the column `wcol`, over a window of rows."""
    try:
        fields = {**_tick_window(rev_ticks, fwd_ticks), "weight_column": wcol}
        return _operation("rolling_wavg", fields, cols)
    except Exception as e:
        raise DHError(
            e, "failed to create a rolling weighted average (tick) UpdateByOperation."
        ) from e


def rolling_wavg_time(
    ts_col: str,
    wcol: str,
    cols: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The average, weighted by the column `wcol`, over a window of time."""
    try:
        fields = {**_time_window(ts_col, rev_time, fwd_time), "weight_column": wcol}
        return _operation("rolling_wavg", fields, cols)
    except Exception as e:
        raise DHError(
            e, "failed to create a rolling weighted average (time) UpdateByOperation."
        ) from e


def _formula(window: dict, formula: str, formula_param: Optional[str], cols) -> UpdateByOperation:
    if formula_param is None:
        # a formula that names its own result column and any columns of the window
        return _operation("rolling_formula", {**window, "formula": formula})
    fields = {**window, "formula": formula, "param_token": formula_param}
    return _operation("rolling_formula", fields, cols)


def rolling_formula_tick(
    formula: str,
    formula_param: Optional[str] = None,
    cols: Optional[Union[str, Sequence[str]]] = None,
    rev_ticks: int = 0,
    fwd_ticks: int = 0,
) -> UpdateByOperation:
    """A formula over a window of rows: `"Out = sum(A) + max(B)"` over the columns as
    arrays, or with `formula_param` one formula applied to each of `cols`."""
    try:
        return _formula(_tick_window(rev_ticks, fwd_ticks), formula, formula_param, cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling formula (tick) UpdateByOperation.") from e


def rolling_formula_time(
    ts_col: str,
    formula: str,
    formula_param: Optional[str] = None,
    cols: Optional[Union[str, Sequence[str]]] = None,
    rev_time: Union[int, str] = 0,
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """A formula over a window of time, in the forms of `rolling_formula_tick`."""
    try:
        window = _time_window(ts_col, rev_time, fwd_time)
        return _formula(window, formula, formula_param, cols)
    except Exception as e:
        raise DHError(e, "failed to create a rolling formula (time) UpdateByOperation.") from e


def rolling_count_where_tick(
    col: str,
    filters: Union[str, Sequence[str]],
    rev_ticks: int,
    fwd_ticks: int = 0,
) -> UpdateByOperation:
    """The number of rows of a window of rows that pass every filter, in the column
    `col`."""
    _result_column(col)
    filters = _engine.to_sequence(filters)
    try:
        fields = {
            **_tick_window(rev_ticks, fwd_ticks),
            "result_column": col,
            "filters": _filters(filters),
        }
        return _operation("rolling_count_where", fields)
    except Exception as e:
        raise DHError(e, "failed to create a rolling count_where UpdateByOperation.") from e


def rolling_count_where_time(
    ts_col: str,
    col: str,
    filters: Union[str, Sequence[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str] = 0,
) -> UpdateByOperation:
    """The number of rows of a window of time that pass every filter, in the column
    `col`."""
    _result_column(col)
    filters = _engine.to_sequence(filters)
    try:
        fields = {
            **_time_window(ts_col, rev_time, fwd_time),
            "result_column": col,
            "filters": _filters(filters),
        }
        return _operation("rolling_count_where", fields)
    except Exception as e:
        raise DHError(e, "failed to create a rolling count_where UpdateByOperation.") from e
