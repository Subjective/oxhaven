//! Checks the `deephaven` package makes when it builds an argument object, before any
//! table operation runs.
//!
//! The reference package builds Java objects in `deephaven.agg` and `deephaven.updateby`,
//! and Java refuses a column name that cannot be one, a duration it cannot read or a
//! window that cannot be at that moment: in the function that makes the object, not in
//! the table method that is given it later. The objects of this package are messages of
//! the table service, which nothing reads until the operation runs, so the package asks
//! here for the checks the table service makes of a request, at the moment the reference
//! makes its own. The answers are those of the handlers: nothing is checked twice in
//! two ways.

use std::sync::OnceLock;

use oxhaven_proto::grpc::update_by_request::UpdateByOperation;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::table::ops::{check_update_by_operation as check_operation, parse_duration_nanos};
use crate::table::support::validate_column_name;

/// Raises unless `name` can name a column, as it is given: `RuntimeError` with the
/// message of Java's `InvalidNameException`.
///
/// Behavior: `io.deephaven.api.ColumnName#of`
#[pyfunction]
fn column_name(name: &str) -> PyResult<()> {
    validate_column_name(name).map_err(PyRuntimeError::new_err)
}

/// The nanoseconds of a duration string, `PT1h30m` or `PT01:30:00`.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseDurationNanos`
#[pyfunction(name = "parse_duration_nanos")]
fn duration_nanos(text: &str) -> PyResult<i64> {
    parse_duration_nanos(text)
        .map_err(|e| PyRuntimeError::new_err(format!("Cannot parse duration: {text}: {e}")))
}

/// The message type of one operation of an `UpdateByRequest`.
fn update_by_operation_descriptor() -> &'static MessageDescriptor {
    static DESCRIPTOR: OnceLock<MessageDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        DescriptorPool::decode(oxhaven_proto::FILE_DESCRIPTOR_SET)
            .expect("the descriptors the stubs were generated from decode")
            .get_message_by_name(
                "io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation",
            )
            .expect("an update by request has operations")
    })
}

/// Raises unless the `UpdateByOperation` message, given in protobuf JSON form, is one the
/// table service accepts in a request: its window or decay, its options, its formula or
/// result column, and its column pairs.
///
/// Behavior: the `@Check` methods of the spec classes in `io.deephaven.api.updateby.spec`
/// and `io.deephaven.api.updateby.spec.UpdateBySpecBase#clause`, by way of
/// `io.deephaven.server.table.ops.UpdateByGrpcImpl#adaptOperation`
#[pyfunction]
fn check_update_by_operation(json: &str) -> PyResult<()> {
    let not_an_operation = |e: String| PyValueError::new_err(format!("not an operation: {e}"));
    let mut deserializer = serde_json::Deserializer::from_str(json);
    let descriptor = update_by_operation_descriptor().clone();
    let message = DynamicMessage::deserialize(descriptor, &mut deserializer)
        .map_err(|e| not_an_operation(e.to_string()))?;
    deserializer
        .end()
        .map_err(|e| not_an_operation(e.to_string()))?;
    let operation: UpdateByOperation = message
        .transcode_to()
        .map_err(|e| not_an_operation(e.to_string()))?;
    check_operation(&operation).map_err(PyRuntimeError::new_err)
}

/// Adds the functions of this module to the native module `_oxhaven`.
pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_function(wrap_pyfunction!(column_name, native)?)?;
    native.add_function(wrap_pyfunction!(duration_nanos, native)?)?;
    native.add_function(wrap_pyfunction!(check_update_by_operation, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! The expectations are what the Java reference answered to the same code, through the
    //! script scenarios `agg_*`, `join_*` and `updateby_*` and by hand.

    use super::super::tests::Python;

    /// Runs Python whose `assert` statements are the test.
    async fn check(code: &str) {
        Python::start().await.run(code);
    }

    /// What the tests share: `rows` reads a table, `refused` gives the text of the
    /// `DHError` a call raises.
    const HELPERS: &str = r#"
from deephaven import DHError, agg, empty_table
NO_INT, NO_DOUBLE =-2**31, -1.7976931348623157e308

def rows(table):
    return [tuple(row) for row in table.iter_tuple()]

def refused(call, error=DHError):
    try:
        call()
    except error as e:
        return str(e)
    raise AssertionError("answered")
"#;

    #[tokio::test]
    async fn names_and_durations_are_checked_as_java_checks_them() {
        check(
            r#"
import _oxhaven
_oxhaven.column_name("Price_2")
for name in (" C ", "", "A B", "1st", "int"):
    try:
        _oxhaven.column_name(name)
    except RuntimeError as e:
        assert str(e).startswith("Invalid column name"), str(e)
    else:
        raise AssertionError(name)
assert _oxhaven.parse_duration_nanos("PT1.5s") == 1_500_000_000
assert _oxhaven.parse_duration_nanos("PT00:01:00") == 60_000_000_000
try:
    _oxhaven.parse_duration_nanos("soon")
except RuntimeError as e:
    assert "soon" in str(e), str(e)
else:
    raise AssertionError("parsed")
"#,
        )
        .await;
    }

    #[tokio::test]
    async fn an_aggregation_is_the_message_of_the_table_service() {
        let code = r#"
assert agg.sum_(["A", "B = C"]).j_aggregation == {
    "columns": {"spec": {"sum": {}}, "match_pairs": ["A", "B = C"]}}
assert agg.pct(0.25, "A").j_agg_spec == {
    "percentile": {"percentile": 0.25, "average_evenly_divided": False}}
assert agg.count_("N").j_aggregation == {"count": {"column_name": "N"}}
assert agg.sorted_last(["A", "B"], "C").j_agg_spec == {
    "sorted_last": {"columns": [{"column_name": "A"}, {"column_name": "B"}]}}
# a Python value is typed as jpy types the Java object it makes
sentinels = [
    (None, {"null_value": "NULL_VALUE"}), (True, {"bool_value": True}),
    (-1, {"byte_value": -1}), (128, {"short_value": 128}), (100000, {"int_value": 100000}),
    (2**40, {"long_value": str(2**40)}), (1.5, {"double_value": 1.5}), ("x", {"string_value": "x"}),
]
for value, message in sentinels:
    spec = agg.unique("A", non_unique_sentinel=value).j_agg_spec["unique"]
    assert spec["non_unique_sentinel"] == message, (value, spec)
assert [a.is_formula for a in (agg.sum_("V"), agg.formula("sum(x)", "x", "V"),
                               agg.formula("A = sum(V)"), agg.count_("C"))] == [False, True, True, False]
# what the reference refuses in the function
assert refused(lambda: agg.count_(1)) == "count_ aggregation requires a string value for the 'col' argument."
assert refused(lambda: agg.unique("S", include_nulls=True)) == (
    "when include_nulls is True, a non-null sentinel value must be provided.")
assert refused(lambda: agg.formula("sum(x)", cols="V")) == (
    "The 'cols' argument is only valid when 'formula_param' is provided.")
for call in (lambda: agg.count_(" C "), lambda: agg.count_where(" C ", "x > 1"),
             lambda: agg.partition(" C "), lambda: agg.sorted_first(" C ", "A"),
             lambda: agg.weighted_avg(" C ", "A"), lambda: agg.weighted_sum("", "A")):
    assert refused(call, RuntimeError).startswith("Invalid column name"), call
# and in the table method
assert refused(lambda: agg.sum_().j_aggregation) == "No columns specified for the aggregation operation."
assert refused(lambda: agg.count_("C").j_agg_spec) == "unsupported aggregation operation."
"#;
        check(&format!("{HELPERS}{code}")).await;
    }

    #[tokio::test]
    async fn aggregations_give_what_java_gives() {
        let code = r#"
t = empty_table(9).update(["K = i % 3", "V = i * 1.5", "N = i", "S = `s` + (i % 2)", "W = i % 4"])
by_key = t.agg_by([agg.sum_("Sum = V"), agg.abs_sum("A = N"), agg.count_("C")], by="K")
assert rows(by_key) == [(0, 13.5, 9, 3), (1, 18.0, 12, 3), (2, 22.5, 15, 3)]
assert rows(t.agg_by(agg.count_where("C", ["N > 3", "V < 10"]), by=["K"])) == [(0, 1), (1, 1), (2, 1)]
assert rows(t.agg_by(agg.unique("N", non_unique_sentinel=100000), "K")) == [(k, 100000) for k in range(3)]
picked = t.agg_by([agg.sorted_first("V", "SF = N"), agg.sorted_last("W", ["SL = N"])], by="K")
assert rows(picked) == [(0, 0, 3), (1, 1, 7), (2, 2, 2)]
sums = [("s0", 5, 30.0, 20, 4), ("s1", 4, 24.0, 16, 8)]
assert rows(t.sum_by("S")) == sums
assert rows(t.agg_all_by(agg.sum_("ignored"), by=["S"])) == sums
assert rows(t.agg_all_by(agg.weighted_avg("W"), "S")) == [("s0", 1.0, 6.0, 4.0), ("s1", 0.75, 6.75, 4.5)]
assert rows(t.count_by("Rows")) == [(9,)]
assert rows(t.view(["K", "N"]).head_by(1, "K")) == [(0, 0), (1, 1), (2, 2)]
assert rows(t.view(["K", "N"]).tail_by(1)) == [(2, 8)]
assert rows(t.view(["K", "N"]).median_by("K")) == [(0, 3), (1, 4), (2, 5)]
empty = t.where("N < 0")
assert rows(empty.agg_by([agg.sum_("V"), agg.count_("C")])) == []
assert rows(empty.agg_by([agg.sum_("V"), agg.count_("C")], preserve_empty=True)) == [(NO_DOUBLE, 0)]
# the messages are the reference's, misspellings included
failures = [
    ("table agg_by operation failed. : No columns specified", lambda: t.agg_by([agg.sum_()], by="K")),
    ("table agg_by operation failed. : missing group-by column names",
     lambda: t.agg_by(agg.sum_("V"), initial_groups=t)),
    ("table agg_by operation failed. : AggFormula does not support reaggregating",
     lambda: t.agg_by(agg.formula("Out = sum(V)", reaggregating=True), by="K")),
    ("table agg_all_by operation failed. : unsupported aggregation operation.",
     lambda: t.agg_all_by(agg.count_("C"))),
    ("table asb_sum_by operation failed. : ", lambda: t.abs_sum_by("Nope")),
    ("table avg_by operation failed. : ", lambda: t.weighted_avg_by("Nope")),
    ("table group-by operation failed. : ", lambda: t.group_by("Nope")),
    ("table head_by operation failed. : ", lambda: t.head_by(1.5, "K")),
]
for text, call in failures:
    assert refused(call).startswith(text), (text, refused(call))
"#;
        check(&format!("{HELPERS}{code}")).await;
    }

    #[tokio::test]
    async fn joins_read_their_columns_as_java_reads_them() {
        let code = r#"
from deephaven.table import MultiJoinInput, MultiJoinTable, NaturalJoinType, multi_join
left = empty_table(6).update(["K = i % 4", "L = i", "T = i * 10 + 5"])
right = empty_table(3).update(["K = i", "R = i * 10", "S = `r` + i"])
dup = empty_table(6).update(["K = i % 3", "R = i * 10", "T = i * 7"])
# one text is split at its commas
joined = left.natural_join(right, on=" K ", joins="Right = R, S")
assert joined.column_names == ["K", "L", "T", "Right", "S"]
assert rows(joined)[2:4] == [(2, 2, 25, 20, "r2"), (3, 3, 35, NO_INT, None)]
last = left.natural_join(dup, on="K", joins="R", type=NaturalJoinType.LAST_MATCH)
assert [row[3] for row in rows(last)] == [30, 40, 50, NO_INT, 30, 40]
assert refused(lambda: left.natural_join(dup, on="K", joins="R")).startswith(
    "table natural_join operation failed. : Natural Join found duplicate right key")
assert refused(lambda: left.natural_join(right, "K", type="FIRST_MATCH")).startswith(
    "table natural_join operation failed. : ")
assert refused(lambda: left.exact_join(right, on="K", joins="S")).startswith(
    "table exact_join operation failed. : ")
assert left.view("L = K").join(right.head(2).view("R")).size == 12
assert left.join(dup, on="K", joins=["RightT = T", "R"]).size == 10
# the last match of an as-of join is the as-of match
before = left.aj(dup, on="K, T > T", joins="RightT = T, R")
assert rows(before)[3:5] == [(3, 3, 35, NO_INT, NO_INT), (0, 4, 45, 21, 30)]
after = left.raj(dup, on=["T < T"], joins="RightT = T")
assert [row[3] for row in rows(after)] == [7, 21, 28, NO_INT, NO_INT, NO_INT]
assert refused(lambda: left.aj(dup, on=[], joins="R")).startswith("table as-of join operation failed. : ")
assert refused(lambda: left.raj(dup, on="T >= T", joins="RightT = T")).startswith(
    "table reverse-as-of join operation failed. : ")
# the last match of a range join is the range
low = empty_table(4).update(["K = i % 2", "Lo = i * 2", "Hi = i * 2 + 5"])
values = empty_table(10).update(["K = i % 2", "V = i", "X = i * 1.5"])
ranged = low.range_join(values, on=["K", "Lo < V < Hi"], aggs=[agg.group(["X", "RV = V"])])
sizes = ranged.update(["N = X.size()", "First = RV[0]"]).drop_columns(["X", "RV"])
assert rows(sizes) == [(0, 0, 5, 2, 2), (1, 2, 7, 2, 3), (0, 4, 9, 2, 6), (1, 6, 11, 2, 7)]
assert refused(lambda: low.range_join(values, on=[], aggs=agg.group("X"))) == (
    "table range_join operation failed. : No match expressions found; must include at least a "
    "range match expression")
# a multi join of tables, and of inputs with their own columns
a = empty_table(3).update(["K = i", "A = i * 2"])
b = empty_table(4).update(["K = 3 - i", "B = i * 3"])
c = empty_table(2).update(["Key = i + 1", "C = `c` + i", "D = i"])
shared = [(0, 0, 9), (1, 2, 6), (2, 4, 3), (3, NO_INT, 0)]
assert rows(multi_join([a, b], on="K").table) == shared
assert rows(MultiJoinTable([a, b], on=["K"]).table) == shared
assert rows(multi_join([a, b], on=" K ,").table) == shared
chosen = multi_join([MultiJoinInput(a, "K"), MultiJoinInput(b, on=["K"], joins="B"),
                     MultiJoinInput(c, on="K = Key", joins=["C"])]).table
assert rows(chosen) == [(0, 0, 9, None), (1, 2, 6, "c0"), (2, 4, 3, "c1"), (3, NO_INT, 0, None)]
built = "failed to build a MultiJoinTable object. : "
assert refused(lambda: multi_join([MultiJoinInput(a, "K")], on="K")) == (
    built + "on parameter is not permitted when MultiJoinInput objects are provided.")
assert refused(lambda: multi_join([a, MultiJoinInput(b, "K")])).startswith(built + "input must be a Table")
assert refused(lambda: multi_join([a, b])).startswith(built)
assert refused(lambda: MultiJoinInput(a, "K,A")) == (
    'failed to build a MultiJoinInput object. : Invalid column name "K,A"')
assert refused(lambda: MultiJoinInput(a, "K", joins="A B")).startswith("failed to build a MultiJoinInput object.")
"#;
        check(&format!("{HELPERS}{code}")).await;
    }

    #[tokio::test]
    async fn update_by_operations_are_checked_when_they_are_made() {
        let code = r#"
import deephaven.updateby as uby
t = empty_table(10).update(["K = i % 2", "V = (i % 4 == 3) ? NULL_DOUBLE : i * 0.5 + 1",
                            "N = 10 - i", "T = '2024-01-01T00:00:00Z' + i * SECOND"])
summed = t.update_by(uby.cum_sum(["A = N", " B = V "])).view(["A", "B"])
assert rows(summed)[:4] == [(10, 1.0), (19, 2.5), (27, 4.5), (34, 4.5)]
deltas = t.update_by(uby.delta("D = V", uby.DeltaControl.VALUE_DOMINATES), by=[]).view("D")
assert [row[0] for row in rows(deltas)][:5] == [1.0, 0.5, 0.5, NO_DOUBLE, 3.0]
by_key = t.update_by([uby.rolling_sum_tick("S = N", rev_ticks=2), uby.cum_max("M = N")], by="K").view(["S", "M"])
assert rows(by_key)[:4] == [(10, 10), (9, 9), (18, 10), (16, 9)]
# a window in time, as a duration string and in nanoseconds
for rev, fwd in (("PT2s", "PT00:00:01"), (2_000_000_000, 1_000_000_000)):
    grouped = t.update_by(uby.rolling_group_time("T", "G = N", rev_time=rev, fwd_time=fwd))
    sizes = grouped.update("Size = G.size()").view(["Size", "First = G[0]"])
    assert rows(sizes)[:4] == [(2, 10), (3, 10), (4, 10), (4, 9)], rows(sizes)
nans = t.update(["B = (i % 4 == 1) ? Double.NaN : i * 1.0"])
skipped = nans.update_by(uby.ema_tick(2, "E = B")).view("E")
assert rows(skipped)[:3] == [(0.0,), (0.0,), (0.7869386805747332,)]
control = uby.OperationControl(on_nan=uby.BadDataBehavior.POISON, big_value_context=uby.MathContext.DECIMAL64)
assert control.j_op_control == {"on_null_value": "SKIP", "on_nan_value": "POISON",
                                "big_value_context": {"precision": 16, "rounding_mode": "HALF_EVEN"}}
poisoned = rows(nans.update_by(uby.ema_tick(2, "E = B", control)).view("E"))
assert poisoned[0] == (0.0,) and all(e != e for (e,) in poisoned[1:]), poisoned
# count where and the rolling formula, by one parameter and by the columns it names
c = empty_table(6).update(["N = i", "T = '2024-01-01T00:00:00Z' + i * SECOND"])
assert rows(c.update_by(uby.cum_count_where("Big", "N > 2")).view("Big")) == [(0,), (0,), (0,), (1,), (2,), (3,)]
assert rows(c.update_by(uby.rolling_count_where_tick("Big", "N > 2", 2)).view("Big")) == [
    (0,), (0,), (0,), (1,), (2,), (2,)]
assert rows(c.update_by(uby.rolling_count_where_time("T", "Big", ["N > 2"], "PT1s")).view("Big")) == [
    (0,), (0,), (0,), (1,), (2,), (2,)]
sums = [(0,), (1,), (3,), (5,), (7,), (9,)]
assert rows(c.update_by(uby.rolling_formula_tick("sum(x)", "x", "F = N", rev_ticks=2)).view("F")) == sums
assert rows(c.update_by(uby.rolling_formula_tick("F = sum(N)", rev_ticks=2)).view("F")) == sums
assert rows(c.update_by(uby.rolling_formula_time("T", "F = sum(N)", rev_time="PT1s")).view("F")) == sums
# what Java refuses when the operation is made
made = [
    ('failed to create a cumulative sum UpdateByOperation. : Invalid column name "A B"',
     lambda: uby.cum_sum("A B")),
    ("failed to create a rolling sum (tick) UpdateByOperation. : UpdateBy rolling window size must be non-negative",
     lambda: uby.rolling_sum_tick("S = V", rev_ticks=-5)),
    ("failed to create a rolling average (tick) UpdateByOperation. : UpdateBy rolling window size may not exceed",
     lambda: uby.rolling_avg_tick("S = V", rev_ticks=2**40)),
    ("failed to create a rolling minimum (tick) UpdateByOperation. : ", lambda: uby.rolling_min_tick("S = V", 1.5)),
    ("failed to create a rolling sum (time) UpdateByOperation. : UpdateBy rolling window size must be non-negative",
     lambda: uby.rolling_sum_time("T", "S = V", rev_time=-5)),
    ("failed to create a rolling maximum (time) UpdateByOperation. : WindowScale.timestampCol() must not be an empty",
     lambda: uby.rolling_max_time("", "S = V", rev_time=5)),
    ("failed to create a rolling product (time) UpdateByOperation. : Cannot parse duration: soon",
     lambda: uby.rolling_prod_time("T", "S = V", rev_time="soon")),
    ("failed to create a time-decay EMA UpdateByOperation. : ", lambda: uby.ema_time("T", 5.5, "E = V")),
    ("failed to create a tick-decay EM Std UpdateByOperation. : ", lambda: uby.emstd_tick("2", "E = V")),
    ("failed to create a rolling formula (tick) UpdateByOperation. : formula must not be empty",
     lambda: uby.rolling_formula_tick("", rev_ticks=2)),
    ('failed to create a rolling formula (time) UpdateByOperation. : Invalid column name "sum(N)"',
     lambda: uby.rolling_formula_time("T", "sum(N)", rev_time=2)),
    ('failed to create a cumulative count_where UpdateByOperation. : Invalid column name "C C"',
     lambda: uby.cum_count_where("C C", "N > 5")),
    ("count_where aggregation requires a string value for the 'col' argument.",
     lambda: uby.rolling_count_where_tick(None, "N > 5", 1)),
    ("failed to create a delta UpdateByOperation. : ", lambda: uby.delta("D = N", "NULL_DOMINATES")),
    ("failed to build an OperationControl object. : ", lambda: uby.OperationControl(on_null="SKIP")),
]
for text, call in made:
    assert refused(call).startswith(text), (text, refused(call))
# a window may start after the row, and an operator may have no column
ahead = uby.rolling_sum_tick("S = V", rev_ticks=-5, fwd_ticks=6)
assert ahead.j_updateby_op["column"]["match_pairs"] == ["S = V"]
assert t.update_by(uby.cum_sum([])).column_names == t.column_names
# and what it refuses when the table is known
assert refused(lambda: t.update_by([])).startswith("table update-by operation failed. : ")
assert refused(lambda: t.update_by(uby.rolling_sum_time("K", "S = V", rev_time=5))).startswith(
    "table update-by operation failed. : Unsupported timestamp column type")
"#;
        check(&format!("{HELPERS}{code}")).await;
    }

    #[tokio::test]
    async fn what_the_engine_has_not_ported_is_refused_by_the_java_name() {
        let code = r#"
import deephaven.updateby as uby
from deephaven.table import keyed_transpose
t = empty_table(6).update(["K = i % 2", "C = `c` + (i % 3)", "N = i", "T = '2024-01-01T00:00:00Z' + i * SECOND"])
not_ported = [
    ("CountWhereOperator#applyFilters", lambda: t.agg_by(agg.count_where("All", []), by="K")),
    ("KeyedTranspose#keyedTranspose", lambda: keyed_transpose(t, agg.sum_("N"), "K", "T")),
]
for java, call in not_ported:
    text = refused(call)
    assert "Unimplemented: not ported: io.deephaven." in text, text
    assert java in text, text
assert refused(lambda: keyed_transpose(t, agg.sum_("N"), "K", "T")).startswith(
    "keyed_transpose operation failed. : ")
parts = t.agg_by(agg.partition("P"), by="K")
assert [(c.name, str(c.data_type)) for c in parts.columns] == [
    ("K", "int"), ("P", "io.deephaven.engine.table.impl.QueryTable")], parts.columns
assert parts.size == 2
"#;
        check(&format!("{HELPERS}{code}")).await;
    }
}
