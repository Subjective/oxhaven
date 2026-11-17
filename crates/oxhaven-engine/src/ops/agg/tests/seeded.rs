//! Aggregation with initial groups (`aggregate_seeded`): the groups a table of keys names
//! before the parent is read.
//!
//! The expected rows of `EVERY` and of the steps of the changing table are what the Java
//! server printed for the same tables and aggregations, made in its Python console with
//! `agg_by(..., initial_groups=...)`: the static table as `empty_table(8).update(...)`, the
//! changing one as a keyed input table given the same rows in the same steps.

use super::super::aggregate_seeded;
use super::random::{every_aggregation, parent_columns};
use super::*;
use crate::graph::SteppedUpdateGraph;
use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_testkit::{
    at_least_for, test_seeds, ConsistencyChecker, RandomUpdates, Recorder, ScriptedTable,
    TableChange,
};

fn seeded(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    initial_groups: &Arc<dyn Table>,
    by: &[&str],
) -> Result<Arc<dyn Table>, AggregateError> {
    aggregate_seeded(
        parent,
        aggregations,
        preserve_empty,
        initial_groups,
        by,
        &oxhaven_expr::Compiler,
    )
}

fn ints(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<Int32Array>())
}

/// A static table with the one `int` column `K`.
fn keys(values: &[Option<i32>]) -> Arc<dyn Table> {
    static_table(&[("K", ColumnType::Int, ints(values.iter().copied()))])
}

/// The rows `rows` of `empty_table(8).update(["K = i % 3", "I = i", "D = i * 1.5",
/// "S = `s` + (i % 2)", "L = (long)i", "B = i % 2 == 0", "W = i % 4"])`, with `K` replaced
/// where `key` says so.
fn source_rows(
    rows: &[i32],
    key: impl Fn(i32) -> i32,
) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let each = || rows.iter().copied();
    vec![
        ("K", ColumnType::Int, ints(each().map(|i| Some(key(i))))),
        ("I", ColumnType::Int, ints(each().map(Some))),
        (
            "D",
            ColumnType::Double,
            Arc::new(
                each()
                    .map(|i| Some(f64::from(i) * 1.5))
                    .collect::<Float64Array>(),
            ),
        ),
        (
            "S",
            ColumnType::String,
            Arc::new(
                each()
                    .map(|i| Some(format!("s{}", i % 2)))
                    .collect::<StringArray>(),
            ),
        ),
        (
            "L",
            ColumnType::Long,
            Arc::new(each().map(|i| Some(i64::from(i))).collect::<Int64Array>()),
        ),
        (
            "B",
            ColumnType::Boolean,
            Arc::new(each().map(|i| Some(i % 2 == 0)).collect::<BooleanArray>()),
        ),
        ("W", ColumnType::Int, ints(each().map(|i| Some(i % 4)))),
    ]
}

fn values(rows: &[i32], key: impl Fn(i32) -> i32) -> Vec<ArrayRef> {
    let columns = source_rows(rows, key);
    columns.into_iter().map(|(_, _, values)| values).collect()
}

/// One aggregation of every operator.
fn every() -> Vec<Aggregation> {
    let named = |columns: &[&str]| columns.iter().map(|name| name.to_string()).collect();
    let weight = || "W".to_string();
    vec![
        agg(AggSpec::Sum, &["Sum = D"]),
        agg(AggSpec::AbsSum, &["Abs = I"]),
        agg(AggSpec::Avg, &["Avg = D"]),
        agg(AggSpec::Min, &["Min = I"]),
        agg(AggSpec::Max, &["Max = S"]),
        agg(AggSpec::First, &["First = S"]),
        agg(AggSpec::Last, &["Last = L"]),
        count("Count"),
        agg(AggSpec::Std, &["Std = D"]),
        agg(AggSpec::Var, &["Var = I"]),
        agg(
            AggSpec::Median {
                average_evenly_divided: true,
            },
            &["Med = D"],
        ),
        agg(
            AggSpec::Percentile {
                percentile: 0.3,
                average_evenly_divided: false,
            },
            &["Pct = I"],
        ),
        agg(AggSpec::CountDistinct { count_nulls: false }, &["CD = S"]),
        agg(AggSpec::CountDistinct { count_nulls: true }, &["CDN = S"]),
        agg(
            AggSpec::Distinct {
                include_nulls: false,
            },
            &["Dis = S"],
        ),
        agg(
            AggSpec::Distinct {
                include_nulls: true,
            },
            &["DisN = I"],
        ),
        agg(
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            &["Un = S"],
        ),
        agg(
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Int(-1),
            },
            &["UnS = I"],
        ),
        agg(
            AggSpec::Unique {
                include_nulls: true,
                non_unique: Sentinel::String("x".to_string()),
            },
            &["UnN = S"],
        ),
        agg(AggSpec::WeightedAvg { weight: weight() }, &["WAvg = D"]),
        agg(AggSpec::WeightedSum { weight: weight() }, &["WSum = D"]),
        agg(AggSpec::WeightedSum { weight: weight() }, &["WSumI = I"]),
        agg(
            AggSpec::SortedFirst {
                columns: named(&["D"]),
            },
            &["SF = I"],
        ),
        agg(
            AggSpec::SortedLast {
                columns: named(&["D"]),
            },
            &["SL = S"],
        ),
        Aggregation::count_where("CW", ["I > 3"]),
        agg(AggSpec::Sum, &["SumB = B"]),
        agg(AggSpec::Sum, &["SumI = I"]),
        agg(AggSpec::Avg, &["AvgI = I"]),
        Aggregation::formula("Fm", "sum(I) + 1"),
        // After the formula: Java does not name the column of a formula that follows a
        // `group` among the modified columns.
        agg(AggSpec::Group, &["Grp = I"]),
        agg(
            AggSpec::Formula {
                formula: "len(x)".to_string(),
                param_token: "x".to_string(),
            },
            &["FL = I"],
        ),
    ]
}

/// The row of a group that never had a row, after its key.
const NEVER: &str = "null|null|null|null|null|null|null|0|null|null|null|null|null|null|[]|[]|null|null|null|null|null|null|null|null|0|null|null|null|null|[]|0";
/// The row of a group that lost its rows, after its key: as [`NEVER`], but the two
/// `distinct` columns are null.
const EMPTIED: &str = "null|null|null|null|null|null|null|0|null|null|null|null|null|null|null|null|null|null|null|null|null|null|null|null|0|null|null|null|null|[]|0";
const ONE: &str = "18.0|12|6.0|1|s1|s1|7|3|4.5|9.0|6.0|4|2|2|[s0, s1]|[1, 4, 7]|null|-1|x|8.25|33.0|22|1|s1|2|1|12|4.0|13|[1, 4, 7]|3";
const ONE_OF_TWO_ROWS: &str = "7.5|5|3.75|1|s1|s1|4|2|3.181980515339464|4.5|3.75|1|2|2|[s0, s1]|[1, 4]|null|-1|x|1.5|1.5|1|1|s0|1|1|5|2.5|6|[1, 4]|2";
const ZERO: &str = "13.5|9|4.5|0|s1|s0|6|3|4.5|9.0|4.5|3|2|2|[s0, s1]|[0, 3, 6]|null|-1|x|6.3|31.5|21|0|s0|1|2|9|3.0|10|[0, 3, 6]|3";
const TWO: &str = "10.5|7|5.25|2|s1|s0|5|2|3.181980515339464|4.5|5.25|2|2|2|[s0, s1]|[2, 5]|null|-1|x|4.5|13.5|9|2|s1|1|1|7|3.5|8|[2, 5]|2";
const THREE_OF_ROW_SEVEN: &str = "10.5|7|10.5|7|s1|s1|7|1|NaN|NaN|10.5|7|1|1|[s1]|[7]|s1|7|s1|10.5|31.5|21|7|s1|1|0|7|7.0|8|[7]|1";

fn keyed(rows: &[(i32, &str)]) -> Vec<String> {
    rows.iter()
        .map(|(key, row)| format!("{key}|{row}"))
        .collect()
}

/// The initial groups of the recorded results: `empty_table(4).update("K = 5 - i * 2")`.
fn recorded_groups() -> Arc<dyn Table> {
    keys(&[Some(5), Some(3), Some(1), Some(-1)])
}

#[test]
fn initial_groups_come_first_and_the_rest_follow_as_they_are_seen() {
    let all: Vec<i32> = (0..8).collect();
    let parent = static_table(&source_rows(&all, |i| i % 3));
    let kept = seeded(&parent, &every(), true, &recorded_groups(), &["K"]).expect("kept");
    assert_eq!(
        rows(&*kept),
        keyed(&[
            (5, NEVER),
            (3, NEVER),
            (1, ONE),
            (-1, NEVER),
            (0, ZERO),
            (2, TWO)
        ])
    );
    assert!(kept.is_flat());
    assert_eq!(*kept.row_set(), RowSet::flat(6));

    // Without `preserve_empty` the groups without rows have no row, and the others keep
    // the order the initial groups gave them.
    let dropped = seeded(&parent, &every(), false, &recorded_groups(), &["K"]).expect("dropped");
    assert_eq!(rows(&*dropped), keyed(&[(1, ONE), (0, ZERO), (2, TWO)]));
    assert_eq!(*dropped.row_set(), RowSet::from_keys([2, 4, 5]));
    assert!(!dropped.is_flat());

    // The same aggregations without initial groups: the order of the parent's rows.
    let plain = aggregate(&parent, &every(), true, &["K"]).expect("plain");
    assert_eq!(rows(&*plain), keyed(&[(0, ZERO), (1, ONE), (2, TWO)]));
}

#[test]
fn a_key_named_twice_is_one_group_and_other_columns_are_not_read() {
    let all: Vec<i32> = (0..9).collect();
    let parent = static_table(&source_rows(&all, |i| i % 3));
    let sum = [agg(AggSpec::Sum, &["D"])];
    // Observed: K [0, 7, 1, 2], and [0, 1, 2] without `preserve_empty`.
    let twice = keys(&[Some(0), Some(7), Some(0), Some(7), Some(0), Some(7)]);
    let kept = seeded(&parent, &sum, true, &twice, &["K"]).expect("kept");
    assert_eq!(rows(&*kept), ["0|13.5", "7|null", "1|18.0", "2|22.5"]);
    let dropped = seeded(&parent, &sum, false, &twice, &["K"]).expect("dropped");
    assert_eq!(rows(&*dropped), ["0|13.5", "1|18.0", "2|22.5"]);
    assert_eq!(*dropped.row_set(), RowSet::from_keys([0, 2, 3]));

    // A null is a key like any other. Observed: K [0, null, 10, 1, 2].
    let with_null = keys(&[Some(0), None, Some(10)]);
    let result = seeded(&parent, &[count("C")], true, &with_null, &["K"]).expect("null key");
    assert_eq!(rows(&*result), ["0|3", "null|0", "10|0", "1|3", "2|3"]);

    // The initial groups have a column `D` of their own, and their `K` is not first.
    let wide = static_table(&[
        ("Z", ColumnType::Int, ints([Some(0), Some(1)])),
        ("K", ColumnType::Int, ints([Some(4), Some(2)])),
        (
            "D",
            ColumnType::Double,
            Arc::new(Float64Array::from(vec![100.0, 100.0])),
        ),
    ]);
    let result = seeded(&parent, &sum, true, &wide, &["K"]).expect("wide");
    assert_eq!(rows(&*result), ["4|null", "2|22.5", "0|13.5", "1|18.0"]);
}

#[test]
fn initial_groups_of_two_key_columns_are_matched_by_name() {
    let all: Vec<i32> = (0..9).collect();
    let parent = static_table(&source_rows(&all, |i| i % 3));
    // empty_table(4).update(["K = i", "S = i % 2 == 0 ? `s0` : null"]), grouped by S, K.
    let initial = static_table(&[
        ("K", ColumnType::Int, ints((0..4).map(Some))),
        (
            "S",
            ColumnType::String,
            Arc::new(StringArray::from(vec![Some("s0"), None, Some("s0"), None])),
        ),
    ]);
    let result = seeded(
        &parent,
        &[agg(AggSpec::Sum, &["D"])],
        true,
        &initial,
        &["S", "K"],
    )
    .expect("two");
    // Observed.
    assert_eq!(
        rows(&*result),
        [
            "s0|0|9.0",
            "null|1|null",
            "s0|2|15.0",
            "null|3|null",
            "s1|1|12.0",
            "s1|0|4.5",
            "s0|1|6.0",
            "s1|2|7.5"
        ]
    );
}

#[test]
fn initial_groups_are_refused_as_java_refuses_them() {
    let all: Vec<i32> = (0..9).collect();
    let parent = static_table(&source_rows(&all, |i| i % 3));
    let sum = [agg(AggSpec::Sum, &["D"])];
    let refusal =
        |initial: &Arc<dyn Table>, by: &[&str]| match seeded(&parent, &sum, false, initial, by) {
            Err(AggregateError::InvalidInitialGroups(message)) => message,
            other => panic!("not refused: {:?}", other.map(|_| ())),
        };
    assert_eq!(
        refusal(&keys(&[Some(1)]), &[]),
        "aggregation: initial groups must not be specified if no group-by columns are specified"
    );
    let other_name = static_table(&[("J", ColumnType::Int, ints([Some(1)]))]);
    assert_eq!(
        refusal(&other_name, &["K"]),
        "aggregation: not all group-by columns [K] are present in initial groups with columns \
         [J]. Missing columns: [K]"
    );
    let other_type = static_table(&[(
        "K",
        ColumnType::Long,
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )]);
    assert_eq!(
        refusal(&other_type, &["K"]),
        "aggregation: column definition mismatch between input table and initial groups table \
         for K; input has [K, int], initial groups has [K, long]"
    );
    // Behavior: ColumnDefinition#describeForCompatibility, with `Class#toString` of each
    // type and, for a vector or an array, of its component type.
    let described = [
        (ColumnType::LocalDate, "[K, class java.time.LocalDate]"),
        (ColumnType::Boolean, "[K, class java.lang.Boolean]"),
        (
            ColumnType::vector(ColumnType::Int),
            "[K, interface io.deephaven.vector.IntVector, int]",
        ),
        (
            ColumnType::vector(ColumnType::Instant),
            "[K, interface io.deephaven.vector.ObjectVector, class java.time.Instant]",
        ),
        (ColumnType::array(ColumnType::Long), "[K, class [J, long]"),
        (
            ColumnType::array(ColumnType::String),
            "[K, class [Ljava.lang.String;, class java.lang.String]",
        ),
    ];
    for (column_type, text) in described {
        assert_eq!(
            super::super::describe_for_compatibility("K", &column_type),
            text
        );
    }
    // A group-by column the parent lacks is found first.
    assert_eq!(
        seeded(&parent, &sum, false, &other_name, &["J"]).err(),
        Some(AggregateError::UnknownColumn("J".to_string()))
    );
}

/// An update of an aggregation as text.
fn describe(update: &oxhaven_contracts::update::TableUpdate) -> String {
    assert!(update.shifted.is_empty());
    let keys = |rows: &RowSet| rows.iter().collect::<Vec<_>>();
    format!(
        "added {:?} removed {:?} modified {:?}",
        keys(&update.added),
        keys(&update.removed),
        keys(&update.modified)
    )
}

struct Ticking {
    graph: Arc<SteppedUpdateGraph>,
    source: Arc<ScriptedTable>,
    parent: Arc<dyn Table>,
}

impl Ticking {
    /// A refreshing table of the columns of [`source_rows`] with the rows `rows`, each at
    /// the row key of its number.
    fn new(rows: &[i32]) -> Ticking {
        let graph = SteppedUpdateGraph::new();
        let (definition, batch) = batch(&source_rows(rows, |i| i % 3));
        let source = ScriptedTable::new(
            graph.clone(),
            definition,
            RowSet::from_keys(rows.iter().map(|&row| RowKey::from(row))),
            batch.columns().to_vec(),
        );
        let parent: Arc<dyn Table> = source.table();
        Ticking {
            graph,
            source,
            parent,
        }
    }

    fn step(
        &self,
        change: TableChange,
        recorder: &Recorder,
        checker: &ConsistencyChecker,
    ) -> String {
        let before = recorder.events().len();
        self.source.push(change);
        self.graph.run_cycle();
        checker.assert_consistent();
        match &recorder.events()[before..] {
            [] => "none".to_string(),
            [Ok(update)] => describe(update),
            other => panic!("unexpected events {other:?}"),
        }
    }
}

fn add(rows: &[i32], key: impl Fn(i32) -> i32) -> TableChange {
    TableChange::new().add(rows.iter().map(|&row| RowKey::from(row)), values(rows, key))
}

#[test]
fn preserved_initial_groups_of_a_changing_table_fill_and_empty_in_place() {
    let ticking = Ticking::new(&[]);
    let result =
        seeded(&ticking.parent, &every(), true, &recorded_groups(), &["K"]).expect("seeded");
    assert!(result.is_refreshing());
    let checker = ConsistencyChecker::attach(result.clone());
    let recorder = Recorder::attach(result.clone());
    let step = |change| ticking.step(change, &recorder, &checker);
    assert_eq!(
        rows(&*result),
        keyed(&[(5, NEVER), (3, NEVER), (1, NEVER), (-1, NEVER)])
    );

    // Rows of groups that are not among the initial ones: new rows after them.
    assert_eq!(
        step(add(&[0, 2, 3, 5, 6], |i| i % 3)),
        "added [4, 5] removed [] modified []"
    );
    assert_eq!(
        rows(&*result),
        keyed(&[
            (5, NEVER),
            (3, NEVER),
            (1, NEVER),
            (-1, NEVER),
            (0, ZERO),
            (2, TWO)
        ])
    );

    // Rows of an initial group: its row is modified where it is.
    assert_eq!(
        step(add(&[1, 4], |i| i % 3)),
        "added [] removed [] modified [2]"
    );
    assert_eq!(
        rows(&*result),
        keyed(&[
            (5, NEVER),
            (3, NEVER),
            (1, ONE_OF_TWO_ROWS),
            (-1, NEVER),
            (0, ZERO),
            (2, TWO)
        ])
    );

    // The group loses its rows and keeps its row.
    assert_eq!(
        step(TableChange::new().remove([1, 4])),
        "added [] removed [] modified [2]"
    );
    assert_eq!(
        rows(&*result),
        keyed(&[
            (5, NEVER),
            (3, NEVER),
            (1, EMPTIED),
            (-1, NEVER),
            (0, ZERO),
            (2, TWO)
        ])
    );

    // A group that came later empties, and another initial group gets a row.
    let change = TableChange::new()
        .remove([0, 3, 6])
        .add([7], values(&[7], |_| 3));
    assert_eq!(step(change), "added [] removed [] modified [1, 4]");
    assert_eq!(
        rows(&*result),
        keyed(&[
            (5, NEVER),
            (3, THREE_OF_ROW_SEVEN),
            (1, EMPTIED),
            (-1, NEVER),
            (0, EMPTIED),
            (2, TWO)
        ])
    );
}

#[test]
fn initial_groups_that_all_have_rows_order_a_changing_table_without_preserve_empty() {
    let ticking = Ticking::new(&[0, 1, 2, 3]);
    let sum = [agg(AggSpec::Sum, &["I"]), count("N")];
    let initial = keys(&[Some(2), Some(0)]);
    let result = seeded(&ticking.parent, &sum, false, &initial, &["K"]).expect("seeded");
    let checker = ConsistencyChecker::attach(result.clone());
    let recorder = Recorder::attach(result.clone());
    let step = |change| ticking.step(change, &recorder, &checker);
    assert_eq!(rows(&*result), ["2|2|1", "0|3|2", "1|1|1"]);
    assert!(!result.is_flat());

    // A group empties and returns, in its place and with its key.
    assert_eq!(
        step(TableChange::new().remove([2])),
        "added [] removed [0] modified []"
    );
    assert_eq!(rows(&*result), ["0|3|2", "1|1|1"]);
    assert_eq!(
        step(add(&[5], |i| i % 3)),
        "added [0] removed [] modified []"
    );
    assert_eq!(rows(&*result), ["2|5|1", "0|3|2", "1|1|1"]);
}

/// What Java 42.9 printed for the same steps of a keyed input table with
/// `agg_by([sum, count, formula], by="K", initial_groups=...)`: an initial group that has
/// no rows at the start shows null keys once it has rows, and a formula reads that null.
#[test]
fn an_initial_group_without_rows_of_a_changing_table_shows_null_keys() {
    let ticking = Ticking::new(&[0, 1, 2, 3]);
    let aggregations = [
        agg(AggSpec::Sum, &["I"]),
        count("N"),
        Aggregation::Formula {
            column: "F".to_string(),
            expression: "isNull(K) ? -1 : K".to_string(),
        },
    ];
    let initial = keys(&[Some(7), Some(1), Some(9)]);
    let result = seeded(&ticking.parent, &aggregations, false, &initial, &["K"]).expect("seeded");
    let checker = ConsistencyChecker::attach(result.clone());
    let recorder = Recorder::attach(result.clone());
    let step = |change| ticking.step(change, &recorder, &checker);
    assert_eq!(rows(&*result), ["1|1|1|1", "0|3|2|0", "2|2|1|2"]);
    assert!(!result.is_flat());

    // The first row of an initial group that had none, and a group that is new.
    assert_eq!(
        step(add(&[5, 6], |i| i + 2)),
        "added [0, 5] removed [] modified []"
    );
    assert_eq!(
        rows(&*result),
        ["null|5|1|-1", "1|1|1|1", "0|3|2|0", "2|2|1|2", "8|6|1|8"]
    );

    // The group loses its row and gets another: its key stays null. A group that had
    // rows at the start keeps its key through the same.
    assert_eq!(
        step(TableChange::new().remove([5, 1])),
        "added [] removed [0, 1] modified []"
    );
    assert_eq!(
        step(add(&[4, 7, 10], |i| match i {
            4 => 7,
            7 => 1,
            _ => 9,
        })),
        "added [0, 1, 2] removed [] modified []"
    );
    assert_eq!(
        rows(&*result),
        [
            "null|4|1|-1",
            "1|7|1|1",
            "null|10|1|-1",
            "0|3|2|0",
            "2|2|1|2",
            "8|6|1|8"
        ]
    );
}

fn append_only(rows: &[i32]) -> (Arc<SteppedUpdateGraph>, Arc<dyn Table>) {
    let graph = SteppedUpdateGraph::new();
    let (definition, initial) = batch(&source_rows(rows, |i| i % 3));
    let parent: Arc<dyn Table> =
        crate::table::append_only_input_table(graph.clone(), definition, Some(&initial))
            .expect("an input");
    (graph, parent)
}

/// Java accepts these and its result fails when the group gets its first row: the first
/// and last operators of an add-only table never record that a group appeared.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.BaseAddOnlyFirstOrLastChunkedOperator"
)]
fn a_first_of_an_add_only_table_with_an_initial_group_without_rows_is_refused() {
    let (_graph, parent) = append_only(&[0, 1, 2, 3]);
    let first = [agg(AggSpec::Sum, &["I"]), agg(AggSpec::First, &["D"])];
    let _ = seeded(&parent, &first, false, &keys(&[Some(7)]), &["K"]);
}

/// With a count before it Java computes, and so with any other aggregation alone.
#[test]
fn an_add_only_table_shows_null_keys_for_an_initial_group_without_rows() {
    for (aggregations, first_row) in [
        (vec![count("N"), agg(AggSpec::Last, &["I"])], "null|1|5"),
        (vec![agg(AggSpec::Max, &["M = I"]), count("N")], "null|5|1"),
    ] {
        let (graph, parent) = append_only(&[0, 1, 2, 3]);
        let result =
            seeded(&parent, &aggregations, false, &keys(&[Some(7)]), &["K"]).expect("seeded");
        let checker = ConsistencyChecker::attach(result.clone());
        assert!(!result.is_flat());
        let (_, more) = batch(&source_rows(&[5, 6], |i| i + 2));
        let writer = parent.input_table().expect("an input table");
        writer.add(more).expect("an add");
        graph.run_cycle();
        checker.assert_consistent();
        assert_eq!(rows(&*result)[0], first_row);
    }
}

#[test]
fn the_initial_groups_are_read_once() {
    // The initial groups are themselves a changing table.
    let groups = Ticking::new(&[0, 1]);
    let all: Vec<i32> = (0..8).collect();
    let parent = static_table(&source_rows(&all, |i| i % 3));
    let initial_groups: Arc<dyn Table> = groups.parent.clone();
    let sum = [agg(AggSpec::Sum, &["I"])];
    let result = seeded(&parent, &sum, true, &initial_groups, &["K"]).expect("seeded");
    assert!(!result.is_refreshing());
    assert_eq!(rows(&*result), ["0|9", "1|12", "2|7"]);
    groups.source.push(add(&[9], |_| 40));
    groups.graph.run_cycle();
    assert_eq!(rows(&*result), ["0|9", "1|12", "2|7"]);
}

/// The key columns of the current rows of `table`, as a static table.
fn key_columns(table: &dyn Table, count: usize) -> Arc<dyn Table> {
    let columns: Vec<(&str, ColumnType, ArrayRef)> = table
        .definition()
        .columns()
        .iter()
        .zip(read(table))
        .take(count)
        .map(|(column, values)| (column.name.as_str(), column.column_type.clone(), values))
        .collect();
    static_table(&columns)
}

/// Every aggregation of a randomly changing table with initial groups, checked after
/// every cycle against the same aggregation of a static copy of the parent, whose initial
/// groups are the groups the result has: that gives the static result the result's rows
/// in the result's order, whatever the history was.
///
/// With `preserve_empty` the initial groups are random keys, some of which the parent
/// never has. Without it they are the keys of the parent's first rows in reverse, so that
/// each has a row (see `an_initial_group_without_rows_of_a_changing_table_needs_preserve_empty`).
///
/// Returns the number of updates the result delivered.
fn random_run(seed: u64, by: &'static [&'static str], preserve_empty: bool) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    let columns = parent_columns();
    let initial_rows = 6;
    let values: Vec<ArrayRef> = columns
        .iter()
        .map(|(_, column_type)| random.values(column_type, initial_rows))
        .collect();
    let type_of = |name: &str| {
        let column = columns.iter().find(|(column, _)| *column == name);
        column.expect("a column of the parent").1.clone()
    };
    let initial_columns: Vec<(&str, ColumnType, ArrayRef)> = by
        .iter()
        .map(|&name| {
            let column_type = type_of(name);
            let values = if preserve_empty {
                random.values(&column_type, 5)
            } else {
                let index = columns.iter().position(|(column, _)| *column == name);
                let values = &values[index.expect("a column of the parent")];
                let reversed: arrow::array::UInt64Array =
                    (0..initial_rows).rev().map(Some).collect();
                arrow::compute::take(values, &reversed, None).expect("reversed keys")
            };
            (name, column_type, values)
        })
        .collect();
    let initial_groups = static_table(&initial_columns);
    let source = ScriptedTable::new(
        graph.clone(),
        oxhaven_testkit::definition(&columns),
        RowSet::flat(initial_rows),
        values,
    );
    let parent: Arc<dyn Table> = source.table();
    // `distinct` is left out, because Java's result depends on the history: a group
    // without values is an empty vector if it is an initial group that never lost its
    // rows, and a null otherwise (`EMPTIED` and `NEVER` above), and the static copy has
    // every group as an initial group.
    let aggregations: Vec<Aggregation> = every_aggregation(false, by)
        .into_iter()
        .filter(|aggregation| {
            !matches!(
                aggregation,
                Aggregation::Columns {
                    spec: AggSpec::Distinct { .. },
                    ..
                }
            )
        })
        .collect();
    let result = seeded(&parent, &aggregations, preserve_empty, &initial_groups, by)
        .expect("a seeded aggregation");
    let recompute = {
        let (parent, result) = (parent.clone(), result.clone());
        move || {
            let groups = key_columns(&*result, by.len());
            seeded(
                &static_copy(&*parent),
                &aggregations,
                preserve_empty,
                &groups,
                by,
            )
            .expect("a static aggregation")
        }
    };
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
    checker.assert_consistent();
    for _ in 0..40 {
        source.push(random.next_change(&*parent, 5));
        graph.run_cycle();
        checker.assert_consistent();
        assert!(!result.is_failed(), "seed {seed}");
    }
    checker.updates_seen()
}

#[test]
fn random_updates_with_preserved_initial_groups() {
    let mut updates = 0;
    for seed in test_seeds(700..730) {
        updates += random_run(seed, &["K"], true);
        updates += random_run(seed, &["K", "G"], true);
    }
    assert!(
        updates > at_least_for(1000, 700..730),
        "only {updates} updates were checked"
    );
}

#[test]
fn random_updates_with_initial_groups_that_have_rows() {
    let mut updates = 0;
    for seed in test_seeds(800..830) {
        updates += random_run(seed, &["K"], false);
        updates += random_run(seed, &["G", "K"], false);
    }
    assert!(
        updates > at_least_for(1000, 800..830),
        "only {updates} updates were checked"
    );
}
