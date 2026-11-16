//! Tests of keyed transpose.
//!
//! The expected names, rows and failures are what the Java server gave for the same
//! tables, made in its Python console with `deephaven.table.keyed_transpose`: a static
//! table as `empty_table(n).update(...)`, a changing one as a keyed input table given the
//! same rows in the same steps.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{AggSpec, Pair};
use crate::table::in_memory_table;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray,
    UInt16Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_testkit::{
    at_least_for, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable, TableChange,
};

type Columns = Vec<(&'static str, ColumnType, ArrayRef)>;

fn ints(values: impl IntoIterator<Item = i32>) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(values))
}

fn strs<S: AsRef<str>>(values: impl IntoIterator<Item = Option<S>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<StringArray>())
}

fn batch(columns: &Columns) -> (TableDefinition, RecordBatch) {
    let definition = TableDefinition::new(
        columns
            .iter()
            .map(|(name, column_type, _)| ColumnDefinition::new(*name, column_type.clone()))
            .collect(),
    )
    .expect("distinct names");
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns.iter().map(|(_, _, array)| array.clone()).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch");
    (definition, batch)
}

fn static_table(columns: &Columns) -> Arc<dyn Table> {
    let (definition, batch) = batch(columns);
    in_memory_table(definition, &[batch]).expect("a table")
}

/// `empty_table(count).update(["R = i % 2", "C = `c` + (i % 3)", "V = i"])`, the rows
/// `rows` of it.
fn rcv(rows: impl IntoIterator<Item = i32> + Clone) -> Columns {
    vec![
        (
            "R",
            ColumnType::Int,
            ints(rows.clone().into_iter().map(|i| i % 2)),
        ),
        (
            "C",
            ColumnType::String,
            strs(
                rows.clone()
                    .into_iter()
                    .map(|i| Some(format!("c{}", i % 3))),
            ),
        ),
        ("V", ColumnType::Int, ints(rows)),
    ]
}

fn sum(pair: &str) -> Aggregation {
    let pair = match pair.split_once(" = ") {
        Some((output, input)) => Pair::new(output, input),
        None => Pair::of(pair),
    };
    Aggregation::columns(AggSpec::Sum, [pair])
}

fn transposed(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    row_by: &[&str],
    column_by: &[&str],
) -> Result<Arc<dyn Table>, TransposeError> {
    keyed_transpose(
        source,
        aggregations,
        row_by,
        column_by,
        None,
        NewColumnBehavior::Fail,
        &oxhaven_expr::Compiler,
    )
}

fn names(table: &dyn Table) -> String {
    let columns = table.definition().columns().iter();
    columns
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The rows of `table` as text: cells joined by `|`, null as `null`.
fn rows(table: &dyn Table) -> Vec<String> {
    let read = || {
        let rows = table.row_set();
        let columns: Vec<ArrayRef> = (0..table.definition().columns().len())
            .map(|index| table.column(index).chunk(&rows).expect("a readable column"))
            .collect();
        (0..rows.size() as usize)
            .map(|row| {
                let cells = columns.iter().map(|column| {
                    if column.is_null(row) {
                        "null".to_string()
                    } else {
                        array_value_to_string(column, row).expect("a printable value")
                    }
                });
                cells.collect::<Vec<_>>().join("|")
            })
            .collect()
    };
    match table.update_graph() {
        Some(graph) => locked(&*graph, read),
        None => read(),
    }
}

#[test]
fn one_aggregation_by_one_column_names_the_columns_after_the_values() {
    let source = static_table(&rcv(0..6));
    let result = transposed(&source, &[sum("V")], &["R"], &["C"]).expect("transposed");
    assert_eq!(names(&*result), "R, c0, c1, c2");
    assert_eq!(rows(&*result), ["0|0|4|2", "1|3|1|5"]);
    assert!(!result.is_refreshing());
}

#[test]
fn several_aggregations_put_the_output_name_first() {
    let source = static_table(&rcv(0..6));
    let aggregations = [sum("V"), Aggregation::count("N")];
    let result = transposed(&source, &aggregations, &["R"], &["C"]).expect("transposed");
    assert_eq!(names(&*result), "R, V_c0, N_c0, V_c1, N_c1, V_c2, N_c2");
    assert_eq!(rows(&*result), ["0|0|1|4|1|2|1", "1|3|1|1|1|5|1"]);

    // An output with the name of a value, and one that would be a row-by column.
    let aggregations = [sum("c0 = V"), Aggregation::count("R2")];
    let result = transposed(&source, &aggregations, &["R"], &["C"]).expect("transposed");
    assert_eq!(
        names(&*result),
        "R, c0_c0, R2_c0, c0_c1, R2_c1, c0_c2, R2_c2"
    );
}

/// `empty_table(8).update(["R = i % 2", "C = `c` + (i % 3)", "D = i % 2 == 0 ? `x` : `y`",
/// "V = i", "Q = i % 4"])`
fn wide() -> Arc<dyn Table> {
    let mut columns = rcv(0..8);
    let letters = (0..8).map(|i| Some(if i % 2 == 0 { "x" } else { "y" }));
    columns.insert(2, ("D", ColumnType::String, strs(letters)));
    columns.push(("Q", ColumnType::Int, ints((0..8).map(|i| i % 4))));
    static_table(&columns)
}

#[test]
fn several_column_by_columns_join_their_values() {
    let result = transposed(&wide(), &[sum("V")], &["R"], &["C", "D"]).expect("transposed");
    assert_eq!(names(&*result), "R, c0_x, c1_y, c2_x, c0_y, c1_x, c2_y");
    assert_eq!(
        rows(&*result),
        ["0|6|null|2|null|4|null", "1|null|8|null|3|null|5"]
    );

    let aggregations = [
        sum("V"),
        Aggregation::columns(AggSpec::Max, [Pair::new("M", "V")]),
    ];
    let result = transposed(&wide(), &aggregations, &["R", "Q"], &["C", "D"]).expect("two by two");
    assert_eq!(
        names(&*result),
        "R, Q, V_c0_x, M_c0_x, V_c1_y, M_c1_y, V_c2_x, M_c2_x, V_c0_y, M_c0_y, V_c1_x, M_c1_x, \
         V_c2_y, M_c2_y"
    );
    assert_eq!(
        rows(&*result),
        [
            "0|0|0|0|null|null|null|null|null|null|4|4|null|null",
            "0|2|6|6|null|null|2|2|null|null|null|null|null|null",
            "1|1|null|null|1|1|null|null|null|null|null|null|5|5",
            "1|3|null|null|7|7|null|null|3|3|null|null|null|null"
        ]
    );
}

#[test]
fn a_column_may_be_a_row_by_and_a_column_by_column() {
    let source = static_table(&rcv(0..6));
    let result = transposed(&source, &[sum("V")], &["R", "C"], &["C"]).expect("transposed");
    assert_eq!(names(&*result), "R, C, c0, c1, c2");
    assert_eq!(
        rows(&*result),
        [
            "0|c0|0|null|null",
            "1|c0|3|null|null",
            "1|c1|null|1|null",
            "0|c1|null|4|null",
            "0|c2|null|null|2",
            "1|c2|null|null|5"
        ]
    );
    // `empty_table(8)` with the same columns, by "R" and "R".
    let source = static_table(&rcv(0..8));
    let result = transposed(&source, &[sum("V")], &["R"], &["R"]).expect("transposed");
    assert_eq!(names(&*result), "R, column_0, column_1");
    assert_eq!(rows(&*result), ["0|12|null", "1|null|16"]);
}

#[test]
fn values_become_legal_and_distinct_column_names() {
    // C = `1-2.3/4`, `12`, `a b`, `ab`, null, `int`, `a-b`, `null`
    let values = [
        Some("1-2.3/4"),
        Some("12"),
        Some("a b"),
        Some("ab"),
        None,
        Some("int"),
        Some("a-b"),
        Some("null"),
    ];
    let source = static_table(&vec![
        ("R", ColumnType::Int, ints((0..8).map(|i| i % 2))),
        ("C", ColumnType::String, strs(values)),
        ("V", ColumnType::Int, ints(0..8)),
    ]);
    let result = transposed(&source, &[sum("V")], &["R"], &["C"]).expect("transposed");
    assert_eq!(
        names(&*result),
        "R, column_1234, column_12, ab, ab2, column_null, column_int, ab3, column_null2"
    );
    assert_eq!(
        rows(&*result),
        [
            "0|0|null|2|null|4|null|6|null",
            "1|null|1|null|3|null|5|null|7"
        ]
    );
}

#[test]
fn values_of_each_type_are_named_as_java_prints_them() {
    let by = |column_type: ColumnType, values: ArrayRef| {
        let source = static_table(&vec![
            ("R", ColumnType::Int, ints((0..6).map(|i| i % 2))),
            ("K", column_type, values),
            ("V", ColumnType::Int, ints(0..6)),
        ]);
        let result = transposed(&source, &[sum("V")], &["R"], &["K"]).expect("transposed");
        (names(&*result), rows(&*result))
    };
    let rows_of_three = ["0|0|4|2", "1|3|1|5"].map(String::from).to_vec();
    let numbered = (
        "R, column_0, column_1, column_2".to_string(),
        rows_of_three.clone(),
    );
    assert_eq!(by(ColumnType::Int, ints((0..6).map(|i| i % 3))), numbered);
    let longs = arrow::array::Int64Array::from_iter_values((0..6).map(|i| i % 3));
    assert_eq!(by(ColumnType::Long, Arc::new(longs)), numbered);
    let shorts = arrow::array::Int16Array::from_iter_values((0..6).map(|i| i % 3));
    assert_eq!(by(ColumnType::Short, Arc::new(shorts)), numbered);
    let bytes = arrow::array::Int8Array::from_iter_values((0..6).map(|i| i % 3));
    assert_eq!(by(ColumnType::Byte, Arc::new(bytes)), numbered);
    let chars = UInt16Array::from_iter_values((0..6).map(|i| 65 + i % 3));
    assert_eq!(
        by(ColumnType::Char, Arc::new(chars)),
        ("R, A, B, C".to_string(), rows_of_three.clone())
    );
    let doubles = Float64Array::from_iter_values((0..6).map(|i| f64::from(i % 3) * 1.5));
    assert_eq!(
        by(ColumnType::Double, Arc::new(doubles)),
        (
            "R, column_00, column_15, column_30".to_string(),
            rows_of_three.clone()
        )
    );
    let flags = BooleanArray::from((0..6).map(|i| i % 3 == 0).collect::<Vec<_>>());
    assert_eq!(
        by(ColumnType::Boolean, Arc::new(flags)),
        (
            "R, column_true, column_false".to_string(),
            vec!["0|0|6".to_string(), "1|3|6".to_string()]
        )
    );
    // NI = i % 3 == 0 ? null : i % 3
    let with_null: Int32Array = (0..6).map(|i| (i % 3 != 0).then_some(i % 3)).collect();
    assert_eq!(
        by(ColumnType::Int, Arc::new(with_null)),
        (
            "R, column_null, column_1, column_2".to_string(),
            rows_of_three
        )
    );
}

/// Observed on Java 42.9: `float` values 0.5, 1.5 and NaN and the instants
/// 2023-11-14T22:13:20Z, the same and a half second, and null name these columns.
#[test]
fn floats_and_instants_name_columns() {
    let by = |column_type: ColumnType, values: ArrayRef| {
        let source = static_table(&vec![
            ("R", ColumnType::Int, ints((0..6).map(|i| i % 2))),
            ("K", column_type, values),
            ("V", ColumnType::Int, ints(0..6)),
        ]);
        let result = transposed(&source, &[sum("V")], &["R"], &["K"]).expect("transposed");
        (names(&*result), rows(&*result))
    };
    let rows_of_three = ["0|0|4|2", "1|3|1|5"].map(String::from).to_vec();
    let floats = arrow::array::Float32Array::from_iter_values(
        (0..6).map(|i| [0.5, 1.5, f32::NAN][i % 3]),
    );
    assert_eq!(
        by(ColumnType::Float, Arc::new(floats)),
        (
            "R, column_05, column_15, NaN".to_string(),
            rows_of_three.clone()
        )
    );
    let instants: arrow::array::TimestampNanosecondArray = (0..6)
        .map(|i| {
            [
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_500_000_000),
                None,
            ][i % 3]
        })
        .collect();
    assert_eq!(
        by(
            ColumnType::Instant,
            Arc::new(instants.with_timezone("UTC"))
        ),
        (
            "R, column_20231114T221320Z, column_20231114T221320500Z, column_null".to_string(),
            rows_of_three
        )
    );
}

#[test]
fn what_java_refuses_is_refused_with_its_text() {
    let source = static_table(&rcv(0..6));
    let refusal =
        |source: &Arc<dyn Table>, aggregations: &[Aggregation], rows: &[&str], columns: &[&str]| {
            match transposed(source, aggregations, rows, columns) {
                Err(error) => error.to_string(),
                Ok(_) => panic!("not refused"),
            }
        };
    assert_eq!(
        refusal(&source, &[], &["R"], &["C"]),
        "No aggregations defined"
    );
    assert_eq!(
        refusal(&source, &[sum("V")], &[], &["C"]),
        "No rowByColumns defined"
    );
    assert_eq!(
        refusal(&source, &[sum("V")], &["R"], &[]),
        "No columnByColumns defined"
    );
    let empty = static_table(&rcv(0..0));
    assert_eq!(
        refusal(&empty, &[sum("V")], &["R"], &["C"]),
        "The source table has no values for column by columns"
    );
    assert_eq!(
        transposed(&source, &[sum("V")], &["R"], &["Z"]).err(),
        Some(TransposeError::Aggregate(AggregateError::UnknownColumn(
            "Z".to_string()
        )))
    );
    // C = i < 2 ? `` : `-`
    let unnamed = static_table(&vec![
        ("R", ColumnType::Int, ints(0..4)),
        (
            "C",
            ColumnType::String,
            strs([Some(""), Some(""), Some("-"), Some("-")]),
        ),
        ("V", ColumnType::Int, ints(0..4)),
    ]);
    assert_eq!(
        refusal(&unnamed, &[sum("V")], &["R"], &["C"]),
        "Can not legalize column name "
    );
    // C = i < 2 ? `R` : `x`: a value with the name of a row-by column.
    let clash = static_table(&vec![
        ("R", ColumnType::Int, ints((0..4).map(|i| i % 2))),
        (
            "C",
            ColumnType::String,
            strs([Some("R"), Some("R"), Some("x"), Some("x")]),
        ),
        ("V", ColumnType::Int, ints(0..4)),
    ]);
    assert_eq!(
        refusal(&clash, &[sum("V")], &["R"], &["C"]),
        "Column R defined in table key columns and table 0"
    );
}

#[test]
fn initial_groups_give_columns_and_rows_the_source_has_no_values_for() {
    let source = static_table(&rcv(0..6));
    // empty_table(3).update(["R = 5 - i", "C = `c` + (i * 2)"])
    let initial = static_table(&vec![
        ("R", ColumnType::Int, ints((0..3).map(|i| 5 - i))),
        (
            "C",
            ColumnType::String,
            strs((0..3).map(|i| Some(format!("c{}", i * 2)))),
        ),
    ]);
    let with = |initial: &Arc<dyn Table>, aggregations: &[Aggregation]| {
        keyed_transpose(
            &source,
            aggregations,
            &["R"],
            &["C"],
            Some(initial),
            NewColumnBehavior::Fail,
            &oxhaven_expr::Compiler,
        )
        .expect("transposed")
    };
    let result = with(&initial, &[sum("V")]);
    assert_eq!(names(&*result), "R, c0, c2, c4, c1");
    assert_eq!(
        rows(&*result),
        [
            "5|null|null|null|null",
            "0|0|2|null|4",
            "1|3|5|null|1",
            "4|null|null|null|null",
            "3|null|null|null|null"
        ]
    );
    // Initial groups without a row are no initial groups.
    let none = static_table(&vec![
        ("R", ColumnType::Int, ints([])),
        ("C", ColumnType::String, strs(Vec::<Option<&str>>::new())),
    ]);
    let result = with(&none, &[sum("V"), Aggregation::count("N")]);
    assert_eq!(names(&*result), "R, V_c0, N_c0, V_c1, N_c1, V_c2, N_c2");
    assert_eq!(rows(&*result), ["0|0|1|4|1|2|1", "1|3|1|1|1|5|1"]);
}

/// A changing table of the columns of [`rcv`] and the transposes of it under test.
struct Ticking {
    graph: Arc<SteppedUpdateGraph>,
    source: Arc<ScriptedTable>,
    parent: Arc<dyn Table>,
}

impl Ticking {
    /// With the rows `rows`, each at the row key of its number.
    fn new(rows: std::ops::Range<i32>) -> Ticking {
        let graph = SteppedUpdateGraph::new();
        let (definition, batch) = batch(&rcv(rows.clone()));
        let source = ScriptedTable::new(
            graph.clone(),
            definition,
            RowSet::from_keys(rows.map(RowKey::from)),
            batch.columns().to_vec(),
        );
        let parent: Arc<dyn Table> = source.table();
        Ticking {
            graph,
            source,
            parent,
        }
    }

    fn transposed(
        &self,
        aggregations: &[Aggregation],
        initial_groups: Option<&Arc<dyn Table>>,
        behavior: NewColumnBehavior,
    ) -> (Arc<dyn Table>, Arc<ConsistencyChecker>) {
        let result = keyed_transpose(
            &self.parent,
            aggregations,
            &["R"],
            &["C"],
            initial_groups,
            behavior,
            &oxhaven_expr::Compiler,
        )
        .expect("transposed");
        assert!(result.is_refreshing());
        let checker = ConsistencyChecker::attach(result.clone());
        checker.assert_consistent();
        (result, checker)
    }

    fn step(&self, change: TableChange) {
        self.source.push(change);
        self.graph.run_cycle();
    }

    /// Adds the rows `rows` of [`rcv`], with `R` and `C` as given where they are.
    fn add(&self, rows: &[i32], r: Option<i32>, c: Option<&str>) {
        let mut columns = rcv(rows.iter().copied());
        if let Some(r) = r {
            columns[0].2 = ints(rows.iter().map(|_| r));
        }
        if let Some(c) = c {
            columns[1].2 = strs(rows.iter().map(|_| Some(c)));
        }
        let values = columns.into_iter().map(|(_, _, values)| values).collect();
        self.step(TableChange::new().add(rows.iter().map(|&row| RowKey::from(row)), values));
    }

    fn remove(&self, rows: &[i32]) {
        self.step(TableChange::new().remove(rows.iter().map(|&row| RowKey::from(row))));
    }
}

const SUM_AND_COUNT: &str = "R, V_c0, N_c0, V_c1, N_c1, V_c2, N_c2";

#[test]
fn rows_of_known_values_change_the_result_and_a_new_value_fails_or_is_ignored() {
    let ticking = Ticking::new(0..4);
    let aggregations = [sum("V"), Aggregation::count("N")];
    let (failing, failing_checker) =
        ticking.transposed(&aggregations, None, NewColumnBehavior::Fail);
    let (ignoring, ignoring_checker) =
        ticking.transposed(&aggregations, None, NewColumnBehavior::Ignore);
    let both = |expected: &[&str]| {
        failing_checker.assert_consistent();
        ignoring_checker.assert_consistent();
        assert_eq!(rows(&*failing), expected);
        assert_eq!(rows(&*ignoring), expected);
        assert!(!failing.is_failed());
    };
    assert_eq!(names(&*failing), SUM_AND_COUNT);
    assert_eq!(names(&*ignoring), SUM_AND_COUNT);
    both(&["0|0|1|null|null|2|1", "1|3|1|1|1|null|null"]);

    ticking.add(&[4], None, None);
    both(&["0|0|1|4|1|2|1", "1|3|1|1|1|null|null"]);
    ticking.remove(&[1]);
    both(&["0|0|1|4|1|2|1", "1|3|1|null|null|null|null"]);
    // A row of a new value of the row-by column.
    ticking.add(&[6], Some(7), None);
    both(&[
        "0|0|1|4|1|2|1",
        "1|3|1|null|null|null|null",
        "7|6|1|null|null|null|null",
    ]);

    // A value of the column-by column that has no columns.
    ticking.add(&[5], None, Some("zz"));
    ignoring_checker.assert_consistent();
    assert!(failing.is_failed());
    assert!(!ignoring.is_failed());
    assert_eq!(names(&*ignoring), SUM_AND_COUNT);
    assert_eq!(
        rows(&*ignoring),
        [
            "0|0|1|4|1|2|1",
            "1|3|1|null|null|null|null",
            "7|6|1|null|null|null|null"
        ]
    );
    // The values that have columns go on changing. The only row of `R` 1 left is of the
    // value without columns, so `R` 1 has no row.
    ticking.remove(&[0, 3]);
    ignoring_checker.assert_consistent();
    assert_eq!(
        rows(&*ignoring),
        ["0|null|null|4|1|2|1", "7|6|1|null|null|null|null"]
    );
}

#[test]
fn a_value_that_loses_its_rows_and_returns_fails_the_result_unless_ignored() {
    let ticking = Ticking::new(0..4);
    let aggregations = [sum("V"), Aggregation::count("N")];
    let (failing, checker) = ticking.transposed(&aggregations, None, NewColumnBehavior::Fail);
    let (ignoring, ignoring_checker) =
        ticking.transposed(&aggregations, None, NewColumnBehavior::Ignore);
    ticking.add(&[4], None, None);
    ticking.remove(&[1]);
    // The last row of `c1` leaves: its table leaves the table of tables, which is no
    // failure.
    ticking.remove(&[4]);
    checker.assert_consistent();
    assert!(!failing.is_failed());
    assert_eq!(
        rows(&*failing),
        ["0|0|1|null|null|2|1", "1|3|1|null|null|null|null"]
    );
    // Rows of `c1` again: its table returns to the table of tables as a new one.
    ticking.add(&[1, 4], None, None);
    assert!(failing.is_failed());
    ignoring_checker.assert_consistent();
    assert!(!ignoring.is_failed());
    assert_eq!(rows(&*ignoring), ["0|0|1|4|1|2|1", "1|3|1|1|1|null|null"]);
}

#[test]
fn a_row_that_moves_to_another_known_value_is_no_new_column() {
    let ticking = Ticking::new(0..4);
    let (result, checker) = ticking.transposed(&[sum("V")], None, NewColumnBehavior::Fail);
    assert_eq!(rows(&*result), ["0|0|null|2", "1|3|1|null"]);
    // Row 0 changes its `C` from `c0` to `c1`.
    ticking.step(TableChange::new().modify([0], [(1, strs([Some("c1")]))]));
    checker.assert_consistent();
    assert!(!result.is_failed());
    assert_eq!(rows(&*result), ["0|null|0|2", "1|3|1|null"]);
    // `c0` loses its last row, and has one again.
    ticking.remove(&[3]);
    checker.assert_consistent();
    assert!(!result.is_failed());
    ticking.add(&[3], None, None);
    assert!(result.is_failed());
}

#[test]
fn initial_groups_keep_the_columns_of_a_changing_table() {
    let ticking = Ticking::new(0..4);
    // empty_table(2).update(["R = 9", "C = `c` + (i * 2 + 1)"])
    let initial = static_table(&vec![
        ("R", ColumnType::Int, ints([9, 9])),
        ("C", ColumnType::String, strs([Some("c1"), Some("c3")])),
    ]);
    let (result, checker) =
        ticking.transposed(&[sum("V")], Some(&initial), NewColumnBehavior::Fail);
    assert_eq!(names(&*result), "R, c1, c3, c0, c2");
    assert_eq!(
        rows(&*result),
        [
            "9|null|null|null|null",
            "1|1|null|3|null",
            "0|null|null|0|2"
        ]
    );
    // `c0` loses its rows and keeps its table, because the groups are kept.
    ticking.remove(&[0, 3]);
    checker.assert_consistent();
    assert!(!result.is_failed());
    assert_eq!(
        rows(&*result),
        [
            "9|null|null|null|null",
            "1|1|null|null|null",
            "0|null|null|null|2"
        ]
    );
    ticking.add(&[5], None, None);
    checker.assert_consistent();
    assert!(!result.is_failed());
    assert_eq!(
        rows(&*result),
        [
            "9|null|null|null|null",
            "1|1|null|null|5",
            "0|null|null|null|2"
        ]
    );
}

/// A transpose of a randomly changing table whose initial groups are every pair of a
/// row-by and a column-by value the table can have, so that no value is ever new and no
/// group ever leaves: checked after every cycle against the transpose of a static copy.
/// Returns the number of updates the result delivered.
fn random_run(seed: u64) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    let columns = [
        ("K", ColumnType::String),
        ("C", ColumnType::Char),
        ("I", ColumnType::Int),
        ("D", ColumnType::Double),
    ];
    let values = columns
        .iter()
        .map(|(_, column_type)| random.values(column_type, 6))
        .collect();
    let source = ScriptedTable::new(
        graph.clone(),
        oxhaven_testkit::definition(&columns),
        RowSet::flat(6),
        values,
    );
    let parent: Arc<dyn Table> = source.table();
    // The values `RandomUpdates` has for a string and for a char, and null.
    let letters: Vec<Option<String>> = ('a'..='h')
        .map(|letter| Some(letter.to_string()))
        .chain([None])
        .collect();
    let chars: Vec<Option<u16>> = (b'a'..=b'h')
        .map(|c| Some(u16::from(c)))
        .chain([None])
        .collect();
    let pairs = || {
        letters
            .iter()
            .flat_map(|k| chars.iter().map(move |c| (k, c)))
    };
    let initial = static_table(&vec![
        (
            "K",
            ColumnType::String,
            strs(pairs().map(|(k, _)| k.clone())),
        ),
        (
            "C",
            ColumnType::Char,
            Arc::new(pairs().map(|(_, c)| *c).collect::<UInt16Array>()) as ArrayRef,
        ),
    ]);
    let aggregations = vec![
        sum("SI = I"),
        Aggregation::count("N"),
        Aggregation::columns(AggSpec::Last, [Pair::new("LD", "D")]),
        Aggregation::columns(AggSpec::Min, [Pair::new("MD", "D")]),
        Aggregation::columns(AggSpec::Avg, [Pair::new("AI", "I")]),
    ];
    let make = move |source: &Arc<dyn Table>| {
        keyed_transpose(
            source,
            &aggregations,
            &["K"],
            &["C"],
            Some(&initial),
            NewColumnBehavior::Fail,
            &oxhaven_expr::Compiler,
        )
        .expect("transposed")
    };
    let result = make(&parent);
    assert_eq!(result.definition().columns().len(), 1 + 9 * 5);
    let recompute = {
        let parent = parent.clone();
        move || {
            let rows = parent.row_set();
            let copied: Columns = parent
                .definition()
                .columns()
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let values = parent
                        .column(index)
                        .chunk(&rows)
                        .expect("a readable column");
                    let name: &'static str = columns[index].0;
                    (name, column.column_type.clone(), values)
                })
                .collect();
            make(&static_table(&copied))
        }
    };
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
    checker.assert_consistent();
    for _ in 0..30 {
        source.push(random.next_change(&*parent, 5));
        graph.run_cycle();
        checker.assert_consistent();
        assert!(!result.is_failed(), "seed {seed}");
    }
    // A sample of the cells, so that a result of nulls alone would not pass.
    let cells = result.column(2).chunk(&result.row_set()).expect("a column");
    assert_eq!(cells.len(), 9);
    assert!(cells.as_primitive::<arrow::datatypes::Int64Type>().len() == 9);
    checker.updates_seen()
}

#[test]
fn random_updates_with_every_group_among_the_initial_groups() {
    let updates: u64 = test_seeds(0..40).map(random_run).sum();
    assert!(
        updates > at_least_for(600, 0..40),
        "only {updates} updates were checked"
    );
}
