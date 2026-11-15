//! The incremental cases of `update_by`, the requests it refuses, and random updates.
//!
//! Every ticking test runs through `Ticking`, which checks each update for consistency and
//! compares the result after each cycle with `update_by` of a static copy of the source.
//! The values themselves are checked against Java in `corpus.rs`; the tests here check
//! which rows and columns an update reports, and that a result kept up to date equals one
//! computed from scratch.

use super::{
    update_by_with, BadDataBehavior, DeltaControl, EmSpec, RollingWindow, UpdateByError,
    UpdateByOperation, UpdateBySpec, WindowScale,
};
use crate::ops::sort::testing::{keys, shifts, static_table, Ticking};
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray};
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{test_seeds, RandomUpdates, TableChange, TableSnapshot};
use std::sync::Arc;

/// `update_by` with the formula compiler, which the count-where operators need.
fn update_by(
    source: &Arc<dyn Table>,
    operations: &[UpdateByOperation],
    by: &[&str],
) -> Result<Arc<dyn Table>, UpdateByError> {
    update_by_with(source, operations, by, &oxhaven_expr::Compiler)
}

fn op(spec: UpdateBySpec, columns: &[&str]) -> UpdateByOperation {
    UpdateByOperation::new(spec, columns)
}

fn ints(values: &[i32]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn texts(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn doubles(values: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

/// Rows 0, 10, 20, 30, 40 with `K` a, b, a, b, a and `I` 1 to 5; the result column `R`
/// (index 3) is made by `operations` over the buckets of `by`.
fn ticking(operations: Vec<UpdateByOperation>, by: &'static [&'static str]) -> Ticking {
    Ticking::new(
        [0, 10, 20, 30, 40],
        &[
            ("K", ColumnType::String, texts(&["a", "b", "a", "b", "a"])),
            ("I", ColumnType::Int, ints(&[1, 2, 3, 4, 5])),
            ("D", ColumnType::Double, doubles(&[0.5, 1.5, 2.5, 3.5, 4.5])),
        ],
        move |source| update_by(source, &operations, by).unwrap(),
    )
}

fn cum_sum(by: &'static [&'static str]) -> Ticking {
    ticking(vec![op(UpdateBySpec::CumSum, &["R = I"])], by)
}

fn rolling_sum(rev: i64, fwd: i64) -> Ticking {
    let window = RollingWindow::ticks(rev, fwd);
    ticking(vec![op(UpdateBySpec::RollingSum(window), &["R = I"])], &[])
}

fn row(k: &str, i: i32, d: f64) -> Vec<ArrayRef> {
    vec![texts(&[k]), ints(&[i]), doubles(&[d])]
}

/// The added, removed and modified rows of an update and its modified columns.
fn shape(update: &TableUpdate) -> (Vec<RowKey>, Vec<RowKey>, Vec<RowKey>, ModifiedColumnSet) {
    (
        keys(&update.added),
        keys(&update.removed),
        keys(&update.modified),
        update.modified_columns.clone(),
    )
}

const R: usize = 3;

#[test]
fn an_added_row_recomputes_the_cumulative_rows_from_it_on() {
    let t = cum_sum(&[]);
    let update = t
        .step(TableChange::new().add([25], row("a", 10, 0.0)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![25], vec![], vec![30, 40], ModifiedColumnSet::of([R]))
    );
}

#[test]
fn a_row_added_at_the_end_modifies_nothing() {
    let t = cum_sum(&[]);
    let update = t
        .step(TableChange::new().add([50], row("a", 10, 0.0)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![50], vec![], vec![], ModifiedColumnSet::empty())
    );
}

#[test]
fn a_removed_row_recomputes_the_cumulative_rows_after_it() {
    let t = cum_sum(&[]);
    let update = t.step(TableChange::new().remove([20])).unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![20], vec![30, 40], ModifiedColumnSet::of([R]))
    );
    // The first row: everything is recomputed. The last row: nothing is.
    let update = t.step(TableChange::new().remove([0])).unwrap();
    assert_eq!(keys(&update.modified), vec![10, 30, 40]);
    let update = t.step(TableChange::new().remove([40])).unwrap();
    assert_eq!(keys(&update.modified), Vec::<RowKey>::new());
}

#[test]
fn a_modified_input_recomputes_from_the_modified_row() {
    let t = cum_sum(&[]);
    let update = t
        .step(TableChange::new().modify([10], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (
            vec![],
            vec![],
            vec![10, 20, 30, 40],
            ModifiedColumnSet::of([1, R])
        )
    );
}

#[test]
fn a_modified_column_that_is_no_input_recomputes_nothing() {
    for t in [cum_sum(&[]), rolling_sum(2, 0)] {
        let update = t
            .step(TableChange::new().modify([10], [(2, doubles(&[9.0]))]))
            .unwrap();
        assert_eq!(
            shape(&update),
            (vec![], vec![], vec![10], ModifiedColumnSet::of([2]))
        );
    }
}

#[test]
fn shifted_rows_keep_their_results() {
    for t in [cum_sum(&[]), cum_sum(&["K"]), rolling_sum(2, 1)] {
        let update = t.step(TableChange::new().shift(20, 40, 5)).unwrap();
        assert_eq!(shifts(&update), vec![(20, 40, 5)]);
        assert_eq!(
            shape(&update),
            (vec![], vec![], vec![], ModifiedColumnSet::empty())
        );
    }
}

#[test]
fn a_shift_with_a_remove_and_an_add_recomputes_in_the_new_key_space() {
    let t = cum_sum(&[]);
    let change = TableChange::new()
        .remove([10])
        .shift(20, 40, 100)
        .add([15], row("b", 6, 0.0));
    let update = t.step(change).unwrap();
    assert_eq!(
        shape(&update),
        (
            vec![15],
            vec![10],
            vec![120, 130, 140],
            ModifiedColumnSet::of([R])
        )
    );
}

#[test]
fn a_row_that_changes_bucket_is_removed_from_one_and_added_to_the_other() {
    let t = cum_sum(&["K"]);
    let update = t
        .step(TableChange::new().modify([20], [(0, texts(&["b"]))]))
        .unwrap();
    // Bucket a loses row 20, so row 40 after it is recomputed; bucket b gains it between
    // rows 10 and 30, so it and row 30 are.
    assert_eq!(
        shape(&update),
        (
            vec![],
            vec![],
            vec![20, 30, 40],
            ModifiedColumnSet::of([0, R])
        )
    );
}

#[test]
fn only_the_buckets_that_changed_are_recomputed() {
    let t = cum_sum(&["K"]);
    let update = t
        .step(TableChange::new().add([5], row("b", 9, 0.0)))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![10, 30]);
    let update = t
        .step(TableChange::new().add([45], row("z", 9, 0.0)))
        .unwrap();
    assert_eq!(keys(&update.modified), Vec::<RowKey>::new());
    // The new bucket z empties and fills again.
    t.step(TableChange::new().remove([45])).unwrap();
    let update = t
        .step(TableChange::new().add([1, 2], [row("z", 1, 0.0), row("z", 2, 0.0)].concat_rows()))
        .unwrap();
    assert_eq!(keys(&update.added), vec![1, 2]);
}

trait ConcatRows {
    fn concat_rows(&self) -> Vec<ArrayRef>;
}

impl ConcatRows for [Vec<ArrayRef>; 2] {
    fn concat_rows(&self) -> Vec<ArrayRef> {
        (0..self[0].len())
            .map(|c| arrow::compute::concat(&[self[0][c].as_ref(), self[1][c].as_ref()]).unwrap())
            .collect()
    }
}

#[test]
fn a_rolling_window_recomputes_the_rows_whose_window_holds_the_change() {
    // Two rows back: an added or modified row is in its own window and the next row's.
    let t = rolling_sum(2, 0);
    let update = t
        .step(TableChange::new().add([25], row("a", 10, 0.0)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![25], vec![], vec![30], ModifiedColumnSet::of([R]))
    );
    let update = t
        .step(TableChange::new().modify([0], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![0, 10]);
    let update = t.step(TableChange::new().remove([20])).unwrap();
    assert_eq!(keys(&update.modified), vec![25]);

    // One row back and two forward.
    let t = rolling_sum(1, 2);
    let update = t.step(TableChange::new().remove([30])).unwrap();
    assert_eq!(keys(&update.modified), vec![10, 20]);
    let update = t
        .step(TableChange::new().modify([40], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![10, 20, 40]);
}

#[test]
fn an_empty_table_fills_and_empties() {
    let bys: [&'static [&'static str]; 2] = [&[], &["K"]];
    for by in bys {
        let operations = vec![
            op(UpdateBySpec::CumSum, &["R = I"]),
            op(
                UpdateBySpec::RollingAvg(RollingWindow::ticks(2, 1)),
                &["A = I"],
            ),
        ];
        let t = Ticking::new(
            [],
            &[
                ("K", ColumnType::String, texts(&[])),
                ("I", ColumnType::Int, ints(&[])),
                ("D", ColumnType::Double, doubles(&[])),
            ],
            move |source| update_by(source, &operations, by).unwrap(),
        );
        assert!(t.step(TableChange::new()).is_none());
        t.step(TableChange::new().add([3], row("a", 1, 0.0)))
            .unwrap();
        t.step(TableChange::new().remove([3])).unwrap();
        assert_eq!(t.result.row_set().size(), 0);
        t.step(TableChange::new().add([3], row("a", 1, 0.0)))
            .unwrap();
    }
}

#[test]
fn a_failed_parent_fails_the_result() {
    let t = cum_sum(&["K"]);
    assert_eq!(t.fail("boom").message, "boom");
    assert!(t.result.is_failed());
}

#[test]
fn results_replace_columns_of_the_same_name_in_place() {
    let source = static_table(&[
        ("K", ColumnType::String, texts(&["a", "b"])),
        ("I", ColumnType::Int, ints(&[1, 2])),
        ("D", ColumnType::Double, doubles(&[0.5, 1.5])),
    ]);
    let operations = [
        op(UpdateBySpec::CumMax, &["Top = D"]),
        op(UpdateBySpec::CumSum, &["I"]),
    ];
    let result = update_by(&source, &operations, &["K"]).unwrap();
    let columns: Vec<(&str, &ColumnType)> = result
        .definition()
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), &c.column_type))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("K", &ColumnType::String),
            ("I", &ColumnType::Long),
            ("D", &ColumnType::Double),
            ("Top", &ColumnType::Double),
        ]
    );
}

fn refused(operations: &[UpdateByOperation], by: &[&str]) -> String {
    let source = static_table(&[
        ("K", ColumnType::String, texts(&["a", "b"])),
        ("I", ColumnType::Int, ints(&[1, 2])),
    ]);
    match update_by(&source, operations, by) {
        Err(UpdateByError::InvalidArgument(message)) => message,
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("not refused"),
    }
}

#[test]
fn bad_requests_are_errors() {
    let sum = |columns: &[&str]| op(UpdateBySpec::CumSum, columns);
    assert!(refused(&[], &[]).contains("At least one operator"));
    assert!(refused(&[sum(&["R = Nope"])], &[]).contains("Nope"));
    assert!(refused(&[sum(&["R = I"])], &["Nope"]).contains("Missing byColumns"));
    assert!(refused(&[sum(&["R = I"]), sum(&["R = I"])], &[]).contains("unique"));
    assert!(refused(&[sum(&["R = K"])], &[]).contains("Can not perform"));
    assert!(refused(&[sum(&["1R = I"])], &[]).contains("Invalid column name"));
    let backwards = RollingWindow::ticks(-3, 2);
    assert!(
        refused(&[op(UpdateBySpec::RollingSum(backwards), &["R = I"])], &[])
            .contains("non-negative")
    );
    let delta = op(UpdateBySpec::Delta(DeltaControl::ZeroDominates), &["R = K"]);
    assert!(refused(&[delta], &[]).contains("Can not perform"));
}

fn em() -> EmSpec {
    EmSpec {
        window: WindowScale::Ticks(5.0),
        on_null: Some(BadDataBehavior::Skip),
        on_nan: None,
    }
}

/// The cells of the vector column at `column`: null, or the `int` elements.
fn int_vectors(table: &dyn Table, column: usize) -> Vec<Option<Vec<Option<i32>>>> {
    let cells = TableSnapshot::current(table).unwrap().columns[column].clone();
    let cells = cells.as_any().downcast_ref::<ListArray>().unwrap();
    cells
        .iter()
        .map(|cell| {
            cell.map(|cell| {
                let cell = cell.as_any().downcast_ref::<Int32Array>().unwrap();
                cell.iter().collect()
            })
        })
        .collect()
}

fn cell(values: &[i32]) -> Option<Vec<Option<i32>>> {
    Some(values.iter().copied().map(Some).collect())
}

fn rolling_group(window: RollingWindow, by: &'static [&'static str]) -> Ticking {
    ticking(vec![op(UpdateBySpec::RollingGroup(window), &["R = I"])], by)
}

#[test]
fn a_rolling_group_holds_the_values_of_each_window() {
    let t = rolling_group(RollingWindow::ticks(2, 1), &[]);
    assert_eq!(
        t.result.definition().columns()[R].column_type,
        ColumnType::vector(ColumnType::Int)
    );
    assert_eq!(
        int_vectors(&*t.result, R),
        vec![
            cell(&[1, 2]),
            cell(&[1, 2, 3]),
            cell(&[2, 3, 4]),
            cell(&[3, 4, 5]),
            cell(&[4, 5])
        ]
    );
    // Per bucket, and a window ahead of its row, which is empty at the end of the bucket.
    let t = rolling_group(RollingWindow::ticks(-1, 2), &["K"]);
    assert_eq!(
        int_vectors(&*t.result, R),
        vec![cell(&[5]), cell(&[]), cell(&[]), cell(&[]), cell(&[])]
    );
}

#[test]
fn a_rolling_group_over_no_rows_is_empty_where_the_other_operators_fail() {
    let t = rolling_group(RollingWindow::ticks(0, 0), &[]);
    assert_eq!(int_vectors(&*t.result, R), vec![cell(&[]); 5]);
    let source = static_table(&[("I", ColumnType::Int, ints(&[1, 2]))]);
    let window = || RollingWindow::ticks(0, 0);
    let both = [
        op(UpdateBySpec::RollingGroup(window()), &["R = I"]),
        op(UpdateBySpec::RollingSum(window()), &["Q = I"]),
    ];
    let error = update_by(&source, &both, &[] as &[&str]).err().unwrap();
    assert!(error.to_string().contains("Assertion failed"), "{error}");
}

#[test]
fn a_rolling_group_writes_the_rows_whose_window_holds_the_change() {
    let t = rolling_group(RollingWindow::ticks(2, 0), &[]);
    let update = t
        .step(TableChange::new().add([25], row("a", 10, 0.0)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![25], vec![], vec![30], ModifiedColumnSet::of([R]))
    );
    assert_eq!(int_vectors(&*t.result, R)[4], cell(&[10, 4]));
    let update = t
        .step(TableChange::new().modify([0], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![], vec![0, 10], ModifiedColumnSet::of([1, R]))
    );
    assert_eq!(int_vectors(&*t.result, R)[1], cell(&[7, 2]));
    // A column that is no input writes nothing.
    let update = t
        .step(TableChange::new().modify([0], [(2, doubles(&[9.0]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![], vec![0], ModifiedColumnSet::of([2]))
    );
    let update = t.step(TableChange::new().remove([20])).unwrap();
    assert_eq!(keys(&update.modified), vec![25]);
    assert_eq!(int_vectors(&*t.result, R)[2], cell(&[2, 10]));
}

fn cum_where(result: &str, filters: &[&str]) -> UpdateByOperation {
    let spec = UpdateBySpec::CumCountWhere {
        result_column: result.into(),
        filters: filters.iter().map(|f| f.to_string()).collect(),
    };
    op(spec, &[])
}

fn rolling_where(result: &str, filters: &[&str], window: RollingWindow) -> UpdateByOperation {
    let spec = UpdateBySpec::RollingCountWhere {
        window,
        result_column: result.into(),
        filters: filters.iter().map(|f| f.to_string()).collect(),
    };
    op(spec, &[])
}

fn long_values(table: &dyn Table, column: usize) -> Vec<Option<i64>> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let values = snapshot.columns[column]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    values.iter().collect()
}

#[test]
fn a_cumulative_count_where_recomputes_from_the_first_change_on() {
    // `I` is 1 to 5 and `D` 0.5 to 4.5: rows 20, 30 and 40 pass.
    let t = ticking(vec![cum_where("R", &["I > 2", "D + 0 < 9"])], &[]);
    assert_eq!(
        long_values(&*t.result, R),
        [0, 0, 1, 2, 3].map(Some).to_vec()
    );
    // A row that does not pass changes no count, and the rows after it are recomputed.
    let update = t
        .step(TableChange::new().add([15], row("a", 0, 0.0)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![15], vec![], vec![20, 30, 40], ModifiedColumnSet::of([R]))
    );
    // A modified column of either filter recomputes from the row on; another does not.
    let update = t
        .step(TableChange::new().modify([10], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![10, 15, 20, 30, 40]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([1, R]));
    let update = t
        .step(TableChange::new().modify([30], [(2, doubles(&[10.0]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![30, 40]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([2, R]));
    let update = t
        .step(TableChange::new().modify([10], [(0, texts(&["z"]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![], vec![10], ModifiedColumnSet::of([0]))
    );
    assert_eq!(
        long_values(&*t.result, R),
        [0, 1, 1, 2, 2, 3].map(Some).to_vec()
    );
    let update = t.step(TableChange::new().remove([10])).unwrap();
    assert_eq!(keys(&update.modified), vec![15, 20, 30, 40]);
    assert_eq!(
        long_values(&*t.result, R),
        [0, 0, 1, 1, 2].map(Some).to_vec()
    );
}

#[test]
fn a_cumulative_count_where_counts_within_each_bucket() {
    let t = ticking(vec![cum_where("R", &["I > 1"])], &["K"]);
    assert_eq!(
        long_values(&*t.result, R),
        [0, 1, 1, 2, 2].map(Some).to_vec()
    );
    // Row 20 moves from bucket a to bucket b.
    let update = t
        .step(TableChange::new().modify([20], [(0, texts(&["b"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![20, 30, 40]);
    assert_eq!(
        long_values(&*t.result, R),
        [0, 1, 2, 3, 1].map(Some).to_vec()
    );
}

#[test]
fn a_rolling_count_where_recomputes_the_rows_whose_window_holds_the_change() {
    let window = RollingWindow::ticks(2, 0);
    let t = ticking(vec![rolling_where("R", &["I > 2"], window)], &[]);
    assert_eq!(
        long_values(&*t.result, R),
        [0, 0, 1, 2, 2].map(Some).to_vec()
    );
    let update = t
        .step(TableChange::new().modify([10], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![], vec![10, 20], ModifiedColumnSet::of([1, R]))
    );
    // A column the filter does not read.
    let update = t
        .step(TableChange::new().modify([10], [(2, doubles(&[9.0]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![], vec![10], ModifiedColumnSet::of([2]))
    );
    let update = t.step(TableChange::new().remove([20])).unwrap();
    assert_eq!(keys(&update.modified), vec![30]);
    assert_eq!(
        long_values(&*t.result, R),
        [0, 1, 2, 2].map(Some).to_vec()
    );
}

#[test]
fn a_rolling_count_where_in_time_has_no_count_for_a_row_without_a_timestamp() {
    let t = timed(vec![rolling_where("R", &["I != 3"], time(1, 0))], &[]);
    // Timestamps 100, 101, 101, 105, 110; row 20 does not pass.
    assert_eq!(
        long_values(&*t.result, TR),
        [1, 2, 2, 1, 1].map(Some).to_vec()
    );
    let update = t
        .step(TableChange::new().modify([10], [(3, longs(&[None]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![10, 20]);
    assert_eq!(
        long_values(&*t.result, TR),
        vec![Some(1), None, Some(1), Some(1), Some(1)]
    );
}

#[test]
fn a_count_where_filter_that_fails_on_a_row_fails_the_result() {
    // A formula that is null for a row.
    let filter = "I == 9 ? (Boolean)null : true";
    let source = static_table(&[("I", ColumnType::Int, ints(&[1, 9]))]);
    assert!(matches!(
        update_by(&source, &[cum_where("R", &[filter])], &[]),
        Err(UpdateByError::Failed(_))
    ));
    // A match that became a formula fails on the first rows, in Java's copy of it.
    match update_by(&source, &[cum_where("R", &["I == I"])], &[]) {
        Err(UpdateByError::Failed(failure)) => assert_eq!(
            failure.message,
            "Cannot read the array length because \"keys\" is null"
        ),
        other => panic!("{:?}", other.map(|_| ())),
    }
}

#[test]
fn count_where_requests_java_refuses_are_errors() {
    let window = || RollingWindow::ticks(2, 0);
    for (operations, message) in [
        (vec![cum_where("R", &["Nope > 3"])], "Nope"),
        (vec![cum_where("R", &["I + 1"])], "boolean required"),
        (
            vec![cum_where("R", &["i > 3"])],
            "UpdateBy CountWhere operator does not support refreshing filters",
        ),
        (
            vec![rolling_where("R", &["I > 0", "I + k > 3"], window())],
            "UpdateBy CountWhere operator does not support refreshing filters",
        ),
        (vec![cum_where("R", &["I_.size() > 3"])], "I_"),
        (
            vec![cum_where("R", &["I > 0"]), cum_where("R", &["I > 1"])],
            "resulting column names must be unique {R}",
        ),
        (
            vec![rolling_where("R", &["I > 0"], RollingWindow::ticks(-3, 2))],
            "non-negative",
        ),
    ] {
        let refused = refused(&operations, &[]);
        assert!(refused.contains(message), "{refused}");
    }
    // A window that holds no row fails Java's assertion.
    let source = static_table(&[("I", ColumnType::Int, ints(&[1, 2]))]);
    let empty = rolling_where("R", &["I > 0"], RollingWindow::ticks(0, 0));
    match update_by(&source, &[empty], &[]) {
        Err(UpdateByError::Failed(failure)) => assert_eq!(
            failure.message,
            "Assertion failed: asserted buffer.size() >= 1, instead buffer.size() == 0."
        ),
        other => panic!("{:?}", other.map(|_| ())),
    }
    // Without a compiler for the filters.
    assert!(matches!(
        super::update_by(&source, &[cum_where("R", &["I > 0"])], &[] as &[&str]),
        Err(UpdateByError::InvalidArgument(_))
    ));
}

fn count_where_request(filters: &[&str]) {
    let source = static_table(&[
        ("I", ColumnType::Int, ints(&[1, 2])),
        ("L", ColumnType::Long, longs(&[Some(1), Some(2)])),
        ("B", ColumnType::Boolean, random_free_booleans()),
    ]);
    let _ = update_by(&source, &[cum_where("R", filters)], &[]);
}

fn random_free_booleans() -> ArrayRef {
    Arc::new(arrow::array::BooleanArray::from(vec![true, false]))
}

#[test]
#[should_panic(expected = "a count-where without filters")]
fn a_count_where_without_filters_is_unported() {
    count_where_request(&[]);
}

#[test]
#[should_panic(expected = "a count-where filter with i, ii, k or a column array")]
fn a_comparison_with_a_row_variable_is_unported() {
    count_where_request(&["I > ii"]);
}

#[test]
#[should_panic(expected = "the Java server shuts down")]
fn a_comparison_of_columns_before_a_boolean_filter_is_unported() {
    count_where_request(&["I < L", "B == true"]);
}

#[test]
fn a_comparison_of_columns_after_a_boolean_filter_is_answered() {
    let source = static_table(&[
        ("I", ColumnType::Int, ints(&[1, 2])),
        ("L", ColumnType::Long, longs(&[Some(5), Some(0)])),
        ("B", ColumnType::Boolean, random_free_booleans()),
    ]);
    let filters = ["B != null", "I < L"];
    let table = update_by(&source, &[cum_where("R", &filters)], &[]).unwrap();
    assert_eq!(long_values(&*table, 3), vec![Some(1), Some(1)]);
}

#[test]
fn the_standard_deviations_and_the_weighted_average_are_refused_like_the_others() {
    let window = RollingWindow::ticks(3, 0);
    let wavg = |weight: &str, column: &str| {
        let spec = UpdateBySpec::RollingWAvg {
            window: window.clone(),
            weight_column: weight.into(),
        };
        refused(&[op(spec, &[column])], &[])
    };
    assert_eq!(wavg("Nope", "R = I"), "Column Nope does not exist");
    assert_eq!(wavg("I", "R = Nope"), "Column Nope does not exist");
    assert!(wavg("K", "R = I").contains("Can not perform RollingWAvg on weight column type"));
    assert!(wavg("I", "R = K").contains("Can not perform"));
    let std = op(UpdateBySpec::RollingStd(window), &["R = K"]);
    assert!(refused(&[std], &[]).contains("Can not perform"));
    let em_std = op(UpdateBySpec::EmStd(em()), &["R = K"]);
    assert!(refused(&[em_std], &[]).contains("Can not perform"));
}

/// The result of a row is kept with the average and the variance it came from, so a run
/// that starts inside the table continues exactly where a computation from scratch is.
#[test]
fn an_exponential_standard_deviation_continues_from_its_variance() {
    let t = ticking(vec![op(UpdateBySpec::EmStd(em()), &["R = D"])], &[]);
    let results = |t: &Ticking| {
        let snapshot = TableSnapshot::current(&*t.result).unwrap();
        let array = snapshot.columns[R]
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        array.iter().map(|v| v.unwrap()).collect::<Vec<f64>>()
    };
    let before = results(&t);
    let update = t
        .step(TableChange::new().add([50], row("a", 0, 0.75)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![50], vec![], vec![], ModifiedColumnSet::empty())
    );
    let after = results(&t);
    assert!(before[0].is_nan());
    assert_eq!(after[1..5], before[1..]);
    // The fold of the last row by hand, from the state after the fifth.
    let alpha = (-1.0f64 / 5.0).exp();
    let (mut average, mut variance) = (0.5f64, 0.0f64);
    for value in [1.5, 2.5, 3.5, 4.5, 0.75] {
        variance = alpha * (variance + (1.0 - alpha) * ((value - average) * (value - average)));
        average = average * alpha + (1.0 - alpha) * value;
    }
    // The square of the fifth result is not the variance it is the root of, which is
    // what makes Java's result for the added row differ.
    assert_ne!((before[4] * before[4]).to_bits(), {
        let (mut average, mut variance) = (0.5f64, 0.0f64);
        for value in [1.5, 2.5, 3.5, 4.5] {
            variance = alpha * (variance + (1.0 - alpha) * ((value - average) * (value - average)));
            average = average * alpha + (1.0 - alpha) * value;
        }
        variance.to_bits()
    });
    assert_eq!(after[5].to_bits(), variance.sqrt().to_bits());
}

/// Every ported operator on every column it takes. `float` and `double` columns are left
/// out of the rolling minimum and maximum: the random values include NaN, and with a NaN in
/// a window Java's result depends on which rows it recomputed, so a result kept up to date
/// legitimately differs from one computed from scratch (see `rolling::float_extremes`).
fn everything() -> Vec<UpdateByOperation> {
    let window = |rev, fwd| RollingWindow::ticks(rev, fwd);
    vec![
        op(
            UpdateBySpec::CumSum,
            &[
                "Cs1 = B", "Cs2 = I", "Cs3 = L", "Cs4 = F", "Cs5 = D", "Cs6 = C",
            ],
        ),
        op(UpdateBySpec::CumProd, &["Cp1 = I", "Cp2 = L", "Cp3 = D"]),
        op(
            UpdateBySpec::CumMin,
            &["Mn1 = I", "Mn2 = D", "Mn3 = S", "Mn4 = T", "Mn5 = C"],
        ),
        op(
            UpdateBySpec::CumMax,
            &["Mx1 = L", "Mx2 = F", "Mx3 = S", "Mx4 = Y"],
        ),
        op(
            UpdateBySpec::ForwardFill,
            &["Ff1 = B", "Ff2 = S", "Ff3 = D", "Ff4 = T", "Ff5 = H"],
        ),
        op(
            UpdateBySpec::Delta(DeltaControl::NullDominates),
            &["Dl1 = I", "Dl2 = D", "Dl3 = C"],
        ),
        op(
            UpdateBySpec::Delta(DeltaControl::ValueDominates),
            &["Dl4 = Y", "Dl5 = F"],
        ),
        op(
            UpdateBySpec::RollingSum(window(3, 0)),
            &["Rs1 = I", "Rs2 = D", "Rs3 = B"],
        ),
        op(
            UpdateBySpec::RollingAvg(window(3, 0)),
            &["Ra1 = L", "Ra2 = F"],
        ),
        op(
            UpdateBySpec::RollingMin(window(2, 2)),
            &["Rn1 = I", "Rn2 = S", "Rn3 = T"],
        ),
        op(
            UpdateBySpec::RollingMax(window(2, 2)),
            &["Rx1 = L", "Rx2 = C"],
        ),
        op(
            UpdateBySpec::RollingCount(window(-1, 3)),
            &["Rc1 = S", "Rc2 = D"],
        ),
        op(
            UpdateBySpec::RollingProduct(window(4, -1)),
            &["Rp1 = I", "Rp2 = D"],
        ),
        op(UpdateBySpec::RollingSum(window(0, 3)), &["Rs4 = L"]),
        op(UpdateBySpec::RollingCount(window(1, 0)), &["Rc3 = B"]),
        op(
            UpdateBySpec::RollingStd(window(3, 0)),
            &["Sd1 = I", "Sd2 = D", "Sd3 = B", "Sd4 = C"],
        ),
        op(
            UpdateBySpec::RollingStd(window(2, 2)),
            &["Sd5 = L", "Sd6 = F", "Sd7 = Y"],
        ),
        op(
            UpdateBySpec::RollingWAvg {
                window: window(3, 0),
                weight_column: "H".into(),
            },
            &["Wa1 = I", "Wa2 = D", "Wa3 = C"],
        ),
        op(
            UpdateBySpec::RollingWAvg {
                window: window(-1, 3),
                weight_column: "D".into(),
            },
            &["Wa4 = L", "Wa5 = Y", "Wa6 = F"],
        ),
        op(
            UpdateBySpec::RollingGroup(window(3, 0)),
            &["G1 = I", "G2 = D", "G3 = S", "G4 = B", "G5 = T", "G6 = K"],
        ),
        op(
            UpdateBySpec::RollingGroup(window(-1, 3)),
            &["G7 = C", "G8 = F"],
        ),
        op(UpdateBySpec::RollingGroup(window(0, 0)), &["G9 = L"]),
        op(UpdateBySpec::RollingGroup(window(2, 2)), &["G10 = Y"]),
        cum_where("Cw1", &["I > 0"]),
        cum_where("Cw2", &["D >= 0", "S != null", "L % 2 == 0"]),
        cum_where("Cw3", &["B == true"]),
        cum_where("Cw4", &["isNull(T) || isNull(C)"]),
        cum_where("Cw5", &["K in `a`, `b`"]),
        rolling_where("Rw1", &["I > 0"], window(3, 0)),
        rolling_where("Rw2", &["F < 0", "Y != null"], window(2, 2)),
        rolling_where("Rw3", &["H + I > 0"], window(-1, 3)),
        rolling_where("Rw4", &["S > `m`"], window(1, 0)),
        rolling_where("Rw5", &["I < L"], window(4, 1)),
    ]
}

fn random_updates(by: &'static [&'static str]) {
    let types = [
        ("K", ColumnType::String),
        ("B", ColumnType::Boolean),
        ("Y", ColumnType::Byte),
        ("C", ColumnType::Char),
        ("H", ColumnType::Short),
        ("I", ColumnType::Int),
        ("L", ColumnType::Long),
        ("F", ColumnType::Float),
        ("D", ColumnType::Double),
        ("S", ColumnType::String),
        ("T", ColumnType::Instant),
    ];
    for seed in test_seeds(0..24) {
        let mut random = RandomUpdates::new(seed);
        let columns: Vec<(&str, ColumnType, ArrayRef)> = types
            .iter()
            .map(|(name, column_type)| (*name, column_type.clone(), random.values(column_type, 6)))
            .collect();
        let t = Ticking::new([0, 2, 4, 6, 8, 10], &columns, move |source| {
            update_by(source, &everything(), by).unwrap()
        });
        for _ in 0..30 {
            let change = random.next_change(&*t.script.table(), 5);
            t.step(change);
        }
    }
}

#[test]
fn random_updates_match_a_recompute_without_buckets() {
    random_updates(&[]);
}

#[test]
fn random_updates_match_a_recompute_with_buckets() {
    random_updates(&["K"]);
}

#[test]
fn random_updates_match_a_recompute_with_two_bucket_columns() {
    random_updates(&["K", "B"]);
}

fn longs(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// A window of `rev` nanoseconds back and `fwd` forward on the timestamp column `T`.
fn time(rev: i64, fwd: i64) -> RollingWindow {
    let side = |nanos| WindowScale::Time {
        column: "T".into(),
        nanos,
    };
    RollingWindow {
        rev: side(rev),
        fwd: side(fwd),
    }
}

/// As `ticking`, with a timestamp column `T` (index 3) of 100, 101, 101, 105, 110 and the
/// result column at index 4.
fn timed(operations: Vec<UpdateByOperation>, by: &'static [&'static str]) -> Ticking {
    Ticking::new(
        [0, 10, 20, 30, 40],
        &[
            ("K", ColumnType::String, texts(&["a", "b", "a", "b", "a"])),
            ("I", ColumnType::Int, ints(&[1, 2, 3, 4, 5])),
            ("D", ColumnType::Double, doubles(&[0.5, 1.5, 2.5, 3.5, 4.5])),
            (
                "T",
                ColumnType::Long,
                longs(&[Some(100), Some(101), Some(101), Some(105), Some(110)]),
            ),
        ],
        move |source| update_by(source, &operations, by).unwrap(),
    )
}

fn timed_row(k: &str, i: i32, t: Option<i64>) -> Vec<ArrayRef> {
    vec![texts(&[k]), ints(&[i]), doubles(&[0.0]), longs(&[t])]
}

const TR: usize = 4;

#[test]
fn a_window_in_time_recomputes_the_rows_whose_window_holds_the_change() {
    // Two nanoseconds back: row 0 is in the windows of rows 0, 10 and 20.
    let t = timed(
        vec![op(UpdateBySpec::RollingSum(time(2, 0)), &["R = I"])],
        &[],
    );
    let update = t
        .step(TableChange::new().modify([0], [(1, ints(&[7]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (
            vec![],
            vec![],
            vec![0, 10, 20],
            ModifiedColumnSet::of([1, TR])
        )
    );
    // At 106 the new row is in no other row's window.
    let update = t
        .step(TableChange::new().add([35], timed_row("a", 1, Some(106))))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![35], vec![], vec![], ModifiedColumnSet::empty())
    );
    // Row 20 moves from 101 to 103: its old place was in the window of row 10, its new
    // one is in that of row 30, and it has a new window itself.
    let update = t
        .step(TableChange::new().modify([20], [(3, longs(&[Some(103)]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (
            vec![],
            vec![],
            vec![10, 20, 30],
            ModifiedColumnSet::of([3, TR])
        )
    );
    // Row 10, at 101, was in the window of row 20.
    let update = t.step(TableChange::new().remove([10])).unwrap();
    assert_eq!(keys(&update.modified), vec![20]);
}

#[test]
fn a_row_without_a_timestamp_is_in_no_window() {
    let t = timed(
        vec![op(UpdateBySpec::RollingSum(time(10, 10)), &["R = I"])],
        &[],
    );
    let update = t
        .step(TableChange::new().add([25], timed_row("a", 100, None)))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![25], vec![], vec![], ModifiedColumnSet::empty())
    );
    let sums = TableSnapshot::current(&*t.result).unwrap().columns[TR].clone();
    let sums = sums.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(sums.is_null(3));
    assert_eq!(sums.value(0), 15);
    // Giving it a timestamp puts it in every window.
    let update = t
        .step(TableChange::new().modify([25], [(3, longs(&[Some(103)]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![0, 10, 20, 25, 30, 40]);
}

#[test]
fn a_rolling_group_in_time_holds_the_rows_of_the_window() {
    let group = || vec![op(UpdateBySpec::RollingGroup(time(2, 0)), &["R = I"])];
    let t = timed(group(), &[]);
    let rows123 = || cell(&[1, 2, 3]);
    assert_eq!(
        int_vectors(&*t.result, TR),
        vec![cell(&[1]), rows123(), rows123(), cell(&[4]), cell(&[5])]
    );
    // A row at 102 is in its own window and in none after it; rows 10 and 20 are in its.
    let update = t
        .step(TableChange::new().add([25], timed_row("a", 9, Some(102))))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![25], vec![], vec![], ModifiedColumnSet::empty())
    );
    assert_eq!(int_vectors(&*t.result, TR)[3], cell(&[1, 2, 3, 9]));
    let update = t.step(TableChange::new().remove([10])).unwrap();
    assert_eq!(keys(&update.modified), vec![20, 25]);
    assert_eq!(int_vectors(&*t.result, TR)[2], cell(&[1, 3, 9]));
    // A window that holds no row is an empty vector.
    let t = timed(
        vec![op(UpdateBySpec::RollingGroup(time(-1, 3)), &["R = I"])],
        &[],
    );
    assert_eq!(
        int_vectors(&*t.result, TR),
        vec![cell(&[2, 3]), cell(&[]), cell(&[]), cell(&[]), cell(&[])]
    );
}

/// Behavior: the `invert` of the affected rows in
/// io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingBase#processWindowBucketOperatorSet
#[test]
fn a_rolling_group_in_time_fails_on_a_row_without_a_timestamp_before_one_with() {
    let group = || vec![op(UpdateBySpec::RollingGroup(time(2, 0)), &["R = I"])];
    let stamps = |stamps: &[Option<i64>]| {
        static_table(&[
            ("I", ColumnType::Int, ints(&[1, 2, 3])),
            ("T", ColumnType::Long, longs(stamps)),
        ])
    };
    let run = |source: &Arc<dyn Table>, operations: &[UpdateByOperation]| {
        update_by(source, operations, &[] as &[&str])
            .map(|_| ())
            .map_err(|error| error.to_string())
    };
    let refused = Err("keys argument has elements not in the rowSet".to_owned());
    for failing in [[None, Some(1), Some(2)], [Some(1), None, Some(2)]] {
        assert_eq!(run(&stamps(&failing), &group()), refused);
        // The other operators have nothing against it.
        let sum = [op(UpdateBySpec::RollingSum(time(2, 0)), &["R = I"])];
        assert_eq!(run(&stamps(&failing), &sum), Ok(()));
    }
    // Rows without a timestamp after every row that has one, or nothing else, are
    // answered, with a null and not an empty vector.
    for answered in [[Some(1), Some(2), None], [None, None, None]] {
        let source = stamps(&answered);
        let result = update_by(&source, &group(), &[] as &[&str]).ok().unwrap();
        assert_eq!(int_vectors(&*result, 2)[2], None);
    }
}

#[test]
fn a_rolling_group_in_time_takes_the_timestamp_column() {
    let t = timed(
        vec![op(UpdateBySpec::RollingGroup(time(1, 0)), &["R = T"])],
        &[],
    );
    assert_eq!(
        t.result.definition().columns()[TR].column_type,
        ColumnType::vector(ColumnType::Long)
    );
}

#[test]
fn a_decreasing_timestamp_fails_the_result() {
    let em = || {
        op(
            UpdateBySpec::Ema(EmSpec {
                window: WindowScale::Time {
                    column: "T".into(),
                    nanos: 5,
                },
                on_null: None,
                on_nan: None,
            }),
            &["R = D"],
        )
    };
    for operations in [
        vec![op(UpdateBySpec::RollingSum(time(2, 0)), &["R = I"])],
        vec![em()],
    ] {
        let t = timed(operations.clone(), &["K"]);
        // In another bucket, a smaller timestamp is fine.
        t.step(TableChange::new().add([45], timed_row("z", 1, Some(3))))
            .unwrap();
        t.script
            .push(TableChange::new().add([50], timed_row("a", 1, Some(4))));
        t.graph.run_cycle();
        assert!(t.result.is_failed());

        let source = static_table(&[
            ("K", ColumnType::String, texts(&["a", "a"])),
            ("I", ColumnType::Int, ints(&[1, 2])),
            ("D", ColumnType::Double, doubles(&[0.5, 1.5])),
            ("T", ColumnType::Long, longs(&[Some(5), Some(4)])),
        ]);
        let refused = update_by(&source, &operations, &[] as &[&str]);
        assert!(matches!(refused, Err(UpdateByError::Failed(_))));
        assert!(update_by(&source, &operations, &["T"]).is_ok());
    }
}

#[test]
fn an_exponential_operator_recomputes_from_the_first_change_on() {
    let ema = op(
        UpdateBySpec::Ema(EmSpec {
            window: WindowScale::Ticks(2.0),
            on_null: None,
            on_nan: None,
        }),
        &["R = D"],
    );
    let t = timed(vec![ema], &[]);
    let update = t
        .step(TableChange::new().modify([20], [(2, doubles(&[9.0]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), vec![20, 30, 40]);
    // The timestamp column is not one of its inputs.
    let update = t
        .step(TableChange::new().modify([20], [(3, longs(&[Some(102)]))]))
        .unwrap();
    assert_eq!(
        shape(&update),
        (vec![], vec![], vec![20], ModifiedColumnSet::of([3]))
    );
}

#[test]
fn bad_requests_in_time_are_errors() {
    let source = static_table(&[
        ("I", ColumnType::Int, ints(&[1, 2])),
        ("J", ColumnType::Int, ints(&[1, 2])),
        ("T", ColumnType::Long, longs(&[Some(1), Some(2)])),
        ("U", ColumnType::Long, longs(&[Some(1), Some(2)])),
    ]);
    let refused =
        |operations: &[UpdateByOperation]| match update_by(&source, operations, &[] as &[&str]) {
            Err(UpdateByError::InvalidArgument(message)) => message,
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("not refused"),
        };
    let on = |column: &str| {
        op(
            UpdateBySpec::Ems(EmSpec {
                window: WindowScale::Time {
                    column: column.into(),
                    nanos: 3,
                },
                on_null: None,
                on_nan: None,
            }),
            &["R = I"],
        )
    };
    assert!(refused(&[on("J")]).contains("Unsupported timestamp column type"));
    assert!(refused(&[on("Nope")]).contains("Nope"));
    // The timestamp column cannot be an input of an operation in time.
    let mut itself = on("T");
    itself.columns = vec!["R = T".into()];
    assert!(refused(&[itself]).contains("Requested output column 'R'"));
    let mut other = on("U");
    other.columns = vec!["Q = I".into()];
    assert!(refused(&[on("T"), other]).contains("more than one timestamp"));
    assert!(
        refused(&[op(UpdateBySpec::RollingSum(time(-3, 2)), &["R = I"])]).contains("non-negative")
    );
}

/// Rewrites the timestamps that `change` gives added rows and modified rows of `table` in
/// column `column` so that the table's timestamps never decrease in key order: each takes
/// a value between those of the rows around it that keep theirs, picked by the value the
/// change had for it. Nulls stay, unless `stamp_all`.
fn rising(
    mut change: TableChange,
    table: &dyn Table,
    column: usize,
    stamp_all: bool,
) -> TableChange {
    let snapshot = TableSnapshot::current(table).unwrap();
    let stamps = snapshot.columns[column]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    let update = &change.update;
    // Every row after the change: its key, and its timestamp if it keeps it.
    let mut rows: Vec<(i64, Option<Option<i64>>)> = Vec::new();
    for (position, key) in snapshot.rows.iter().enumerate() {
        if !update.removed.contains(key) {
            let stamp = stamps.is_valid(position).then(|| stamps.value(position));
            rows.push((update.shifted.apply_to_key(key), Some(stamp)));
        }
    }
    let modified = change.modified_values.contains_key(&column);
    for (key, kept) in rows.iter_mut() {
        if modified && update.modified.contains(*key) {
            *kept = None;
        }
    }
    rows.extend(update.added.iter().map(|key| (key, None)));
    rows.sort_by_key(|(key, _)| *key);
    // The next timestamp kept after each row.
    let mut above = vec![None; rows.len()];
    let mut next = None;
    for (index, (_, kept)) in rows.iter().enumerate().rev() {
        above[index] = next;
        if let Some(Some(stamp)) = kept {
            next = Some(*stamp);
        }
    }
    let added: Vec<i64> = update.added.iter().collect();
    let modified_keys: Vec<i64> = update.modified.iter().collect();
    let generated = |values: &ArrayRef, index: usize| {
        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
        let value = values.is_valid(index).then(|| values.value(index));
        value.or(stamp_all.then_some(0))
    };
    let mut new_added = vec![None; added.len()];
    let mut new_modified = vec![None; modified_keys.len()];
    // In key order, so that each value picked bounds the next one from below.
    let mut low = 0;
    for (index, (key, kept)) in rows.iter().enumerate() {
        match kept {
            Some(Some(stamp)) => low = *stamp,
            Some(None) => {}
            None => {
                let high = above[index].unwrap_or(low + 20);
                let mut pick = |value: Option<i64>| {
                    value.map(|value| {
                        low += value.unsigned_abs() as i64 % (high - low + 1);
                        low
                    })
                };
                if let Ok(index) = added.binary_search(key) {
                    new_added[index] = pick(generated(&change.added_values[column], index));
                } else {
                    let index = modified_keys.binary_search(key).unwrap();
                    new_modified[index] = pick(generated(&change.modified_values[&column], index));
                }
            }
        }
    }
    if !added.is_empty() {
        change.added_values[column] = longs(&new_added);
    }
    if modified {
        change.modified_values.insert(column, longs(&new_modified));
    }
    change
}

/// Every operation in time, and the exponential operators by ticks, on the timestamp
/// column `T`. As in `everything`, `float` and `double` columns are left out of the rolling
/// minimum and maximum.
fn everything_in_time() -> Vec<UpdateByOperation> {
    let em = |window, on_null, on_nan| EmSpec {
        window,
        on_null,
        on_nan,
    };
    let decay = |nanos| WindowScale::Time {
        column: "T".into(),
        nanos,
    };
    vec![
        op(
            UpdateBySpec::RollingSum(time(5, 0)),
            &["Rs1 = I", "Rs2 = D", "Rs3 = B"],
        ),
        op(
            UpdateBySpec::RollingAvg(time(3, 3)),
            &["Ra1 = L", "Ra2 = F"],
        ),
        op(
            UpdateBySpec::RollingMin(time(-2, 6)),
            &["Rn1 = I", "Rn2 = S", "Rn3 = X"],
        ),
        op(
            UpdateBySpec::RollingMax(time(4, -1)),
            &["Rx1 = L", "Rx2 = C"],
        ),
        op(
            UpdateBySpec::RollingCount(time(0, 0)),
            &["Rc1 = S", "Rc2 = D"],
        ),
        op(
            UpdateBySpec::RollingProduct(time(6, 0)),
            &["Rp1 = I", "Rp2 = D"],
        ),
        op(
            UpdateBySpec::RollingSum(RollingWindow::ticks(2, 1)),
            &["Rs4 = I"],
        ),
        op(UpdateBySpec::CumSum, &["Cs1 = I"]),
        op(
            UpdateBySpec::Ema(em(WindowScale::Ticks(2.5), None, None)),
            &["Ea1 = I", "Ea2 = D"],
        ),
        op(
            UpdateBySpec::Ema(em(decay(4), None, Some(BadDataBehavior::Reset))),
            &["Ea3 = D", "Ea4 = F"],
        ),
        op(
            UpdateBySpec::Ems(em(decay(3), Some(BadDataBehavior::Reset), None)),
            &["Es1 = L", "Es2 = Y"],
        ),
        op(
            UpdateBySpec::EmMin(em(
                WindowScale::Ticks(1.5),
                None,
                Some(BadDataBehavior::Poison),
            )),
            &["En1 = D"],
        ),
        op(
            UpdateBySpec::EmMax(em(decay(2), None, None)),
            &["Ex1 = C", "Ex2 = H"],
        ),
        op(
            UpdateBySpec::EmStd(em(
                WindowScale::Ticks(2.5),
                None,
                Some(BadDataBehavior::Poison),
            )),
            &["Ed1 = I", "Ed2 = D", "Ed3 = C"],
        ),
        // Not poisoned by NaN in time: as Java, the operator refuses the value after a NaN
        // that came before any other. And on `D` and `F` with the treatment of NaN that the
        // averages above have: the timestamp a run is seeded with is found by the rule of
        // the first operator in time on the column (`Plan::seed_timestamp`), so operators
        // with different rules on one column depend, as in Java, on the updates so far.
        op(
            UpdateBySpec::EmStd(em(decay(4), Some(BadDataBehavior::Reset), None)),
            &["Ed4 = I", "Ed5 = H", "Ed6 = L"],
        ),
        op(
            UpdateBySpec::EmStd(em(decay(3), None, Some(BadDataBehavior::Reset))),
            &["Ed7 = D", "Ed8 = F", "Ed9 = Y"],
        ),
        op(
            UpdateBySpec::RollingStd(time(5, 0)),
            &["Sd1 = I", "Sd2 = D"],
        ),
        op(
            UpdateBySpec::RollingStd(time(-2, 6)),
            &["Sd3 = L", "Sd4 = F"],
        ),
        op(
            UpdateBySpec::RollingWAvg {
                window: time(3, 3),
                weight_column: "H".into(),
            },
            &["Wa1 = I", "Wa2 = D"],
        ),
        op(
            UpdateBySpec::RollingWAvg {
                window: RollingWindow::ticks(2, 1),
                weight_column: "D".into(),
            },
            &["Wa3 = L"],
        ),
        op(
            UpdateBySpec::RollingGroup(RollingWindow::ticks(2, 1)),
            &["G8 = F"],
        ),
        rolling_where("Rw1", &["I > 0"], time(5, 0)),
        rolling_where("Rw2", &["D < 0 || isNull(D)", "S != null"], time(-2, 6)),
        rolling_where("Rw3", &["H + I > 4"], time(3, 3)),
        rolling_where("Rw4", &["S in `a`, `b`"], time(0, 0)),
        rolling_where("Rw5", &["L != null"], RollingWindow::ticks(2, 1)),
        cum_where("Cw1", &["T != null", "I + H > 0"]),
    ]
}

/// Rolling groups in time, which fail on a row without a timestamp, sharing their windows
/// with other operators and not.
fn groups_in_time() -> Vec<UpdateByOperation> {
    vec![
        op(
            UpdateBySpec::RollingGroup(time(5, 0)),
            &["G1 = I", "G2 = S", "G3 = T"],
        ),
        op(UpdateBySpec::RollingSum(time(5, 0)), &["Rs1 = I"]),
        op(UpdateBySpec::RollingGroup(time(-2, 6)), &["G4 = D"]),
        op(
            UpdateBySpec::RollingGroup(time(4, 4)),
            &["G5 = X", "G6 = B"],
        ),
        op(UpdateBySpec::RollingGroup(time(0, 0)), &["G7 = L"]),
        op(UpdateBySpec::RollingCount(time(0, 0)), &["Rc1 = L"]),
    ]
}

fn random_updates_in_time(by: &'static [&'static str]) {
    random_updates_with(by, everything_in_time, Some(2));
}

fn random_updates_of_groups_in_time(by: &'static [&'static str]) {
    random_updates_with(by, groups_in_time, None);
}

/// Random updates of a table with a timestamp column `T`, which has no timestamp in the
/// row at `unstamped` and in some of the rows that come, or in none.
fn random_updates_with(
    by: &'static [&'static str],
    operations: fn() -> Vec<UpdateByOperation>,
    unstamped: Option<usize>,
) {
    let types = [
        ("K", ColumnType::String),
        ("B", ColumnType::Boolean),
        ("Y", ColumnType::Byte),
        ("C", ColumnType::Char),
        ("H", ColumnType::Short),
        ("I", ColumnType::Int),
        ("L", ColumnType::Long),
        ("F", ColumnType::Float),
        ("D", ColumnType::Double),
        ("S", ColumnType::String),
        ("X", ColumnType::Instant),
        ("T", ColumnType::Long),
    ];
    let t_index = types.len() - 1;
    for seed in test_seeds(0..24) {
        let mut random = RandomUpdates::new(seed);
        let mut columns: Vec<(&str, ColumnType, ArrayRef)> = types
            .iter()
            .map(|(name, column_type)| (*name, column_type.clone(), random.values(column_type, 6)))
            .collect();
        let mut stamps = [Some(0), Some(3), Some(3), Some(3), Some(7), Some(12)];
        if let Some(row) = unstamped {
            stamps[row] = None;
        }
        columns[t_index].2 = longs(&stamps);
        let t = Ticking::new([0, 2, 4, 6, 8, 10], &columns, move |source| {
            update_by(source, &operations(), by).unwrap()
        });
        for _ in 0..30 {
            let table = t.script.table();
            let change = random.next_change(&*table, 5);
            let change = rising(change, &*table, t_index, unstamped.is_none());
            t.step(change);
        }
    }
}

#[test]
fn random_updates_in_time_match_a_recompute_without_buckets() {
    random_updates_in_time(&[]);
}

#[test]
fn random_updates_in_time_match_a_recompute_with_buckets() {
    random_updates_in_time(&["K"]);
}

#[test]
fn random_updates_of_groups_in_time_match_a_recompute_without_buckets() {
    random_updates_of_groups_in_time(&[]);
}

#[test]
fn random_updates_of_groups_in_time_match_a_recompute_with_buckets() {
    random_updates_of_groups_in_time(&["K"]);
}
