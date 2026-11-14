//! The incremental cases of `update_by`, the requests it refuses, and random updates.
//!
//! Every ticking test runs through `Ticking`, which checks each update for consistency and
//! compares the result after each cycle with `update_by` of a static copy of the source.
//! The values themselves are checked against Java in `corpus.rs`; the tests here check
//! which rows and columns an update reports, and that a result kept up to date equals one
//! computed from scratch.

use super::{
    update_by, BadDataBehavior, DeltaControl, EmSpec, RollingWindow, UpdateByError,
    UpdateByOperation, UpdateBySpec, WindowScale,
};
use crate::ops::sort::testing::{keys, shifts, static_table, Ticking};
use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{RandomUpdates, TableChange};
use std::sync::Arc;

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

fn request(spec: UpdateBySpec) {
    let source = static_table(&[("I", ColumnType::Int, ints(&[1, 2]))]);
    let _ = update_by(&source, &[op(spec, &["R = I"])], &[] as &[&str]);
}

fn em() -> EmSpec {
    EmSpec {
        window: WindowScale::Ticks(5.0),
        on_null: Some(BadDataBehavior::Skip),
        on_nan: None,
    }
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor#makeEmaOperator"
)]
fn ema_is_unported() {
    request(UpdateBySpec::Ema(em()));
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor#makeEmStdOperator"
)]
fn em_std_is_unported() {
    request(UpdateBySpec::EmStd(em()));
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor#makeRollingStdOperator"
)]
fn rolling_std_is_unported() {
    request(UpdateBySpec::RollingStd(RollingWindow::ticks(3, 0)));
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTime#computeAffectedRowsAndOperators"
)]
fn a_window_in_time_is_unported() {
    let side = || WindowScale::Time {
        column: "T".into(),
        nanos: 1_000,
    };
    request(UpdateBySpec::RollingSum(RollingWindow {
        rev: side(),
        fwd: side(),
    }));
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
    for seed in 0..24 {
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
