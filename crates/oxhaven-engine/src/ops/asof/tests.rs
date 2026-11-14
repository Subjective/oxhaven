//! Unit tests of the as-of joins: the static result, the refusals, then each kind of change
//! of either table.

use super::{aj, as_of_join, raj, AsOfJoinError, AsOfMatch, AsOfRule, JoinAddition, JoinMatch};
use crate::ops::sort::testing::{cells, frozen, graph, keys, scripted, static_table, Columns};
use arrow::array::{ArrayRef, Float64Array, Int32Array, TimestampNanosecondArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    definition, ints, longs, strings, ConsistencyChecker, Recorder, ReferenceTable, ScriptedTable,
    TableChange,
};
use std::sync::Arc;

use AsOfRule::{GreaterThan, GreaterThanEqual, LessThan, LessThanEqual};

const RULES: [AsOfRule; 4] = [GreaterThanEqual, GreaterThan, LessThanEqual, LessThan];

fn opt_ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn on(columns: &[&str]) -> Vec<JoinMatch> {
    columns
        .iter()
        .map(|column| JoinMatch::same(*column))
        .collect()
}

fn add(columns: &[&str]) -> Vec<JoinAddition> {
    let columns = columns.iter();
    columns.map(|column| JoinAddition::same(*column)).collect()
}

fn x_y(rule: AsOfRule) -> AsOfMatch {
    AsOfMatch::new("X", rule, "Y")
}

/// The values of the column `name`, in row order, as text.
fn column(table: &dyn Table, name: &str) -> Vec<String> {
    let index = table.definition().index_of(name);
    cells(table, index.unwrap_or_else(|| panic!("no column {name}")))
}

fn names(table: &dyn Table) -> Vec<&str> {
    let columns = table.definition().columns().iter();
    columns.map(|column| column.name.as_str()).collect()
}

/// A left table `X` and a right table `Y`, `V`, where `V` counts the right rows from 1.
fn stamp_tables(
    column_type: ColumnType,
    left: ArrayRef,
    right: ArrayRef,
) -> (Arc<dyn Table>, Arc<dyn Table>) {
    let values = longs(1..=right.len() as i64);
    let left = static_table(&[("X", column_type.clone(), left)]);
    let right = static_table(&[("Y", column_type, right), ("V", ColumnType::Long, values)]);
    (left, right)
}

/// `V` of the join of the two tables on the stamps alone.
fn v_of(left: &Arc<dyn Table>, right: &Arc<dyn Table>, rule: AsOfRule) -> Vec<String> {
    let result = as_of_join(left, right, &[], &x_y(rule), &add(&["V"])).unwrap();
    column(&*result, "V")
}

// ---- The static result ---------------------------------------------------------------------

#[test]
fn each_rule_takes_the_closest_right_row_on_its_side() {
    // The right table is not sorted and has each stamp twice.
    let (left, right) = stamp_tables(
        ColumnType::Int,
        ints([5, 10, 15, 20, 25]),
        ints([20, 10, 10, 20]),
    );
    // Of the rows of one stamp, `aj` takes the last and `raj` the first.
    assert_eq!(
        v_of(&left, &right, GreaterThanEqual),
        ["null", "3", "3", "4", "4"]
    );
    assert_eq!(
        v_of(&left, &right, GreaterThan),
        ["null", "null", "3", "3", "4"]
    );
    assert_eq!(
        v_of(&left, &right, LessThanEqual),
        ["2", "2", "1", "1", "null"]
    );
    assert_eq!(
        v_of(&left, &right, LessThan),
        ["2", "1", "1", "null", "null"]
    );
}

#[test]
fn a_null_stamp_is_the_least_value_and_nan_the_greatest() {
    let (left, right) = stamp_tables(
        ColumnType::Double,
        Arc::new(Float64Array::from(vec![
            None,
            Some(f64::NAN),
            Some(1.0),
            Some(-0.0),
        ])),
        Arc::new(Float64Array::from(vec![None, Some(f64::NAN), Some(0.0)])),
    );
    // A null joins a null and NaN joins NaN when equal stamps do; -0.0 equals 0.0.
    assert_eq!(v_of(&left, &right, GreaterThanEqual), ["1", "2", "3", "3"]);
    assert_eq!(v_of(&left, &right, GreaterThan), ["null", "3", "3", "1"]);
    assert_eq!(v_of(&left, &right, LessThanEqual), ["1", "2", "2", "3"]);
    assert_eq!(v_of(&left, &right, LessThan), ["3", "null", "2", "2"]);
}

#[test]
fn a_right_row_is_looked_for_among_the_rows_of_the_left_rows_key() {
    let left = static_table(&[
        (
            "K",
            ColumnType::Int,
            opt_ints(&[Some(1), Some(2), None, Some(3)]),
        ),
        ("X", ColumnType::Int, ints([10, 10, 10, 10])),
    ]);
    let right = static_table(&[
        (
            "K",
            ColumnType::Int,
            opt_ints(&[Some(2), Some(1), None, Some(1)]),
        ),
        ("Y", ColumnType::Int, ints([5, 20, 10, 7])),
        ("V", ColumnType::Long, longs([1, 2, 3, 4])),
    ]);
    let v = |rule| {
        let result = as_of_join(&left, &right, &on(&["K"]), &x_y(rule), &add(&["V"])).unwrap();
        column(&*result, "V")
    };
    // A null key equals a null key.
    assert_eq!(v(GreaterThanEqual), ["4", "1", "3", "null"]);
    assert_eq!(v(GreaterThan), ["4", "1", "null", "null"]);
    assert_eq!(v(LessThanEqual), ["2", "null", "3", "null"]);
    assert_eq!(v(LessThan), ["2", "null", "null", "null"]);
}

#[test]
fn the_result_has_the_left_rows_at_their_keys_and_the_added_columns() {
    let left: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&[
            ("K", ColumnType::Int),
            ("X", ColumnType::Int),
            ("Tag", ColumnType::String),
        ]),
        RowSet::from_keys([3, 7, 20]),
        vec![ints([1, 1, 2]), ints([10, 5, 10]), strings(["a", "b", "c"])],
    );
    let right = static_table(&[
        ("K", ColumnType::Int, ints([1, 2])),
        ("Y", ColumnType::Int, ints([10, 3])),
        ("V", ColumnType::Long, longs([100, 200])),
        ("Other", ColumnType::String, strings(["o", "p"])),
    ]);
    // Without additions: the right stamp column, then every right column that is not
    // named like the left column of a match.
    let result = as_of_join(&left, &right, &on(&["K"]), &x_y(GreaterThanEqual), &[]).unwrap();
    assert_eq!(names(&*result), ["K", "X", "Tag", "Y", "V", "Other"]);
    assert_eq!(keys(&result.row_set()), [3, 7, 20]);
    assert_eq!(column(&*result, "Tag"), ["a", "b", "c"]);
    assert_eq!(column(&*result, "Y"), ["10", "null", "3"]);
    assert_eq!(column(&*result, "V"), ["100", "null", "200"]);
    assert_eq!(column(&*result, "Other"), ["o", "null", "p"]);
    assert!(!result.is_refreshing() && !result.is_flat());
    assert_eq!(result.attributes(), Default::default());

    // An addition that renames the right stamp column takes its place.
    let renamed = [JoinAddition::same("V"), JoinAddition::renamed("At", "Y")];
    let result = as_of_join(&left, &right, &on(&["K"]), &x_y(GreaterThanEqual), &renamed).unwrap();
    assert_eq!(names(&*result), ["K", "X", "Tag", "At", "V"]);
    assert_eq!(column(&*result, "At"), ["10", "null", "3"]);
}

// ---- Refusals ------------------------------------------------------------------------------

/// A left table `K`, `X`, `Tag` and a right table `K`, `Y`, `V`, one row each.
fn small_tables() -> (Arc<dyn Table>, Arc<dyn Table>) {
    let left = static_table(&left_columns(&[1], &[10], &["a"]));
    let right = static_table(&[
        ("K", ColumnType::Int, ints([1])),
        ("Y", ColumnType::Int, ints([10])),
        ("V", ColumnType::Long, longs([100])),
    ]);
    (left, right)
}

#[test]
fn aj_and_raj_refuse_the_rules_of_the_other() {
    let (left, right) = small_tables();
    for rule in RULES {
        let run = |operation: &str| {
            let join = if operation == "aj" { aj } else { raj };
            join(&left, &right, &on(&["K"]), &x_y(rule), &add(&["V"]))
        };
        let (allowed, refused) = if rule.is_aj() {
            ("aj", "raj")
        } else {
            ("raj", "aj")
        };
        assert!(run(allowed).is_ok(), "{rule:?}");
        match run(refused) {
            Err(AsOfJoinError::UnsupportedRule { operation, rule: r }) => {
                assert_eq!((operation, r), (refused, rule));
            }
            other => panic!("{rule:?}: {:?}", other.map(|_| ())),
        }
    }
}

#[test]
fn unknown_and_conflicting_columns_are_refused() {
    let (left, right) = small_tables();
    let join = |exact: &[&str], as_of: AsOfMatch, add: &[JoinAddition]| {
        as_of_join(&left, &right, &on(exact), &as_of, add).err()
    };
    let unknown = |name: &str| Some(AsOfJoinError::NoSuchColumn(vec![name.to_string()]));
    let rule = GreaterThanEqual;
    assert_eq!(join(&["Tag"], x_y(rule), &add(&["V"])), unknown("Tag"));
    assert_eq!(
        join(&["K"], AsOfMatch::new("Nope", rule, "Y"), &add(&["V"])),
        unknown("Nope")
    );
    assert_eq!(
        join(&["K"], AsOfMatch::new("X", rule, "Nope"), &add(&["V"])),
        unknown("Nope")
    );
    assert_eq!(join(&["K"], x_y(rule), &add(&["Nope"])), unknown("Nope"));
    assert_eq!(
        join(&["K"], x_y(rule), &[JoinAddition::renamed("Tag", "V")]),
        Some(AsOfJoinError::ConflictingColumns(vec!["Tag".into()]))
    );
}

#[test]
fn stamps_of_different_types_are_refused_but_for_the_types_java_reads_alike() {
    let instants: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(vec![5, 9]).with_timezone("UTC"));
    let (left, right) = stamp_tables(ColumnType::Long, longs([7, 9]), longs([5, 9]));
    let instant_right = static_table(&[
        ("Y", ColumnType::Instant, instants),
        ("V", ColumnType::Long, longs([1, 2])),
    ]);
    // An `Instant` is its nanoseconds.
    assert_eq!(v_of(&left, &instant_right, GreaterThanEqual), ["1", "2"]);
    let int_right = static_table(&[
        ("Y", ColumnType::Int, ints([5, 9])),
        ("V", ColumnType::Long, longs([1, 2])),
    ]);
    assert_eq!(
        as_of_join(
            &left,
            &int_right,
            &[],
            &x_y(GreaterThanEqual),
            &add(&["V"])
        )
        .err(),
        Some(AsOfJoinError::StampTypes {
            left: ColumnType::Long,
            right: ColumnType::Int
        })
    );
    drop(right);
}

/// A left table `K` (int), `X` and a right table `KL` (long), `Y`, `V`.
fn mismatched_keys(left_rows: usize, right_rows: usize) -> (Arc<dyn Table>, Arc<dyn Table>) {
    let left = static_table(&[
        ("K", ColumnType::Int, ints(vec![1; left_rows])),
        ("X", ColumnType::Int, ints(vec![1; left_rows])),
    ]);
    let right = static_table(&[
        ("KL", ColumnType::Long, longs(vec![1; right_rows])),
        ("Y", ColumnType::Int, ints(vec![1; right_rows])),
        ("V", ColumnType::Long, longs(vec![1; right_rows])),
    ]);
    (left, right)
}

fn k_kl(left: &Arc<dyn Table>, right: &Arc<dyn Table>) -> Result<Arc<dyn Table>, AsOfJoinError> {
    let exact = [JoinMatch::new("K", "KL")];
    as_of_join(left, right, &exact, &x_y(GreaterThanEqual), &add(&["V"]))
}

#[test]
fn exact_match_columns_of_different_types_are_refused_when_a_right_row_is_read() {
    let (left, right) = mismatched_keys(1, 1);
    assert_eq!(
        k_kl(&left, &right).err(),
        Some(AsOfJoinError::KeyTypes {
            left: "K".into(),
            left_type: ColumnType::Int,
            right: "KL".into(),
            right_type: ColumnType::Long
        })
    );
    // No right row is read from a right table without rows.
    let (left, right) = mismatched_keys(1, 0);
    assert_eq!(column(&*k_kl(&left, &right).unwrap(), "V"), ["null"]);
}

#[test]
fn a_static_left_table_without_rows_is_joined_without_a_look_at_the_match_columns() {
    let (left, right) = mismatched_keys(0, 1);
    let result = k_kl(&left, &right).unwrap();
    assert_eq!(names(&*result), ["K", "X", "KL", "Y", "V"]);
    assert!(result.row_set().is_empty());

    // Left match columns that do not exist and stamps of different types pass.
    let exact = [JoinMatch::new("Nope", "KL")];
    let as_of = AsOfMatch::new("AlsoNope", LessThan, "V");
    assert!(as_of_join(&left, &right, &exact, &as_of, &add(&["Y"])).is_ok());
    // The names of the added columns are still checked.
    assert_eq!(
        as_of_join(&left, &right, &exact, &as_of, &add(&["Nope"])).err(),
        Some(AsOfJoinError::NoSuchColumn(vec!["Nope".into()]))
    );

    // The result is static even when the right table is not.
    let graph = graph();
    let live: Arc<dyn Table> = scripted(&graph, [1], &right_columns(&[1], &[1], &[1])).table();
    let left = static_table(&left_columns(&[], &[], &[]));
    let result = as_of_join(&left, &live, &on(&["K"]), &x_y(LessThan), &add(&["V"])).unwrap();
    assert!(!result.is_refreshing());
}

#[test]
fn refreshing_tables_of_two_graphs_are_refused() {
    let (one, other) = (graph(), graph());
    let left: Arc<dyn Table> = scripted(&one, [1], &left_columns(&[1], &[1], &["a"])).table();
    let right: Arc<dyn Table> = scripted(&other, [1], &right_columns(&[1], &[1], &[1])).table();
    assert_eq!(
        as_of_join(
            &left,
            &right,
            &on(&["K"]),
            &x_y(GreaterThanEqual),
            &add(&["V"])
        )
        .err(),
        Some(AsOfJoinError::UpdateGraphConflict)
    );
}

// ---- Ticking -------------------------------------------------------------------------------

/// A table of a ticking test: static, or scripted at the given row keys.
enum Side<'a> {
    Fixed(Columns<'a>),
    Live(&'a [RowKey], Columns<'a>),
}

type Build = dyn Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync;

/// A join of two tables, with every update recorded and checked.
struct Pair {
    graph: Arc<dyn UpdateGraph>,
    left: Option<Arc<ScriptedTable>>,
    right: Option<Arc<ScriptedTable>>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Pair {
    /// `build` on the two tables; after every step the result must equal `build` on static
    /// copies of them.
    fn new(
        left: Side,
        right: Side,
        build: impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
    ) -> Self {
        let build: Arc<Build> = Arc::new(build);
        let graph = graph();
        let table = |side: Side| -> (Option<Arc<ScriptedTable>>, Arc<dyn Table>) {
            match side {
                Side::Fixed(columns) => (None, static_table(columns)),
                Side::Live(keys, columns) => {
                    let script = scripted(&graph, keys.iter().copied(), columns);
                    let table: Arc<dyn Table> = script.table();
                    (Some(script), table)
                }
            }
        };
        let (left, left_table) = table(left);
        let (right, right_table) = table(right);
        let result = build(&left_table, &right_table);
        let recorder = Recorder::attach(result.clone());
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            build(&frozen(&*left_table), &frozen(&*right_table))
        });
        checker.assert_consistent();
        Self {
            graph,
            left,
            right,
            result,
            recorder,
            checker,
        }
    }

    fn run(
        &self,
        left: Option<TableChange>,
        right: Option<TableChange>,
    ) -> Option<Result<TableUpdate, TableFailure>> {
        let before = self.recorder.events().len();
        if let Some(change) = left {
            self.left.as_ref().expect("a live left table").push(change);
        }
        if let Some(change) = right {
            self.right
                .as_ref()
                .expect("a live right table")
                .push(change);
        }
        self.graph.run_cycle();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one event in a cycle");
        events.pop()
    }

    /// Applies the changes in one cycle and returns the update the result delivered.
    ///
    /// # Panics
    /// If the result failed, is then inconsistent, or differs from the join of static
    /// copies of the tables.
    fn step(&self, left: Option<TableChange>, right: Option<TableChange>) -> Option<TableUpdate> {
        let event = self.run(left, right);
        self.checker.assert_consistent();
        event.map(|event| event.expect("an update"))
    }

    fn left(&self, change: TableChange) -> Option<TableUpdate> {
        self.step(Some(change), None)
    }

    fn right(&self, change: TableChange) -> Option<TableUpdate> {
        self.step(None, Some(change))
    }

    fn v(&self) -> Vec<String> {
        column(&*self.result, "V")
    }
}

/// Left columns `K` (int), `X` (int) and `Tag` (string).
fn left_columns(
    keys: &[i32],
    stamps: &[i32],
    tags: &[&str],
) -> [(&'static str, ColumnType, ArrayRef); 3] {
    [
        ("K", ColumnType::Int, ints(keys.iter().copied())),
        ("X", ColumnType::Int, ints(stamps.iter().copied())),
        ("Tag", ColumnType::String, strings(tags.iter().copied())),
    ]
}

/// Right columns `K` (int), `Y` (int), `V` (long) and `Other` (string), which the tests do
/// not add.
fn right_columns(
    keys: &[i32],
    stamps: &[i32],
    values: &[i64],
) -> [(&'static str, ColumnType, ArrayRef); 4] {
    let other = vec!["o"; keys.len()];
    [
        ("K", ColumnType::Int, ints(keys.iter().copied())),
        ("Y", ColumnType::Int, ints(stamps.iter().copied())),
        ("V", ColumnType::Long, longs(values.iter().copied())),
        ("Other", ColumnType::String, strings(other)),
    ]
}

fn left_row(key: i32, stamp: i32, tag: &str) -> Vec<ArrayRef> {
    vec![ints([key]), ints([stamp]), strings([tag])]
}

fn right_row(key: i32, stamp: i32, value: i64) -> Vec<ArrayRef> {
    vec![ints([key]), ints([stamp]), longs([value]), strings(["o"])]
}

/// The join on `K` and the stamps that adds `V`: result columns K, X, Tag, Y, V.
fn keyed(
    rule: AsOfRule,
) -> impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static {
    move |left, right| as_of_join(left, right, &on(&["K"]), &x_y(rule), &add(&["V"])).unwrap()
}

/// The join on the stamps alone that adds `V`: result columns K, X, Tag, Y, V.
fn unkeyed(
    rule: AsOfRule,
) -> impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static {
    move |left, right| as_of_join(left, right, &[], &x_y(rule), &add(&["V"])).unwrap()
}

// Columns of the left table and of the result.
const K: usize = 0;
const X: usize = 1;
const TAG: usize = 2;
const Y: usize = 3;
const V: usize = 4;
// Columns of the right table.
const RIGHT_K: usize = 0;
const RIGHT_Y: usize = 1;
const RIGHT_V: usize = 2;
const RIGHT_OTHER: usize = 3;

const LEFT_KEYS: [RowKey; 4] = [10, 20, 30, 40];
const RIGHT_KEYS: [RowKey; 3] = [5, 6, 7];

/// Left rows (key, stamp) (1, 10), (1, 20), (1, 30), (2, 20) at row keys 10, 20, 30, 40.
fn the_left() -> [(&'static str, ColumnType, ArrayRef); 3] {
    left_columns(&[1, 1, 1, 2], &[10, 20, 30, 20], &["a", "b", "c", "d"])
}

/// Right rows (key, stamp, V) (1, 10, 100), (1, 20, 200), (2, 15, 300) at row keys 5, 6, 7.
fn the_right() -> [(&'static str, ColumnType, ArrayRef); 4] {
    right_columns(&[1, 1, 2], &[10, 20, 15], &[100, 200, 300])
}

/// Both tables live. Under `>=` the left rows have V 100, 200, 200, 300; under `<=` they
/// have 100, 200, null, null.
fn both_live(rule: AsOfRule) -> Pair {
    Pair::new(
        Side::Live(&LEFT_KEYS, &the_left()),
        Side::Live(&RIGHT_KEYS, &the_right()),
        keyed(rule),
    )
}

#[test]
fn the_result_is_refreshing_when_either_table_is() {
    for (left_live, right_live) in [(true, true), (true, false), (false, true)] {
        let (left, right) = (the_left(), the_right());
        let side = |live: bool, keys: &'static [RowKey], columns| {
            if live {
                Side::Live(keys, columns)
            } else {
                Side::Fixed(columns)
            }
        };
        let pair = Pair::new(
            side(left_live, &LEFT_KEYS, &left),
            side(right_live, &RIGHT_KEYS, &right),
            keyed(GreaterThanEqual),
        );
        assert!(pair.result.is_refreshing());
        assert_eq!(pair.v(), ["100", "200", "200", "300"]);
    }
}

#[test]
fn left_adds_and_removes_pass_through_with_their_matches() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .left(TableChange::new().remove([20]).add(
            [15, 50],
            vec![ints([1, 7]), ints([15, 1]), strings(["e", "f"])],
        ))
        .unwrap();
    assert_eq!(keys(&update.added), [15, 50]);
    assert_eq!(keys(&update.removed), [20]);
    assert!(update.modified.is_empty() && update.modified_columns.is_empty());
    assert_eq!(pair.v(), ["100", "100", "200", "300", "null"]);
}

#[test]
fn a_left_modify_reports_the_added_columns_when_a_key_or_stamp_column_changed() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .left(TableChange::new().modify([10], [(TAG, strings(["z"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([TAG]));

    // The stamp.
    let update = pair
        .left(TableChange::new().modify([10], [(X, ints([25]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([X, Y, V]));
    assert_eq!(pair.v(), ["200", "200", "200", "300"]);

    // The key, to one with other right rows and to one without right rows.
    let update = pair
        .left(TableChange::new().modify([30, 40], [(K, ints([9, 1]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [30, 40]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([K, Y, V]));
    assert_eq!(pair.v(), ["200", "200", "null", "200"]);

    // To a stamp before every right stamp.
    pair.left(TableChange::new().modify([20], [(X, ints([9]))]));
    assert_eq!(pair.v(), ["200", "null", "null", "200"]);
}

#[test]
fn left_shifts_pass_through_and_matches_follow_the_rows() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .left(
            TableChange::new()
                .remove([10])
                .shift(20, 30, -15)
                .add([50], left_row(1, 12, "e")),
        )
        .unwrap();
    assert_eq!(
        crate::ops::sort::testing::shifts(&update),
        [(20, 30, -15)]
    );
    assert_eq!(keys(&pair.result.row_set()), [5, 15, 40, 50]);
    assert!(update.modified.is_empty());
    assert_eq!(pair.v(), ["200", "200", "300", "100"]);
    // A later change of the right table reaches the rows at their new keys.
    let update = pair.right(TableChange::new().remove([6])).unwrap();
    assert_eq!(keys(&update.modified), [5, 15]);
    assert_eq!(pair.v(), ["100", "100", "300", "100"]);
    // A row that moves to where another row was.
    let update = pair
        .left(TableChange::new().remove([5]).shift(15, 15, -10))
        .unwrap();
    assert_eq!(keys(&update.removed), [5]);
    assert_eq!(pair.v(), ["100", "300", "100"]);
}

#[test]
fn a_right_add_modifies_the_left_rows_up_to_the_next_right_stamp() {
    let pair = both_live(GreaterThanEqual);
    // A key no left row has.
    assert_eq!(
        pair.right(TableChange::new().add([8], right_row(9, 1, 900))),
        None
    );
    // Before the first left stamp of the key: the left row at the next right stamp keeps
    // its right row.
    assert_eq!(
        pair.right(TableChange::new().add([9], right_row(1, 5, 50))),
        None
    );
    // Between the stamps of the second and third left row.
    let update = pair
        .right(TableChange::new().add([11], right_row(1, 25, 250)))
        .unwrap();
    assert!(update.added.is_empty() && update.removed.is_empty() && update.shifted.is_empty());
    assert_eq!(keys(&update.modified), [30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([Y, V]));
    assert_eq!(pair.v(), ["100", "200", "250", "300"]);
    // Between the first and the second, and before the first: the left rows up to the next
    // right stamp have a right row with their own stamp.
    assert_eq!(
        pair.right(TableChange::new().add([12], right_row(1, 19, 190))),
        None
    );
    assert_eq!(
        pair.right(TableChange::new().add([13], right_row(1, 9, 90))),
        None
    );
    assert_eq!(pair.v(), ["100", "200", "250", "300"]);
    assert_eq!(column(&*pair.result, "Y"), ["10", "20", "25", "15"]);
}

#[test]
fn a_right_add_reaches_backwards_for_raj() {
    let pair = both_live(LessThanEqual);
    assert_eq!(pair.v(), ["100", "200", "null", "null"]);
    let update = pair
        .right(TableChange::new().add([8], right_row(1, 35, 350)))
        .unwrap();
    assert_eq!(keys(&update.modified), [30]);
    assert_eq!(pair.v(), ["100", "200", "350", "null"]);
    // Between two left stamps, where the later left row has a right row with its stamp.
    assert_eq!(
        pair.right(TableChange::new().add([12], right_row(1, 15, 150))),
        None
    );
    let update = pair
        .right(TableChange::new().add([9], right_row(2, 99, 990)))
        .unwrap();
    assert_eq!(keys(&update.modified), [40]);
    assert_eq!(pair.v(), ["100", "200", "350", "990"]);
    // Closer to the last left stamp of the key.
    assert_eq!(
        pair.right(TableChange::new().add([11], right_row(1, 31, 310))),
        Some(TableUpdate {
            modified: RowSet::from_keys([30]),
            modified_columns: ModifiedColumnSet::of([Y, V]),
            ..TableUpdate::empty()
        })
    );
}

#[test]
fn a_right_remove_gives_its_left_rows_the_next_right_row_or_none() {
    let pair = both_live(GreaterThanEqual);
    let update = pair.right(TableChange::new().remove([6])).unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([Y, V]));
    assert_eq!(pair.v(), ["100", "100", "100", "300"]);
    let update = pair.right(TableChange::new().remove([5, 7])).unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30, 40]);
    assert_eq!(pair.v(), ["null", "null", "null", "null"]);
}

#[test]
fn of_right_rows_with_one_stamp_aj_follows_the_last_and_raj_the_first() {
    let pair = both_live(GreaterThanEqual);
    // Before the row that has the stamp: nothing changes.
    assert_eq!(
        pair.right(TableChange::new().add([4], right_row(1, 20, 199))),
        None
    );
    let update = pair
        .right(TableChange::new().add([9], right_row(1, 20, 201)))
        .unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(pair.v(), ["100", "201", "201", "300"]);
    let update = pair.right(TableChange::new().remove([9])).unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(pair.v(), ["100", "200", "200", "300"]);

    let pair = both_live(LessThanEqual);
    assert_eq!(
        pair.right(TableChange::new().add([9], right_row(1, 20, 201))),
        None
    );
    let update = pair
        .right(TableChange::new().add([4], right_row(1, 20, 199)))
        .unwrap();
    assert_eq!(keys(&update.modified), [20]);
    assert_eq!(pair.v(), ["100", "199", "null", "null"]);
}

#[test]
fn a_strict_rule_passes_over_the_right_row_with_the_left_stamp() {
    let pair = both_live(GreaterThan);
    assert_eq!(pair.v(), ["null", "100", "200", "300"]);
    // A right row with the stamp of the first left row changes nothing for that row, but
    // is the new right row of the second.
    let update = pair
        .right(TableChange::new().add([9], right_row(1, 10, 101)))
        .unwrap();
    assert_eq!(keys(&update.modified), [20]);
    assert_eq!(pair.v(), ["null", "101", "200", "300"]);

    let pair = both_live(LessThan);
    assert_eq!(pair.v(), ["200", "null", "null", "null"]);
    let update = pair
        .right(TableChange::new().add([9], right_row(1, 30, 301)))
        .unwrap();
    assert_eq!(keys(&update.modified), [20]);
    assert_eq!(pair.v(), ["200", "301", "null", "null"]);
}

#[test]
fn a_right_modify_reports_the_added_columns_it_changed() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .right(TableChange::new().modify([6], [(RIGHT_V, longs([222]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([V]));
    assert_eq!(pair.v(), ["100", "222", "222", "300"]);
    // A column the join does not add.
    assert_eq!(
        pair.right(TableChange::new().modify([6], [(RIGHT_OTHER, strings(["p"]))])),
        None
    );
}

#[test]
fn a_right_stamp_change_moves_the_row_among_the_stamps() {
    let pair = both_live(GreaterThanEqual);
    // Still the right row of the same left rows, with another stamp.
    let update = pair
        .right(TableChange::new().modify([6], [(RIGHT_Y, ints([12]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([Y, V]));
    assert_eq!(column(&*pair.result, "Y"), ["10", "12", "12", "15"]);
    // Past the stamps of all left rows but the last.
    let update = pair
        .right(TableChange::new().modify([6], [(RIGHT_Y, ints([30]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(pair.v(), ["100", "100", "200", "300"]);
    // Before them all.
    let update = pair
        .right(TableChange::new().modify([5, 6], [(RIGHT_Y, ints([1, 40]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30]);
    assert_eq!(pair.v(), ["100", "100", "100", "300"]);
    assert_eq!(column(&*pair.result, "Y"), ["1", "1", "1", "15"]);
}

#[test]
fn a_right_key_change_moves_the_row_from_the_old_key_to_the_new() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .right(TableChange::new().modify([7], [(RIGHT_K, ints([1]))]))
        .unwrap();
    // The left row of key 2 lost its right row. At key 1 the row is between two right
    // rows, and the left rows after it keep theirs.
    assert_eq!(keys(&update.modified), [40]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([Y, V]));
    assert_eq!(pair.v(), ["100", "200", "200", "null"]);
    let update = pair
        .right(TableChange::new().modify([5, 6], [(RIGHT_K, ints([2, 2]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30, 40]);
    assert_eq!(pair.v(), ["null", "300", "300", "200"]);
}

#[test]
fn a_right_shift_changes_nothing_in_the_result() {
    let pair = both_live(GreaterThanEqual);
    assert_eq!(pair.right(TableChange::new().shift(5, 7, 10)), None);
    assert_eq!(pair.v(), ["100", "200", "200", "300"]);
    // The shifted rows are still the right rows: a modify at the new key arrives.
    let update = pair
        .right(
            TableChange::new()
                .shift(16, 17, 4)
                .modify([15], [(RIGHT_V, longs([111]))]),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([V]));
    assert_eq!(pair.v(), ["111", "200", "200", "300"]);
    // Shifted and modified in one cycle.
    let update = pair
        .right(
            TableChange::new()
                .shift(20, 21, 1)
                .modify([21], [(RIGHT_V, longs([222]))]),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [20, 30]);
    assert_eq!(pair.v(), ["111", "222", "222", "300"]);
    // A removed row and a shift to its row key.
    let update = pair
        .right(TableChange::new().remove([15]).shift(21, 22, -6))
        .unwrap();
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(pair.v(), ["null", "222", "222", "300"]);
}

#[test]
fn a_right_row_replaced_at_its_row_key_modifies_its_left_rows() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .right(
            TableChange::new()
                .remove([5])
                .add([5], right_row(1, 10, 555)),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([Y, V]));
    assert_eq!(pair.v(), ["555", "200", "200", "300"]);
}

#[test]
fn changes_of_both_tables_in_one_cycle_make_one_update() {
    let pair = both_live(GreaterThanEqual);
    let update = pair
        .step(
            Some(
                TableChange::new()
                    .remove([10])
                    .add([50], left_row(1, 26, "e"))
                    .modify([40], [(TAG, strings(["z"]))]),
            ),
            Some(TableChange::new().add([8], right_row(1, 25, 250))),
        )
        .unwrap();
    assert_eq!(keys(&update.added), [50]);
    assert_eq!(keys(&update.removed), [10]);
    // Row 30 by the right table's change, row 40 by the left table's.
    assert_eq!(keys(&update.modified), [30, 40]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([TAG, Y, V]));
    assert_eq!(pair.v(), ["200", "250", "300", "250"]);

    // A left row that changes its stamp while its right row goes.
    let update = pair
        .step(
            Some(TableChange::new().modify([20], [(X, ints([27]))])),
            Some(TableChange::new().remove([6])),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [20]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([X, Y, V]));
    assert_eq!(pair.v(), ["250", "250", "300", "250"]);
}

/// Java, with two refreshing tables and exact-match columns, reports every left row of a
/// key when the key gets its first right rows, whether or not they are the right rows of
/// those left rows (`processInitial` in `BucketedChunkedAjMergedListener#process`). With a
/// static left table it reports the rows whose right row changed.
#[test]
fn the_first_right_rows_of_a_key_modify_all_its_left_rows_when_both_tables_tick() {
    let pair = both_live(GreaterThanEqual);
    pair.left(TableChange::new().add([50], left_row(9, 10, "e")));
    let update = pair
        .right(TableChange::new().add([8], right_row(9, 99, 990)))
        .unwrap();
    assert_eq!(keys(&update.modified), [50]);
    assert_eq!(pair.v(), ["100", "200", "200", "300", "null"]);
    // Only the first time.
    assert_eq!(
        pair.right(TableChange::new().add([9], right_row(9, 98, 980))),
        None
    );
    pair.right(TableChange::new().remove([8, 9]));
    assert_eq!(
        pair.right(TableChange::new().add([8], right_row(9, 99, 990))),
        None
    );

    let left = left_columns(&[1, 9], &[10, 10], &["a", "e"]);
    let pair = Pair::new(
        Side::Fixed(&left),
        Side::Live(&RIGHT_KEYS, &the_right()),
        keyed(GreaterThanEqual),
    );
    assert_eq!(
        pair.right(TableChange::new().add([8], right_row(9, 99, 990))),
        None
    );
}

#[test]
fn a_static_left_table_follows_a_live_right_table() {
    let pair = Pair::new(
        Side::Fixed(&the_left()),
        Side::Live(&RIGHT_KEYS, &the_right()),
        keyed(LessThan),
    );
    assert_eq!(pair.v(), ["200", "null", "null", "null"]);
    // The left table's row keys are 0 to 3.
    let update = pair
        .right(
            TableChange::new()
                .remove([6])
                .add([9, 11], vec![ints([1, 2]), ints([31, 21]), longs([310, 210]), strings(["o", "o"])]),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [0, 1, 2, 3]);
    assert_eq!(pair.v(), ["310", "310", "310", "210"]);
    let update = pair
        .right(TableChange::new().modify([9], [(RIGHT_Y, ints([15]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [0, 1, 2]);
    assert_eq!(pair.v(), ["310", "null", "null", "210"]);
}

#[test]
fn a_live_left_table_follows_a_static_right_table() {
    let pair = Pair::new(
        Side::Live(&LEFT_KEYS, &the_left()),
        Side::Fixed(&the_right()),
        keyed(GreaterThan),
    );
    assert_eq!(pair.v(), ["null", "100", "200", "300"]);
    let update = pair
        .left(
            TableChange::new()
                .remove([20])
                .shift(30, 40, -5)
                .add([50], left_row(2, 15, "e"))
                .modify([10], [(X, ints([11]))]),
        )
        .unwrap();
    assert_eq!(keys(&update.added), [50]);
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([X, Y, V]));
    assert_eq!(pair.v(), ["100", "200", "300", "null"]);
}

#[test]
fn without_exact_match_columns_every_right_row_is_a_candidate() {
    let pair = Pair::new(
        Side::Live(&LEFT_KEYS, &the_left()),
        Side::Live(&RIGHT_KEYS, &the_right()),
        unkeyed(GreaterThanEqual),
    );
    assert_eq!(pair.v(), ["100", "200", "200", "200"]);
    let update = pair
        .right(TableChange::new().add([8], right_row(5, 30, 400)))
        .unwrap();
    assert_eq!(keys(&update.modified), [30]);
    let update = pair.right(TableChange::new().remove([6])).unwrap();
    assert_eq!(keys(&update.modified), [20, 40]);
    assert_eq!(pair.v(), ["100", "300", "400", "300"]);
    // A change of the right table's `K` is no change of a key here, and `K` is not added.
    assert_eq!(
        pair.right(TableChange::new().modify([7], [(RIGHT_K, ints([8]))])),
        None
    );
    // Nor is one of the left table's.
    let update = pair
        .left(TableChange::new().modify([10], [(K, ints([8]))]))
        .unwrap();
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([K]));
    // The right table loses all its rows and the left table gets one.
    let update = pair
        .step(
            Some(TableChange::new().add([50], left_row(1, 99, "e"))),
            Some(TableChange::new().remove([5, 7, 8])),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30, 40]);
    assert_eq!(pair.v(), ["null", "null", "null", "null", "null"]);
}

#[test]
fn exact_match_columns_of_different_types_fail_the_result_when_a_right_row_arrives() {
    let graph = graph();
    let left = scripted(
        &graph,
        [1],
        &[
            ("K", ColumnType::Int, ints([1])),
            ("X", ColumnType::Int, ints([1])),
        ],
    );
    let right = scripted(
        &graph,
        [],
        &[
            ("KL", ColumnType::Long, longs([])),
            ("Y", ColumnType::Int, ints([])),
            ("V", ColumnType::Long, longs([])),
        ],
    );
    let (left_table, right_table): (Arc<dyn Table>, Arc<dyn Table>) = (left.table(), right.table());
    let result = k_kl(&left_table, &right_table).unwrap();
    let recorder = Recorder::attach(result.clone());
    // The left table's rows are not a reason to fail.
    left.push(TableChange::new().add([2], vec![ints([1]), ints([2])]));
    graph.run_cycle();
    assert!(!result.is_failed());
    assert_eq!(column(&*result, "V"), ["null", "null"]);

    right.push(TableChange::new().add([1], vec![longs([1]), ints([1]), longs([1])]));
    graph.run_cycle();
    assert!(result.is_failed());
    let failure = recorder.events().pop().unwrap().unwrap_err();
    let expected = AsOfJoinError::KeyTypes {
        left: "K".into(),
        left_type: ColumnType::Int,
        right: "KL".into(),
        right_type: ColumnType::Long,
    };
    assert_eq!(failure.message, expected.to_string());
}

#[test]
fn a_failure_of_either_table_fails_the_result() {
    for side in [0, 1] {
        let pair = both_live(GreaterThanEqual);
        let script = if side == 0 { &pair.left } else { &pair.right };
        script.as_ref().unwrap().push_failure("broken");
        pair.graph.run_cycle();
        assert_eq!(
            pair.recorder.events().pop(),
            Some(Err(TableFailure {
                message: "broken".into()
            }))
        );
        assert!(pair.result.is_failed());
        // A join of a failed table is refused.
        let failed = pair.result.clone();
        let (other, _) = small_tables();
        assert!(matches!(
            as_of_join(
                &other,
                &failed,
                &on(&["K"]),
                &x_y(GreaterThanEqual),
                &add(&["V"])
            ),
            Err(AsOfJoinError::Table(_))
        ));
    }
}
