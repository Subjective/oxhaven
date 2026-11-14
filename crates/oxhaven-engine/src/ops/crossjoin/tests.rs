//! Unit tests of the cross join: the static result and its refusals, then each kind of
//! change of either table, checked by the consistency checker against the join of static
//! copies of the tables.

use super::{cross_join, CrossJoinError, DEFAULT_RESERVE_BITS};
use crate::ops::join::{JoinAddition, JoinError, JoinMatch};
use crate::ops::sort::testing::{cells, frozen, graph, keys, scripted, shifts, static_table};
use arrow::array::ArrayRef;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    ints, longs, strings, ConsistencyChecker, Recorder, ReferenceTable, ScriptedTable, TableChange,
};
use std::sync::Arc;

fn on(columns: &[&str]) -> Vec<JoinMatch> {
    columns.iter().map(|c| JoinMatch::same(*c)).collect()
}

fn add(columns: &[&str]) -> Vec<JoinAddition> {
    columns.iter().map(|c| JoinAddition::same(*c)).collect()
}

fn column(table: &dyn Table, name: &str) -> Vec<String> {
    let index = table.definition().index_of(name);
    cells(table, index.unwrap_or_else(|| panic!("no column {name}")))
}

fn names(table: &dyn Table) -> Vec<String> {
    let columns = table.definition().columns().iter();
    columns.map(|column| column.name.clone()).collect()
}

/// Left columns `K` (int) and `Tag` (string).
fn left_columns(k: &[i32], tags: &[&str]) -> [(&'static str, ColumnType, ArrayRef); 2] {
    [
        ("K", ColumnType::Int, ints(k.iter().copied())),
        ("Tag", ColumnType::String, strings(tags.iter().copied())),
    ]
}

/// Right columns `K` (int), `V` (long) and `Other` (string), which the tests do not add.
fn right_columns(k: &[i32], v: &[i64]) -> [(&'static str, ColumnType, ArrayRef); 3] {
    let other = vec!["o"; k.len()];
    [
        ("K", ColumnType::Int, ints(k.iter().copied())),
        ("V", ColumnType::Long, longs(v.iter().copied())),
        ("Other", ColumnType::String, strings(other)),
    ]
}

fn left_row(k: i32, tag: &str) -> Vec<ArrayRef> {
    vec![ints([k]), strings([tag])]
}

fn right_row(k: i32, v: i64) -> Vec<ArrayRef> {
    vec![ints([k]), longs([v]), strings(["o"])]
}

// ---- Static results -------------------------------------------------------------------------

#[test]
fn a_static_join_pairs_each_left_row_with_its_matching_right_rows_in_order() {
    let left = static_table(&left_columns(&[2, 1, 3, 2], &["a", "b", "c", "d"]));
    let right = static_table(&right_columns(&[2, 1, 2, 4], &[10, 11, 12, 13]));
    let result = cross_join(
        &left,
        &right,
        &on(&["K"]),
        &add(&["V"]),
        DEFAULT_RESERVE_BITS,
    );
    let result = result.unwrap();
    assert_eq!(names(&*result), ["K", "Tag", "V"]);
    assert_eq!(column(&*result, "Tag"), ["a", "a", "b", "d", "d"]);
    assert_eq!(column(&*result, "V"), ["10", "12", "11", "10", "12"]);
    assert!(!result.is_refreshing());
    assert!(!result.is_flat());
    let attributes = result.attributes();
    assert!(!attributes.add_only && !attributes.append_only && !attributes.blink);
    // Both static: the right part has the bits of the largest matched group, 2 rows.
    assert_eq!(keys(&result.row_set()), [0, 1, 2, 6, 7]);
}

#[test]
fn a_join_without_match_columns_pairs_every_row_with_every_row() {
    let left = static_table(&left_columns(&[1, 2], &["a", "b"]));
    let right = static_table(&right_columns(&[7, 8, 9], &[10, 11, 12]));
    let result = cross_join(&left, &right, &[], &add(&["V"]), DEFAULT_RESERVE_BITS).unwrap();
    assert_eq!(column(&*result, "Tag"), ["a", "a", "a", "b", "b", "b"]);
    assert_eq!(column(&*result, "V"), ["10", "11", "12", "10", "11", "12"]);
    // The right row key in the low bits: 2 bits for the right table's last key, 2.
    assert_eq!(keys(&result.row_set()), [0, 1, 2, 4, 5, 6]);
}

#[test]
fn the_default_additions_are_the_right_columns_not_matched() {
    let left = static_table(&left_columns(&[1], &["a"]));
    let right = static_table(&right_columns(&[1, 1], &[10, 11]));
    let result = cross_join(&left, &right, &on(&["K"]), &[], DEFAULT_RESERVE_BITS).unwrap();
    assert_eq!(names(&*result), ["K", "Tag", "V", "Other"]);
    assert_eq!(column(&*result, "V"), ["10", "11"]);
}

#[test]
fn refusals_name_the_column_or_the_limit() {
    let left = static_table(&left_columns(&[1], &["a"]));
    let right = static_table(&right_columns(&[1], &[10]));
    let refused = |on: &[JoinMatch], add: &[JoinAddition], reserve: i32| {
        cross_join(&left, &right, on, add, reserve).err()
    };
    assert!(matches!(
        refused(&on(&["Q"]), &add(&["V"]), 10),
        Some(CrossJoinError::Join(JoinError::NoSuchColumn(_)))
    ));
    assert!(matches!(
        refused(&on(&["K"]), &[JoinAddition::renamed("Tag", "V")], 10),
        Some(CrossJoinError::Join(JoinError::ConflictingColumns(_)))
    ));
    // Without match columns the default additions include `K`, which the left table has.
    assert!(matches!(
        refused(&[], &[], 10),
        Some(CrossJoinError::Join(JoinError::ConflictingColumns(_)))
    ));
    // Two static tables do not use the reserve.
    assert!(refused(&on(&["K"]), &[], 0).is_none());
    assert!(refused(&[], &add(&["V"]), 0).is_none());
    assert!(refused(&[], &add(&["V"]), 63).is_none());
}

#[test]
fn a_refreshing_join_needs_a_reserve_of_1_to_62_bits() {
    let graph = graph();
    let script = scripted(&graph, [0, 1], &left_columns(&[1, 1], &["a", "b"]));
    let left: Arc<dyn Table> = script.table();
    let right = static_table(&right_columns(&[1], &[10]));
    let keyed = |reserve| cross_join(&left, &right, &on(&["K"]), &add(&["V"]), reserve).err();
    assert_eq!(keyed(0), Some(CrossJoinError::ReserveBits(0)));
    assert_eq!(keyed(63), Some(CrossJoinError::ReserveBits(63)));
    assert_eq!(
        keyed(-1).unwrap().to_string(),
        "Assertion failed: asserted newNumShiftBits < 63 and > 0, instead newNumShiftBits == -1"
    );
    assert_eq!(keyed(62), None);
    // Without match columns the row key space is checked first, and a reserve below the
    // bits of the right table's last key is raised to them.
    let zero = |reserve| cross_join(&left, &right, &[], &add(&["V"]), reserve).err();
    assert!(matches!(zero(63), Some(CrossJoinError::OutOfKeySpace(_))));
    assert_eq!(zero(62), None);
    // The right table's last key, 0, needs no bits, and Java's shift state takes none.
    assert_eq!(zero(0), Some(CrossJoinError::ReserveBits(0)));
}

#[test]
fn a_join_that_needs_more_than_63_bits_of_row_key_is_refused() {
    let big = |key: RowKey| -> Arc<dyn Table> {
        let rows = oxhaven_contracts::rowset::RowSet::from_keys([0, key]);
        let columns = left_columns(&[1, 1], &["a", "b"]);
        let definition =
            oxhaven_testkit::definition(&[("K", ColumnType::Int), ("Tag", ColumnType::String)]);
        let values = columns.iter().map(|(_, _, v)| v.clone()).collect();
        ReferenceTable::new_static(definition, rows, values)
    };
    let right = static_table(&right_columns(&[1], &[10]));
    // A left row key of 62 bits leaves one bit for the right rows.
    for keyed in [true, false] {
        let on = if keyed { on(&["K"]) } else { Vec::new() };
        let join = |left: &Arc<dyn Table>| cross_join(left, &right, &on, &add(&["V"]), 10);
        assert!(join(&big(1 << 61)).is_ok(), "keyed {keyed}");
        let error = join(&big(1 << 62)).err();
        assert!(
            matches!(error, Some(CrossJoinError::OutOfKeySpace(_))),
            "keyed {keyed}"
        );
    }
}

#[test]
fn refreshing_tables_of_two_graphs_are_refused() {
    let (one, other) = (graph(), graph());
    let left: Arc<dyn Table> = scripted(&one, [1], &left_columns(&[1], &["a"])).table();
    let right: Arc<dyn Table> = scripted(&other, [1], &right_columns(&[1], &[1])).table();
    assert_eq!(
        cross_join(&left, &right, &on(&["K"]), &add(&["V"]), 10).err(),
        Some(CrossJoinError::Join(JoinError::UpdateGraphConflict))
    );
}

// ---- Changes --------------------------------------------------------------------------------

/// A live or static table of a pair.
enum Side<'a> {
    Live(&'a [RowKey], &'a [(&'a str, ColumnType, ArrayRef)]),
    Fixed(&'a [(&'a str, ColumnType, ArrayRef)]),
}

type Build = dyn Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync;

/// A join of two tables, with every update recorded and checked against the join of static
/// copies of the tables.
struct Pair {
    graph: Arc<dyn UpdateGraph>,
    left: Option<Arc<ScriptedTable>>,
    right: Option<Arc<ScriptedTable>>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Pair {
    fn new(left: Side, right: Side, build: Arc<Build>) -> Self {
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

    fn tags(&self) -> Vec<String> {
        column(&*self.result, "Tag")
    }
}

fn keyed(reserve: i32) -> Arc<Build> {
    Arc::new(move |left, right| {
        cross_join(left, right, &on(&["K"]), &add(&["V"]), reserve).unwrap()
    })
}

fn zero(reserve: i32) -> Arc<Build> {
    Arc::new(move |left, right| cross_join(left, right, &[], &add(&["V"]), reserve).unwrap())
}

/// Left rows `a` (key 1) at 10 and `b` (key 2) at 20; right rows 100 (key 1) at 5 and 200
/// (key 2) at 6.
fn both_live(build: Arc<Build>) -> Pair {
    Pair::new(
        Side::Live(&[10, 20], &left_columns(&[1, 2], &["a", "b"])),
        Side::Live(&[5, 6], &right_columns(&[1, 2], &[100, 200])),
        build,
    )
}

#[test]
fn an_empty_cycle_delivers_nothing() {
    for build in [keyed(1), zero(1)] {
        let pair = both_live(build);
        assert_eq!(
            pair.step(Some(TableChange::new()), Some(TableChange::new())),
            None
        );
    }
}

#[test]
fn right_rows_added_to_a_group_add_result_rows_and_grow_the_bits() {
    let pair = both_live(keyed(1));
    // One bit: left row 10 at 20, left row 20 at 40.
    assert_eq!(keys(&pair.result.row_set()), [20, 40]);
    let update = pair
        .right(TableChange::new().add([7], right_row(1, 101)))
        .unwrap();
    assert_eq!(pair.v(), ["100", "101", "200"]);
    // Still one bit: the group of key 1 has two rows.
    assert_eq!(keys(&update.added), [21]);
    assert!(update.shifted.is_empty());
    let update = pair
        .right(TableChange::new().add([8], right_row(1, 102)))
        .unwrap();
    // Two bits now: every left row moves.
    assert_eq!(keys(&pair.result.row_set()), [40, 41, 42, 80]);
    assert_eq!(keys(&update.added), [42]);
    assert_eq!(shifts(&update), [(20, 21, 20), (40, 41, 40)]);
}

#[test]
fn right_rows_removed_and_modified_in_place() {
    let pair = both_live(keyed(4));
    pair.right(TableChange::new().add(
        [7, 8],
        vec![ints([1, 1]), longs([101, 102]), strings(["o", "o"])],
    ));
    assert_eq!(pair.v(), ["100", "101", "102", "200"]);
    let update = pair.right(TableChange::new().remove([7])).unwrap();
    assert_eq!(pair.v(), ["100", "102", "200"]);
    assert_eq!(keys(&update.removed), [161]);
    assert_eq!(shifts(&update), [(162, 162, -1)]);
    let update = pair
        .right(TableChange::new().modify([8], [(1, longs([103]))]))
        .unwrap();
    assert_eq!(pair.v(), ["100", "103", "200"]);
    assert_eq!(keys(&update.modified), [161]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([2]));
}

#[test]
fn a_right_row_that_changes_key_moves_to_another_group() {
    let pair = both_live(keyed(1));
    let update = pair
        .right(TableChange::new().modify([5], [(0, ints([2]))]))
        .unwrap();
    assert_eq!(pair.tags(), ["b", "b"]);
    assert_eq!(pair.v(), ["100", "200"]);
    assert!(update.modified.is_empty());
    assert_eq!(keys(&update.removed), [20]);
    // A key no left row has.
    let update = pair
        .right(TableChange::new().modify([6], [(0, ints([9]))]))
        .unwrap();
    assert_eq!(pair.v(), ["100"]);
    assert!(update.added.is_empty());
}

#[test]
fn left_rows_added_removed_modified_and_shifted() {
    let pair = both_live(keyed(1));
    let update = pair
        .left(TableChange::new().add([30], left_row(1, "c")))
        .unwrap();
    assert_eq!(pair.tags(), ["a", "b", "c"]);
    assert_eq!(keys(&update.added), [60]);
    let update = pair.left(TableChange::new().remove([10])).unwrap();
    assert_eq!(keys(&update.removed), [20]);
    let update = pair
        .left(TableChange::new().modify([30], [(1, strings(["d"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [60]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([1]));
    let update = pair.left(TableChange::new().shift(20, 30, 5)).unwrap();
    assert_eq!(update.shifted.apply_to_key(40), 50);
    assert_eq!(update.shifted.apply_to_key(60), 70);
    // A key change: the row leaves its group for another.
    let update = pair
        .left(TableChange::new().modify([35], [(0, ints([2]))]))
        .unwrap();
    assert!(update.modified.is_empty());
    assert_eq!(keys(&update.removed), [70]);
    assert_eq!(keys(&update.added), [70]);
    assert_eq!(pair.v(), ["200", "200"]);
    // A key no right row has.
    let update = pair
        .left(TableChange::new().modify([25], [(0, ints([7]))]))
        .unwrap();
    assert_eq!(keys(&update.removed), [50]);
    assert_eq!(pair.tags(), ["d"]);
}

#[test]
fn both_tables_change_in_one_cycle() {
    for build in [keyed(1), keyed(10), zero(1), zero(10)] {
        let pair = both_live(build);
        pair.step(
            Some(
                TableChange::new()
                    .remove([10])
                    .shift(20, 20, 1)
                    .add([22, 30], vec![ints([1, 2]), strings(["c", "d"])])
                    .modify([21], [(1, strings(["e"]))]),
            ),
            Some(
                TableChange::new()
                    .remove([5])
                    .shift(6, 6, 3)
                    .add(
                        [2, 12],
                        vec![ints([2, 1]), longs([201, 101]), strings(["o", "o"])],
                    )
                    .modify([9], [(1, longs([202]))]),
            ),
        );
        pair.step(
            Some(TableChange::new().modify([22], [(0, ints([2]))])),
            Some(TableChange::new().modify([2, 12], [(0, ints([1, 2]))])),
        );
        assert!(!pair.result.is_failed());
    }
}

#[test]
fn a_join_without_match_columns_follows_both_tables() {
    let pair = both_live(zero(1));
    // The right table's last key, 6, needs three bits.
    assert_eq!(keys(&pair.result.row_set()), [85, 86, 165, 166]);
    let update = pair
        .right(TableChange::new().add([9], right_row(3, 300)))
        .unwrap();
    // Key 9 needs four bits: every left row moves.
    assert_eq!(keys(&pair.result.row_set()), [165, 166, 169, 325, 326, 329]);
    assert_eq!(keys(&update.added), [169, 329]);
    let update = pair
        .left(TableChange::new().modify([10], [(1, strings(["x"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [165, 166, 169]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([1]));
    let update = pair
        .right(TableChange::new().modify([6], [(1, longs([201]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [166, 326]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([2]));
}

#[test]
fn a_ticking_left_table_with_an_empty_static_right_table_never_updates() {
    let pair = Pair::new(
        Side::Live(&[10, 20], &left_columns(&[1, 2], &["a", "b"])),
        Side::Fixed(&right_columns(&[], &[])),
        zero(1),
    );
    assert!(pair.result.row_set().is_empty());
    assert_eq!(
        pair.left(TableChange::new().add([30], left_row(3, "c"))),
        None
    );
}

/// Java listens to a refreshing table of a join without match columns only when the other
/// table is refreshing or has rows, and a result that listens to nothing is static. The
/// results below are those of the Java server (Deephaven 42.9) for the same pairs.
#[test]
fn a_join_without_match_columns_of_a_ticking_and_an_empty_static_table_is_static() {
    let graph = graph();
    let live = |keys: &[RowKey], columns: &[(&str, ColumnType, ArrayRef)]| {
        scripted(&graph, keys.iter().copied(), columns)
    };
    let left_script = live(&[10, 20], &left_columns(&[1, 2], &["a", "b"]));
    let right_script = live(&[5, 6], &right_columns(&[1, 2], &[100, 200]));
    let (left, right): (Arc<dyn Table>, Arc<dyn Table>) =
        (left_script.table(), right_script.table());
    let no_left_yet: Arc<dyn Table> = live(&[], &left_columns(&[], &[])).table();
    let no_right_yet: Arc<dyn Table> = live(&[], &right_columns(&[], &[])).table();
    let no_left = static_table(&left_columns(&[], &[]));
    let no_right = static_table(&right_columns(&[], &[]));
    let some_left = static_table(&left_columns(&[1], &["a"]));
    let some_right = static_table(&right_columns(&[1], &[100]));
    let join = |left: &Arc<dyn Table>, right: &Arc<dyn Table>, on: &[JoinMatch]| {
        cross_join(left, right, on, &add(&["V"]), 10).unwrap()
    };

    // a static table without rows on either side: there is no pair and there never is one
    let unheard = [
        join(&left, &no_right, &[]),
        join(&no_left, &right, &[]),
        join(&no_left_yet, &no_right, &[]),
        join(&no_left, &no_right_yet, &[]),
    ];
    for result in &unheard {
        assert!(!result.is_refreshing());
        assert!(result.row_set().is_empty());
        assert_eq!(result.definition().columns().len(), 3);
    }
    left_script.push(TableChange::new().add([30], left_row(3, "c")));
    right_script.push(TableChange::new().remove([5]));
    graph.run_cycle();
    for result in &unheard {
        assert!(result.row_set().is_empty());
        assert!(!result.is_failed());
    }

    // every other pair with a refreshing table is refreshing: a static table with rows, two
    // refreshing tables with or without rows, and any join with match columns
    let k = on(&["K"]);
    let heard: [(&Arc<dyn Table>, &Arc<dyn Table>, &[JoinMatch]); 8] = [
        (&left, &some_right, &[]),
        (&some_left, &right, &[]),
        (&left, &right, &[]),
        (&no_left_yet, &no_right_yet, &[]),
        (&left, &no_right_yet, &[]),
        (&no_left_yet, &right, &[]),
        (&left, &no_right, &k),
        (&no_left, &right, &k),
    ];
    for (index, (left, right, on)) in heard.into_iter().enumerate() {
        assert!(join(left, right, on).is_refreshing(), "pair {index}");
    }
    assert!(!join(&no_left, &no_right, &[]).is_refreshing());
    assert!(!join(&some_left, &no_right, &[]).is_refreshing());

    // the refreshing table is still read: its row keys must leave room for the reserve,
    // and a failed one is refused
    let wide: Arc<dyn Table> = live(&[0, 1 << 60], &left_columns(&[1, 1], &["a", "b"])).table();
    assert!(matches!(
        cross_join(&wide, &no_right, &[], &add(&["V"]), 10),
        Err(CrossJoinError::OutOfKeySpace(_))
    ));
    assert_eq!(
        cross_join(&left, &no_right, &[], &add(&["V"]), 0).err(),
        Some(CrossJoinError::ReserveBits(0))
    );
    left_script.push_failure("broken");
    graph.run_cycle();
    assert!(matches!(
        cross_join(&left, &no_right, &[], &add(&["V"]), 10),
        Err(CrossJoinError::Join(JoinError::Table(_)))
    ));
}

#[test]
fn a_failure_of_either_table_fails_the_result() {
    for build in [keyed(10), zero(10)] {
        for side in [0, 1] {
            let pair = both_live(build.clone());
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
            let other = static_table(&right_columns(&[1], &[1]));
            assert!(matches!(
                cross_join(&pair.result, &other, &on(&["K"]), &add(&["Other"]), 10),
                Err(CrossJoinError::Join(JoinError::Table(_)))
            ));
        }
    }
}

#[test]
fn a_cycle_that_runs_out_of_row_key_space_fails_the_result() {
    let pair = Pair::new(
        Side::Live(&[0, 1 << 60], &left_columns(&[1, 1], &["a", "b"])),
        Side::Live(&[0, 1], &right_columns(&[1, 1], &[10, 11])),
        keyed(2),
    );
    let run = pair.run(
        None,
        Some(TableChange::new().add(
            [2, 3, 4],
            vec![
                ints([1, 1, 1]),
                longs([12, 13, 14]),
                strings(["o", "o", "o"]),
            ],
        )),
    );
    // Five rows in a group need three bits; with the 61 of the left key that is too many.
    let failure = run.unwrap().unwrap_err();
    assert!(
        failure.message.starts_with("join out of rowSet space"),
        "{}",
        failure.message
    );
    assert!(pair.result.is_failed());
}
