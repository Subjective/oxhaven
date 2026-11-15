//! `where_in` and `where_not_in`: the static result, each incremental case of the left
//! table and of the set table with the update it gives, refusals and failures, and random
//! updates of both tables against the static path and a model. The results recorded from
//! Java are in `corpus.rs`.

use super::where_in::dynamic_where;
use super::{where_in, where_not_in, WhereInError};
use crate::ops::join::JoinMatch;
use crate::ops::sort::testing::{blink_table, cells, frozen, graph, keys, scripted, static_table};
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, Int8Array,
    TimestampNanosecondArray,
};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    at_least_for, definition, ints, strings, test_seeds, ConsistencyChecker, RandomUpdates,
    Recorder, ScriptedTable, TableChange,
};
use std::collections::BTreeSet;
use std::sync::Arc;

fn on(pairs: &[&str]) -> Vec<JoinMatch> {
    pairs
        .iter()
        .map(|pair| match pair.split_once('=') {
            Some((left, right)) => JoinMatch::new(left, right),
            None => JoinMatch::same(*pair),
        })
        .collect()
}

/// The left table: `K`, which names the row, the key `G` and `S`.
fn left_columns(k: &[i32], g: &[i32], s: &[&str]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("K", ColumnType::Int, ints(k.iter().copied())),
        ("G", ColumnType::Int, ints(g.iter().copied())),
        ("S", ColumnType::String, strings(s.iter().copied())),
    ]
}

fn left_row(k: i32, g: i32, s: &str) -> Vec<ArrayRef> {
    vec![ints([k]), ints([g]), strings([s])]
}

/// The set table: the key `X` and `W`.
fn set_columns(x: &[i32], w: &[&str]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("X", ColumnType::Int, ints(x.iter().copied())),
        ("W", ColumnType::String, strings(w.iter().copied())),
    ]
}

fn set_row(x: i32, w: &str) -> Vec<ArrayRef> {
    vec![ints([x]), strings([w])]
}

/// The `K` of the result's rows, in order.
fn ks(table: &dyn Table) -> Vec<String> {
    cells(table, 0)
}

/// How a rig makes its result from the left table and the set table.
type Build = fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Result<Arc<dyn Table>, WhereInError>;

fn included(left: &Arc<dyn Table>, set: &Arc<dyn Table>) -> Result<Arc<dyn Table>, WhereInError> {
    where_in(left, set, &on(&["G=X"]))
}

fn excluded(left: &Arc<dyn Table>, set: &Arc<dyn Table>) -> Result<Arc<dyn Table>, WhereInError> {
    where_not_in(left, set, &on(&["G=X"]))
}

/// The filter on the set table itself, without the `select_distinct` in between.
fn included_raw(
    left: &Arc<dyn Table>,
    set: &Arc<dyn Table>,
) -> Result<Arc<dyn Table>, WhereInError> {
    dynamic_where(left, set, &on(&["G=X"]), true)
}

fn excluded_raw(
    left: &Arc<dyn Table>,
    set: &Arc<dyn Table>,
) -> Result<Arc<dyn Table>, WhereInError> {
    dynamic_where(left, set, &on(&["G=X"]), false)
}

/// A scripted left table, a scripted set table and a result, with every update of the
/// result recorded, and checked against the static path on copies of the two tables.
struct Rig {
    graph: Arc<dyn UpdateGraph>,
    left: Arc<ScriptedTable>,
    set: Arc<ScriptedTable>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Rig {
    /// The left rows `(K, G)` at the row keys `10 * K`, the set rows `X` at the row keys
    /// `0, 1, ...`.
    fn new(left: &[(i32, i32)], set: &[i32], build: Build) -> Self {
        let graph = graph();
        let k: Vec<i32> = left.iter().map(|(k, _)| *k).collect();
        let g: Vec<i32> = left.iter().map(|(_, g)| *g).collect();
        let s = vec!["s"; left.len()];
        let w = vec!["w"; set.len()];
        let left = scripted(
            &graph,
            k.iter().map(|k| RowKey::from(*k) * 10),
            &left_columns(&k, &g, &s),
        );
        let set = scripted(&graph, 0..set.len() as RowKey, &set_columns(set, &w));
        let left_table: Arc<dyn Table> = left.table();
        let set_table: Arc<dyn Table> = set.table();
        let result = build(&left_table, &set_table).unwrap();
        let recorder = Recorder::attach(result.clone());
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            build(&frozen(&*left_table), &frozen(&*set_table)).unwrap()
        });
        checker.assert_consistent();
        Self {
            graph,
            left,
            set,
            result,
            recorder,
            checker,
        }
    }

    /// Applies the changes in one cycle and returns the result's update, if it delivered
    /// one.
    fn step(&self, left: Option<TableChange>, set: Option<TableChange>) -> Option<TableUpdate> {
        let before = self.recorder.events().len();
        if let Some(change) = left {
            self.left.push(change);
        }
        if let Some(change) = set {
            self.set.push(change);
        }
        self.graph.run_cycle();
        self.checker.assert_consistent();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one update in a cycle");
        events.pop().map(|event| event.unwrap())
    }

    /// As [`Rig::step`], for a change that must show: the row keys the result added,
    /// removed and modified.
    fn changed(
        &self,
        left: Option<TableChange>,
        set: Option<TableChange>,
    ) -> (Vec<RowKey>, Vec<RowKey>, Vec<RowKey>) {
        let update = self.step(left, set).expect("an update");
        (
            keys(&update.added),
            keys(&update.removed),
            keys(&update.modified),
        )
    }

    fn ks(&self) -> Vec<String> {
        ks(&*self.result)
    }
}

const NONE: Vec<RowKey> = Vec::new();

// ---------------------------------------------------------------------------------------
// static tables

#[test]
fn the_rows_with_a_key_of_the_set_stay_in_the_left_tables_order() {
    let left = static_table(&left_columns(
        &[1, 2, 3, 4, 5],
        &[7, 8, 7, 9, 6],
        &["a", "b", "c", "d", "e"],
    ));
    let set = static_table(&set_columns(&[9, 7, 7, 5], &["p", "q", "r", "s"]));
    let result = where_in(&left, &set, &on(&["G=X"])).unwrap();
    assert_eq!(ks(&*result), ["1", "3", "4"]);
    // The rows keep their row keys and all of their columns.
    assert_eq!(keys(&result.row_set()), [0, 2, 3]);
    assert_eq!(result.definition(), left.definition());
    assert_eq!(cells(&*result, 2), ["a", "c", "d"]);
    let other = where_not_in(&left, &set, &on(&["G=X"])).unwrap();
    assert_eq!(ks(&*other), ["2", "5"]);
    assert_eq!(keys(&other.row_set()), [1, 4]);

    // Of two static tables the result is static and never flat; to Java a static table
    // is add-only and append-only, and the result takes that over.
    for table in [&result, &other] {
        assert!(!table.is_refreshing() && !table.is_flat());
        let attributes = table.attributes();
        assert!(attributes.add_only && attributes.append_only && !attributes.blink);
    }
}

#[test]
fn several_match_columns_are_one_key() {
    let left = static_table(&left_columns(
        &[1, 2, 3, 4],
        &[7, 7, 8, 8],
        &["a", "b", "a", "b"],
    ));
    let set = static_table(&set_columns(&[7, 8, 9], &["a", "b", "a"]));
    let both = on(&["G=X", "S=W"]);
    assert_eq!(ks(&*where_in(&left, &set, &both).unwrap()), ["1", "4"]);
    assert_eq!(ks(&*where_not_in(&left, &set, &both).unwrap()), ["2", "3"]);
    // The order of the pairs does not matter, and a set column may be matched twice.
    let swapped = on(&["S=W", "G=X"]);
    assert_eq!(ks(&*where_in(&left, &set, &swapped).unwrap()), ["1", "4"]);
    let left = static_table(&[
        ("K", ColumnType::Int, ints([1, 2, 3])),
        ("A", ColumnType::String, strings(["a", "a", "b"])),
        ("B", ColumnType::String, strings(["a", "b", "b"])),
    ]);
    let twice = on(&["A=W", "B=W"]);
    assert_eq!(ks(&*where_in(&left, &set, &twice).unwrap()), ["1", "3"]);
}

#[test]
fn null_nan_and_the_two_zeros_are_one_key_each() {
    let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
    let doubles = |values: Vec<Option<f64>>| -> ArrayRef { Arc::new(Float64Array::from(values)) };
    let left = static_table(&[
        ("K", ColumnType::Int, ints([1, 2, 3, 4, 5, 6])),
        (
            "D",
            ColumnType::Double,
            doubles(vec![
                Some(0.0),
                Some(-0.0),
                Some(f64::NAN),
                Some(other_nan),
                None,
                Some(1.5),
            ]),
        ),
    ]);
    let set_of =
        |values: Vec<Option<f64>>| static_table(&[("D", ColumnType::Double, doubles(values))]);
    let found = |set: Vec<Option<f64>>| ks(&*where_in(&left, &set_of(set), &on(&["D"])).unwrap());
    assert_eq!(found(vec![Some(-0.0)]), ["1", "2"]);
    assert_eq!(found(vec![Some(0.0)]), ["1", "2"]);
    assert_eq!(found(vec![Some(f64::NAN)]), ["3", "4"]);
    assert_eq!(found(vec![Some(other_nan)]), ["3", "4"]);
    assert_eq!(found(vec![None]), ["5"]);
    assert_eq!(
        found(vec![Some(1.5), None, Some(f64::NAN)]),
        ["3", "4", "5", "6"]
    );
    let others = where_not_in(&left, &set_of(vec![None, Some(0.0)]), &on(&["D"])).unwrap();
    assert_eq!(ks(&*others), ["3", "4", "6"]);

    // In a key of several columns too.
    let left = static_table(&[
        ("K", ColumnType::Int, ints([1, 2, 3])),
        ("G", ColumnType::Int, ints([7, 7, 8])),
        (
            "F",
            ColumnType::Float,
            Arc::new(Float32Array::from(vec![Some(-0.0), Some(f32::NAN), None])),
        ),
    ]);
    let set = static_table(&[
        ("G", ColumnType::Int, ints([7, 7, 8, 8])),
        (
            "F",
            ColumnType::Float,
            Arc::new(Float32Array::from(vec![
                Some(0.0),
                Some(f32::NAN),
                Some(0.0),
                Some(1.0),
            ])),
        ),
    ]);
    let result = where_in(&left, &set, &on(&["G", "F"])).unwrap();
    assert_eq!(ks(&*result), ["1", "2"]);
}

#[test]
fn a_boolean_matches_a_byte_and_an_instant_a_long() {
    let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
    ]));
    let instants = TimestampNanosecondArray::from(vec![Some(5), None, Some(-1), Some(7)]);
    let instants: ArrayRef = Arc::new(instants.with_timezone("UTC"));
    let left = static_table(&[
        ("K", ColumnType::Int, ints([1, 2, 3, 4])),
        ("B", ColumnType::Boolean, booleans.clone()),
        ("T", ColumnType::Instant, instants.clone()),
    ]);
    let bytes = |values: Vec<Option<i8>>| -> ArrayRef { Arc::new(Int8Array::from(values)) };
    let longs = |values: Vec<Option<i64>>| -> ArrayRef { Arc::new(Int64Array::from(values)) };
    let set = |y: Vec<Option<i8>>, l: Vec<Option<i64>>| {
        static_table(&[
            ("Y", ColumnType::Byte, bytes(y)),
            ("L", ColumnType::Long, longs(l)),
        ])
    };
    // True is 1, false is 0, and a null Boolean is a null byte and no other byte.
    let by_byte = |y: Vec<Option<i8>>| {
        let l = vec![Some(0); y.len()];
        let set = set(y, l);
        ks(&*where_in(&left, &set, &on(&["B=Y"])).unwrap())
    };
    assert_eq!(by_byte(vec![Some(1)]), ["1", "4"]);
    assert_eq!(by_byte(vec![Some(0)]), ["2"]);
    assert_eq!(by_byte(vec![None]), ["3"]);
    assert_eq!(by_byte(vec![Some(-1), Some(2), Some(i8::MIN + 1)]), [""; 0]);
    let by_long = |l: Vec<Option<i64>>| {
        let y = vec![Some(0); l.len()];
        let set = set(y, l);
        ks(&*where_in(&left, &set, &on(&["T=L"])).unwrap())
    };
    assert_eq!(by_long(vec![Some(7), None, Some(6)]), ["2", "4"]);

    // And the other way round.
    let left = set(vec![Some(1), Some(0), None, Some(-1)], vec![Some(5); 4]);
    let set = static_table(&[
        ("B", ColumnType::Boolean, booleans),
        ("T", ColumnType::Instant, instants),
    ]);
    let result = where_in(&left, &set, &on(&["Y=B"])).unwrap();
    assert_eq!(cells(&*result, 0), ["1", "0", "null"]);
    let result = where_in(&left, &set, &on(&["L=T"])).unwrap();
    assert_eq!(result.row_set().size(), 4);
}

#[test]
fn without_match_columns_the_set_decides_by_having_rows() {
    let left = static_table(&left_columns(&[1, 2], &[7, 8], &["a", "b"]));
    let some = static_table(&set_columns(&[1], &["a"]));
    let none = static_table(&set_columns(&[], &[]));
    assert_eq!(ks(&*where_in(&left, &some, &[]).unwrap()), ["1", "2"]);
    assert_eq!(ks(&*where_in(&left, &none, &[]).unwrap()), [""; 0]);
    assert_eq!(ks(&*where_not_in(&left, &some, &[]).unwrap()), [""; 0]);
    assert_eq!(ks(&*where_not_in(&left, &none, &[]).unwrap()), ["1", "2"]);

    // While the set table ticks as well.
    let rig = Rig::new(&[(1, 7), (2, 8)], &[], |left, set| where_in(left, set, &[]));
    assert_eq!(rig.ks(), [""; 0]);
    let add = |key: RowKey| TableChange::new().add([key], set_row(5, "w"));
    assert_eq!(rig.changed(None, Some(add(0))), (vec![10, 20], NONE, NONE));
    assert_eq!(rig.step(None, Some(add(1))), None);
    assert_eq!(rig.step(None, Some(TableChange::new().remove([0]))), None);
    let remove = TableChange::new().remove([1]);
    assert_eq!(rig.changed(None, Some(remove)), (NONE, vec![10, 20], NONE));
}

#[test]
fn columns_a_table_does_not_have_and_pairs_of_other_types_are_refused() {
    let left = static_table(&left_columns(&[1], &[7], &["a"]));
    let set = static_table(&set_columns(&[7], &["a"]));
    let refused = |pairs: &[&str]| where_in(&left, &set, &on(pairs)).err().unwrap();
    let missing = |names: &[&str]| {
        WhereInError::NoSuchColumn(names.iter().map(|name| name.to_string()).collect())
    };
    assert_eq!(refused(&["Nope=X"]), missing(&["Nope"]));
    assert_eq!(refused(&["G=Nope"]), missing(&["Nope"]));
    // The set table's columns are looked up first.
    assert_eq!(refused(&["Nope=Nada", "No=W"]), missing(&["Nada"]));
    assert_eq!(refused(&["Nope=X", "No=W"]), missing(&["Nope", "No"]));
    // Columns come before types.
    assert_eq!(refused(&["S=X", "Nope=X"]), missing(&["Nope"]));
    assert_eq!(
        refused(&["G=X", "S=X"]),
        WhereInError::MismatchedTypes {
            left: "S".to_string(),
            left_type: ColumnType::String,
            right: "X".to_string(),
            right_type: ColumnType::Int,
        }
    );
    let inverted = where_not_in(&left, &set, &on(&["G=W"])).err().unwrap();
    assert!(matches!(inverted, WhereInError::MismatchedTypes { .. }));

    // Against a ticking set table the same.
    let graph = graph();
    let ticking: Arc<dyn Table> = scripted(&graph, [0], &set_columns(&[7], &["a"])).table();
    let refused = |pairs: &[&str]| where_in(&left, &ticking, &on(pairs)).err().unwrap();
    assert_eq!(refused(&["G=Nope"]), missing(&["Nope"]));
    assert_eq!(refused(&["Nope=Nada"]), missing(&["Nada"]));
    assert_eq!(refused(&["Nope=X"]), missing(&["Nope"]));
    assert!(matches!(
        refused(&["S=X"]),
        WhereInError::MismatchedTypes { .. }
    ));
}

#[test]
fn every_pair_of_types_but_the_reinterpreted_ones_is_refused() {
    let types = [
        ColumnType::Boolean,
        ColumnType::Byte,
        ColumnType::Char,
        ColumnType::Short,
        ColumnType::Int,
        ColumnType::Long,
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::String,
        ColumnType::Instant,
    ];
    let mut random = RandomUpdates::new(1);
    for left_type in &types {
        for set_type in &types {
            let left = static_table(&[("A", left_type.clone(), random.values(left_type, 3))]);
            let set = static_table(&[("B", set_type.clone(), random.values(set_type, 3))]);
            let allowed = left_type == set_type
                || matches!(
                    (left_type, set_type),
                    (ColumnType::Boolean, ColumnType::Byte)
                        | (ColumnType::Byte, ColumnType::Boolean)
                        | (ColumnType::Instant, ColumnType::Long)
                        | (ColumnType::Long, ColumnType::Instant)
                );
            let result = where_in(&left, &set, &on(&["A=B"]));
            match result {
                Ok(_) => assert!(allowed, "{left_type:?} with {set_type:?} was accepted"),
                Err(error) => {
                    assert!(!allowed, "{left_type:?} with {set_type:?}: {error}");
                    assert!(matches!(error, WhereInError::MismatchedTypes { .. }));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// the left table ticks, the set of keys does not change

#[test]
fn rows_of_the_left_table_come_go_and_move_as_in_a_where() {
    for build in [included as Build, included_raw] {
        let rig = Rig::new(&[(1, 7), (2, 8), (3, 7)], &[7, 9], build);
        assert_eq!(rig.ks(), ["1", "3"]);
        assert!(rig.result.is_refreshing() && !rig.result.is_flat());

        // Added rows are filtered.
        let add = TableChange::new().add(
            [40, 50],
            vec![ints([4, 5]), ints([9, 8]), strings(["s", "s"])],
        );
        assert_eq!(rig.changed(Some(add), None), (vec![40], NONE, NONE));
        // An added row that does not pass shows nothing.
        let add = TableChange::new().add([60], left_row(6, 1, "s"));
        assert_eq!(rig.step(Some(add), None), None);
        // Removed rows leave if they were there.
        let remove = TableChange::new().remove([10, 20]);
        assert_eq!(rig.changed(Some(remove), None), (NONE, vec![10], NONE));
        // A row whose key changes comes in, goes out, or stays and is modified.
        let modify = TableChange::new().modify([30, 40, 50], [(1, ints([8, 7, 9]))]);
        assert_eq!(
            rig.changed(Some(modify), None),
            (vec![50], vec![30], vec![40])
        );
        // A change of another column is passed on for the rows of the result, unfiltered.
        let modify = TableChange::new().modify([30, 40], [(2, strings(["x", "y"]))]);
        let update = rig.step(Some(modify), None).unwrap();
        assert_eq!(keys(&update.modified), [40]);
        assert_eq!(update.modified_columns, ModifiedColumnSet::of([2]));
        assert!(update.added.is_empty() && update.removed.is_empty());
        // Shifts are the left table's.
        let shift = TableChange::new().shift(40, 60, 5);
        let update = rig.step(Some(shift.clone()), None).unwrap();
        assert_eq!(update.shifted, shift.update.shifted);
        assert_eq!(keys(&rig.result.row_set()), [45, 55]);
        // Everything at once: 45 is removed, 55 moves to 56 and loses its key, 30 moves
        // to 31 and gets a key, 70 is added.
        let all = TableChange::new()
            .remove([45])
            .shift(30, 65, 1)
            .add([70], left_row(7, 7, "s"))
            .modify([31, 56], [(1, ints([9, 1]))]);
        assert_eq!(
            rig.changed(Some(all), None),
            (vec![31, 70], vec![45, 55], NONE)
        );
        assert_eq!(rig.ks(), ["3", "7"]);
    }
}

#[test]
fn the_other_rows_are_in_a_where_not_in() {
    for build in [excluded as Build, excluded_raw] {
        let rig = Rig::new(&[(1, 7), (2, 8), (3, 7)], &[7, 9], build);
        assert_eq!(rig.ks(), ["2"]);
        let add = TableChange::new().add(
            [40, 50],
            vec![ints([4, 5]), ints([9, 8]), strings(["s", "s"])],
        );
        assert_eq!(rig.changed(Some(add), None), (vec![50], NONE, NONE));
        let modify = TableChange::new().modify([20, 30, 50], [(1, ints([7, 8, 6]))]);
        assert_eq!(
            rig.changed(Some(modify), None),
            (vec![30], vec![20], vec![50])
        );
    }
}

#[test]
fn a_static_set_keeps_the_left_tables_attributes() {
    let graph = graph();
    let set = static_table(&set_columns(&[7], &["a"]));
    let ticking_set: Arc<dyn Table> = scripted(&graph, [0], &set_columns(&[7], &["a"])).table();
    let types = [
        ("K", ColumnType::Int),
        ("G", ColumnType::Int),
        ("S", ColumnType::String),
    ];
    let (blink, _) = blink_table(&graph, &types);
    for build in [included as Build, excluded] {
        let result = build(&blink, &set).unwrap();
        let attributes = result.attributes();
        assert!(attributes.blink && !attributes.add_only && !attributes.append_only);
        // A set that ticks takes add-only and append-only away, also from a static left
        // table, whose result then refreshes.
        let fixed = static_table(&left_columns(&[1], &[7], &["a"]));
        let result = build(&fixed, &ticking_set).unwrap();
        let attributes = result.attributes();
        assert!(result.is_refreshing() && !result.is_flat());
        assert!(!attributes.blink && !attributes.add_only && !attributes.append_only);
    }
    let input = crate::table::append_only_input_table(graph.clone(), definition(&types), None);
    let input: Arc<dyn Table> = input.unwrap();
    let result = where_in(&input, &set, &on(&["G=X"])).unwrap();
    let attributes = result.attributes();
    assert!(attributes.add_only && attributes.append_only);
    // The result is written through the left table's input table.
    assert!(result.input_table().is_some());
    let result = where_in(&input, &ticking_set, &on(&["G=X"])).unwrap();
    let attributes = result.attributes();
    assert!(!attributes.add_only && !attributes.append_only);
    assert!(result.input_table().is_some());
}

// ---------------------------------------------------------------------------------------
// the set table ticks

#[test]
fn a_key_that_arrives_brings_its_rows_and_a_key_that_leaves_takes_them() {
    let rig = Rig::new(&[(1, 7), (2, 8), (3, 7), (4, 9)], &[7], included);
    assert_eq!(rig.ks(), ["1", "3"]);
    let add = |key: RowKey, x: i32| TableChange::new().add([key], set_row(x, "w"));
    // A new key: the rows that were out are filtered.
    assert_eq!(rig.changed(None, Some(add(1, 9))), (vec![40], NONE, NONE));
    // A second row with a key of the set changes nothing, nor does a key no row has.
    assert_eq!(rig.step(None, Some(add(2, 7))), None);
    assert_eq!(rig.step(None, Some(add(3, 1))), None);
    // One of the two rows of a key goes: nothing. The last one goes: its rows leave.
    assert_eq!(rig.step(None, Some(TableChange::new().remove([0]))), None);
    let remove = TableChange::new().remove([2]);
    assert_eq!(rig.changed(None, Some(remove)), (NONE, vec![10, 30], NONE));
    // A key replaced by another in one cycle: everything is filtered.
    let replace = TableChange::new().remove([1]).add([4], set_row(8, "w"));
    assert_eq!(rig.changed(None, Some(replace)), (vec![20], vec![40], NONE));
    // A row of the set that changes its key does the same.
    let modify = TableChange::new().modify([4], [(0, ints([7]))]);
    assert_eq!(
        rig.changed(None, Some(modify)),
        (vec![10, 30], vec![20], NONE)
    );
    // Changes of the set table that leave its keys alone show nothing.
    let modify = TableChange::new().modify([3, 4], [(1, strings(["x", "y"]))]);
    assert_eq!(rig.step(None, Some(modify)), None);
    assert_eq!(
        rig.step(None, Some(TableChange::new().shift(3, 4, 10))),
        None
    );
    assert_eq!(rig.ks(), ["1", "3"]);
}

#[test]
fn a_where_not_in_loses_rows_to_a_key_that_arrives() {
    let rig = Rig::new(&[(1, 7), (2, 8), (3, 7), (4, 9)], &[7], excluded);
    assert_eq!(rig.ks(), ["2", "4"]);
    let add = TableChange::new().add([1], set_row(9, "w"));
    assert_eq!(rig.changed(None, Some(add)), (NONE, vec![40], NONE));
    let remove = TableChange::new().remove([0]);
    assert_eq!(rig.changed(None, Some(remove)), (vec![10, 30], NONE, NONE));
    let replace = TableChange::new().remove([1]).add([2], set_row(8, "w"));
    assert_eq!(rig.changed(None, Some(replace)), (vec![40], vec![20], NONE));
    assert_eq!(rig.ks(), ["1", "3", "4"]);
}

/// Without the `select_distinct` in between, the rows of the set table are the keys, and
/// a row that is modified can change its key.
#[test]
fn a_set_row_that_changes_its_key_swaps_the_keys() {
    let rig = Rig::new(&[(1, 7), (2, 8), (3, 7), (4, 9)], &[7, 9], included_raw);
    assert_eq!(rig.ks(), ["1", "3", "4"]);
    // The key 9 becomes the key 8: everything is filtered again.
    let modify = TableChange::new().modify([1], [(0, ints([8]))]);
    assert_eq!(rig.changed(None, Some(modify)), (vec![20], vec![40], NONE));
    // Rows written with the keys they had, and a change of the other column: nothing.
    let modify = TableChange::new().modify([0, 1], [(0, ints([7, 8]))]);
    assert_eq!(rig.step(None, Some(modify)), None);
    let modify = TableChange::new().modify([0], [(1, strings(["x"]))]);
    assert_eq!(rig.step(None, Some(modify)), None);
    // A row that moves and changes its key: the old key is where the row was.
    let moved = TableChange::new()
        .shift(1, 1, 4)
        .modify([5], [(0, ints([9]))]);
    assert_eq!(rig.changed(None, Some(moved)), (vec![40], vec![20], NONE));
    assert_eq!(rig.ks(), ["1", "3", "4"]);

    let rig = Rig::new(&[(1, 7), (2, 8), (3, 7), (4, 9)], &[7, 9], excluded_raw);
    assert_eq!(rig.ks(), ["2"]);
    let modify = TableChange::new().modify([1], [(0, ints([8]))]);
    assert_eq!(rig.changed(None, Some(modify)), (vec![40], vec![20], NONE));
}

/// The left table's change and a change of the set of keys in one cycle, for each of the
/// three ways of filtering again and both operations. The left table removes a row of the
/// result and one outside it, shifts, adds a row that passes and one that does not, and
/// modifies rows that stay in, stay out, come in and go out.
#[test]
fn both_tables_change_in_one_cycle() {
    // Left rows by K: 1 and 2 have the key 7, 3 and 4 the key 8, 5 and 6 the key 9,
    // 7 the key 5 and 8 the key 6.
    let left = [
        (1, 7),
        (2, 7),
        (3, 8),
        (4, 8),
        (5, 9),
        (6, 9),
        (7, 5),
        (8, 6),
    ];
    let left_change = || {
        TableChange::new()
            .remove([10, 30])
            .shift(40, 80, 3)
            .add(
                [90, 100],
                vec![ints([9, 10]), ints([7, 8]), strings(["s", "s"])],
            )
            // 2 keeps the key 7, 4 goes from 8 to 7, 5 from 9 to 8, 7 keeps 5.
            .modify([20, 43, 53, 73], [(1, ints([7, 7, 8, 5]))])
    };
    // What the set does, the K of the result afterwards for where_in, and the rows it
    // must report as added, removed and modified.
    type Case = (TableChange, &'static [&'static str], [Vec<RowKey>; 3]);
    let cases = |set: fn() -> TableChange,
                 ks: &'static [&'static str],
                 rows: [Vec<RowKey>; 3]|
     -> Case { (set(), ks, rows) };
    let included_cases = [
        // The set of keys {7, 9} gets 6: the unmatched rows are filtered.
        cases(
            || TableChange::new().add([2], set_row(6, "w")),
            &["2", "4", "6", "8", "9"],
            [vec![43, 83, 90], vec![10, 50], vec![20]],
        ),
        // It loses 9: the matched rows are filtered.
        cases(
            || TableChange::new().remove([1]),
            &["2", "4", "9"],
            [vec![43, 90], vec![10, 50, 60], vec![20]],
        ),
        // It gets 6 and loses 9: all rows are filtered.
        cases(
            || TableChange::new().remove([1]).add([2], set_row(6, "w")),
            &["2", "4", "8", "9"],
            [vec![43, 83, 90], vec![10, 50, 60], vec![20]],
        ),
    ];
    for (set_change, ks, [added, removed, modified]) in included_cases {
        let rig = Rig::new(&left, &[7, 9], included);
        assert_eq!(rig.ks(), ["1", "2", "5", "6"]);
        let update = rig.step(Some(left_change()), Some(set_change)).unwrap();
        assert_eq!(rig.ks(), ks);
        assert_eq!(keys(&update.added), added, "added, {ks:?}");
        assert_eq!(keys(&update.removed), removed, "removed, {ks:?}");
        assert_eq!(keys(&update.modified), modified, "modified, {ks:?}");
        assert_eq!(update.shifted, left_change().update.shifted);
        assert_eq!(update.modified_columns, ModifiedColumnSet::of([1]));
    }

    let excluded_cases = [
        // {7, 9} gets 6: the rows of the result are filtered.
        cases(
            || TableChange::new().add([2], set_row(6, "w")),
            &["5", "7", "10"],
            [vec![53, 100], vec![30, 40, 80], vec![73]],
        ),
        // It loses 9: the rows outside the result are filtered.
        cases(
            || TableChange::new().remove([1]),
            &["5", "6", "7", "8", "10"],
            [vec![53, 63, 100], vec![30, 40], vec![73]],
        ),
        cases(
            || TableChange::new().remove([1]).add([2], set_row(6, "w")),
            &["5", "6", "7", "10"],
            [vec![53, 63, 100], vec![30, 40, 80], vec![73]],
        ),
    ];
    for (set_change, ks, [added, removed, modified]) in excluded_cases {
        let rig = Rig::new(&left, &[7, 9], excluded);
        assert_eq!(rig.ks(), ["3", "4", "7", "8"]);
        let update = rig.step(Some(left_change()), Some(set_change)).unwrap();
        assert_eq!(rig.ks(), ks);
        assert_eq!(keys(&update.added), added, "added, {ks:?}");
        assert_eq!(keys(&update.removed), removed, "removed, {ks:?}");
        assert_eq!(keys(&update.modified), modified, "modified, {ks:?}");
        assert_eq!(update.shifted, left_change().update.shifted);
    }
}

#[test]
fn rows_modified_in_another_column_are_reported_when_the_keys_change_too() {
    let rig = Rig::new(&[(1, 7), (2, 8), (3, 9)], &[7], included);
    let modify = TableChange::new().modify([10, 20, 30], [(2, strings(["x", "y", "z"]))]);
    let add = TableChange::new().add([1], set_row(8, "w"));
    // 1 stays and is modified, 2 comes in and is added, 3 stays out.
    let update = rig.step(Some(modify), Some(add)).unwrap();
    assert_eq!(keys(&update.added), [20]);
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([2]));
    assert!(update.removed.is_empty());
}

#[test]
fn a_set_table_with_a_key_twice_fails_the_result_once_it_changes() {
    // At the start a key may be there twice: Java adds the first keys unchecked.
    let rig = Rig::new(&[(1, 7), (2, 8)], &[7, 7], included_raw);
    assert_eq!(rig.ks(), ["1"]);
    rig.set.push(TableChange::new().add([2], set_row(7, "w")));
    rig.graph.run_cycle();
    let failure: TableFailure = rig.recorder.events().pop().unwrap().unwrap_err();
    assert!(failure.message.contains("key already in set"), "{failure}");
    assert!(rig.result.is_failed());

    // Removing both rows of the key takes it out twice.
    let rig = Rig::new(&[(1, 7), (2, 8)], &[7, 7], included_raw);
    rig.set.push(TableChange::new().remove([0, 1]));
    rig.graph.run_cycle();
    let failure: TableFailure = rig.recorder.events().pop().unwrap().unwrap_err();
    assert!(
        failure.message.contains("key not found in set"),
        "{failure}"
    );

    // Through `where_in` the set is distinct, and neither happens.
    let rig = Rig::new(&[(1, 7), (2, 8)], &[7, 7], included);
    let add = TableChange::new().add([2], set_row(7, "w"));
    assert_eq!(rig.step(None, Some(add)), None);
    let remove = TableChange::new().remove([0, 1, 2]);
    assert_eq!(rig.changed(None, Some(remove)), (NONE, vec![10], NONE));
}

#[test]
fn a_failed_table_fails_the_result() {
    for fail_left in [true, false] {
        let rig = Rig::new(&[(1, 7)], &[7], included);
        let source = if fail_left { &rig.left } else { &rig.set };
        source.push_failure("broken");
        rig.graph.run_cycle();
        let failure: TableFailure = rig.recorder.events().pop().unwrap().unwrap_err();
        assert!(failure.message.contains("broken"), "{failure}");
        assert!(rig.result.is_failed());
        let left: Arc<dyn Table> = rig.left.table();
        let set: Arc<dyn Table> = rig.set.table();
        let refused = where_in(&left, &set, &on(&["G=X"]));
        assert!(matches!(refused, Err(WhereInError::Table(_))));
    }
}

#[test]
fn tables_of_two_update_graphs_are_refused() {
    let left: Arc<dyn Table> = scripted(&graph(), [0], &left_columns(&[1], &[7], &["a"])).table();
    let set: Arc<dyn Table> = scripted(&graph(), [0], &set_columns(&[7], &["a"])).table();
    let refused = where_in(&left, &set, &on(&["G=X"])).err().unwrap();
    assert_eq!(refused, WhereInError::UpdateGraphConflict);
}

// ---------------------------------------------------------------------------------------
// random updates against the static path and a model

/// The key of each row of `table` over `columns`, as text, with one text for both zeros.
fn key_texts(table: &dyn Table, columns: &[usize]) -> Vec<String> {
    let cells: Vec<Vec<String>> = columns.iter().map(|index| cells(table, *index)).collect();
    (0..table.row_set().size() as usize)
        .map(|row| {
            let key: Vec<&str> = cells
                .iter()
                .map(|column| match column[row].as_str() {
                    "-0.0" => "0.0",
                    other => other,
                })
                .collect();
            key.join("|")
        })
        .collect()
}

/// The row keys of `left` whose key is (`inclusion`) or is not a key of `set`, found by
/// other means than the operation's.
fn model(left: &dyn Table, set: &dyn Table, columns: &[usize], inclusion: bool) -> Vec<RowKey> {
    let wanted: BTreeSet<String> = key_texts(set, columns).into_iter().collect();
    let rows = left.row_set();
    rows.iter()
        .zip(key_texts(left, columns))
        .filter(|(_, key)| wanted.contains(key) == inclusion)
        .map(|(row, _)| row)
        .collect()
}

#[test]
fn random_updates_match_the_static_path_and_the_model() {
    // Both tables have these columns, so that keys of every kind meet.
    let types = [
        ("G", ColumnType::Int),
        ("S", ColumnType::String),
        ("D", ColumnType::Double),
        ("Z", ColumnType::Boolean),
    ];
    let key_sets: [&[&str]; 5] = [&["G"], &["S", "Z"], &["D"], &["Z", "G", "S"], &[]];
    let mut updates = 0;
    for seed in test_seeds(0..90) {
        let graph = graph();
        let mut random = RandomUpdates::new(seed);
        // Which tables tick: both, the left one, the set.
        let (left_ticks, set_ticks) =
            [(true, true), (true, false), (false, true)][seed as usize % 3];
        let mut table = |size: u64, ticks: bool| -> (Option<Arc<ScriptedTable>>, Arc<dyn Table>) {
            let values: Vec<ArrayRef> = types
                .iter()
                .map(|(_, column_type)| random.values(column_type, size))
                .collect();
            let keys = RowSet::from_keys((0..size as RowKey).map(|k| 3 * k + 1));
            if ticks {
                let script = ScriptedTable::new(graph.clone(), definition(&types), keys, values);
                let table: Arc<dyn Table> = script.table();
                (Some(script), table)
            } else {
                let columns: Vec<(&str, ColumnType, ArrayRef)> = types
                    .iter()
                    .zip(values)
                    .map(|((name, column_type), values)| (*name, column_type.clone(), values))
                    .collect();
                (None, static_table(&columns))
            }
        };
        let (left_script, left) = table(if seed % 7 == 0 { 0 } else { 12 }, left_ticks);
        let (set_script, set) = table(if seed % 5 == 0 { 0 } else { 4 }, set_ticks);

        let mut cases = Vec::new();
        for by in key_sets {
            for inclusion in [true, false] {
                let pairs = on(by);
                let build = move |left: &Arc<dyn Table>, set: &Arc<dyn Table>| {
                    if inclusion {
                        where_in(left, set, &pairs).unwrap()
                    } else {
                        where_not_in(left, set, &pairs).unwrap()
                    }
                };
                let result = build(&left, &set);
                let recompute = {
                    let (left, set) = (left.clone(), set.clone());
                    move || build(&frozen(&*left), &frozen(&*set))
                };
                let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
                let columns: Vec<usize> = by
                    .iter()
                    .map(|name| left.definition().index_of(name).unwrap())
                    .collect();
                cases.push((result, checker, columns, inclusion));
            }
        }
        let mut left_chooser = RandomUpdates::new(seed ^ 0x1ef7);
        let mut set_chooser = RandomUpdates::new(seed ^ 0x5e7);
        for step in 0..40 {
            // Each table changes in two cycles of three, so that either changes alone
            // and both change together.
            if let Some(script) = &left_script {
                if step % 3 != 1 {
                    script.push(left_chooser.next_change(&*left, 6));
                }
            }
            if let Some(script) = &set_script {
                if step % 3 != 2 {
                    script.push(set_chooser.next_change(&*set, 2));
                }
            }
            graph.run_cycle();
            for (result, checker, columns, inclusion) in &cases {
                let place = format!("seed {seed}, step {step}, by {columns:?}, in {inclusion}");
                if let Err(e) = checker.verify() {
                    panic!("{place}: {e}");
                }
                let expected = if columns.is_empty() {
                    let all = set.row_set().is_empty() != *inclusion;
                    let rows = if all {
                        left.row_set()
                    } else {
                        Arc::new(RowSet::empty())
                    };
                    keys(&rows)
                } else {
                    model(&*left, &*set, columns, *inclusion)
                };
                assert_eq!(keys(&result.row_set()), expected, "{place}");
            }
        }
        updates += cases.iter().map(|case| case.1.updates_seen()).sum::<u64>();
    }
    assert!(updates > at_least_for(5000, 0..90), "{updates} updates");
}
