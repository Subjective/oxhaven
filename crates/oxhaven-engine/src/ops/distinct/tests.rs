use super::{select_distinct, DistinctError};
use crate::ops::sort::testing::{
    blink_table, cells, graph, keys, keys_of, scripted, static_table, Columns,
};
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{
    at_least_for, definition, doubles, ints, list_of, strings, test_seeds, ConsistencyChecker,
    RandomUpdates, Recorder, ScriptedTable, TableChange,
};
use std::collections::BTreeSet;
use std::sync::Arc;

fn distinct(source: &Arc<dyn Table>, columns: &[&str]) -> Arc<dyn Table> {
    let columns: Vec<String> = columns.iter().map(|name| name.to_string()).collect();
    select_distinct(source, &columns).unwrap()
}

/// The rows of `table` in order, the cells of a row joined by `|`.
fn rows(table: &dyn Table) -> Vec<String> {
    let columns: Vec<Vec<String>> = (0..table.definition().columns().len())
        .map(|column| cells(table, column))
        .collect();
    (0..table.row_set().size() as usize)
        .map(|row| {
            let cells: Vec<&str> = columns.iter().map(|column| column[row].as_str()).collect();
            cells.join("|")
        })
        .collect()
}

/// `G`, an int, and `S`, a string.
fn columns(g: &[i32], s: &[&str]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("G", ColumnType::Int, ints(g.iter().copied())),
        ("S", ColumnType::String, strings(s.iter().copied())),
    ]
}

fn row(g: i32, s: &str) -> Vec<ArrayRef> {
    vec![ints([g]), strings([s])]
}

/// A scripted source and its `select_distinct`, with every update of the result recorded
/// and checked.
struct Rig {
    graph: Arc<dyn UpdateGraph>,
    source: Arc<ScriptedTable>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Rig {
    fn new(keys: &[RowKey], columns: Columns, by: &[&str]) -> Self {
        let graph = graph();
        let source = scripted(&graph, keys.iter().copied(), columns);
        let result = distinct(&(source.table() as Arc<dyn Table>), by);
        let recorder = Recorder::attach(result.clone());
        let checker = ConsistencyChecker::attach(result.clone());
        checker.assert_consistent();
        Self {
            graph,
            source,
            result,
            recorder,
            checker,
        }
    }

    /// Applies `change` in one cycle and returns the result's update, if it delivered one.
    fn step(&self, change: TableChange) -> Option<TableUpdate> {
        let before = self.recorder.events().len();
        self.source.push(change);
        self.graph.run_cycle();
        self.checker.assert_consistent();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one update in a cycle");
        events.pop().map(|event| event.unwrap())
    }

    /// As [`Rig::step`], for a change that must show: the keys the result added and
    /// removed.
    fn changed(&self, change: TableChange) -> (Vec<RowKey>, Vec<RowKey>) {
        let update = self.step(change).expect("an update");
        assert!(update.modified.is_empty() && update.shifted.is_empty());
        (keys(&update.added), keys(&update.removed))
    }
}

// ---------------------------------------------------------------------------------------
// static sources

#[test]
fn the_result_is_in_the_order_of_first_appearance() {
    let source = static_table(&columns(
        &[3, 1, 3, 2, 1, 3],
        &["a", "b", "a", "b", "c", "a"],
    ));
    let result = distinct(&source, &["G"]);
    assert_eq!(rows(&*result), ["3", "1", "2"]);
    assert_eq!(keys_of(&*result), [0, 1, 2]);
    assert!(!result.is_refreshing() && result.is_flat());
    // To Java a static table is add-only and append-only, and the result takes that over.
    let attributes = result.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);

    assert_eq!(rows(&*distinct(&source, &["S"])), ["a", "b", "c"]);
    assert_eq!(
        rows(&*distinct(&source, &["G", "S"])),
        ["3|a", "1|b", "2|b", "1|c"]
    );
    // The columns come in the order asked for.
    assert_eq!(
        rows(&*distinct(&source, &["S", "G"])),
        ["a|3", "b|1", "b|2", "c|1"]
    );
}

#[test]
fn a_column_named_twice_counts_once_at_its_first_place() {
    let source = static_table(&columns(&[3, 1, 3], &["a", "b", "a"]));
    let result = distinct(&source, &["G", "S", "G"]);
    let names: Vec<&str> = result
        .definition()
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["G", "S"]);
    assert_eq!(rows(&*result), ["3|a", "1|b"]);
}

#[test]
fn null_nan_and_the_two_zeros_are_one_key_each() {
    let nan = f64::NAN;
    let other_nan = f64::from_bits(nan.to_bits() ^ 1);
    assert!(other_nan.is_nan());
    let values: ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![
        Some(-0.0),
        None,
        Some(nan),
        Some(0.0),
        Some(other_nan),
        None,
        Some(1.0),
        Some(-0.0),
    ]));
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(0.0),
        None,
        Some(f32::NAN),
        Some(-0.0),
        Some(-f32::NAN),
        None,
        Some(1.0),
        Some(0.0),
    ]));
    let source = static_table(&[
        ("D", ColumnType::Double, values),
        ("F", ColumnType::Float, floats),
    ]);
    // The key shows the value of its first row: -0.0 for the doubles, 0.0 for the floats.
    assert_eq!(
        rows(&*distinct(&source, &["D"])),
        ["-0.0", "null", "NaN", "1.0"]
    );
    assert_eq!(
        rows(&*distinct(&source, &["F"])),
        ["0.0", "null", "NaN", "1.0"]
    );
    assert_eq!(rows(&*distinct(&source, &["D", "F"])).len(), 4);
}

#[test]
fn a_null_in_one_key_column_is_not_a_null_in_another() {
    let g: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![
        None,
        Some(1),
        None,
        Some(1),
    ]));
    let s: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
        Some("x"),
        None,
        None,
        Some(""),
    ]));
    let source = static_table(&[("G", ColumnType::Int, g), ("S", ColumnType::String, s)]);
    assert_eq!(
        rows(&*distinct(&source, &["G", "S"])),
        ["null|x", "1|null", "null|null", "1|"]
    );
}

#[test]
fn strings_that_run_into_each_other_are_different_keys() {
    // The character U+0001 is the byte that precedes a value in a key.
    let a = strings(["ab", "a", "", "ab", "a\u{1}b", "a"]);
    let b = strings(["c", "bc", "abc", "c", "c", "b\u{1}c"]);
    let source = static_table(&[("A", ColumnType::String, a), ("B", ColumnType::String, b)]);
    assert_eq!(keys_of(&*distinct(&source, &["A", "B"])), [0, 1, 2, 3, 4]);
}

#[test]
fn every_column_type_is_a_key() {
    let nanos = TimestampNanosecondArray::from(vec![Some(5), None, Some(5), Some(6), None]);
    let all: Vec<(&str, ColumnType, ArrayRef)> = vec![
        (
            "Z",
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(true),
                Some(false),
                None,
            ])),
        ),
        (
            "B",
            ColumnType::Byte,
            Arc::new(Int8Array::from(vec![Some(5), None, Some(5), Some(6), None])),
        ),
        (
            "C",
            ColumnType::Char,
            Arc::new(UInt16Array::from(vec![
                Some(5),
                None,
                Some(5),
                Some(6),
                None,
            ])),
        ),
        (
            "H",
            ColumnType::Short,
            Arc::new(Int16Array::from(vec![
                Some(5),
                None,
                Some(5),
                Some(6),
                None,
            ])),
        ),
        (
            "L",
            ColumnType::Long,
            Arc::new(Int64Array::from(vec![
                Some(5),
                None,
                Some(5),
                Some(6),
                None,
            ])),
        ),
        (
            "T",
            ColumnType::Instant,
            Arc::new(nanos.with_timezone("UTC")),
        ),
    ];
    let source = static_table(&all);
    for (name, column_type, _) in &all {
        let result = distinct(&source, &[name]);
        assert_eq!(keys_of(&*result), [0, 1, 2], "{name}");
        assert_eq!(result.definition().columns()[0].column_type, *column_type);
        // The text of an Instant in the zone "UTC" needs a feature of Arrow that is off.
        if *name == "T" {
            continue;
        }
        let shown = cells(&*result, 0);
        assert_eq!(shown[1], "null", "{name}");
        assert!(
            shown[0] != shown[2] && shown[2] != "null",
            "{name}: {shown:?}"
        );
    }
}

#[test]
fn without_columns_the_result_has_a_row_while_the_source_has_rows() {
    let source = static_table(&columns(&[3, 1], &["a", "b"]));
    let result = distinct(&source, &[]);
    assert!(result.definition().columns().is_empty());
    assert_eq!(keys_of(&*result), [0]);
    assert!(result.is_flat() && !result.is_refreshing());

    let empty = static_table(&columns(&[], &[]));
    assert!(keys_of(&*distinct(&empty, &[])).is_empty());
    assert!(keys_of(&*distinct(&empty, &["G"])).is_empty());
}

#[test]
fn columns_the_table_does_not_have_are_refused() {
    let source = static_table(&columns(&[3, 1], &["a", "b"]));
    let ask = |names: &[&str]| {
        let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
        select_distinct(&source, &names).map(|_| ())
    };
    let missing = |names: &[&str]| {
        Err(DistinctError::NoSuchColumn(
            names.iter().map(|name| name.to_string()).collect(),
        ))
    };
    assert_eq!(ask(&["Nope"]), missing(&["Nope"]));
    assert_eq!(
        ask(&["G", "Nope", "S", "Other"]),
        missing(&["Nope", "Other"])
    );
    // Names only: Java's handler refuses an expression as a column that is not there.
    assert_eq!(ask(&["X=G"]), missing(&["X=G"]));
    assert_eq!(
        ask(&["Nope", "Other"]).unwrap_err().to_string(),
        "column(s) not found: Nope, Other"
    );
}

// ---------------------------------------------------------------------------------------
// refreshing sources

#[test]
fn a_new_key_is_appended_and_a_key_without_rows_leaves() {
    let rig = Rig::new(
        &[10, 20, 30],
        &columns(&[1, 2, 1], &["a", "b", "c"]),
        &["G"],
    );
    assert!(rig.result.is_refreshing() && !rig.result.is_flat());
    assert_eq!(rig.result.attributes(), Default::default());
    assert_eq!(rows(&*rig.result), ["1", "2"]);

    // A row of a key that is there changes nothing.
    assert!(rig
        .step(TableChange::new().add([40], row(2, "d")))
        .is_none());
    assert_eq!(
        rig.changed(TableChange::new().add([5], row(3, "e"))),
        (vec![2], vec![])
    );
    assert_eq!(rows(&*rig.result), ["1", "2", "3"]);

    // One of two rows of a key goes, then the other.
    assert!(rig.step(TableChange::new().remove([10])).is_none());
    assert_eq!(
        rig.changed(TableChange::new().remove([30])),
        (vec![], vec![0])
    );
    assert_eq!(rows(&*rig.result), ["2", "3"]);
    assert_eq!(keys_of(&*rig.result), [1, 2]);
}

#[test]
fn a_key_that_comes_back_takes_its_old_place_and_shows_its_first_value() {
    let source = [("D", ColumnType::Double, doubles([1.0, -0.0, 2.0]))];
    let rig = Rig::new(&[0, 1, 2], &source, &["D"]);
    assert_eq!(rows(&*rig.result), ["1.0", "-0.0", "2.0"]);
    assert_eq!(
        rig.changed(TableChange::new().remove([1])),
        (vec![], vec![1])
    );
    assert_eq!(rows(&*rig.result), ["1.0", "2.0"]);
    assert_eq!(
        rig.changed(TableChange::new().add([7], vec![doubles([3.0])])),
        (vec![3], vec![])
    );
    // The other zero brings the key back: between 1.0 and 2.0, and as -0.0.
    assert_eq!(
        rig.changed(TableChange::new().add([8], vec![doubles([0.0])])),
        (vec![1], vec![])
    );
    assert_eq!(rows(&*rig.result), ["1.0", "-0.0", "2.0", "3.0"]);
    assert_eq!(keys_of(&*rig.result), [0, 1, 2, 3]);
}

#[test]
fn a_row_that_changes_its_key_moves_to_the_group_of_the_new_key() {
    let rig = Rig::new(
        &[10, 20, 30],
        &columns(&[1, 2, 1], &["a", "b", "c"]),
        &["G"],
    );
    // To a new key, from a key that keeps a row.
    let change = TableChange::new().modify([10], [(0, ints([7]))]);
    assert_eq!(rig.changed(change), (vec![2], vec![]));
    assert_eq!(rows(&*rig.result), ["1", "2", "7"]);
    // To a key that is there, from a key that loses its last row.
    let change = TableChange::new().modify([20], [(0, ints([1]))]);
    assert_eq!(rig.changed(change), (vec![], vec![1]));
    assert_eq!(rows(&*rig.result), ["1", "7"]);
    // Two rows swap their keys: nothing shows.
    let change = TableChange::new().modify([10, 30], [(0, ints([1, 7]))]);
    assert!(rig.step(change).is_none());
    // A row is written with the key it has.
    assert!(rig
        .step(TableChange::new().modify([10], [(0, ints([1]))]))
        .is_none());
    // A change of a column that is not a key.
    assert!(rig
        .step(TableChange::new().modify([10], [(1, strings(["z"]))]))
        .is_none());
    assert_eq!(rows(&*rig.result), ["1", "7"]);
}

#[test]
fn a_key_from_a_changed_row_comes_before_a_key_from_an_added_row() {
    let rig = Rig::new(&[10, 20], &columns(&[1, 2], &["a", "b"]), &["G"]);
    // The added row is the first row of the source and the changed row its last.
    let change = TableChange::new()
        .add([5], row(8, "x"))
        .modify([20], [(0, ints([9]))]);
    assert_eq!(rig.changed(change), (vec![2, 3], vec![1]));
    assert_eq!(rows(&*rig.result), ["1", "9", "8"]);
}

#[test]
fn a_row_that_moves_and_changes_keeps_the_count_of_its_groups() {
    let rig = Rig::new(
        &[10, 20, 30],
        &columns(&[1, 2, 3], &["a", "b", "c"]),
        &["G"],
    );
    // Rows alone move: nothing shows.
    assert!(rig.step(TableChange::new().shift(20, 30, 5)).is_none());
    // The row that was at 25 moves to 45 and changes its key in the same cycle.
    let change = TableChange::new()
        .shift(25, 35, 20)
        .modify([45], [(0, ints([3]))]);
    assert_eq!(rig.changed(change), (vec![], vec![1]));
    assert_eq!(rows(&*rig.result), ["1", "3"]);
    // Key 3 has two rows now, and key 1 one.
    assert!(rig.step(TableChange::new().remove([55])).is_none());
    assert_eq!(
        rig.changed(TableChange::new().remove([10, 45])),
        (vec![], vec![0, 2])
    );
    assert!(rows(&*rig.result).is_empty());
}

#[test]
fn a_row_removed_and_a_row_added_with_its_key_in_one_cycle_change_nothing() {
    let rig = Rig::new(&[10, 20], &columns(&[1, 2], &["a", "b"]), &["G", "S"]);
    let change = TableChange::new().remove([10]).add([10], row(1, "a"));
    assert!(rig.step(change).is_none());
    let change = TableChange::new().remove([10]).add([10], row(1, "A"));
    assert_eq!(rig.changed(change), (vec![2], vec![0]));
    assert_eq!(rows(&*rig.result), ["2|b", "1|A"]);
}

#[test]
fn without_columns_the_row_comes_and_goes_with_the_sources_rows() {
    let rig = Rig::new(&[], &columns(&[], &[]), &[]);
    assert!(rig.result.is_flat() && rig.result.is_refreshing());
    assert!(keys_of(&*rig.result).is_empty());
    assert_eq!(
        rig.changed(TableChange::new().add([4], row(1, "a"))),
        (vec![0], vec![])
    );
    assert!(rig.step(TableChange::new().add([5], row(1, "a"))).is_none());
    assert!(rig.step(TableChange::new().remove([4])).is_none());
    assert_eq!(
        rig.changed(TableChange::new().remove([5])),
        (vec![], vec![0])
    );
    assert!(keys_of(&*rig.result).is_empty());
}

#[test]
fn a_failed_source_fails_the_result() {
    let rig = Rig::new(&[10], &columns(&[1], &["a"]), &["G"]);
    rig.source.push_failure("broken");
    rig.graph.run_cycle();
    let failure: TableFailure = rig.recorder.events().pop().unwrap().unwrap_err();
    assert!(failure.message.contains("broken"), "{failure}");
    assert!(rig.result.is_failed());
    let source: Arc<dyn Table> = rig.source.table();
    let refused = select_distinct(&source, &["G".to_string()]);
    assert!(matches!(refused, Err(DistinctError::Table(_))));
}

#[test]
fn the_result_of_a_blink_table_keeps_every_key_it_has_seen() {
    let graph = graph();
    let (table, blink) = blink_table(&graph, &[("G", ColumnType::Int), ("S", ColumnType::String)]);
    let result = distinct(&table, &["G"]);
    let any = distinct(&table, &[]);
    let checkers = [
        ConsistencyChecker::attach(result.clone()),
        ConsistencyChecker::attach(any.clone()),
    ];
    assert!(result.is_flat() && !result.attributes().blink && !result.attributes().add_only);
    let cycle = |rows: Option<Vec<ArrayRef>>| {
        if let Some(rows) = rows {
            blink.push(rows);
        }
        graph.run_cycle();
        checkers
            .iter()
            .for_each(|checker| checker.assert_consistent());
    };
    assert!(keys_of(&*any).is_empty());
    cycle(Some(vec![ints([2, 1, 2]), strings(["a", "b", "c"])]));
    assert_eq!(rows(&*result), ["2", "1"]);
    assert_eq!(keys_of(&*any), [0]);
    // The rows of the last cycle go and none come.
    cycle(None);
    assert_eq!(rows(&*result), ["2", "1"]);
    assert_eq!(keys_of(&*any), [0]);
    cycle(Some(vec![ints([1, 3]), strings(["d", "e"])]));
    assert_eq!(rows(&*result), ["2", "1", "3"]);
}

// ---------------------------------------------------------------------------------------
// random updates against a model

/// What a `select_distinct` must show, kept by other means than the operation's: the keys
/// in the order they were first met, each with the text of the row that brought it, of
/// which those are shown that a row of the source has now.
struct Model {
    by: Vec<usize>,
    /// The key and the text shown for it.
    met: Vec<(String, String)>,
}

impl Model {
    /// The key and the text of each of the source's rows, in row order.
    fn read(&self, source: &dyn Table) -> Vec<(String, String)> {
        let columns: Vec<Vec<String>> = self.by.iter().map(|index| cells(source, *index)).collect();
        (0..source.row_set().size() as usize)
            .map(|row| {
                let cells: Vec<&str> = columns.iter().map(|column| column[row].as_str()).collect();
                // The two zeros are one key.
                let key: Vec<&str> = cells
                    .iter()
                    .map(|cell| if *cell == "-0.0" { "0.0" } else { cell })
                    .collect();
                (key.join("|"), cells.join("|"))
            })
            .collect()
    }

    /// Takes in the source after `update`, or as it is at the start, and returns the rows
    /// the result must have.
    fn step(&mut self, source: &dyn Table, update: Option<&TableUpdate>) -> Vec<String> {
        let now = self.read(source);
        let rows = source.row_set();
        let positions: Vec<u64> = match update {
            None => (0..rows.size()).collect(),
            Some(update) => {
                let changed = update.modified.iter().chain(update.added.iter());
                changed.map(|key| rows.find(key).unwrap()).collect()
            }
        };
        for position in positions {
            let (key, text) = &now[position as usize];
            if !self.met.iter().any(|(met, _)| met == key) {
                self.met.push((key.clone(), text.clone()));
            }
        }
        let present: BTreeSet<&String> = now.iter().map(|(key, _)| key).collect();
        let met = self.met.iter().filter(|(key, _)| present.contains(key));
        met.map(|(_, text)| text.clone()).collect()
    }
}

#[test]
fn random_updates_match_the_model() {
    let types = [
        ("G", ColumnType::Int),
        ("S", ColumnType::String),
        ("D", ColumnType::Double),
        ("Z", ColumnType::Boolean),
    ];
    random_updates(
        &types,
        &[&["G"], &["S", "Z"], &["D"], &["Z", "G", "S"], &[]],
    );
}

/// Behavior: io.deephaven.vector.IntVector#equals and ObjectVector#equals: vectors are
/// one key when their elements are, whatever rows of the source they come from.
#[test]
fn random_updates_of_vector_keys_match_the_model() {
    let types = [
        ("G", ColumnType::Int),
        ("V", ColumnType::vector(ColumnType::Boolean)),
        ("W", ColumnType::vector(ColumnType::String)),
        ("N", ColumnType::vector(ColumnType::Int)),
    ];
    random_updates(&types, &[&["V"], &["G", "V"], &["W"], &["N", "V"]]);
}

/// A `select_distinct` by each of `key_sets` of a table of `types` under random updates,
/// against [`Model`].
fn random_updates(types: &[(&str, ColumnType)], key_sets: &[&[&str]]) {
    let mut updates = 0;
    for seed in test_seeds(0..60) {
        let graph = graph();
        let mut random = RandomUpdates::new(seed);
        let size = if seed % 5 == 0 { 0 } else { 8 };
        let values = types
            .iter()
            .map(|(_, column_type)| random.values(column_type, size));
        let script = ScriptedTable::new(
            graph.clone(),
            definition(types),
            oxhaven_contracts::rowset::RowSet::from_keys((0..size as RowKey).map(|k| 3 * k + 1)),
            values.collect(),
        );
        let source: Arc<dyn Table> = script.table();
        let recorder = Recorder::attach(source.clone());
        let mut cases = Vec::new();
        for by in key_sets.iter().copied() {
            let result = distinct(&source, by);
            let checker = ConsistencyChecker::attach(result.clone());
            let by = by
                .iter()
                .map(|name| source.definition().index_of(name).unwrap());
            let mut model = Model {
                by: by.collect(),
                met: Vec::new(),
            };
            let expected = model.step(&*source, None);
            cases.push((result, checker, model, expected));
        }
        for step in 0..40 {
            let before = recorder.events().len();
            script.push(random.next_change(&*source, 8));
            graph.run_cycle();
            let update = recorder
                .events()
                .split_off(before)
                .pop()
                .map(|event| event.unwrap());
            updates += u64::from(update.is_some());
            for (result, checker, model, expected) in &mut cases {
                if update.is_some() {
                    *expected = model.step(&*source, update.as_ref());
                }
                if let Err(e) = checker.verify() {
                    panic!("seed {seed}, step {step}, by {:?}: {e}", model.by);
                }
                if model.by.is_empty() {
                    let size = u64::from(!source.row_set().is_empty());
                    assert_eq!(result.row_set().size(), size, "seed {seed}, step {step}");
                } else {
                    assert_eq!(
                        rows(&**result),
                        *expected,
                        "seed {seed}, step {step}, by {:?}",
                        model.by
                    );
                }
            }
        }
    }
    assert!(updates > at_least_for(1000, 0..60), "{updates} updates");
}

/// Behavior: java.time.ZonedDateTime#equals and java.time.Period#equals: one instant in
/// two zones is two keys, and so are `P1Y` and `P12M`; nulls are one key.
#[test]
fn zoned_date_times_and_periods_are_keys_by_every_field() {
    let zoned = crate::ops::zoned::tests::zoned(&[
        Some((5, "UTC")),
        Some((5, "Asia/Tokyo")),
        Some((5, "UTC")),
        None,
        None,
    ]);
    let arrow::datatypes::DataType::Struct(fields) = ColumnType::Period.arrow_type() else {
        unreachable!()
    };
    let int = |values: [i32; 5]| -> ArrayRef {
        Arc::new(arrow::array::Int32Array::from(values.to_vec()))
    };
    let period: ArrayRef = Arc::new(arrow::array::StructArray::new(
        fields,
        vec![
            int([1, 0, 1, 0, 7]),
            int([0, 12, 0, 0, 7]),
            int([0, 0, 0, 0, 7]),
        ],
        Some(vec![true, true, true, false, false].into()),
    ));
    let source = static_table(&[
        ("Z", ColumnType::ZonedDateTime, zoned),
        ("P", ColumnType::Period, period),
    ]);
    assert_eq!(distinct(&source, &["Z"]).row_set().size(), 3);
    assert_eq!(distinct(&source, &["P"]).row_set().size(), 3);
    assert_eq!(distinct(&source, &["Z", "P"]).row_set().size(), 3);
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#selectDistinct with
/// io.deephaven.vector.IntVector#equals and ObjectVector#equals: the rows are those of
/// Deephaven 42.9 for these tables. A null vector, an empty vector and a vector of one
/// null are three keys.
#[test]
fn vectors_are_keys_by_their_contents() {
    let ints_of = ColumnType::vector(ColumnType::Int);
    let strings_of = ColumnType::vector(ColumnType::String);
    let two = Some(2);
    let one = Some(1);
    let v = list_of(
        &ints_of,
        [two, None, None, None, two, None, one, None, two],
        ints([1, 2, 1, 2, 7, 1, 2]),
    );
    let w_elements: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("b"),
        None,
        Some("a"),
        Some("b"),
        None,
        Some("z"),
        None,
        Some("q"),
        Some("a"),
        Some("b"),
    ]));
    let w = list_of(
        &strings_of,
        [two, None, one, None, two, one, one, two, two],
        w_elements,
    );
    let e_elements: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 2, 2]));
    let e = list_of(
        &ints_of,
        [
            Some(0),
            one,
            Some(0),
            Some(0),
            one,
            Some(0),
            Some(0),
            one,
            one,
        ],
        e_elements,
    );
    let source = static_table(&[
        ("V", ints_of.clone(), v),
        ("W", strings_of.clone(), w),
        ("E", ints_of.clone(), e),
        ("X", ColumnType::Int, ints(1..10)),
    ]);
    let result = distinct(&source, &["V"]);
    assert_eq!(result.definition().columns()[0].column_type, ints_of);
    assert_eq!(rows(&*result), ["[1, 2]", "null", "[7]"]);
    assert_eq!(
        rows(&*distinct(&source, &["W"])),
        ["[a, b]", "null", "[null]", "[z]", "[null, q]"]
    );
    assert_eq!(
        rows(&*distinct(&source, &["V", "W"])),
        [
            "[1, 2]|[a, b]",
            "null|null",
            "null|[null]",
            "[7]|[z]",
            "null|[null, q]"
        ]
    );
    assert_eq!(distinct(&source, &["W", "X", "V"]).row_set().size(), 9);
    assert_eq!(rows(&*distinct(&source, &["E"])), ["[]", "[1]", "[2]"]);
}

/// Java compares vectors of doubles by a rule that depends on the class of the vector, and
/// arrays by identity (owner item 7.6 of the review items), so neither is a key yet.
#[test]
fn vectors_of_doubles_and_arrays_are_not_keys_yet() {
    for column_type in [
        ColumnType::vector(ColumnType::Double),
        ColumnType::array(ColumnType::Int),
    ] {
        let elements = match column_type.list_element().unwrap() {
            ColumnType::Double => doubles([1.0]),
            _ => ints([1]),
        };
        let cells = list_of(&column_type, [Some(1)], elements);
        let source = static_table(&[("V", column_type.clone(), cells)]);
        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = select_distinct(&source, &["V".to_string()]);
        }));
        let message = *refused.unwrap_err().downcast::<String>().unwrap();
        assert!(
            message.starts_with(
                "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make: \
                 a key column of type"
            ),
            "{message}"
        );
    }
}
