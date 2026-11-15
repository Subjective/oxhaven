//! `head_by` and `tail_by`: the refusals, static results, the update for each kind of
//! change of the source, and random changes. Every ticking result has the consistency
//! checker attached, which also compares it with the same operation on a static copy of
//! the source after every cycle.

use super::{head_by, tail_by, HeadByError};
use crate::ops::sort::testing::{
    blink_table, cells, frozen, graph, scripted, static_table, Columns,
};
use arrow::array::{ArrayRef, Float64Array, UInt64Array};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    at_least_for, definition, ints, list_of, strings, test_seeds, ConsistencyChecker,
    RandomUpdates, Recorder, ScriptedTable, TableChange,
};
use std::collections::HashMap;
use std::sync::Arc;

fn names(by: &[&str]) -> Vec<String> {
    by.iter().map(|name| name.to_string()).collect()
}

fn make(source: &Arc<dyn Table>, head: bool, rows: i64, by: &[&str]) -> Arc<dyn Table> {
    let made = if head {
        head_by(source, rows, &names(by))
    } else {
        tail_by(source, rows, &names(by))
    };
    made.expect("a head_by or tail_by")
}

/// The rows of `table` in order, the cells of a row joined by `|`.
fn shown(table: &dyn Table) -> Vec<String> {
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

/// `expected` with its groups in the order `like` has them, a group being the rows with
/// one key in the first `key_columns` columns; the groups `like` lacks come last. The
/// rows of a group keep their order.
///
/// A result that was kept up to date has its groups in the order their keys were first
/// seen over its whole history, and one made now in the order of the current rows. The
/// order of the groups is checked by the scripted tests here and by the recordings.
fn in_order_of(expected: &dyn Table, like: &dyn Table, key_columns: usize) -> Arc<dyn Table> {
    let keys = |table: &dyn Table| -> Vec<String> {
        let columns: Vec<Vec<String>> = (0..key_columns).map(|c| cells(table, c)).collect();
        (0..table.row_set().size() as usize)
            .map(|row| {
                let cells: Vec<&str> = columns.iter().map(|column| column[row].as_str()).collect();
                cells.join("|")
            })
            .collect()
    };
    let mut rank: HashMap<String, usize> = HashMap::new();
    for key in keys(like) {
        let next = rank.len();
        rank.entry(key).or_insert(next);
    }
    let expected_keys = keys(expected);
    let mut order: Vec<u64> = (0..expected_keys.len() as u64).collect();
    order.sort_by_key(|&row| {
        let rank = rank.get(&expected_keys[row as usize]);
        rank.copied().unwrap_or(usize::MAX)
    });
    let order = UInt64Array::from(order);
    let snapshot = oxhaven_testkit::TableSnapshot::current(expected).unwrap();
    let declared = expected.definition().columns();
    let columns: Vec<(&str, ColumnType, ArrayRef)> = declared
        .iter()
        .zip(&snapshot.columns)
        .map(|(column, values)| {
            let values = arrow::compute::take(values, &order, None).unwrap();
            (column.name.as_str(), column.column_type.clone(), values)
        })
        .collect();
    static_table(&columns)
}

/// A scripted source and its `head_by` or `tail_by`, with every update recorded and
/// checked.
struct Rig {
    graph: Arc<dyn UpdateGraph>,
    script: Arc<ScriptedTable>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Rig {
    fn of(
        graph: Arc<dyn UpdateGraph>,
        script: Arc<ScriptedTable>,
        head: bool,
        rows: i64,
        by: &'static [&'static str],
    ) -> Rig {
        let source: Arc<dyn Table> = script.table();
        let result = make(&source, head, rows, by);
        assert!(result.is_refreshing() && !result.is_flat());
        let recorder = Recorder::attach(result.clone());
        let kept = result.clone();
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            let from_scratch = make(&frozen(&*source), head, rows, by);
            in_order_of(&*from_scratch, &*kept, by.len())
        });
        checker.assert_consistent();
        Rig {
            graph,
            script,
            result,
            recorder,
            checker,
        }
    }

    fn new(
        keys: impl IntoIterator<Item = RowKey>,
        columns: Columns,
        head: bool,
        rows: i64,
        by: &'static [&'static str],
    ) -> Rig {
        let graph = graph();
        let script = scripted(&graph, keys, columns);
        Rig::of(graph, script, head, rows, by)
    }

    /// Applies `change` to the source in one cycle and returns the update the result
    /// delivered in it, if it delivered one.
    fn step(&self, change: TableChange) -> Option<TableUpdate> {
        let before = self.recorder.events().len();
        self.script.push(change);
        self.graph.run_cycle();
        self.checker.assert_consistent();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one update in a cycle");
        events.pop().map(|event| event.unwrap())
    }

    fn rows(&self) -> Vec<String> {
        shown(&*self.result)
    }
}

/// The source of most tests: the group `G`, and two columns that are not keys.
fn letters(
    groups: &[&str],
    values: impl IntoIterator<Item = i32>,
) -> [(&'static str, ColumnType, ArrayRef); 3] {
    let values: Vec<i32> = values.into_iter().collect();
    let other = values.iter().map(|v| v * 10);
    [
        ("V", ColumnType::Int, ints(values.clone())),
        ("G", ColumnType::String, strings(groups.iter().copied())),
        ("W", ColumnType::Int, ints(other)),
    ]
}

/// One row of the source of [`letters`].
fn row(group: &str, value: i32) -> Vec<ArrayRef> {
    letters(&[group], [value])
        .map(|(_, _, values)| values)
        .to_vec()
}

/// `a`: 1, 3, 4 and `b`: 2, 5 at the row keys 10, 20, 30, 40, 50.
fn two_groups(head: bool, rows: i64) -> Rig {
    let columns = letters(&["a", "b", "a", "a", "b"], 1..=5);
    Rig::new([10, 20, 30, 40, 50], &columns, head, rows, &["G"])
}

/// The row key of element `offset` of the group in `slot`, while no group keeps more than
/// 1024 rows.
fn key(slot: RowKey, offset: RowKey) -> RowKey {
    (slot << 10) + offset
}

fn keys(keys: impl IntoIterator<Item = RowKey>) -> RowSet {
    RowSet::from_keys(keys)
}

/// The columns of the result that are not keys, for a source of [`letters`].
fn values() -> ModifiedColumnSet {
    ModifiedColumnSet::of([1, 2])
}

fn update(
    added: impl IntoIterator<Item = RowKey>,
    removed: impl IntoIterator<Item = RowKey>,
    modified: impl IntoIterator<Item = RowKey>,
    modified_columns: ModifiedColumnSet,
) -> Option<TableUpdate> {
    Some(TableUpdate {
        added: keys(added),
        removed: keys(removed),
        modified: keys(modified),
        modified_columns,
        ..TableUpdate::empty()
    })
}

// ---------------------------------------------------------------------------------------
// Refusals

/// The line of `fixtures/java_refusals.txt` for one call, after the arrow.
fn java_answers() -> HashMap<String, String> {
    let recorded = include_str!("fixtures/java_refusals.txt");
    let lines = recorded.lines().filter(|line| !line.starts_with("grpc-"));
    lines
        .map(|line| {
            let (call, answer) = line.split_once(" => ").expect("a call and its answer");
            (call.to_string(), answer.to_string())
        })
        .collect()
}

/// Java's exception class for each refusal.
fn exception(error: &HeadByError) -> &'static str {
    match error {
        HeadByError::RowsNotPositive(_) => "io.deephaven.base.verify.RequirementFailure",
        HeadByError::GroupColumns { room, .. } if *room < 0 => {
            "java.lang.NegativeArraySizeException"
        }
        HeadByError::GroupColumns { .. } => "java.lang.ArrayIndexOutOfBoundsException",
        HeadByError::NoOtherColumns(_) | HeadByError::ArrayKey { .. } => {
            "java.lang.IllegalArgumentException"
        }
        HeadByError::Blink | HeadByError::TableColumn(_) => {
            "java.lang.UnsupportedOperationException"
        }
        HeadByError::Failed(_) => panic!("not an exception of headBy"),
    }
}

/// Every call that `fixtures/record_refusals.py` made on the Java server is refused here
/// when Java refused it, for the same reason, which shows the order of the checks, and
/// with Java's message.
#[test]
fn refusals_are_javas_in_javas_order() {
    let columns = [
        ("A", ColumnType::Int, ints([1, 1, 2])),
        ("B", ColumnType::String, strings(["x", "y", "x"])),
    ];
    let fixed = static_table(&columns);
    let blink_columns = [
        ("Timestamp", ColumnType::Instant),
        ("A", ColumnType::Int),
        ("B", ColumnType::String),
    ];
    let (blink, _source) = blink_table(&graph(), &blink_columns);
    let lists = lists();
    let blink_columns = [
        ("Timestamp", ColumnType::Instant),
        ("A", ColumnType::Int),
        ("Arr", ColumnType::array(ColumnType::Int)),
        ("V", ColumnType::vector(ColumnType::Int)),
    ];
    let (blink_lists, _source) = blink_table(&graph(), &blink_columns);
    let answers = java_answers();
    assert_eq!(answers.len(), 39);
    let mut refused = 0;
    for (call, answer) in answers {
        // What Java makes of a vector it groups by is not ported.
        if call == VECTOR_KEY {
            assert!(answer.starts_with("java.lang.IllegalStateException: Array sizes differ"));
            continue;
        }
        refused += usize::from(answer != "OK");
        let words: Vec<&str> = call.split(' ').collect();
        let table = match words[0] {
            "static" => &fixed,
            "blink" => &blink,
            "lists" => &lists,
            "blinklists" => &blink_lists,
            other => panic!("no table {other}"),
        };
        let rows: i64 = words[2].parse().unwrap();
        let by: Vec<&str> = words[3].split(',').filter(|name| *name != "-").collect();
        let made = match words[1] {
            "head" => head_by(table, rows, &names(&by)),
            _ => tail_by(table, rows, &names(&by)),
        };
        match made {
            Ok(_) => assert_eq!(answer, "OK", "{call}"),
            Err(error) => assert_eq!(format!("{}: {error}", exception(&error)), answer, "{call}"),
        }
    }
    assert_eq!(refused, 35);
}

/// The call of `fixtures/java_refusals.txt` that groups by a vector column alone.
const VECTOR_KEY: &str = "lists head 1 V";

/// The table `lists` of `fixtures/record_refusals.py`.
fn lists() -> Arc<dyn Table> {
    use arrow::array::{new_null_array, BooleanArray, Int8Array};
    let one_each =
        |column_type: &ColumnType, elements: ArrayRef| list_of(column_type, [Some(1); 3], elements);
    let vector = ColumnType::vector(ColumnType::Int);
    let array = ColumnType::array(ColumnType::Int);
    let texts = ColumnType::array(ColumnType::String);
    let deep = ColumnType::array(array.clone());
    let cube = ColumnType::array(deep.clone());
    let times = ColumnType::array(ColumnType::Instant);
    let flags = ColumnType::array(ColumnType::Boolean);
    let bytes = ColumnType::array(ColumnType::Byte);
    let columns = [
        ("K", ColumnType::Int, ints([1, 2, 3])),
        (
            "V",
            vector.clone(),
            list_of(&vector, [Some(2), Some(1), Some(1)], ints([1, 2, 3, 4])),
        ),
        ("Arr", array.clone(), one_each(&array, ints([1, 2, 3]))),
        (
            "SArr",
            texts.clone(),
            one_each(&texts, strings(["x", "x", "x"])),
        ),
        (
            "Deep",
            deep.clone(),
            one_each(&deep, one_each(&array, ints([1, 2, 3]))),
        ),
        (
            "Cube",
            cube.clone(),
            one_each(&cube, one_each(&deep, one_each(&array, ints([1, 2, 3])))),
        ),
        (
            "Times",
            times.clone(),
            one_each(&times, new_null_array(&ColumnType::Instant.arrow_type(), 3)),
        ),
        (
            "Flags",
            flags.clone(),
            one_each(&flags, Arc::new(BooleanArray::from(vec![true; 3]))),
        ),
        (
            "Bytes",
            bytes.clone(),
            one_each(&bytes, Arc::new(Int8Array::from(vec![1i8; 3]))),
        ),
    ];
    static_table(&columns)
}

/// Java ungroups a vector that it grouped by, which is not ported; the checks of the
/// request come first, as the blink table of `refusals_are_javas_in_javas_order` shows.
#[test]
#[should_panic(expected = "the group-by column V of type Vector(Int), which Java ungroups")]
fn a_vector_to_group_by_is_not_ported() {
    let words: Vec<&str> = VECTOR_KEY.split(' ').collect();
    assert_eq!(words[1..], ["head", "1", "V"]);
    let _ = head_by(&lists(), 1, &names(&["V"]));
}

/// A table of `fixtures/java_lists.txt`: its name, its column names, the Java classes of
/// its columns and its rows.
struct Recorded {
    name: &'static str,
    columns: Vec<&'static str>,
    classes: Vec<&'static str>,
    rows: Vec<&'static str>,
}

fn java_lists() -> Vec<Recorded> {
    let mut tables: Vec<Recorded> = Vec::new();
    let mut lines = include_str!("fixtures/java_lists.txt").lines();
    while let Some(line) = lines.next() {
        match line.split_once(": ") {
            Some((name, columns)) => tables.push(Recorded {
                name,
                columns: columns.split(',').collect(),
                classes: lines.next().expect("the classes").split(',').collect(),
                rows: Vec::new(),
            }),
            None => tables.last_mut().expect("a table").rows.push(line),
        }
    }
    tables
}

/// A column of vectors or arrays that is not grouped by is kept as it is: Java's
/// `ungroup` takes apart the arrays of the groups, not the cells in them. The source and
/// the results are those `fixtures/record_lists.py` read from Java.
#[test]
fn vectors_and_arrays_that_are_not_grouped_by_are_kept_whole() {
    let vector = ColumnType::vector(ColumnType::Int);
    let array = ColumnType::array(ColumnType::Int);
    let texts = ColumnType::array(ColumnType::String);
    // C and Arr: [1], [2, 5], [3, 4], [6]
    let lengths = [Some(1), Some(2), Some(2), Some(1)];
    let elements = || ints([1, 2, 5, 3, 4, 6]);
    let columns = [
        ("A", ColumnType::Int, ints([1, 1, 2, 3])),
        ("B", ColumnType::String, strings(["x", "y", "x", "x"])),
        ("C", vector.clone(), list_of(&vector, lengths, elements())),
        ("Arr", array.clone(), list_of(&array, lengths, elements())),
        ("K", ColumnType::Int, ints([1, 1, 2, 3])),
        (
            "SArr",
            texts.clone(),
            list_of(&texts, [Some(1); 4], strings(["x", "y", "x", "x"])),
        ),
    ];
    let source = static_table(&columns);

    let recorded = java_lists();
    assert_eq!(recorded.len(), 5);
    for java in recorded {
        let words: Vec<&str> = java.name.split(' ').collect();
        let table = match words[..] {
            ["source"] => source.clone(),
            [kind, rows, by] => {
                let by: Vec<&str> = by.split(',').filter(|name| *name != "-").collect();
                make(&source, kind == "head", rows.parse().unwrap(), &by)
            }
            _ => panic!("no table {}", java.name),
        };
        let declared = table.definition().columns();
        let names: Vec<&str> = declared.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, java.columns, "{}", java.name);
        let classes: Vec<String> = declared
            .iter()
            .map(|c| super::binary_name(&c.column_type))
            .collect();
        assert_eq!(classes, java.classes, "{}", java.name);
        assert_eq!(shown(&*table), java.rows, "{}", java.name);
    }
}

#[test]
fn each_refusal_has_its_variant() {
    let columns = letters(&["a", "b"], [1, 2]);
    let source = static_table(&columns);
    let refused = |rows: i64, by: &[&str]| head_by(&source, rows, &names(by)).err().unwrap();
    assert_eq!(refused(0, &["G"]), HeadByError::RowsNotPositive(0));
    assert_eq!(
        refused(i64::MIN, &["G"]),
        HeadByError::RowsNotPositive(i64::MIN)
    );
    assert_eq!(
        refused(1, &["G", "G"]),
        HeadByError::GroupColumns {
            columns: names(&["G", "G"]),
            room: 1
        }
    );
    assert_eq!(
        refused(1, &["V", "G", "W", "Nope"]),
        HeadByError::GroupColumns {
            columns: names(&["V", "G", "W", "Nope"]),
            room: -1
        }
    );
    assert_eq!(
        refused(1, &["W", "G", "V"]),
        HeadByError::NoOtherColumns(names(&["W", "G", "V"]))
    );
    let tail = tail_by(&source, -3, &names(&["G"])).err().unwrap();
    assert_eq!(tail, HeadByError::RowsNotPositive(-3));
    let lists = lists();
    let refused = |by: &[&str]| tail_by(&lists, 1, &names(by)).err().unwrap();
    assert_eq!(
        refused(&["K", "Deep", "Arr"]),
        HeadByError::ArrayKey {
            column: "Deep".to_string(),
            element: ColumnType::array(ColumnType::Int)
        }
    );
}

/// Java gets to the failed table only when it makes the result, after its checks of the
/// request.
#[test]
fn a_failed_source_is_refused_after_the_checks_of_the_request() {
    let graph = graph();
    let columns = letters(&["a", "b"], [1, 2]);
    let script = scripted(&graph, [0, 1], &columns);
    script.push_failure("the source broke");
    graph.run_cycle();
    let source: Arc<dyn Table> = script.table();
    assert!(source.is_failed());
    let refused = |rows: i64, by: &[&str]| head_by(&source, rows, &names(by)).err().unwrap();
    assert_eq!(refused(0, &["G"]), HeadByError::RowsNotPositive(0));
    assert!(matches!(
        refused(1, &["Nope"]),
        HeadByError::GroupColumns { .. }
    ));
    assert!(matches!(refused(1, &["G"]), HeadByError::Failed(_)));
    let tail = tail_by(&source, 1, &[]).err().unwrap();
    assert!(matches!(tail, HeadByError::Failed(_)));
}

// ---------------------------------------------------------------------------------------
// Static tables

#[test]
fn the_first_and_the_last_rows_of_each_group_in_the_order_of_the_groups() {
    let columns = letters(&["b", "a", "b", "b", "c", "a", "b"], 1..=7);
    let source = static_table(&columns);
    let head = make(&source, true, 2, &["G"]);
    assert!(!head.is_refreshing() && !head.is_flat());
    let declared = head.definition().columns();
    let declared: Vec<&str> = declared.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(declared, ["G", "V", "W"]);
    assert_eq!(
        shown(&*head),
        ["b|1|10", "b|3|30", "a|2|20", "a|6|60", "c|5|50"]
    );
    let tail = make(&source, false, 2, &["G"]);
    assert_eq!(
        shown(&*tail),
        ["b|4|40", "b|7|70", "a|2|20", "a|6|60", "c|5|50"]
    );
    assert_eq!(
        shown(&*make(&source, true, 1, &["G"])),
        ["b|1|10", "a|2|20", "c|5|50"]
    );
    assert_eq!(
        shown(&*make(&source, false, 1, &["G"])),
        ["b|7|70", "a|6|60", "c|5|50"]
    );
    let all = [
        "b|1|10", "b|3|30", "b|4|40", "b|7|70", "a|2|20", "a|6|60", "c|5|50",
    ];
    assert_eq!(shown(&*make(&source, true, 4, &["G"])), all);
    assert_eq!(shown(&*make(&source, false, i64::MAX, &["G"])), all);
}

#[test]
fn the_group_by_columns_come_first_in_the_order_given() {
    let columns = letters(&["b", "a", "b", "b"], [1, 1, 1, 2]);
    let source = static_table(&columns);
    let result = make(&source, false, 1, &["G", "V"]);
    let declared = result.definition().columns();
    let declared: Vec<&str> = declared.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(declared, ["G", "V", "W"]);
    assert_eq!(shown(&*result), ["b|1|10", "a|1|10", "b|2|20"]);
    let result = make(&source, true, 1, &["W", "G"]);
    let declared = result.definition().columns();
    let declared: Vec<&str> = declared.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(declared, ["W", "G", "V"]);
    assert_eq!(shown(&*result), ["10|b|1", "10|a|1", "20|b|2"]);
}

#[test]
fn without_group_by_columns_the_table_is_one_group() {
    let columns = letters(&["b", "a", "b", "b"], 1..=4);
    let source = static_table(&columns);
    assert_eq!(
        shown(&*make(&source, true, 3, &[])),
        ["1|b|10", "2|a|20", "3|b|30"]
    );
    assert_eq!(
        shown(&*make(&source, false, 3, &[])),
        ["2|a|20", "3|b|30", "4|b|40"]
    );
    assert_eq!(shown(&*make(&source, false, 9, &[])).len(), 4);
    let empty = static_table(&letters(&[], []));
    assert_eq!(make(&empty, true, 3, &[]).row_set().size(), 0);
    assert_eq!(make(&empty, false, 3, &["G"]).row_set().size(), 0);
}

/// Behavior: the key columns of an aggregation hold the key of the first row of a group.
#[test]
fn a_key_column_shows_the_first_key_of_its_group() {
    let doubles = [-0.0, f64::NAN, 0.0, 1.0, f64::NAN, 0.0];
    let columns = [
        (
            "D",
            ColumnType::Double,
            Arc::new(Float64Array::from(doubles.to_vec())) as ArrayRef,
        ),
        ("V", ColumnType::Int, ints(1..=6)),
    ];
    let source = static_table(&columns);
    let head = make(&source, true, 2, &["D"]);
    assert_eq!(
        shown(&*head),
        ["-0.0|1", "-0.0|3", "NaN|2", "NaN|5", "1.0|4"]
    );
    let tail = make(&source, false, 2, &["D"]);
    assert_eq!(
        shown(&*tail),
        ["-0.0|3", "-0.0|6", "NaN|2", "NaN|5", "1.0|4"]
    );
}

// ---------------------------------------------------------------------------------------
// Each kind of change

#[test]
fn the_rows_of_the_initial_result_have_a_slot_of_1024_keys_for_each_group() {
    let rig = two_groups(true, 2);
    assert_eq!(rig.rows(), ["a|1|10", "a|3|30", "b|2|20", "b|5|50"]);
    let expected = keys([key(0, 0), key(0, 1), key(1, 0), key(1, 1)]);
    assert_eq!(*rig.result.row_set(), expected);
}

#[test]
fn a_row_added_to_a_full_group_modifies_all_its_kept_rows() {
    let rig = two_groups(true, 2);
    // In front of the kept rows of `a`.
    let change = TableChange::new().add([5], row("a", 6));
    assert_eq!(
        rig.step(change),
        update([], [], [key(0, 0), key(0, 1)], values())
    );
    assert_eq!(rig.rows(), ["a|6|60", "a|1|10", "b|2|20", "b|5|50"]);
    // Behind them: nothing the result shows changes, and Java reports them all the same.
    let change = TableChange::new().add([60], row("a", 7));
    assert_eq!(
        rig.step(change),
        update([], [], [key(0, 0), key(0, 1)], values())
    );
    assert_eq!(rig.rows(), ["a|6|60", "a|1|10", "b|2|20", "b|5|50"]);
}

#[test]
fn a_row_added_to_the_end_of_a_group_moves_the_rows_tail_by_keeps() {
    let rig = two_groups(false, 2);
    assert_eq!(rig.rows(), ["a|3|30", "a|4|40", "b|2|20", "b|5|50"]);
    let change = TableChange::new().add([60], row("b", 6));
    assert_eq!(
        rig.step(change),
        update([], [], [key(1, 0), key(1, 1)], values())
    );
    assert_eq!(rig.rows(), ["a|3|30", "a|4|40", "b|5|50", "b|6|60"]);
    // In front of the group, where tail_by does not look.
    let change = TableChange::new().add([5], row("b", 7));
    assert_eq!(
        rig.step(change),
        update([], [], [key(1, 0), key(1, 1)], values())
    );
    assert_eq!(rig.rows(), ["a|3|30", "a|4|40", "b|5|50", "b|6|60"]);
}

#[test]
fn a_group_that_grows_adds_rows_and_a_new_group_comes_last() {
    let rig = two_groups(true, 4);
    assert_eq!(
        rig.rows(),
        ["a|1|10", "a|3|30", "a|4|40", "b|2|20", "b|5|50"]
    );
    let change = TableChange::new().add(
        [15, 45, 60],
        [row("c", 6), row("b", 7), row("b", 8)].concat_columns(),
    );
    let expected = update(
        [key(1, 2), key(1, 3), key(2, 0)],
        [],
        [key(1, 0), key(1, 1)],
        values(),
    );
    assert_eq!(rig.step(change), expected);
    let rows = [
        "a|1|10", "a|3|30", "a|4|40", "b|2|20", "b|7|70", "b|5|50", "b|8|80", "c|6|60",
    ];
    assert_eq!(rig.rows(), rows);
}

/// Joins the columns of several rows into the columns of one batch of rows.
trait ConcatColumns {
    fn concat_columns(&self) -> Vec<ArrayRef>;
}

impl<const N: usize> ConcatColumns for [Vec<ArrayRef>; N] {
    fn concat_columns(&self) -> Vec<ArrayRef> {
        (0..self[0].len())
            .map(|column| {
                let parts: Vec<&dyn arrow::array::Array> =
                    self.iter().map(|row| row[column].as_ref()).collect();
                arrow::compute::concat(&parts).unwrap()
            })
            .collect()
    }
}

#[test]
fn a_removed_row_modifies_its_group_and_a_group_that_shrinks_removes_rows() {
    let rig = two_groups(true, 2);
    // `a` has 1, 3, 4: without 3 it keeps 1 and 4.
    let change = TableChange::new().remove([30]);
    assert_eq!(
        rig.step(change),
        update([], [], [key(0, 0), key(0, 1)], values())
    );
    assert_eq!(rig.rows(), ["a|1|10", "a|4|40", "b|2|20", "b|5|50"]);
    // Now it has one row less than it may keep.
    let change = TableChange::new().remove([10]);
    assert_eq!(
        rig.step(change),
        update([], [key(0, 1)], [key(0, 0)], values())
    );
    assert_eq!(rig.rows(), ["a|4|40", "b|2|20", "b|5|50"]);
    // Its last row takes the group with it.
    let change = TableChange::new().remove([40]);
    assert_eq!(
        rig.step(change),
        update([], [key(0, 0)], [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows(), ["b|2|20", "b|5|50"]);
}

#[test]
fn a_group_that_comes_back_is_where_it_was() {
    let rig = two_groups(false, 1);
    assert_eq!(rig.rows(), ["a|4|40", "b|5|50"]);
    let change = TableChange::new()
        .remove([10, 30, 40])
        .add([60], row("c", 6));
    assert_eq!(
        rig.step(change),
        update([key(2, 0)], [key(0, 0)], [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows(), ["b|5|50", "c|6|60"]);
    let change = TableChange::new().add([70], row("a", 7));
    assert_eq!(
        rig.step(change),
        update([key(0, 0)], [], [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows(), ["a|7|70", "b|5|50", "c|6|60"]);
}

/// A group that loses every row and gets one in the same cycle was never gone.
#[test]
fn a_group_emptied_and_refilled_in_one_cycle_is_modified() {
    let rig = two_groups(true, 2);
    let change = TableChange::new().remove([20, 50]).add([60], row("b", 6));
    assert_eq!(
        rig.step(change),
        update([], [key(1, 1)], [key(1, 0)], values())
    );
    assert_eq!(rig.rows(), ["a|1|10", "a|3|30", "b|6|60"]);
}

#[test]
fn a_modified_value_modifies_the_kept_rows_of_its_group_in_that_column() {
    let rig = two_groups(true, 2);
    // A kept row.
    let change = TableChange::new().modify([30], [(0, ints([33]))]);
    let only_v = ModifiedColumnSet::of([1]);
    assert_eq!(
        rig.step(change),
        update([], [], [key(0, 0), key(0, 1)], only_v)
    );
    assert_eq!(rig.rows(), ["a|1|10", "a|33|30", "b|2|20", "b|5|50"]);
    // A row that is not kept: Java reports the kept rows of its group.
    let change = TableChange::new().modify([40], [(2, ints([44]))]);
    let only_w = ModifiedColumnSet::of([2]);
    assert_eq!(
        rig.step(change),
        update([], [], [key(0, 0), key(0, 1)], only_w)
    );
    assert_eq!(rig.rows(), ["a|1|10", "a|33|30", "b|2|20", "b|5|50"]);
}

#[test]
fn a_key_modified_to_an_equal_key_changes_nothing() {
    let rig = two_groups(true, 2);
    let change = TableChange::new().modify([10, 50], [(1, strings(["a", "b"]))]);
    assert_eq!(rig.step(change), None);
    assert_eq!(rig.rows(), ["a|1|10", "a|3|30", "b|2|20", "b|5|50"]);
    // With a column that is not a key among the modified ones, the groups are reported.
    let change = TableChange::new().modify([10], [(1, strings(["a"])), (2, ints([11]))]);
    let only_w = ModifiedColumnSet::of([2]);
    assert_eq!(
        rig.step(change),
        update([], [], [key(0, 0), key(0, 1)], only_w)
    );
    assert_eq!(rig.rows(), ["a|1|11", "a|3|30", "b|2|20", "b|5|50"]);
    // In a cycle that changes another group, the group of a row whose key alone was
    // modified is not reported.
    let change = TableChange::new()
        .modify([30], [(1, strings(["a"]))])
        .add([60], row("b", 6));
    assert_eq!(
        rig.step(change),
        update([], [], [key(1, 0), key(1, 1)], values())
    );
}

/// What `ungroup` reads of a column that is not a key: for each group an array of its
/// kept values, with nulls beyond them.
#[test]
fn the_kept_rows_of_a_group_are_an_array_with_nulls_beyond_it() {
    use super::groups::{Groups, Kept};
    use crate::ops::ungroup::sources::ArrayColumn;
    use arrow::array::{Array, Int32Array};
    use oxhaven_column::WritableSource;

    let columns = letters(&["a", "b", "a", "a"], 1..=4);
    let source = static_table(&columns);
    for (head, expected) in [
        (true, [Some(1), Some(3), None, Some(2), None]),
        (false, [Some(3), Some(4), None, Some(2), None]),
    ] {
        let keys = vec![Arc::new(WritableSource::new(ColumnType::String))];
        let groups = Groups::new(source.clone(), vec![1], keys, 2, head).unwrap();
        assert_eq!(groups.initial().unwrap(), RowSet::flat(2));
        let kept = Kept {
            groups: Arc::new(groups),
            column: source.column(0),
        };
        assert_eq!(kept.sizes(&RowSet::flat(2), false).unwrap(), [2, 1]);
        let cells = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)];
        let values = kept.elements(&cells, false).unwrap();
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), expected, "head: {head}");
    }
}

#[test]
fn a_row_that_changes_group_leaves_one_and_joins_the_other() {
    let rig = two_groups(true, 3);
    assert_eq!(
        rig.rows(),
        ["a|1|10", "a|3|30", "a|4|40", "b|2|20", "b|5|50"]
    );
    let change = TableChange::new().modify([30], [(1, strings(["b"]))]);
    let expected = update(
        [key(1, 2)],
        [key(0, 2)],
        [key(0, 0), key(0, 1), key(1, 0), key(1, 1)],
        values(),
    );
    assert_eq!(rig.step(change), expected);
    assert_eq!(
        rig.rows(),
        ["a|1|10", "a|4|40", "b|2|20", "b|3|30", "b|5|50"]
    );
}

/// Behavior: KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys looks up the keys of
/// the modified rows before those of the added rows.
#[test]
fn the_new_group_of_a_modified_row_comes_before_the_new_group_of_an_added_row() {
    let rig = two_groups(true, 2);
    let change = TableChange::new()
        .add([5], row("first", 6))
        .modify([50], [(1, strings(["second"]))]);
    let expected = update([key(2, 0), key(3, 0)], [key(1, 1)], [key(1, 0)], values());
    assert_eq!(rig.step(change), expected);
    assert_eq!(
        rig.rows(),
        ["a|1|10", "a|3|30", "b|2|20", "second|5|50", "first|6|60"]
    );
}

#[test]
fn shifted_rows_are_no_change_of_the_result() {
    let rig = two_groups(true, 2);
    let change = TableChange::new().shift(30, 50, 100);
    assert_eq!(rig.step(change), None);
    assert_eq!(rig.rows(), ["a|1|10", "a|3|30", "b|2|20", "b|5|50"]);
    // The groups know the rows by their new keys.
    let change = TableChange::new()
        .remove([130])
        .modify([150], [(0, ints([55]))]);
    let expected = update(
        [],
        [],
        [key(0, 0), key(0, 1), key(1, 0), key(1, 1)],
        values(),
    );
    assert_eq!(rig.step(change), expected);
    assert_eq!(rig.rows(), ["a|1|10", "a|4|40", "b|2|20", "b|55|50"]);
}

#[test]
fn shifts_with_every_other_kind_of_change() {
    let rig = two_groups(false, 2);
    assert_eq!(rig.rows(), ["a|3|30", "a|4|40", "b|2|20", "b|5|50"]);
    // 10 leaves, 20 to 50 move down by 5, the row now at 25 joins `b`, a row is added
    // where 30 was, and the value of the row now at 45 changes.
    let change = TableChange::new()
        .remove([10])
        .shift(20, 50, -5)
        .modify([25, 45], [(0, ints([3, 55])), (1, strings(["b", "b"]))])
        .add([30], row("c", 6));
    let expected = update(
        [key(2, 0)],
        [key(0, 1)],
        [key(0, 0), key(1, 0), key(1, 1)],
        values(),
    );
    assert_eq!(rig.step(change), expected);
    assert_eq!(rig.rows(), ["a|4|40", "b|3|30", "b|55|50", "c|6|60"]);
}

#[test]
fn a_cycle_without_a_change_delivers_nothing() {
    let rig = two_groups(true, 2);
    assert_eq!(rig.step(TableChange::new()), None);
    assert_eq!(rig.rows(), ["a|1|10", "a|3|30", "b|2|20", "b|5|50"]);
}

#[test]
fn a_source_that_loses_every_row_starts_again() {
    let rig = two_groups(true, 2);
    let all = [key(0, 0), key(0, 1), key(1, 0), key(1, 1)];
    let change = TableChange::new().remove([10, 20, 30, 40, 50]);
    assert_eq!(
        rig.step(change),
        update([], all, [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows().len(), 0);
    // The groups keep their places: `b` is still the second.
    let change = TableChange::new().add([1, 2], [row("b", 1), row("c", 2)].concat_columns());
    assert_eq!(
        rig.step(change),
        update([key(1, 0), key(2, 0)], [], [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows(), ["b|1|10", "c|2|20"]);
}

#[test]
fn one_group_without_group_by_columns_comes_and_goes() {
    let columns = letters(&["a", "b", "a"], 1..=3);
    let rig = Rig::new([10, 20, 30], &columns, false, 2, &[]);
    assert_eq!(rig.rows(), ["2|b|20", "3|a|30"]);
    let all = ModifiedColumnSet::of([0, 1, 2]);
    let change = TableChange::new().remove([30]);
    assert_eq!(rig.step(change), update([], [], [0, 1], all.clone()));
    assert_eq!(rig.rows(), ["1|a|10", "2|b|20"]);
    let change = TableChange::new().remove([10]);
    assert_eq!(rig.step(change), update([], [1], [0], all.clone()));
    let change = TableChange::new().remove([20]);
    assert_eq!(
        rig.step(change),
        update([], [0], [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows().len(), 0);
    let change = TableChange::new().add([7], row("z", 9));
    assert_eq!(
        rig.step(change),
        update([0], [], [], ModifiedColumnSet::empty())
    );
    assert_eq!(rig.rows(), ["9|z|90"]);
    // Every column is one that is not a key.
    let change = TableChange::new().modify([7], [(1, strings(["y"]))]);
    assert_eq!(
        rig.step(change),
        update([], [], [0], ModifiedColumnSet::of([1]))
    );
    assert_eq!(rig.rows(), ["9|y|90"]);
}

#[test]
fn an_empty_source_fills_up() {
    let rig = Rig::new([], &letters(&[], []), true, 2, &["G"]);
    assert_eq!(rig.rows().len(), 0);
    let rows = [row("b", 1), row("a", 2), row("b", 3), row("b", 4)].concat_columns();
    let change = TableChange::new().add([1, 2, 3, 4], rows);
    assert_eq!(
        rig.step(change),
        update(
            [key(0, 0), key(0, 1), key(1, 0)],
            [],
            [],
            ModifiedColumnSet::empty()
        )
    );
    assert_eq!(rig.rows(), ["b|1|10", "b|3|30", "a|2|20"]);
}

#[test]
fn previous_values_are_those_of_the_previous_cycle() {
    // The checker reads the previous values of every row an update removes or modifies,
    // in the cycle; this test only makes sure that such updates are among the checked.
    let rig = two_groups(false, 2);
    let change = TableChange::new()
        .remove([40])
        .shift(50, 50, 7)
        .modify([57], [(0, ints([9]))])
        .add([58], row("a", 8));
    let expected = update(
        [],
        [],
        [key(0, 0), key(0, 1), key(1, 0), key(1, 1)],
        values(),
    );
    assert_eq!(rig.step(change), expected);
    assert_eq!(rig.rows(), ["a|3|30", "a|8|80", "b|2|20", "b|9|50"]);
    assert_eq!(rig.checker.updates_seen(), 1);
}

#[test]
fn a_failure_of_the_source_fails_the_result() {
    let rig = two_groups(true, 2);
    rig.script.push_failure("the source broke");
    rig.graph.run_cycle();
    assert!(rig.result.is_failed());
    let events = rig.recorder.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_ref().unwrap_err().message, "the source broke");
}

// ---------------------------------------------------------------------------------------
// Random changes

/// Runs random changes of a source with the columns `K` (int), `G` (String), `D`
/// (double), `V` (long), `Z` (Boolean), `L` (a vector of int) and `S` (an array of String)
/// through a `head_by` or `tail_by`. Returns the number of updates the result delivered.
fn random_run(seed: u64, head: bool, rows: i64, by: &'static [&'static str], steps: usize) -> u64 {
    let graph = graph();
    let mut random = RandomUpdates::new(seed);
    let columns = [
        ("K", ColumnType::Int),
        ("G", ColumnType::String),
        ("D", ColumnType::Double),
        ("V", ColumnType::Long),
        ("Z", ColumnType::Boolean),
        ("L", ColumnType::vector(ColumnType::Int)),
        ("S", ColumnType::array(ColumnType::String)),
    ];
    let initial = 8;
    let values = columns
        .iter()
        .map(|(_, t)| random.values(t, initial))
        .collect();
    let script = ScriptedTable::new(
        graph.clone(),
        definition(&columns),
        RowSet::flat(initial),
        values,
    );
    let rig = Rig::of(graph, script, head, rows, by);
    let source: Arc<dyn Table> = rig.script.table();
    for step in 0..steps {
        rig.script.push(random.next_change(&*source, 6));
        rig.graph.run_cycle();
        if let Err(error) = rig.checker.verify() {
            panic!("seed {seed}, step {step}: {error}");
        }
        assert!(!rig.result.is_failed(), "seed {seed}, step {step}: failed");
    }
    rig.checker.updates_seen()
}

#[test]
fn random_changes_by_one_key() {
    let mut updates = 0;
    for seed in test_seeds(0..150) {
        updates += random_run(seed, true, 2, &["G"], 30);
        updates += random_run(seed, false, 2, &["G"], 30);
    }
    assert!(
        updates > at_least_for(6000, 0..150),
        "only {updates} updates were checked"
    );
}

#[test]
fn random_changes_keeping_one_row_and_keeping_every_row() {
    let mut updates = 0;
    for seed in test_seeds(1000..1080) {
        updates += random_run(seed, true, 1, &["K"], 30);
        updates += random_run(seed, false, 1, &["G"], 30);
        updates += random_run(seed, false, 1000, &["K"], 30);
        updates += random_run(seed, true, i64::MAX, &["Z"], 30);
    }
    assert!(
        updates > at_least_for(7000, 1000..1080),
        "only {updates} updates were checked"
    );
}

#[test]
fn random_changes_by_several_keys_and_by_a_double() {
    let mut updates = 0;
    for seed in test_seeds(2000..2080) {
        updates += random_run(seed, true, 2, &["G", "Z"], 30);
        updates += random_run(seed, false, 3, &["Z", "K", "G"], 30);
        updates += random_run(seed, false, 2, &["D"], 30);
        updates += random_run(seed, true, 3, &["V", "D"], 30);
    }
    assert!(
        updates > at_least_for(7000, 2000..2080),
        "only {updates} updates were checked"
    );
}

#[test]
fn random_changes_without_keys() {
    let mut updates = 0;
    for seed in test_seeds(3000..3100) {
        updates += random_run(seed, true, 3, &[], 30);
        updates += random_run(seed, false, 3, &[], 30);
    }
    assert!(
        updates > at_least_for(4000, 3000..3100),
        "only {updates} updates were checked"
    );
}
