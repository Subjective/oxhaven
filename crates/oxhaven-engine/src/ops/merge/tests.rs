use super::{flatten, merge, MergeError};
use crate::ops::sort::testing::{
    blink_table, cells, check_random, graph, keys, keys_of, operation, scripted, shifts,
    static_table, tags, Ticking,
};
use crate::ops::sort::{head, reverse, sort, tail, SortColumn};
use arrow::array::{ArrayRef, BooleanArray};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{
    definition, doubles, ints, strings, ConsistencyChecker, ReferenceTable, TableChange,
};
use std::sync::Arc;

/// The size of the key space a small table gets in a merge.
const SLOT: RowKey = 1 << 16;

fn columns(
    k: impl IntoIterator<Item = i32>,
    tag: &[&str],
) -> [(&'static str, ColumnType, ArrayRef); 2] {
    [
        ("K", ColumnType::Int, ints(k)),
        ("Tag", ColumnType::String, strings(tag.iter().copied())),
    ]
}

fn fixed(k: impl IntoIterator<Item = i32>, tag: &[&str]) -> Arc<dyn Table> {
    static_table(&columns(k, tag))
}

fn rows(k: impl IntoIterator<Item = i32>, tag: &[&str]) -> Vec<ArrayRef> {
    vec![ints(k), strings(tag.iter().copied())]
}

fn error(result: Result<Arc<dyn Table>, MergeError>) -> MergeError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("expected an error"),
    }
}

// ---- merge ---------------------------------------------------------------------------------

#[test]
fn a_merge_is_its_tables_one_after_the_other() {
    let tables = [
        fixed([1, 2], &["a", "b"]),
        fixed([3], &["c"]),
        fixed([], &[]),
        fixed([4, 5], &["d", "e"]),
    ];
    let merged = merge(&tables).unwrap();
    assert_eq!(tags(&*merged).concat(), "abcde");
    assert_eq!(cells(&*merged, 0), ["1", "2", "3", "4", "5"]);
    // Every table has its own range of row keys, an empty table too.
    assert_eq!(keys_of(&*merged), [0, 1, SLOT, 3 * SLOT, 3 * SLOT + 1]);
    assert!(!merged.is_refreshing());
    assert!(!merged.is_flat());
    assert_eq!(merged.definition(), tables[0].definition());

    let one = merge(&tables[..1]).unwrap();
    assert_eq!(tags(&*one).concat(), "ab");
}

#[test]
fn a_table_may_be_merged_with_itself() {
    let table = fixed([1, 2], &["a", "b"]);
    let merged = merge(&[table.clone(), table.clone(), table]).unwrap();
    assert_eq!(tags(&*merged).concat(), "ababab");
}

#[test]
fn columns_are_matched_by_name() {
    let first = fixed([1], &["a"]);
    let second = static_table(&[
        ("Tag", ColumnType::String, strings(["b"])),
        ("K", ColumnType::Int, ints([2])),
    ]);
    let merged = merge(&[first.clone(), second.clone()]).unwrap();
    assert_eq!(merged.definition(), first.definition());
    assert_eq!(cells(&*merged, 0), ["1", "2"]);
    assert_eq!(cells(&*merged, 1), ["a", "b"]);
    // The first table decides the order of the columns.
    let merged = merge(&[second.clone(), first]).unwrap();
    assert_eq!(merged.definition(), second.definition());
    assert_eq!(cells(&*merged, 0), ["b", "a"]);
}

#[test]
fn tables_with_other_columns_are_refused() {
    let first = fixed([1], &["a"]);
    let missing = static_table(&[("K", ColumnType::Int, ints([2]))]);
    let extra = static_table(&[
        ("K", ColumnType::Int, ints([2])),
        ("Tag", ColumnType::String, strings(["b"])),
        ("More", ColumnType::Int, ints([2])),
    ]);
    let other_type = static_table(&[
        ("K", ColumnType::String, strings(["2"])),
        ("Tag", ColumnType::String, strings(["b"])),
    ]);
    for (table, part) in [
        (missing, "constituent column 'Tag' is missing"),
        (extra, "expected column 'More' is missing"),
        (
            other_type,
            "column 'K' is Int in expected and String in constituent",
        ),
    ] {
        match error(merge(&[first.clone(), table])) {
            MergeError::IncompatibleDefinitions(message) => {
                assert!(message.contains(part), "{message}");
            }
            other => panic!("unexpected error {other:?}"),
        }
    }
}

#[test]
fn a_merge_of_nothing_is_refused() {
    assert_eq!(
        error(merge(&[])),
        MergeError::IllegalArgument("No non-null tables provided to merge".into())
    );
}

#[test]
fn tables_of_two_update_graphs_are_refused() {
    let first = scripted(&graph(), [1], &columns([1], &["a"]));
    let second = scripted(&graph(), [1], &columns([2], &["b"]));
    let tables: [Arc<dyn Table>; 2] = [first.table(), second.table()];
    assert!(matches!(
        error(merge(&tables)),
        MergeError::IllegalArgument(_)
    ));
    // A static table has no graph to differ in.
    let tables = [tables[0].clone(), fixed([3], &["c"])];
    assert!(merge(&tables).unwrap().is_refreshing());
}

/// `xy`, then the rows a..d at the keys 10, 20, 30, 40, then `z`.
fn ticking_merge() -> Ticking {
    let before = fixed([8, 9], &["x", "y"]);
    let after = fixed([7], &["z"]);
    let ticking = Ticking::new(
        [10, 20, 30, 40],
        &columns([3, 1, 3, 2], &["a", "b", "c", "d"]),
        move |table| merge(&[before.clone(), table.clone(), after.clone()]).unwrap(),
    );
    assert_eq!(tags(&*ticking.result).concat(), "xyabcdz");
    ticking
}

#[test]
fn a_merge_follows_a_constituent() {
    let t = ticking_merge();
    let update = t
        .step(TableChange::new().add([5, 50], rows([0, 0], &["e", "f"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyeabcdfz");
    assert_eq!(keys(&update.added), [SLOT + 5, SLOT + 50]);
    assert!(update.removed.is_empty() && update.shifted.is_empty());

    let update = t.step(TableChange::new().remove([20, 50])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyeacdz");
    assert_eq!(keys(&update.removed), [SLOT + 20, SLOT + 50]);

    let update = t
        .step(TableChange::new().modify([30], [(1, strings(["q"]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyeaqdz");
    assert_eq!(keys(&update.modified), [SLOT + 30]);
    assert!(update.modified_columns.contains(1) && !update.modified_columns.contains(0));

    let update = t.step(TableChange::new().shift(30, 40, 7)).unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyeaqdz");
    assert_eq!(shifts(&update), [(SLOT + 30, SLOT + 40, 7)]);
    assert_eq!(
        keys_of(&*t.result),
        [0, 1, SLOT + 5, SLOT + 10, SLOT + 37, SLOT + 47, 2 * SLOT]
    );

    assert!(t.step(TableChange::new()).is_none());
}

#[test]
fn a_constituent_that_outgrows_its_range_moves_the_ones_after_it() {
    let t = ticking_merge();
    let update = t
        .step(TableChange::new().add([SLOT + 3], rows([0], &["e"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyabcdez");
    assert_eq!(keys(&update.added), [2 * SLOT + 3]);
    // z moves up by the growth.
    assert_eq!(shifts(&update), [(2 * SLOT, 3 * SLOT - 1, SLOT)]);
    assert_eq!(t.result.row_set().last_row_key(), Some(3 * SLOT));

    // A range does not shrink again.
    let update = t.step(TableChange::new().remove([SLOT + 3])).unwrap();
    assert!(update.shifted.is_empty());
    assert_eq!(t.result.row_set().last_row_key(), Some(3 * SLOT));
    assert_eq!(tags(&*t.result).concat(), "xyabcdz");

    // Growth by several units with rows shifted inside the constituent in the same cycle.
    let change = TableChange::new()
        .shift(30, 40, 5 * SLOT)
        .add([20 * SLOT], rows([0], &["f"]));
    t.step(change).unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyabcdfz");
    assert_eq!(t.result.row_set().last_row_key(), Some(22 * SLOT));
}

#[test]
fn modified_columns_are_named_by_the_columns_of_the_result() {
    let first = static_table(&[
        ("Tag", ColumnType::String, strings(["x"])),
        ("K", ColumnType::Int, ints([9])),
    ]);
    let t = Ticking::new([10], &columns([1], &["a"]), move |table| {
        merge(&[first.clone(), table.clone()]).unwrap()
    });
    // K is column 0 of the constituent and column 1 of the result.
    let update = t
        .step(TableChange::new().modify([10], [(0, ints([5]))]))
        .unwrap();
    assert!(update.modified_columns.contains(1) && !update.modified_columns.contains(0));
    assert_eq!(cells(&*t.result, 1), ["9", "5"]);
}

#[test]
fn a_constituent_that_empties_and_refills() {
    let t = ticking_merge();
    let update = t.step(TableChange::new().remove([10, 20, 30, 40])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyz");
    assert_eq!(update.removed.size(), 4);
    t.step(TableChange::new().add([3], rows([0], &["e"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "xyez");
}

#[test]
fn a_ticking_table_merged_with_itself_changes_twice() {
    let t = Ticking::new([10], &columns([1], &["a"]), |table| {
        merge(&[table.clone(), table.clone()]).unwrap()
    });
    let update = t
        .step(TableChange::new().add([20], rows([2], &["b"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "abab");
    assert_eq!(keys(&update.added), [20, SLOT + 20]);
}

#[test]
fn two_ticking_constituents_change_in_one_cycle() {
    let graph = graph();
    let first = scripted(&graph, [1, 2], &columns([1, 2], &["a", "b"]));
    let second = scripted(&graph, [1], &columns([3], &["c"]));
    let tables: [Arc<dyn Table>; 2] = [first.table(), second.table()];
    let merged = merge(&tables).unwrap();
    let checker = ConsistencyChecker::attach(merged.clone());

    first.push(
        TableChange::new()
            .remove([1])
            .add([SLOT], rows([0], &["d"])),
    );
    second.push(TableChange::new().add([0], rows([0], &["e"])));
    graph.run_cycle();
    assert_eq!(tags(&*merged).concat(), "bdec");
    // Only the second changes.
    second.push(TableChange::new().modify([1], [(0, ints([9]))]));
    graph.run_cycle();
    assert_eq!(cells(&*merged, 0), ["2", "0", "0", "9"]);
    checker.assert_consistent();
    assert_eq!(checker.updates_seen(), 2);
}

#[test]
fn a_merge_fails_with_a_constituent() {
    let t = ticking_merge();
    let failure = t.fail("boom");
    assert!(failure.message.contains("boom"), "{failure}");
    assert!(t.result.is_failed());
    let source: Arc<dyn Table> = t.script.table();
    assert!(matches!(
        error(merge(&[fixed([1], &["a"]), source.clone()])),
        MergeError::Table(_)
    ));
    assert!(flatten(&source).is_err());
}

const BLINK_COLUMNS: [(&str, ColumnType); 2] =
    [("K", ColumnType::Int), ("Tag", ColumnType::String)];

#[test]
fn a_merge_of_blink_tables_is_a_blink_table() {
    let graph = graph();
    let (first, first_rows) = blink_table(&graph, &BLINK_COLUMNS);
    let (second, second_rows) = blink_table(&graph, &BLINK_COLUMNS);
    let merged = merge(&[first.clone(), second.clone()]).unwrap();
    assert!(merged.attributes().blink);
    let checker = ConsistencyChecker::attach(merged.clone());

    first_rows.push(rows([1, 2], &["a", "b"]));
    second_rows.push(rows([3], &["c"]));
    graph.run_cycle();
    assert_eq!(tags(&*merged).concat(), "abc");
    second_rows.push(rows([4], &["d"]));
    graph.run_cycle();
    assert_eq!(tags(&*merged).concat(), "d");
    graph.run_cycle();
    assert_eq!(merged.row_set().size(), 0);
    checker.assert_consistent();

    // Not with a table that is not a blink table.
    let mixed = merge(&[first.clone(), fixed([1], &["a"])]).unwrap();
    assert!(!mixed.attributes().blink);

    // Only the last table can make the result add-only: a table before it that grows
    // could move it.
    let growing = head(&first, 5).unwrap();
    assert!(growing.attributes().append_only);
    let attributes = merge(&[fixed([1], &["a"]), growing.clone()])
        .unwrap()
        .attributes();
    assert!(attributes.add_only && attributes.append_only);
    let attributes = merge(&[growing.clone(), growing.clone()])
        .unwrap()
        .attributes();
    assert!(!attributes.add_only && !attributes.append_only);
    let attributes = merge(&[growing, fixed([1], &["a"])]).unwrap().attributes();
    assert!(!attributes.add_only && !attributes.append_only);
}

// ---- flatten -------------------------------------------------------------------------------

#[test]
fn flatten_of_a_static_table() {
    let sparse: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&[("Tag", ColumnType::String)]),
        RowSet::from_keys([3, 7, 100]),
        vec![strings(["a", "b", "c"])],
    );
    let flat = flatten(&sparse).unwrap();
    assert!(flat.is_flat());
    assert!(!flat.is_refreshing());
    assert_eq!(keys_of(&*flat), [0, 1, 2]);
    assert_eq!(tags(&*flat).concat(), "abc");
    // A flat table is its own flattening.
    assert!(Arc::ptr_eq(&flatten(&flat).unwrap(), &flat));

    // A table at the keys of a flat table that is not marked flat.
    let dense = fixed([1, 2], &["a", "b"]);
    assert!(!dense.is_flat());
    let flat = flatten(&dense).unwrap();
    assert!(flat.is_flat());
    assert_eq!(tags(&*flat).concat(), "ab");

    let merged = merge(&[dense.clone(), dense]).unwrap();
    let flat = flatten(&merged).unwrap();
    assert_eq!(keys_of(&*flat), [0, 1, 2, 3]);
    assert_eq!(tags(&*flat).concat(), "abab");
}

#[test]
fn a_flattened_table_follows_its_parent() {
    let t = Ticking::new(
        [10, 20, 30, 40],
        &columns([0, 1, 2, 3], &["a", "b", "c", "d"]),
        |table| flatten(table).unwrap(),
    );
    assert!(t.result.is_flat());
    assert_eq!(keys_of(&*t.result), [0, 1, 2, 3]);

    let update = t
        .step(TableChange::new().add([5, 25], rows([9, 9], &["e", "f"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "eabfcd");
    assert_eq!(keys(&update.added), [0, 3]);
    assert_eq!(shifts(&update), [(0, 1, 1), (2, 3, 2)]);

    let update = t.step(TableChange::new().remove([10, 30])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "ebfd");
    assert_eq!(keys(&update.removed), [1, 4]);
    assert_eq!(shifts(&update), [(2, 3, -1), (5, 5, -2)]);

    let update = t
        .step(TableChange::new().modify([25], [(1, strings(["z"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [2]);
    assert!(update.modified_columns.contains(1) && !update.modified_columns.contains(0));
    assert_eq!(tags(&*t.result).concat(), "ebzd");

    // A shift of the parent moves no position.
    assert!(t.step(TableChange::new().shift(20, 40, 100)).is_none());
    assert!(t.step(TableChange::new()).is_none());
    let update = t
        .step(TableChange::new().modify([140], [(0, ints([7]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [3]);
    assert_eq!(keys_of(&*t.result), [0, 1, 2, 3]);

    t.fail("boom");
    assert!(t.result.is_failed());
}

#[test]
fn flatten_keeps_the_blink_attribute() {
    let graph = graph();
    let (table, blink) = blink_table(&graph, &BLINK_COLUMNS);
    // A reversed blink table is a blink table that is not flat.
    let reversed = reverse(&table).unwrap();
    let flat = flatten(&reversed).unwrap();
    assert!(flat.is_flat() && flat.attributes().blink);
    let checker = ConsistencyChecker::attach(flat.clone());
    blink.push(rows([1, 2], &["a", "b"]));
    graph.run_cycle();
    assert_eq!(tags(&*flat).concat(), "ba");
    blink.push(rows([3], &["c"]));
    graph.run_cycle();
    assert_eq!(tags(&*flat).concat(), "c");
    checker.assert_consistent();
}

// ---- Random updates ------------------------------------------------------------------------

const RANDOM_COLUMNS: [(&str, ColumnType); 4] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
];

fn by_k(table: &Arc<dyn Table>) -> Arc<dyn Table> {
    let columns = [SortColumn::ascending("K"), SortColumn::descending("S")];
    sort(table, &columns).unwrap()
}

#[test]
fn random_updates_through_merges() {
    let operations = [
        operation("merge", |tables| merge(tables).unwrap()),
        operation("merge of two", |tables| merge(&tables[1..]).unwrap()),
        operation("merge of one", |tables| merge(&tables[..1]).unwrap()),
        operation("merge with itself", |tables| {
            merge(&[tables[0].clone(), tables[1].clone(), tables[0].clone()]).unwrap()
        }),
        operation("merge of merges", |tables| {
            let inner = merge(&tables[..2]).unwrap();
            merge(&[tables[2].clone(), inner]).unwrap()
        }),
        operation("sort of merge", |tables| by_k(&merge(tables).unwrap())),
        operation("merge of sorts", |tables| {
            merge(&[by_k(&tables[0]), by_k(&tables[1])]).unwrap()
        }),
        operation("flatten of merge", |tables| {
            flatten(&merge(tables).unwrap()).unwrap()
        }),
        operation("head of merge", |tables| {
            head(&merge(tables).unwrap(), 5).unwrap()
        }),
        operation("tail of merge", |tables| {
            tail(&merge(tables).unwrap(), 5).unwrap()
        }),
        operation("merge of flatten and tail", |tables| {
            merge(&[flatten(&tables[0]).unwrap(), tail(&tables[1], 2).unwrap()]).unwrap()
        }),
        operation("flatten", |tables| flatten(&tables[2]).unwrap()),
        operation("flatten of flatten", |tables| {
            flatten(&flatten(&tables[1]).unwrap()).unwrap()
        }),
    ];
    check_random(0..40, 30, 3, &RANDOM_COLUMNS, &operations);
}

#[test]
fn random_updates_through_a_merge_with_a_static_table() {
    let booleans = BooleanArray::from(vec![Some(true), None]);
    let fixed = static_table(&[
        ("K", ColumnType::Int, ints([4, -4])),
        ("D", ColumnType::Double, doubles([0.5, f64::NAN])),
        ("S", ColumnType::String, strings(["s", "t"])),
        ("B", ColumnType::Boolean, Arc::new(booleans)),
    ]);
    let operations = [
        operation("static first", {
            let fixed = fixed.clone();
            move |tables| merge(&[fixed.clone(), tables[0].clone()]).unwrap()
        }),
        operation("static between", {
            let fixed = fixed.clone();
            move |tables| merge(&[tables[0].clone(), fixed.clone(), tables[0].clone()]).unwrap()
        }),
    ];
    check_random(0..40, 30, 1, &RANDOM_COLUMNS, &operations);
}
