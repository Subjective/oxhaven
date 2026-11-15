use super::{flatten, merge, merge_sorted, MergeError};
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

// ---- merge_sorted --------------------------------------------------------------------------
// The expectations are what Java 42.9 answered to the same calls.

fn pairs(k: impl IntoIterator<Item = i32>, v: impl IntoIterator<Item = i32>) -> Arc<dyn Table> {
    static_table(&[
        ("K", ColumnType::Int, ints(k)),
        ("V", ColumnType::Int, ints(v)),
    ])
}

/// `K` from 0 to 2 with `V = K * 10`, and `K` from 1 to 3 with `V = (K - 1) * 100`.
fn two_sorted() -> (Arc<dyn Table>, Arc<dyn Table>) {
    (pairs(0..3, [0, 10, 20]), pairs(1..4, [0, 100, 200]))
}

/// The rows of a table of `K` and `V` as `K:V`.
fn rows_of(table: &Arc<dyn Table>) -> Vec<String> {
    let (k, v) = (cells(&**table, 0), cells(&**table, 1));
    k.iter().zip(&v).map(|(k, v)| format!("{k}:{v}")).collect()
}

fn sorted_rows(tables: &[Arc<dyn Table>], key: &str) -> Vec<String> {
    rows_of(&merge_sorted(tables, key).unwrap())
}

#[test]
fn a_sorted_merge_takes_the_least_next_row_and_the_earlier_table_on_a_tie() {
    let (a, b) = two_sorted();
    let merged = merge_sorted(&[a.clone(), b.clone()], "K").unwrap();
    assert_eq!(
        rows_of(&merged),
        ["0:0", "1:10", "1:0", "2:20", "2:100", "3:200"]
    );
    // a table of its own: static, the row keys 0..size, not flat, no attributes
    assert!(!merged.is_refreshing());
    assert!(!merged.is_flat());
    assert_eq!(keys_of(&*merged), [0, 1, 2, 3, 4, 5]);
    assert!(crate::table::attribute_map(&*merged).is_empty());

    assert_eq!(
        sorted_rows(&[b.clone(), a.clone()], "K"),
        ["0:0", "1:0", "1:10", "2:100", "2:20", "3:200"]
    );
    assert_eq!(
        sorted_rows(&[a.clone(), b.clone(), a.clone()], "K"),
        ["0:0", "0:0", "1:10", "1:0", "1:10", "2:20", "2:100", "2:20", "3:200"]
    );
    assert_eq!(
        sorted_rows(&[a.clone(), b.clone()], "V"),
        ["0:0", "1:0", "1:10", "2:20", "2:100", "3:200"]
    );
    assert_eq!(
        sorted_rows(std::slice::from_ref(&a), "K"),
        ["0:0", "1:10", "2:20"]
    );
}

#[test]
fn a_sorted_merge_does_not_sort_its_tables() {
    let (a, b) = two_sorted();
    let descending = sort(&a, &[SortColumn::descending("K")]).unwrap();
    assert_eq!(
        sorted_rows(std::slice::from_ref(&descending), "K"),
        ["2:20", "1:10", "0:0"]
    );
    // the least of the next rows at each step, whatever follows them
    assert_eq!(
        sorted_rows(&[descending, b], "K"),
        ["1:0", "2:20", "1:10", "0:0", "2:100", "3:200"]
    );
}

#[test]
fn a_sorted_merge_reads_tables_that_are_not_flat_empty_or_ordered_otherwise() {
    let (a, b) = two_sorted();
    // rows at the keys 0 and 2 of one table, 0 and 2 of the other
    let odd = RowSet::from_keys([0, 2]);
    let part = |table: &Arc<dyn Table>| -> Arc<dyn Table> {
        let snapshot = oxhaven_testkit::TableSnapshot::current(&**table).unwrap();
        ReferenceTable::new_static(table.definition().clone(), odd.clone(), {
            let keep = arrow::array::UInt32Array::from(vec![0, 2]);
            snapshot
                .columns
                .iter()
                .map(|column| arrow::compute::take(column, &keep, None).unwrap())
                .collect()
        })
    };
    assert_eq!(
        sorted_rows(&[part(&a), part(&b)], "K"),
        ["0:0", "1:0", "2:20", "3:200"]
    );

    let none = head(&a, 0).unwrap();
    assert!(sorted_rows(&[none.clone(), head(&b, 0).unwrap()], "K").is_empty());
    assert_eq!(
        sorted_rows(&[none, b.clone()], "K"),
        ["1:0", "2:100", "3:200"]
    );

    // the columns are matched by name; the result has the first table's order
    let turned = static_table(&[
        ("V", ColumnType::Int, ints([0, 100, 200])),
        ("K", ColumnType::Int, ints(1..4)),
    ]);
    let merged = merge_sorted(&[a, turned], "K").unwrap();
    assert_eq!(
        rows_of(&merged),
        ["0:0", "1:10", "1:0", "2:20", "2:100", "3:200"]
    );

    // no tables: a table without columns or rows
    let empty = merge_sorted(&[], "K").unwrap();
    assert!(empty.definition().columns().is_empty());
    assert_eq!(empty.row_set().size(), 0);
    assert!(!empty.is_flat());
}

#[test]
fn keys_are_compared_as_java_compares_the_boxed_values() {
    let keyed = |column_type: ColumnType, k: ArrayRef, v: &[i32]| {
        static_table(&[
            ("K", column_type, k),
            ("V", ColumnType::Int, ints(v.iter().copied())),
        ])
    };
    // -0.0 before 0.0, and NaN the greatest and equal to itself
    let d1 = keyed(
        ColumnType::Double,
        doubles([-1.0, -0.0, 0.0, f64::NAN]),
        &[1, 2, 3, 4],
    );
    let d2 = keyed(
        ColumnType::Double,
        doubles([-0.0, 0.0, 1.0, f64::NAN]),
        &[10, 20, 30, 40],
    );
    let values = |tables: &[Arc<dyn Table>]| cells(&*merge_sorted(tables, "K").unwrap(), 1);
    assert_eq!(
        values(&[d1.clone(), d2.clone()]),
        ["1", "2", "10", "3", "20", "30", "4", "40"]
    );
    assert_eq!(
        values(&[d2, d1]),
        ["1", "10", "2", "20", "3", "30", "40", "4"]
    );
    // strings by code unit: upper case first
    let s1 = keyed(ColumnType::String, strings(["A", "a", "b"]), &[1, 2, 3]);
    let s2 = keyed(ColumnType::String, strings(["B", "a", "c"]), &[10, 20, 30]);
    assert_eq!(values(&[s1, s2]), ["1", "10", "2", "20", "3", "30"]);
    // false before true
    let flags = || {
        keyed(
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![false, true])),
            &[1, 2],
        )
    };
    assert_eq!(values(&[flags(), flags()]), ["1", "1", "2", "2"]);
}

#[test]
fn a_null_key_is_refused_only_where_it_is_compared() {
    let with_null = |k: [Option<i32>; 3]| {
        static_table(&[
            (
                "K",
                ColumnType::Int,
                Arc::new(arrow::array::Int32Array::from(k.to_vec())) as ArrayRef,
            ),
            ("V", ColumnType::Int, ints(0..3)),
        ])
    };
    let (a, _) = two_sorted();
    // one table alone has no key to be compared with
    let alone = with_null([Some(0), None, Some(2)]);
    assert_eq!(
        cells(
            &*merge_sorted(std::slice::from_ref(&alone), "K").unwrap(),
            1
        ),
        ["0", "1", "2"]
    );
    // the null is the key of the cursor that is compared
    assert_eq!(
        error(merge_sorted(&[alone, a.clone()], "K")),
        MergeError::NullKey(
            "Cannot invoke \"java.lang.Comparable.compareTo(Object)\" because \
             \"this.currentKey\" is null"
                .into()
        )
    );
    // the null is the key it is compared with
    let first = with_null([None, Some(1), Some(2)]);
    assert!(matches!(
        error(merge_sorted(&[first, a.clone()], "K")),
        MergeError::NullKey(_)
    ));
    // the null comes when the other table has no rows left
    let late = with_null([Some(7), None, Some(9)]);
    assert_eq!(
        cells(&*merge_sorted(&[a, late], "K").unwrap(), 1),
        ["0", "10", "20", "0", "1", "2"]
    );
}

#[test]
fn a_sorted_merge_refuses_what_java_refuses() {
    let (a, b) = two_sorted();
    let both = |other: Arc<dyn Table>, key: &str| error(merge_sorted(&[a.clone(), other], key));
    assert_eq!(
        both(b.clone(), "Q"),
        MergeError::NoSuchColumn(
            "Unknown column names [Q], available column names are [K, V]".into()
        )
    );
    assert_eq!(
        both(b.clone(), ""),
        MergeError::NoSuchColumn(
            "Unknown column names [], available column names are [K, V]".into()
        )
    );
    let other_name = static_table(&[
        ("K", ColumnType::Int, ints(1..4)),
        ("W", ColumnType::Int, ints(1..4)),
    ]);
    assert_eq!(
        both(other_name, "K"),
        MergeError::Runtime("Incompatible column sources: [K, V] and [K, W]".into())
    );
    let more = static_table(&[
        ("K", ColumnType::Int, ints(1..4)),
        ("V", ColumnType::Int, ints(1..4)),
        ("Z", ColumnType::Int, ints(1..4)),
    ]);
    assert_eq!(
        both(more, "K"),
        MergeError::Runtime("Incompatible column sources: [K, V] and [K, V, Z]".into())
    );
    let fewer = static_table(&[("K", ColumnType::Int, ints(1..4))]);
    assert_eq!(
        both(fewer, "K"),
        MergeError::Runtime("Incompatible column sources: [K, V] and [K]".into())
    );
    let other_type = static_table(&[
        ("K", ColumnType::Int, ints(1..4)),
        ("V", ColumnType::Long, oxhaven_testkit::longs([1, 2, 3])),
    ]);
    assert_eq!(
        both(other_type, "K"),
        MergeError::Runtime(
            "Requirement failed: required source.getType() == innerSources.get(0).getType(), \
             instead source.getType() == long, innerSources.get(0).getType() == int."
                .into()
        )
    );

    let graph = graph();
    let ticking: Arc<dyn Table> = scripted(&graph, [1], &columns([1], &["a"])).table();
    assert_eq!(
        error(merge_sorted(&[fixed([1], &["a"]), ticking], "K")),
        MergeError::Unsupported("mergeSorted does not yet support refreshing tables!".into())
    );
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.util.MergeSortedHelper")]
fn a_sorted_merge_by_a_vector_column_is_not_ported() {
    let vectors = || {
        let column_type = ColumnType::vector(ColumnType::Int);
        let k = oxhaven_testkit::list_of(&column_type, [Some(1), Some(1)], ints([1, 2]));
        static_table(&[("K", column_type, k)])
    };
    let _ = merge_sorted(&[vectors(), vectors()], "K");
}

#[test]
fn a_merge_is_marked_as_one() {
    let merged = merge(&[fixed([1], &["a"]), fixed([2], &["b"])]).unwrap();
    assert_eq!(
        crate::table::attribute_map(&*merged),
        [("MergedTable", crate::table::AttributeValue::Bool(true))]
    );
}
