//! Rollups and their views. The expansions and the order of rows of the scripted cases
//! are what Java 42.9 was observed to answer (`oracle/scenarios/python/rollup_*.py`).

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{AggSpec, Aggregation, Pair};
use crate::ops::filter::CheckedFilters;
use crate::ops::sort::SortColumn;
use arrow::array::{Array, Int32Array, Int8Array};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    at_least_for, definition, ints, strings, test_seeds, RandomUpdates, ReferenceTable,
    ScriptedTable, TableChange,
};
use std::collections::BTreeMap;

fn cell(column: &ArrayRef, row: usize) -> String {
    if column.is_null(row) {
        "-".to_string()
    } else {
        array_value_to_string(column, row).expect("a printable cell")
    }
}

/// The rows of a snapshot, each as the text of its cells joined by `|`, null as `-`.
fn text(snapshot: &Snapshot) -> Vec<String> {
    (0..snapshot.rows as usize)
        .map(|row| {
            snapshot
                .columns
                .iter()
                .map(|column| cell(column, row))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn aggregations() -> Vec<Aggregation> {
    vec![
        Aggregation::columns(AggSpec::Sum, [Pair::of("Qty")]),
        Aggregation::count("Rows"),
    ]
}

/// The table of the IDE's rollup route.
fn trades() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[
            ("Sym", ColumnType::String),
            ("Ex", ColumnType::String),
            ("Qty", ColumnType::Int),
        ]),
        RowSet::flat(6),
        vec![
            strings(["AAPL", "AAPL", "MSFT", "AAPL", "MSFT", "GOOG"]),
            strings(["N", "Q", "N", "N", "Q", "Q"]),
            ints([10, 20, 30, 40, 50, 60]),
        ],
    )
}

fn trades_rollup() -> Arc<dyn Hierarchy> {
    Arc::new(rollup(&trades(), &aggregations(), false, &["Sym", "Ex"], None).unwrap())
}

/// A key table of the rollup of [`trades`]: per row a depth, the key values and an
/// action.
fn keys(rows: &[(i32, &str, &str, i8)]) -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[
            ("Sym", ColumnType::String),
            ("Ex", ColumnType::String),
            ("__DEPTH__", ColumnType::Int),
            ("Action", ColumnType::Byte),
        ]),
        RowSet::flat(rows.len() as u64),
        vec![
            strings(rows.iter().map(|row| row.1)),
            strings(rows.iter().map(|row| row.2)),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
}

const EXPAND: i8 = 1;
const EXPAND_ALL: i8 = 3;
const CONTRACT: i8 = 4;

fn view_of(rows: &[(i32, &str, &str, i8)]) -> Vec<String> {
    let view = View::new(
        trades_rollup(),
        Some(keys(rows)),
        Some("Action".to_string()),
    );
    text(&view.snapshot(None, 0, 100).unwrap())
}

const COLLAPSED: [&str; 4] = [
    "1|true|-|-|210|6",
    "2|false|AAPL|-|70|3",
    "2|false|MSFT|-|80|2",
    "2|false|GOOG|-|60|1",
];

const ALL: [&str; 9] = [
    "1|true|-|-|210|6",
    "2|true|AAPL|-|70|3",
    "3|-|AAPL|N|50|2",
    "3|-|AAPL|Q|20|1",
    "2|true|MSFT|-|80|2",
    "3|-|MSFT|N|30|1",
    "3|-|MSFT|Q|50|1",
    "2|true|GOOG|-|60|1",
    "3|-|GOOG|Q|60|1",
];

#[test]
fn without_a_key_table_the_grand_total_is_expanded() {
    let view = View::new(trades_rollup(), None, None);
    let snapshot = view.snapshot(None, 0, 100).unwrap();
    assert_eq!(snapshot.size, 4);
    assert_eq!(text(&snapshot), COLLAPSED);
    let names: Vec<&str> = view
        .hierarchy()
        .columns()
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["__DEPTH__", "__EXPANDED__", "Sym", "Ex", "Qty", "Rows"]
    );
}

#[test]
fn an_expanded_node_shows_its_children_after_it() {
    assert_eq!(
        view_of(&[(2, "AAPL", "", EXPAND)]),
        [
            "1|true|-|-|210|6",
            "2|true|AAPL|-|70|3",
            "3|-|AAPL|N|50|2",
            "3|-|AAPL|Q|20|1",
            "2|false|MSFT|-|80|2",
            "2|false|GOOG|-|60|1",
        ]
    );
    // without an action column every row expands
    let view = View::new(
        trades_rollup(),
        Some(keys(&[(2, "GOOG", "", CONTRACT)])),
        None,
    );
    assert_eq!(
        text(&view.snapshot(None, 0, 100).unwrap()),
        [
            "1|true|-|-|210|6",
            "2|false|AAPL|-|70|3",
            "2|false|MSFT|-|80|2",
            "2|true|GOOG|-|60|1",
            "3|-|GOOG|Q|60|1",
        ]
    );
}

#[test]
fn expand_all_expands_what_is_not_contracted() {
    assert_eq!(view_of(&[(0, "", "", EXPAND_ALL)]), ALL);
    assert_eq!(view_of(&[(1, "", "", EXPAND_ALL)]), ALL);
    assert_eq!(
        view_of(&[(1, "", "", EXPAND_ALL), (2, "MSFT", "", CONTRACT)]),
        [
            "1|true|-|-|210|6",
            "2|true|AAPL|-|70|3",
            "3|-|AAPL|N|50|2",
            "3|-|AAPL|Q|20|1",
            "2|false|MSFT|-|80|2",
            "2|true|GOOG|-|60|1",
            "3|-|GOOG|Q|60|1",
        ]
    );
    // a contraction without an expand-all above it does nothing
    assert_eq!(view_of(&[(2, "MSFT", "", CONTRACT)]), COLLAPSED);
    // the grand total contracted: one row
    assert_eq!(view_of(&[(1, "", "", CONTRACT)]), ["1|false|-|-|210|6"]);
    // the root contracted: nothing
    assert!(view_of(&[(0, "", "", CONTRACT)]).is_empty());
}

#[test]
fn expand_to_depth_stops_after_its_levels() {
    // 0x70 is one level, which is a plain expand
    assert_eq!(view_of(&[(0, "", "", 0x70)]), ["1|false|-|-|210|6"]);
    assert_eq!(view_of(&[(0, "", "", 0x71)]), COLLAPSED);
    assert_eq!(view_of(&[(0, "", "", 0x72)]), ALL);
    assert_eq!(view_of(&[(1, "", "", 0x71)]), ALL);
}

#[test]
fn the_last_row_of_a_key_counts_and_unknown_keys_do_not() {
    assert_eq!(
        view_of(&[(2, "AAPL", "", EXPAND), (2, "AAPL", "", CONTRACT)]),
        COLLAPSED
    );
    assert_eq!(view_of(&[(2, "TSLA", "", EXPAND)]), COLLAPSED);
    // a row of the last level cannot be expanded
    assert_eq!(view_of(&[(3, "AAPL", "N", EXPAND)]), COLLAPSED);
    // key values beyond the depth are ignored
    assert_eq!(
        view_of(&[(2, "GOOG", "nothing", EXPAND)]),
        [
            "1|true|-|-|210|6",
            "2|false|AAPL|-|70|3",
            "2|false|MSFT|-|80|2",
            "2|true|GOOG|-|60|1",
            "3|-|GOOG|Q|60|1",
        ]
    );
}

#[test]
fn a_bad_key_table_is_refused() {
    let refused = |rows: &[(i32, &str, &str, i8)]| {
        View::new(
            trades_rollup(),
            Some(keys(rows)),
            Some("Action".to_string()),
        )
        .snapshot(None, 0, 10)
        .unwrap_err()
    };
    assert_eq!(
        refused(&[(4, "AAPL", "N", EXPAND)]),
        HierarchicalError::IllegalArgument(
            "Invalid depth 4, maximum for this rollup is 3".to_string()
        )
    );
    assert_eq!(
        refused(&[(2, "AAPL", "", 2)]),
        HierarchicalError::IllegalArgument("Unrecognized key table action 2".to_string())
    );
    let view = View::new(trades_rollup(), None, None);
    assert!(matches!(
        view.snapshot(Some(&[0, 2]), 0, 10),
        Err(HierarchicalError::IllegalArgument(_))
    ));
}

#[test]
fn a_viewport_is_a_range_of_the_whole_walk() {
    let view = View::new(
        trades_rollup(),
        Some(keys(&[(0, "", "", EXPAND_ALL)])),
        Some("Action".to_string()),
    );
    for first in 0..11 {
        for count in 0..11 {
            let snapshot = view.snapshot(None, first, count).unwrap();
            assert_eq!(snapshot.size, 9);
            let expected: Vec<&str> = ALL
                .iter()
                .copied()
                .skip(first as usize)
                .take(count as usize)
                .collect();
            assert_eq!(text(&snapshot), expected, "rows {first} +{count}");
        }
    }
    // a subset of the columns, in the order asked
    let snapshot = view.snapshot(Some(&[4, 0, 1]), 1, 2).unwrap();
    assert_eq!(text(&snapshot), ["70|2|true", "50|3|-"]);
}

#[test]
fn a_rollup_without_keys_is_the_grand_total() {
    let rollup: Arc<dyn Hierarchy> =
        Arc::new(rollup(&trades(), &aggregations(), false, &[], None).unwrap());
    let snapshot = View::new(rollup, None, None).snapshot(None, 0, 10).unwrap();
    assert_eq!(text(&snapshot), ["1|-|210|6"]);
}

#[test]
fn java_refuses_some_aggregations() {
    let refused = |spec: AggSpec| {
        rollup(
            &trades(),
            &[Aggregation::columns(spec, [Pair::of("Qty")])],
            false,
            &["Sym"],
            None,
        )
        .err()
    };
    assert_eq!(
        refused(AggSpec::Median {
            average_evenly_divided: true
        }),
        Some(HierarchicalError::Unsupported(
            "AggMedian is not supported for rollup()".to_string()
        ))
    );
    assert_eq!(
        refused(AggSpec::Percentile {
            percentile: 0.5,
            average_evenly_divided: false
        }),
        Some(HierarchicalError::Unsupported(
            "AggPercentile is not supported for rollup()".to_string()
        ))
    );
    assert_eq!(refused(AggSpec::Max), None);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#makeRollup"
)]
fn constituents_are_not_ported() {
    let _ = rollup(&trades(), &aggregations(), true, &["Sym"], None);
}

/// What a rollup of `source` by its columns `K` and `G` with the sum of `V` and a count
/// must show with everything expanded: per key prefix, the sum and the count.
fn expected_rows(source: &dyn Table) -> Vec<String> {
    let rows = source.row_set();
    let columns: Vec<ArrayRef> = (0..3)
        .map(|index| source.column(index).chunk(&rows).unwrap())
        .collect();
    let values = columns[2].as_any().downcast_ref::<Int32Array>().unwrap();
    let mut groups: BTreeMap<(usize, String, String), (Option<i64>, i64)> = BTreeMap::new();
    for row in 0..rows.size() as usize {
        let (k, g) = (cell(&columns[0], row), cell(&columns[1], row));
        for key in [
            (1, "-".to_string(), "-".to_string()),
            (2, k.clone(), "-".to_string()),
            (3, k, g),
        ] {
            let group = groups.entry(key).or_insert((None, 0));
            group.1 += 1;
            if values.is_valid(row) {
                group.0 = Some(group.0.unwrap_or(0) + i64::from(values.value(row)));
            }
        }
    }
    groups
        .into_iter()
        .map(|((depth, k, g), (sum, count))| {
            let sum = sum.map_or("-".to_string(), |sum| sum.to_string());
            format!("{depth}|{k}|{g}|{sum}|{count}")
        })
        .collect()
}

/// Random changes of the source: after every cycle the walk with everything expanded
/// shows each group once, with the values of its rows, every row after its parent, and
/// any viewport is a range of it.
fn random_rollup(seed: u64) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    let columns = [
        ("K", ColumnType::String),
        ("G", ColumnType::Int),
        ("V", ColumnType::Int),
    ];
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&columns),
        RowSet::flat(20),
        columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, 20))
            .collect(),
    );
    let parent: Arc<dyn Table> = source.table();
    let aggregations = [
        Aggregation::columns(AggSpec::Sum, [Pair::of("V")]),
        Aggregation::count("N"),
    ];
    let rollup: Arc<dyn Hierarchy> =
        Arc::new(rollup(&parent, &aggregations, false, &["K", "G"], None).unwrap());
    let mut rows_seen = 0;
    for cycle in 0..30 {
        source.push(random.next_change(&*parent, 12));
        graph.run_cycle();
        // the root expanded, then every node of the first level by key
        let first_level = View::new(rollup.clone(), None, None)
            .snapshot(None, 0, 1000)
            .unwrap();
        let keys = ReferenceTable::new_static(
            definition(&[
                ("__DEPTH__", ColumnType::Int),
                ("K", ColumnType::String),
                ("G", ColumnType::Int),
            ]),
            RowSet::flat(first_level.rows - 1),
            vec![
                ints(vec![2; first_level.rows as usize - 1]),
                first_level.columns[2].slice(1, first_level.rows as usize - 1),
                ints(vec![0; first_level.rows as usize - 1]),
            ],
        );
        let view = View::new(rollup.clone(), Some(keys), None);
        let whole = view.snapshot(None, 0, 100_000).unwrap();
        assert_eq!(whole.size, whole.rows);
        let shown = text(&whole);
        // every row follows its parent
        let mut path: Vec<String> = Vec::new();
        let mut canonical = Vec::new();
        for row in &shown {
            let cells: Vec<&str> = row.split('|').collect();
            let depth: usize = cells[0].parse().unwrap();
            path.truncate(depth.saturating_sub(2));
            for (level, key) in path.iter().enumerate() {
                assert_eq!(cells[2 + level], key, "seed {seed}, cycle {cycle}: {row}");
            }
            if depth > 1 {
                path.push(cells[depth].to_string());
            }
            let expanded = if depth < 3 { "true" } else { "-" };
            assert_eq!(cells[1], expanded, "seed {seed}, cycle {cycle}: {row}");
            canonical.push(format!(
                "{depth}|{}|{}|{}|{}",
                cells[2], cells[3], cells[4], cells[5]
            ));
        }
        canonical.sort();
        let mut expected = expected_rows(&*parent);
        expected.sort();
        assert_eq!(canonical, expected, "seed {seed}, cycle {cycle}");
        let first = seed + cycle;
        let window = view.snapshot(None, first, 5).unwrap();
        let expected: Vec<&String> = shown.iter().skip(first as usize).take(5).collect();
        assert_eq!(
            text(&window).iter().collect::<Vec<_>>(),
            expected,
            "seed {seed}"
        );
        rows_seen += whole.rows;
    }
    rows_seen
}

#[test]
fn random_changes_keep_the_walk_right() {
    let mut rows = 0;
    for seed in test_seeds(0..40) {
        rows += random_rollup(seed);
    }
    assert!(rows >= at_least_for(8000, 0..40), "only {rows} rows");
}

/// Observed on Java 42.9: `Duplicate output columns found: Ex used 2 times`, from the
/// level that has the null column of the group-by column it no longer groups by.
#[test]
fn a_result_named_like_a_group_by_column_is_refused() {
    let named = |output: &str, by: &[&str]| {
        let aggregations = [Aggregation::columns(
            AggSpec::Max,
            [Pair::new(output, "Qty")],
        )];
        rollup(&trades(), &aggregations, false, by, None).err()
    };
    assert_eq!(
        named("Ex", &["Sym", "Ex"]),
        Some(HierarchicalError::IllegalArgument(
            "Duplicate output columns found: Ex used 2 times".to_string()
        ))
    );
    assert_eq!(
        named("Most", &["Sym", "Sym"]),
        Some(HierarchicalError::IllegalArgument(
            "Duplicate output columns found: Sym used 2 times".to_string()
        ))
    );
    assert_eq!(named("Most", &["Sym", "Ex"]), None);
}

fn sorted_first(sort: &str, pairs: &[&str], by: &[&str]) -> Result<RollupTable, HierarchicalError> {
    let spec = AggSpec::SortedFirst {
        columns: vec![sort.to_string()],
    };
    let pairs = pairs.iter().map(|name| Pair::of(*name));
    rollup(
        &trades(),
        &[Aggregation::columns(spec, pairs)],
        false,
        by,
        None,
    )
}

/// Java sorts the coarser levels by the finer level's columns, so only a sort column that
/// is one of the aggregation's own results gives there what the source gives; without
/// group-by columns there is one level and nothing to sort again.
#[test]
fn a_sorted_first_sorts_by_its_own_results() {
    let rollup: Arc<dyn Hierarchy> = Arc::new(sorted_first("Qty", &["Qty"], &["Sym"]).unwrap());
    let all = keys(&[(0, "", "", EXPAND_ALL)]);
    let view = View::new(rollup, Some(all), Some("Action".to_string()));
    assert_eq!(
        text(&view.snapshot(None, 0, 10).unwrap()),
        ["1|true|-|10", "2|-|AAPL|10", "2|-|MSFT|30", "2|-|GOOG|60"]
    );
    let flat: Arc<dyn Hierarchy> = Arc::new(sorted_first("Qty", &["Ex"], &[]).unwrap());
    let snapshot = View::new(flat, None, None).snapshot(None, 0, 10).unwrap();
    assert_eq!(text(&snapshot), ["1|-|N"]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator"
)]
fn a_sorted_first_by_another_column_is_not_ported() {
    let _ = sorted_first("Qty", &["Ex"], &["Sym"]);
}

/// Observed on Java 42.9 (`rollup_refused`): the levels have the group-by columns alone.
#[test]
fn a_rollup_without_aggregations_has_the_group_by_columns() {
    let rollup: Arc<dyn Hierarchy> =
        Arc::new(rollup(&trades(), &[], false, &["Sym"], None).unwrap());
    let snapshot = View::new(rollup, None, None).snapshot(None, 0, 10).unwrap();
    assert_eq!(
        text(&snapshot),
        ["1|true|-", "2|-|AAPL", "2|-|MSFT", "2|-|GOOG"]
    );
}

/// Java asks the key table for every group-by column, also for one that no key of the
/// table reaches down to.
#[test]
fn a_key_table_has_every_group_by_column_of_the_rollup() {
    let keys: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&[
            ("Sym", ColumnType::String),
            ("Ex", ColumnType::Int),
            ("__DEPTH__", ColumnType::Int),
        ]),
        RowSet::flat(1),
        vec![strings(["AAPL"]), ints([0]), ints([2])],
    );
    let view = View::new(trades_rollup(), Some(keys), None);
    assert!(matches!(
        view.snapshot(None, 0, 10),
        Err(HierarchicalError::IllegalArgument(_))
    ));
}

// Sorts and filters of a rollup (task 7.8.1). The orders of the scripted cases are what
// Java 42.9 answers for the same rollup with the same sorts and filters applied
// (`oracle/scenarios/python/rollup_sorted.py`, `rollup_filtered.py`).

fn trades_table() -> RollupTable {
    rollup(&trades(), &aggregations(), false, &["Sym", "Ex"], None).unwrap()
}

fn everything(rollup: RollupTable) -> View {
    View::new(
        Arc::new(rollup),
        Some(keys(&[(0, "", "", EXPAND_ALL)])),
        Some("Action".to_string()),
    )
}

fn all_rows(rollup: RollupTable) -> Vec<String> {
    text(&everything(rollup).snapshot(None, 0, 100).unwrap())
}

#[test]
fn a_sort_orders_the_rows_of_every_node() {
    let sorted = trades_table().with_sorts(&[SortColumn::descending("Qty")]);
    let rows = [
        "1|true|-|-|210|6",
        "2|true|MSFT|-|80|2",
        "3|-|MSFT|Q|50|1",
        "3|-|MSFT|N|30|1",
        "2|true|AAPL|-|70|3",
        "3|-|AAPL|N|50|2",
        "3|-|AAPL|Q|20|1",
        "2|true|GOOG|-|60|1",
        "3|-|GOOG|Q|60|1",
    ];
    let view = everything(sorted);
    assert_eq!(text(&view.snapshot(None, 0, 100).unwrap()), rows);
    // a viewport that begins inside one sorted node and ends inside another
    let window = view.snapshot(None, 3, 3).unwrap();
    assert_eq!(window.size, 9);
    assert_eq!(text(&window), rows[3..6]);
    // only some columns, in another order than the hierarchy has them
    let some = view.snapshot(Some(&[5, 1, 0, 2]), 1, 2).unwrap();
    assert_eq!(text(&some), ["2|true|2|MSFT", "1|-|3|MSFT"]);
}

#[test]
fn a_node_expanded_by_key_is_found_where_the_sort_puts_it() {
    let sorted = trades_table().with_sorts(&[SortColumn::ascending("Sym")]);
    let view = View::new(
        Arc::new(sorted),
        Some(keys(&[(2, "MSFT", "", EXPAND), (2, "AAPL", "", EXPAND)])),
        Some("Action".to_string()),
    );
    assert_eq!(
        text(&view.snapshot(None, 0, 100).unwrap()),
        [
            "1|true|-|-|210|6",
            "2|true|AAPL|-|70|3",
            "3|-|AAPL|N|50|2",
            "3|-|AAPL|Q|20|1",
            "2|false|GOOG|-|60|1",
            "2|true|MSFT|-|80|2",
            "3|-|MSFT|N|30|1",
            "3|-|MSFT|Q|50|1",
        ]
    );
}

/// A level has nulls in the group-by columns below its own, so a sort by one leaves its
/// rows as they are.
#[test]
fn a_sort_by_a_group_by_column_orders_the_levels_that_have_it() {
    let sorted = trades_table().with_sorts(&[SortColumn::descending("Ex")]);
    assert_eq!(
        all_rows(sorted),
        [
            "1|true|-|-|210|6",
            "2|true|AAPL|-|70|3",
            "3|-|AAPL|Q|20|1",
            "3|-|AAPL|N|50|2",
            "2|true|MSFT|-|80|2",
            "3|-|MSFT|Q|50|1",
            "3|-|MSFT|N|30|1",
            "2|true|GOOG|-|60|1",
            "3|-|GOOG|Q|60|1",
        ]
    );
}

#[test]
fn later_sorts_come_first_and_a_column_sorts_once() {
    let once = trades_table().with_sorts(&[
        SortColumn::ascending("Rows"),
        SortColumn::descending("Rows"),
    ]);
    assert_eq!(once.sorts(), [SortColumn::ascending("Rows")]);
    let twice = once.with_sorts(&[SortColumn::descending("Sym")]);
    assert_eq!(
        twice.sorts(),
        [SortColumn::descending("Sym"), SortColumn::ascending("Rows")]
    );
    let again = twice.with_sorts(&[
        SortColumn::descending("Rows"),
        SortColumn::ascending("Rows").absolute(),
    ]);
    assert_eq!(
        again.sorts(),
        [
            SortColumn::descending("Rows"),
            SortColumn::ascending("Rows").absolute(),
            SortColumn::descending("Sym"),
        ]
    );
    // Rows descending, then Sym descending
    assert_eq!(
        text(
            &View::new(Arc::new(again), None, None)
                .snapshot(None, 0, 100)
                .unwrap()
        ),
        [
            "1|true|-|-|210|6",
            "2|false|AAPL|-|70|3",
            "2|false|MSFT|-|80|2",
            "2|false|GOOG|-|60|1",
        ]
    );
}

#[test]
fn sorts_are_checked_against_the_columns_of_the_nodes() {
    let rollup = trades_table();
    assert_eq!(
        rollup.check_sorts(&[SortColumn::ascending("Qty"), SortColumn::ascending("Nope")]),
        Err(HierarchicalError::IllegalArgument(
            "Unknown column names [Nope], available column names are [Sym, Ex, Qty, Rows]".into()
        ))
    );
    assert!(matches!(
        rollup.check_sorts(&[SortColumn::ascending("Sym").absolute()]),
        Err(HierarchicalError::IllegalArgument(_))
    ));
    assert_eq!(
        rollup.check_sorts(&[
            SortColumn::descending("Qty").absolute(),
            SortColumn::ascending("Ex")
        ]),
        Ok(())
    );
    // sorts that were not checked fail the snapshot, not the rollup
    let unchecked = rollup.with_sorts(&[SortColumn::ascending("Nope")]);
    assert!(View::new(Arc::new(unchecked), None, None)
        .snapshot(None, 0, 10)
        .is_err());
}

fn filtered(rollup: &RollupTable, filters: &[&str]) -> Result<RollupTable, HierarchicalError> {
    let checked = CheckedFilters::parse(filters, rollup.source().definition(), &Compiler)
        .map_err(|error| HierarchicalError::IllegalArgument(error.to_string()))?;
    rollup.with_filters(&checked, &Compiler)
}

#[test]
fn a_filter_of_group_by_columns_keeps_whole_groups() {
    let rollup = trades_table();
    let kept = filtered(&rollup, &["Ex != `N`"]).unwrap();
    // the source is still the whole table
    assert_eq!(kept.source().row_set().size(), 6);
    assert_eq!(
        all_rows(kept),
        [
            "1|true|-|-|130|3",
            "2|true|AAPL|-|20|1",
            "3|-|AAPL|Q|20|1",
            "2|true|MSFT|-|50|1",
            "3|-|MSFT|Q|50|1",
            "2|true|GOOG|-|60|1",
            "3|-|GOOG|Q|60|1",
        ]
    );
    // a filter of a filtered rollup filters what the first one left, and sorts stay
    let sorted = filtered(&rollup, &["Ex != `N`"])
        .unwrap()
        .with_sorts(&[SortColumn::descending("Qty")]);
    let fewer = filtered(&sorted, &["Sym != `GOOG`"]).unwrap();
    assert_eq!(fewer.sorts(), [SortColumn::descending("Qty")]);
    assert_eq!(
        all_rows(fewer),
        [
            "1|true|-|-|70|2",
            "2|true|MSFT|-|50|1",
            "3|-|MSFT|Q|50|1",
            "2|true|AAPL|-|20|1",
            "3|-|AAPL|Q|20|1",
        ]
    );
    // a key that the filter left out names no node
    let view = View::new(
        Arc::new(filtered(&rollup, &["Sym == `GOOG`"]).unwrap()),
        Some(keys(&[(2, "AAPL", "", EXPAND), (2, "GOOG", "", EXPAND)])),
        Some("Action".to_string()),
    );
    assert_eq!(
        text(&view.snapshot(None, 0, 100).unwrap()),
        ["1|true|-|-|60|1", "2|true|GOOG|-|60|1", "3|-|GOOG|Q|60|1"]
    );
}

/// A table like [`trades`] with a column that no aggregation reads and that is no
/// group-by column.
fn trades_with_venue() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[
            ("Sym", ColumnType::String),
            ("Ex", ColumnType::String),
            ("Qty", ColumnType::Int),
            ("Lot", ColumnType::Int),
        ]),
        RowSet::flat(6),
        vec![
            strings(["AAPL", "AAPL", "MSFT", "AAPL", "MSFT", "GOOG"]),
            strings(["N", "Q", "N", "N", "Q", "Q"]),
            ints([10, 20, 30, 40, 50, 60]),
            ints([1, 2, 3, 4, 5, 6]),
        ],
    )
}

#[test]
fn a_filter_of_another_column_filters_the_source() {
    let rollup = rollup(
        &trades_with_venue(),
        &aggregations(),
        false,
        &["Sym", "Ex"],
        None,
    )
    .unwrap();
    let kept = filtered(&rollup, &["Lot > 2", "Sym != `GOOG`"]).unwrap();
    // the filtered table is the source now
    assert_eq!(kept.source().row_set().size(), 3);
    assert_eq!(
        all_rows(kept),
        [
            "1|true|-|-|120|3",
            "2|true|MSFT|-|80|2",
            "3|-|MSFT|N|30|1",
            "3|-|MSFT|Q|50|1",
            "2|true|AAPL|-|40|1",
            "3|-|AAPL|N|40|1",
        ]
    );
    // nothing left: Java shows no row, not a grand total of nothing
    let none = filtered(&rollup, &["Sym == `none`"]).unwrap();
    assert_eq!(all_rows(none), Vec::<String>::new());
}

#[test]
fn a_filter_reads_no_aggregated_column_and_no_other_row() {
    let rollup = rollup(
        &trades_with_venue(),
        &aggregations(),
        false,
        &["Sym", "Ex"],
        None,
    )
    .unwrap();
    let refused = |filter: &str| match filtered(&rollup, &[filter]) {
        Err(HierarchicalError::IllegalArgument(message)) => message,
        Err(other) => panic!("{filter}: {other}"),
        Ok(_) => panic!("{filter} was accepted"),
    };
    assert_eq!(
        refused("Qty > 15"),
        "Invalid filter found: Qty > 15 may only use non-aggregation columns, which are \
         [Sym, Ex, Lot], but has used [Qty]"
    );
    assert_eq!(
        refused("Sym_[i - 1] == `AAPL`"),
        "Invalid filter found: Sym_[i - 1] == `AAPL` may not use column arrays"
    );
    // a result of the rollup is no column of the source
    refused("Rows > 1");
    refused("Nope > 1");
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#withFilter"
)]
fn a_filter_by_row_position_is_not_ported() {
    let _ = filtered(&trades_table(), &["i > 2"]);
}

/// The rows of a walk as a tree, sorted again node by node, with rows that compare equal
/// left in their order: what a sorted rollup must show when `shown` is what the unsorted
/// one shows.
fn sorted_again(
    shown: &[String],
    compare: &dyn Fn(&[&str], &[&str]) -> std::cmp::Ordering,
) -> Vec<String> {
    fn depth(row: &str) -> usize {
        row.split('|').next().unwrap().parse().unwrap()
    }
    // each row with the rows below it
    fn subtrees(rows: &[String], at: usize) -> Vec<&[String]> {
        let mut out = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let mut end = start + 1;
            while end < rows.len() && depth(&rows[end]) > at {
                end += 1;
            }
            out.push(&rows[start..end]);
            start = end;
        }
        out
    }
    fn sort(
        rows: &[String],
        at: usize,
        compare: &dyn Fn(&[&str], &[&str]) -> std::cmp::Ordering,
        out: &mut Vec<String>,
    ) {
        let mut nodes = subtrees(rows, at);
        nodes.sort_by(|a, b| {
            let (a, b): (Vec<&str>, Vec<&str>) =
                (a[0].split('|').collect(), b[0].split('|').collect());
            compare(&a, &b)
        });
        for node in nodes {
            out.push(node[0].clone());
            sort(&node[1..], at + 1, compare, out);
        }
    }
    let mut out = Vec::new();
    sort(shown, 1, compare, &mut out);
    out
}

/// Random changes of the source of a rollup that is sorted and of rollups that are
/// filtered: after every cycle the sorted one shows the rows of the unsorted one, each
/// node's sorted again, and a filtered one the groups of the rows its filter accepts.
fn random_sorted_and_filtered(seed: u64) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    let columns = [
        ("K", ColumnType::String),
        ("G", ColumnType::Int),
        ("V", ColumnType::Int),
        ("F", ColumnType::Int),
    ];
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&columns),
        RowSet::flat(20),
        columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, 20))
            .collect(),
    );
    let parent: Arc<dyn Table> = source.table();
    let aggregations = [
        Aggregation::columns(AggSpec::Sum, [Pair::of("V")]),
        Aggregation::count("N"),
    ];
    let plain = rollup(&parent, &aggregations, false, &["K", "G"], None).unwrap();
    // by the count, most first, then by the sum, and by the second key where it is one
    let sorts = [
        SortColumn::descending("N"),
        SortColumn::ascending("V"),
        SortColumn::descending("G"),
    ];
    let sorted = plain.with_sorts(&sorts);
    let by_key = filtered(&plain, &["G > -3"]).unwrap();
    let by_other = filtered(&plain, &["F >= 0", "K != `c`"])
        .unwrap()
        .with_sorts(&sorts);
    let all = ReferenceTable::new_static(
        definition(&[
            ("__DEPTH__", ColumnType::Int),
            ("K", ColumnType::String),
            ("G", ColumnType::Int),
            ("Action", ColumnType::Byte),
        ]),
        RowSet::flat(1),
        vec![
            ints([0]),
            strings([""]),
            ints([0]),
            Arc::new(Int8Array::from(vec![EXPAND_ALL])),
        ],
    );
    let view = |rollup: RollupTable| {
        View::new(
            Arc::new(rollup),
            Some(all.clone()),
            Some("Action".to_string()),
        )
    };
    let (plain, sorted, by_key, by_other) =
        (view(plain), view(sorted), view(by_key), view(by_other));
    let number = |cell: &str| cell.parse::<i64>().ok();
    let compare = |a: &[&str], b: &[&str]| {
        let key = |row: &[&str]| {
            // a level below the second key has it; the others have a null, which sorts
            // nothing
            let g = if row[0] == "3" { number(row[3]) } else { None };
            (number(row[5]), number(row[4]), g)
        };
        let (a, b) = (key(a), key(b));
        // null is the least value: `None < Some`
        b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(b.2.cmp(&a.2))
    };
    let canonical = |shown: &[String]| {
        let mut rows: Vec<String> = shown
            .iter()
            .map(|row| {
                let cells: Vec<&str> = row.split('|').collect();
                format!(
                    "{}|{}|{}|{}|{}",
                    cells[0], cells[2], cells[3], cells[4], cells[5]
                )
            })
            .collect();
        rows.sort();
        rows
    };
    let accepted = |filters: &[&str]| {
        let checked = CheckedFilters::parse(filters, parent.definition(), &Compiler).unwrap();
        let rows = checked.apply(&parent, &Compiler).unwrap();
        let mut expected = expected_rows(&*rows);
        expected.sort();
        expected
    };
    let mut rows_seen = 0;
    for cycle in 0..30 {
        source.push(random.next_change(&*parent, 14));
        graph.run_cycle();
        let unsorted = text(&plain.snapshot(None, 0, 100_000).unwrap());
        let whole = sorted.snapshot(None, 0, 100_000).unwrap();
        let shown = text(&whole);
        assert_eq!(
            shown,
            sorted_again(&unsorted, &compare),
            "seed {seed}, cycle {cycle}"
        );
        let first = (seed + cycle) % 7;
        let window = sorted.snapshot(None, first, 6).unwrap();
        assert_eq!(window.size, whole.size);
        let expected: Vec<String> = shown.iter().skip(first as usize).take(6).cloned().collect();
        assert_eq!(text(&window), expected, "seed {seed}, cycle {cycle}");

        let kept = text(&by_key.snapshot(None, 0, 100_000).unwrap());
        assert_eq!(
            canonical(&kept),
            accepted(&["G > -3"]),
            "seed {seed}, cycle {cycle}"
        );
        let kept = text(&by_other.snapshot(None, 0, 100_000).unwrap());
        assert_eq!(
            canonical(&kept),
            accepted(&["F >= 0", "K != `c`"]),
            "seed {seed}, cycle {cycle}"
        );
        // and its nodes are in the order of the sorts
        assert_eq!(
            kept,
            sorted_again(&kept, &compare),
            "seed {seed}, cycle {cycle}"
        );
        rows_seen += whole.rows + kept.len() as u64;
    }
    rows_seen
}

#[test]
fn random_changes_keep_sorted_and_filtered_rollups_right() {
    let mut rows = 0;
    for seed in test_seeds(0..40) {
        rows += random_sorted_and_filtered(seed);
    }
    assert!(rows >= at_least_for(8000, 0..40), "only {rows} rows");
}

/// Java reproduced: where it still fills the viewport, the walk of a sorted node looks up
/// the row of every node expanded below it, and fails for a node whose group was there and
/// has no rows now; a rollup that is not sorted leaves that node out, and so do both for a
/// key that never was one.
///
/// Behavior: HierarchicalTableImpl.LinkedDirective#setRowKeyInParentSorted and
/// NodeTableState#filterKeyAndSortChildDirectives
#[test]
fn a_sorted_node_fails_for_an_expanded_child_that_is_gone() {
    let graph = SteppedUpdateGraph::new();
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&[
            ("Sym", ColumnType::String),
            ("Ex", ColumnType::String),
            ("Qty", ColumnType::Int),
        ]),
        RowSet::flat(3),
        vec![
            strings(["AAPL", "MSFT", "GOOG"]),
            strings(["N", "N", "Q"]),
            ints([10, 30, 60]),
        ],
    );
    let parent: Arc<dyn Table> = source.table();
    let plain = rollup(&parent, &aggregations(), false, &["Sym", "Ex"], None).unwrap();
    let by_qty = plain.with_sorts(&[SortColumn::descending("Qty")]);
    // a sort by a key the first level does not have sorts nothing there, and the node
    // is sorted all the same
    let by_ex = plain.with_sorts(&[SortColumn::ascending("Ex")]);
    let view = |rollup: &RollupTable, rows: &[(i32, &str, &str, i8)]| {
        View::new(
            Arc::new(rollup.with_sorts(&[])),
            Some(keys(rows)),
            Some("Action".to_string()),
        )
    };
    let msft = [(2, "MSFT", "", EXPAND)];
    let never = [(2, "IBM", "", EXPAND)];
    let shown = |rollup: &RollupTable, rows: &[(i32, &str, &str, i8)]| {
        view(rollup, rows)
            .snapshot(None, 0, 100)
            .map(|snapshot| snapshot.rows)
            .map_err(|error| error.to_string())
    };
    assert_eq!(shown(&by_qty, &msft), Ok(5));
    source.push(TableChange::new().remove([1]));
    graph.run_cycle();
    assert_eq!(shown(&plain, &msft), Ok(3));
    for sorted in [&by_qty, &by_ex] {
        let error = shown(sorted, &msft).unwrap_err();
        assert!(error.contains("sort reverse lookup"), "{error}");
        assert_eq!(shown(sorted, &never), Ok(3));
        // a node under a contracted one is not looked at
        assert_eq!(shown(sorted, &[(1, "", "", CONTRACT), msft[0]]), Ok(1));
    }
    // a viewport that is full before the node is reached does not look
    let filled = view(&by_qty, &msft).snapshot(None, 0, 1).unwrap();
    assert_eq!((filled.rows, filled.size), (1, 3));
    assert!(view(&by_qty, &msft).snapshot(None, 0, 2).is_err());
    // the group comes back with the row key it had
    source.push(TableChange::new().add([5], vec![strings(["MSFT"]), strings(["Q"]), ints([99])]));
    graph.run_cycle();
    assert_eq!(
        text(&view(&by_qty, &msft).snapshot(None, 0, 100).unwrap()),
        [
            "1|true|-|-|169|3",
            "2|true|MSFT|-|99|1",
            "3|-|MSFT|Q|99|1",
            "2|false|GOOG|-|60|1",
            "2|false|AAPL|-|10|1",
        ]
    );
}
