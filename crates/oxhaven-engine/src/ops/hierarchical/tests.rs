//! Rollups and their views. The expansions and the order of rows of the scripted cases
//! are what Java 42.9 was observed to answer (`oracle/scenarios/python/rollup_*.py`).

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{AggSpec, Aggregation, Pair};
use arrow::array::{Array, Int32Array, Int8Array};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_testkit::{
    at_least_for, definition, ints, strings, test_seeds, RandomUpdates, ReferenceTable,
    ScriptedTable,
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
    let view = View::new(trades_rollup(), Some(keys(rows)), Some("Action".to_string()));
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
        View::new(trades_rollup(), Some(keys(rows)), Some("Action".to_string()))
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
    let snapshot = View::new(rollup, None, None)
        .snapshot(None, 0, 10)
        .unwrap();
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
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#makeRollup")]
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
            definition(&[("__DEPTH__", ColumnType::Int), ("K", ColumnType::String)]),
            RowSet::flat(first_level.rows - 1),
            vec![
                ints(vec![2; first_level.rows as usize - 1]),
                first_level.columns[2].slice(1, first_level.rows as usize - 1),
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
