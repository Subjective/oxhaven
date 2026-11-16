//! A rollup with constituents (task 7.8.3). The rows of the scripted cases are what Java
//! 42.9 answers for the same rollup (`oracle/scenarios/python/rollup_constituents*.py`).

use super::tests::{
    aggregations, cell, expected_rows, filtered, keys, text, trades, trades_with_venue, EXPAND,
    EXPAND_ALL,
};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{AggSpec, Aggregation, Pair};
use crate::ops::sort::SortColumn;
use arrow::array::{Array, Int32Array};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_testkit::{
    at_least_for, definition, ints, test_seeds, RandomUpdates, ReferenceTable, ScriptedTable,
};

fn with_constituents(source: &Arc<dyn Table>, by: &[&str]) -> RollupTable {
    rollup(source, &aggregations(), true, by, None).unwrap()
}

fn shown(rollup: RollupTable, rows: &[(i32, &str, &str, i8)]) -> Vec<String> {
    let view = View::new(
        Arc::new(rollup),
        Some(keys(rows)),
        Some("Action".to_string()),
    );
    text(&view.snapshot(None, 0, 100).unwrap())
}

fn all_rows(rollup: RollupTable) -> Vec<String> {
    shown(rollup, &[(0, "", "", EXPAND_ALL)])
}

#[test]
fn a_snapshot_has_the_columns_of_the_source_after_those_of_the_levels() {
    let rollup = with_constituents(&trades(), &["Sym", "Ex"]);
    assert!(rollup.includes_constituents());
    let names: Vec<&str> = rollup
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "__DEPTH__",
            "__EXPANDED__",
            "Sym",
            "Ex",
            "Qty",
            "Rows",
            "Sym",
            "Ex",
            "Qty"
        ]
    );
    // the sum is a long, the column it sums an int
    assert_eq!(rollup.columns()[4].column_type, ColumnType::Long);
    assert_eq!(rollup.columns()[8].column_type, ColumnType::Int);
    let plain = super::rollup(&trades(), &aggregations(), false, &["Sym", "Ex"], None).unwrap();
    assert_eq!(plain.columns().len(), 6);
    assert!(plain.constituent_definition().is_none());
}

#[test]
fn a_finest_group_can_be_expanded_and_shows_its_rows_of_the_source() {
    let rollup = with_constituents(&trades(), &["Sym", "Ex"]);
    // by default the grand total alone is expanded, and the finest groups can be
    let closed = View::new(Arc::new(rollup.clone()), None, None);
    assert_eq!(
        text(&closed.snapshot(None, 0, 100).unwrap()),
        [
            "1|true|-|-|210|6|-|-|-",
            "2|false|AAPL|-|70|3|-|-|-",
            "2|false|MSFT|-|80|2|-|-|-",
            "2|false|GOOG|-|60|1|-|-|-",
        ]
    );
    assert_eq!(
        shown(
            rollup.clone(),
            &[(2, "AAPL", "", EXPAND), (3, "AAPL", "N", EXPAND)]
        ),
        [
            "1|true|-|-|210|6|-|-|-",
            "2|true|AAPL|-|70|3|-|-|-",
            "3|true|AAPL|N|50|2|-|-|-",
            "4|-|-|-|-|-|AAPL|N|10",
            "4|-|-|-|-|-|AAPL|N|40",
            "3|false|AAPL|Q|20|1|-|-|-",
            "2|false|MSFT|-|80|2|-|-|-",
            "2|false|GOOG|-|60|1|-|-|-",
        ]
    );
    let all = all_rows(rollup.clone());
    assert_eq!(all.len(), 9 + 6);
    assert_eq!(
        all[all.len() - 2..],
        ["3|true|GOOG|Q|60|1|-|-|-", "4|-|-|-|-|-|GOOG|Q|60"]
    );
    // a key of a finest group that is not there is dropped; a key one deeper is refused
    assert_eq!(shown(rollup.clone(), &[(3, "AAPL", "X", EXPAND)]).len(), 4);
    let too_deep = View::new(
        Arc::new(rollup),
        Some(keys(&[(4, "AAPL", "N", EXPAND)])),
        Some("Action".to_string()),
    );
    assert!(too_deep.snapshot(None, 0, 10).is_err());
}

#[test]
fn without_group_by_columns_the_rows_of_the_source_are_shown() {
    let rollup = with_constituents(&trades(), &[]);
    let open = View::new(Arc::new(rollup.clone()), None, None);
    let rows = text(&open.snapshot(None, 0, 100).unwrap());
    assert_eq!(rows.len(), 7);
    assert_eq!(rows[..2], ["1|true|210|6|-|-|-", "2|-|-|-|AAPL|N|10"]);
    // a viewport inside them
    let window = open.snapshot(None, 3, 2).unwrap();
    assert_eq!(window.size, 7);
    assert_eq!(text(&window), ["2|-|-|-|MSFT|N|30", "2|-|-|-|AAPL|N|40"]);
    // the grand total contracted under an expand-all
    let contracted = ReferenceTable::new_static(
        definition(&[("__DEPTH__", ColumnType::Int), ("A", ColumnType::Byte)]),
        RowSet::flat(1),
        vec![ints([1]), Arc::new(arrow::array::Int8Array::from(vec![4]))],
    );
    let closed = View::new(Arc::new(rollup), Some(contracted), Some("A".to_string()));
    assert_eq!(
        text(&closed.snapshot(None, 0, 100).unwrap()),
        ["1|false|210|6|-|-|-"]
    );
}

#[test]
fn the_rows_of_the_source_are_sorted_by_their_own_sorts() {
    let rollup = with_constituents(&trades(), &["Sym"]);
    let by_qty = rollup.with_constituent_sorts(&[SortColumn::descending("Qty")]);
    let rows = all_rows(by_qty.clone());
    assert_eq!(
        rows[..5],
        [
            "1|true|-|210|6|-|-|-",
            "2|true|AAPL|70|3|-|-|-",
            "3|-|-|-|-|AAPL|N|40",
            "3|-|-|-|-|AAPL|Q|20",
            "3|-|-|-|-|AAPL|N|10",
        ]
    );
    // the sorts of the aggregated nodes do not sort them, and theirs sort no level
    let levels = rollup.with_sorts(&[SortColumn::descending("Qty")]);
    let rows = all_rows(levels);
    assert_eq!(rows[1], "2|true|MSFT|80|2|-|-|-");
    assert_eq!(rows[2..4], ["3|-|-|-|-|MSFT|N|30", "3|-|-|-|-|MSFT|Q|50"]);
    assert_eq!(all_rows(by_qty.clone())[1], "2|true|AAPL|70|3|-|-|-");
    // a later sort decides first, what was sorted by before where it leaves rows equal
    let by_ex = by_qty.with_constituent_sorts(&[SortColumn::ascending("Ex")]);
    assert_eq!(
        all_rows(by_ex)[2..5],
        [
            "3|-|-|-|-|AAPL|N|40",
            "3|-|-|-|-|AAPL|N|10",
            "3|-|-|-|-|AAPL|Q|20",
        ]
    );
}

#[test]
fn constituent_sorts_are_checked_against_the_columns_of_the_source() {
    let rollup = with_constituents(&trades(), &["Sym"]);
    assert_eq!(
        rollup.check_constituent_sorts(&[SortColumn::ascending("Qty")]),
        Ok(())
    );
    assert_eq!(
        rollup.check_constituent_sorts(&[SortColumn::ascending("Rows")]),
        Err(HierarchicalError::IllegalArgument(
            "Unknown column names [Rows], available column names are [Sym, Ex, Qty]".to_string()
        ))
    );
    assert!(rollup
        .check_constituent_sorts(&[SortColumn::ascending("Sym").absolute()])
        .is_err());
    let plain = super::rollup(&trades(), &aggregations(), false, &["Sym"], None).unwrap();
    assert_eq!(
        plain.check_constituent_sorts(&[SortColumn::ascending("Qty")]),
        Err(HierarchicalError::IllegalArgument(
            "Rollup does not have constituent nodes".to_string()
        ))
    );
}

/// Observed on Java 42.9 with `HierarchicalTableService/Apply`: a sort by a result sorts
/// the rows of the source by the result's input, a sort by a count or a group-by column
/// does not sort them.
#[test]
fn the_sorts_of_the_levels_are_translated_for_the_rows_of_the_source() {
    let source = trades_with_venue();
    let aggregations = [
        Aggregation::columns(AggSpec::Sum, [Pair::of("Qty")]),
        Aggregation::count("Rows"),
        Aggregation::columns(AggSpec::Max, [Pair::new("Top", "Lot")]),
        Aggregation::columns(AggSpec::First, [Pair::new("Where", "Ex")]),
    ];
    let rollup = rollup(&source, &aggregations, true, &["Sym"], None).unwrap();
    let translated = rollup.constituent_sorts_of(&[
        SortColumn::descending("Qty"),
        SortColumn::ascending("Rows"),
        SortColumn::ascending("Sym"),
        SortColumn::descending("Top").absolute(),
        SortColumn::ascending("Where").absolute(),
    ]);
    assert_eq!(
        translated,
        [
            SortColumn::descending("Qty"),
            SortColumn::descending("Lot").absolute(),
            // the absolute value of a string is not asked for
            SortColumn::ascending("Ex"),
        ]
    );
    assert_eq!(rollup.check_constituent_sorts(&translated), Ok(()));
}

#[test]
fn a_filter_filters_the_rows_of_the_source_too() {
    let source = trades_with_venue();
    let rollup = with_constituents(&source, &["Sym"])
        .with_constituent_sorts(&[SortColumn::descending("Lot")]);
    // a filter of a group-by column keeps whole groups, with their rows
    let groups = filtered(&rollup, &["Sym != `AAPL`"]).unwrap();
    assert_eq!(
        all_rows(groups),
        [
            "1|true|-|140|3|-|-|-|-",
            "2|true|MSFT|80|2|-|-|-|-",
            "3|-|-|-|-|MSFT|Q|50|5",
            "3|-|-|-|-|MSFT|N|30|3",
            "2|true|GOOG|60|1|-|-|-|-",
            "3|-|-|-|-|GOOG|Q|60|6",
        ]
    );
    // a filter of another column keeps some rows of a group
    let lots = filtered(&rollup, &["Lot % 2 == 1"]).unwrap();
    assert_eq!(
        all_rows(lots),
        [
            "1|true|-|90|3|-|-|-|-",
            "2|true|AAPL|10|1|-|-|-|-",
            "3|-|-|-|-|AAPL|N|10|1",
            "2|true|MSFT|80|2|-|-|-|-",
            "3|-|-|-|-|MSFT|Q|50|5",
            "3|-|-|-|-|MSFT|N|30|3",
        ]
    );
}

/// Random changes of the source of a rollup with constituents, the rows of the source
/// sorted by `V` for odd seeds: after every cycle the walk with everything expanded
/// shows the levels as a rollup without constituents does, and under every finest group
/// its rows of the source, in the source's order or sorted, and any viewport is a range
/// of it.
fn random_constituents(seed: u64) -> u64 {
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
    let sorted = seed % 2 == 1;
    let mut rollup = rollup(&parent, &aggregations, true, &["K", "G"], None).unwrap();
    if sorted {
        rollup = rollup.with_constituent_sorts(&[SortColumn::ascending("V")]);
    }
    let rollup: Arc<dyn Hierarchy> = Arc::new(rollup);
    let everything = ReferenceTable::new_static(
        definition(&[
            ("__DEPTH__", ColumnType::Int),
            ("K", ColumnType::String),
            ("G", ColumnType::Int),
            ("A", ColumnType::Byte),
        ]),
        RowSet::flat(1),
        vec![
            ints([0]),
            oxhaven_testkit::strings([""]),
            ints([0]),
            Arc::new(arrow::array::Int8Array::from(vec![EXPAND_ALL])),
        ],
    );
    let view = View::new(rollup.clone(), Some(everything), Some("A".to_string()));
    let mut rows_seen = 0;
    for cycle in 0..30 {
        source.push(random.next_change(&*parent, 12));
        graph.run_cycle();
        let whole = view.snapshot(None, 0, 100_000).unwrap();
        assert_eq!(whole.size, whole.rows);
        let shown = text(&whole);
        // the rows of the source by group, in its order
        let rows = parent.row_set();
        let values: Vec<ArrayRef> = (0..3)
            .map(|index| parent.column(index).chunk(&rows).unwrap())
            .collect();
        let numbers = values[2].as_any().downcast_ref::<Int32Array>().unwrap();
        let mut by_group: Vec<((String, String), Vec<(Option<i32>, String)>)> = Vec::new();
        for row in 0..rows.size() as usize {
            let key = (cell(&values[0], row), cell(&values[1], row));
            let value = numbers.is_valid(row).then(|| numbers.value(row));
            let shown = format!("4|-|-|-|-|-|{}|{}|{}", key.0, key.1, cell(&values[2], row));
            match by_group.iter_mut().find(|(group, _)| *group == key) {
                Some((_, rows)) => rows.push((value, shown)),
                None => by_group.push((key, vec![(value, shown)])),
            }
        }
        let mut levels = Vec::new();
        let mut at = 0;
        while at < shown.len() {
            let cells: Vec<&str> = shown[at].split('|').collect();
            let depth: usize = cells[0].parse().unwrap();
            assert!(depth <= 3, "seed {seed}, cycle {cycle}: {}", shown[at]);
            assert_eq!(
                cells[1], "true",
                "seed {seed}, cycle {cycle}: {}",
                shown[at]
            );
            assert_eq!(cells[6..], ["-", "-", "-"], "seed {seed}, cycle {cycle}");
            levels.push(format!(
                "{depth}|{}|{}|{}|{}",
                cells[2], cells[3], cells[4], cells[5]
            ));
            at += 1;
            if depth < 3 {
                continue;
            }
            let key = (cells[2].to_string(), cells[3].to_string());
            let (_, group) = by_group
                .iter()
                .find(|(group, _)| *group == key)
                .unwrap_or_else(|| panic!("seed {seed}, cycle {cycle}: no rows of {key:?}"));
            let mut group = group.clone();
            if sorted {
                // stable, null first
                group.sort_by_key(|(value, _)| *value);
            }
            let expected: Vec<&String> = group.iter().map(|(_, shown)| shown).collect();
            let under: Vec<&String> = shown[at..].iter().take(expected.len()).collect();
            assert_eq!(under, expected, "seed {seed}, cycle {cycle}: {key:?}");
            at += expected.len();
        }
        levels.sort();
        let mut expected = expected_rows(&*parent);
        expected.sort();
        assert_eq!(levels, expected, "seed {seed}, cycle {cycle}");
        let first = seed + cycle;
        let window = view.snapshot(None, first, 7).unwrap();
        let expected: Vec<&String> = shown.iter().skip(first as usize).take(7).collect();
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
fn random_changes_keep_the_rows_of_the_source_under_their_groups() {
    let mut rows = 0;
    for seed in test_seeds(0..40) {
        rows += random_constituents(seed);
    }
    assert!(rows >= at_least_for(16000, 0..40), "only {rows} rows");
}
