//! A column of tables in match filters, `where_in` and `where_not_in`. The rows expected
//! are those Java 42.9 answered for the same cases
//! (`tasks/7.6.4-columns-of-tables.md`, "Probes"): the underlying table of
//! `partition_by("K")` naturally joined to the keys `0, 9, 2`, so that the key 9 has a null
//! cell.

use super::tests::{rows, seven_rows};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{aggregate, AggSpec, Aggregation, Pair};
use crate::ops::distinct::{where_in, where_not_in, WhereInError};
use crate::ops::filter::unstructured_filter;
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int32Type, Int64Type};
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    definition, ints, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable,
};

fn with_null_cell(partitioned: &PartitionedTable) -> Arc<dyn Table> {
    let keys: Arc<dyn Table> = oxhaven_testkit::ReferenceTable::new_static(
        definition(&[("K", ColumnType::Int)]),
        RowSet::flat(3),
        vec![ints([0, 9, 2])],
    );
    natural_join(
        &keys,
        partitioned.table(),
        &[JoinMatch::same("K")],
        &[JoinAddition::same(CONSTITUENT)],
        NaturalJoinType::ErrorOnDuplicate,
    )
    .unwrap()
}

/// The keys of the rows of `table`, a table whose first column is `K`.
fn kept(table: &Arc<dyn Table>) -> Vec<String> {
    rows(&**table)
        .iter()
        .map(|row| row.split('|').next().unwrap_or_default().to_string())
        .collect()
}

fn filtered(table: &Arc<dyn Table>, filter: &str) -> Result<Vec<String>, String> {
    unstructured_filter(table, &[filter], &Compiler)
        .map(|result| kept(&result))
        .map_err(|e| e.to_string())
}

#[test]
fn a_match_filter_on_a_column_of_tables_takes_null_alone() {
    let pt = partition_by(&seven_rows(), &["K"], false).unwrap();
    let table = with_null_cell(&pt);
    for (filter, expected) in [
        ("__CONSTITUENT__ != null", vec!["0", "2"]),
        ("__CONSTITUENT__ == null", vec!["9"]),
        ("__CONSTITUENT__ = null", vec!["9"]),
        ("__CONSTITUENT__ in null", vec!["9"]),
        ("__CONSTITUENT__ not in null", vec!["0", "2"]),
        ("__CONSTITUENT__ in null, null", vec!["9"]),
        ("__CONSTITUENT__ icase in null", vec!["9"]),
        ("null != __CONSTITUENT__", vec!["0", "2"]),
    ] {
        let expected = expected.iter().map(|key| key.to_string()).collect();
        assert_eq!(filtered(&table, filter), Ok(expected), "{filter}");
    }
    let refused = filtered(&table, "__CONSTITUENT__ == NULL").unwrap_err();
    assert!(
        refused.contains(
            "Failed to convert literal value <NULL> for column \"__CONSTITUENT__\" of type \
             io.deephaven.engine.table.impl.QueryTable"
        ),
        "{refused}"
    );
}

#[test]
fn where_in_compares_tables_by_identity() {
    let pt = partition_by(&seven_rows(), &["K"], false).unwrap();
    let table = with_null_cell(&pt);
    let on = [JoinMatch::same(CONSTITUENT)];
    let first = crate::ops::sort::head(&table, 1).unwrap();
    assert_eq!(
        kept(&where_in(&table, &table, &on).unwrap()),
        ["0", "9", "2"]
    );
    assert!(kept(&where_not_in(&table, &table, &on).unwrap()).is_empty());
    assert_eq!(kept(&where_in(&table, &first, &on).unwrap()), ["0"]);
    assert_eq!(
        kept(&where_not_in(&table, &first, &on).unwrap()),
        ["9", "2"]
    );
    // The underlying table has the same tables, and no null cell.
    assert_eq!(
        kept(&where_in(&table, pt.table(), &on).unwrap()),
        ["0", "2"]
    );
    // The tables of another partition of the same rows are other tables.
    let other = partition_by(&seven_rows(), &["K"], false).unwrap();
    assert!(kept(&where_in(&table, other.table(), &on).unwrap()).is_empty());
    // A transform's column is of `Table`, not `QueryTable`: Java refuses the pair.
    let identity: Transformer = Arc::new(|tables| Ok(Some(tables[0].clone())));
    let transformed = pt.transform(identity, &[]);
    let refused = where_in(&table, transformed.unwrap().table(), &on).err();
    assert!(
        matches!(refused, Some(WhereInError::MismatchedTypes { .. })),
        "{refused:?}"
    );
}

/// The handles in column `name` of `table`, a null cell as `None`.
fn handles(table: &dyn Table, name: &str) -> Vec<Option<i64>> {
    let index = table.definition().index_of(name).expect("the column");
    let cells = table.column(index).chunk(&table.row_set()).unwrap();
    let cells = cells.as_primitive::<Int64Type>();
    (0..cells.len())
        .map(|row| cells.is_valid(row).then(|| cells.value(row)))
        .collect()
}

fn copies(table: &Arc<dyn Table>) -> Arc<dyn Table> {
    let pair = |output: &str| Pair::new(output, CONSTITUENT);
    let sorted = || vec!["K".to_string()];
    aggregate(
        table,
        &[
            Aggregation::columns(AggSpec::First, [pair("F")]),
            Aggregation::columns(AggSpec::Last, [pair("L")]),
            Aggregation::columns(AggSpec::SortedFirst { columns: sorted() }, [pair("S")]),
            Aggregation::columns(AggSpec::SortedLast { columns: sorted() }, [pair("T")]),
        ],
        false,
        &[],
    )
    .unwrap()
}

/// What `copies` should show for `table`: its first and last cell, and those of its
/// least and greatest key (an `int`), the first row of equal keys.
fn expected_copies(table: &dyn Table) -> Vec<Option<i64>> {
    let cells = handles(table, CONSTITUENT);
    let index = table.definition().index_of("K").unwrap();
    let keys = table.column(index).chunk(&table.row_set()).unwrap();
    let keys = keys.as_primitive::<Int32Type>();
    let mut order: Vec<usize> = (0..keys.len()).collect();
    let key = |row: usize| keys.is_valid(row).then(|| keys.value(row));
    order.sort_by_key(|&row| (key(row), row));
    let at = |row: Option<&usize>| row.and_then(|&row| cells[row]);
    let last_of_greatest = order.iter().rev().max_by_key(|&&row| (key(row), row));
    vec![
        cells.first().copied().flatten(),
        cells.last().copied().flatten(),
        at(order.first()),
        at(last_of_greatest),
    ]
}

fn copied(result: &dyn Table) -> Vec<Option<i64>> {
    ["F", "L", "S", "T"]
        .iter()
        .map(|name| handles(result, name).first().copied().flatten())
        .collect()
}

/// Java 42.9: `first_by`, `last_by`, `agg.sorted_first` and `agg.sorted_last` of a column
/// of tables copy the cells, and the tables can be read after the partitioned table is
/// gone (`tasks/7.6.4-columns-of-tables.md`, "Probes").
#[test]
fn first_and_last_copy_cells_and_keep_their_tables() {
    let (result, expected, sizes) = {
        let pt = partition_by(&seven_rows(), &["K"], false).unwrap();
        let result = copies(pt.table());
        let sizes: Vec<u64> = pt
            .constituents()
            .unwrap()
            .iter()
            .map(|t| t.row_set().size())
            .collect();
        (result, expected_copies(&**pt.table()), sizes)
    };
    assert_eq!(copied(&*result), expected);
    // The keys are 0, 1, 2 in that order: first and sorted first are the table of 0,
    // last and sorted last that of 2.
    assert_eq!(expected[0], expected[2]);
    assert_eq!(expected[1], expected[3]);
    let size = |handle: Option<i64>| resolve(handle.unwrap()).unwrap().row_set().size();
    assert_eq!(size(expected[0]), sizes[0]);
    assert_eq!(size(expected[1]), sizes[2]);
}

/// The same, of a partitioned table whose groups come and go.
#[test]
fn first_and_last_of_a_ticking_column_of_tables_follow_it() {
    for seed in test_seeds(0..20) {
        let graph = SteppedUpdateGraph::new();
        let mut random = RandomUpdates::new(seed);
        let columns = [("K", ColumnType::Int), ("X", ColumnType::Double)];
        let source = ScriptedTable::new(
            graph.clone(),
            definition(&columns),
            RowSet::flat(10),
            vec![
                ints((0..10).map(|i| i % 4)),
                random.values(&ColumnType::Double, 10),
            ],
        );
        let parent: Arc<dyn Table> = source.table();
        let pt = partition_by(&parent, &["K"], false).unwrap();
        let result = copies(pt.table());
        let check = ConsistencyChecker::attach(result.clone());
        for _ in 0..20 {
            source.push(random.next_change(&*parent, 4));
            graph.run_cycle();
            check.assert_consistent();
            let expected = expected_copies(&**pt.table());
            assert_eq!(copied(&*result), expected, "seed {seed}");
        }
    }
}
