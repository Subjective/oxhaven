//! A column of tables in match filters, `where_in` and `where_not_in`. The rows expected
//! are those Java 42.9 answered for the same cases
//! (`tasks/7.6.4-columns-of-tables.md`, "Probes"): the underlying table of
//! `partition_by("K")` naturally joined to the keys `0, 9, 2`, so that the key 9 has a null
//! cell.

use super::tests::{rows, seven_rows};
use super::*;
use crate::ops::distinct::{where_in, where_not_in, WhereInError};
use crate::ops::filter::unstructured_filter;
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use oxhaven_expr::Compiler;
use oxhaven_testkit::{definition, ints};

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
