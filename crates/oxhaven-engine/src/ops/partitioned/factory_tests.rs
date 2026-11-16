//! Partitioned tables made from tables: `PartitionedTable::of`, `of_table` and
//! `of_tables`, and `partitioned_transform` of one whose keys are not unique. The texts and
//! values expected are those Java 42.9 answered for the same cases
//! (`tasks/7.6.3-partitioned-factories.md`, "Probes").

use super::tests::{keys, names, probe, probe_columns, rows, seven_rows, Probe};
use super::*;
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use crate::ops::merge::merge as merge_tables;
use oxhaven_contracts::schema::ColumnDefinition;
use oxhaven_testkit::{definition, ints, ScriptedTable, TableChange};

fn partitioned() -> PartitionedTable {
    partition_by(&seven_rows(), &["K"], false).unwrap()
}

fn constituent_definition() -> TableDefinition {
    definition(&[("K", ColumnType::Int), ("V", ColumnType::Double)])
}

fn error(made: Result<PartitionedTable, PartitionedError>) -> String {
    made.err().expect("refused").to_string()
}

#[test]
fn of_keeps_what_it_is_told_and_checks_the_constituents() {
    let pt = partitioned();
    let made = PartitionedTable::of(
        pt.table(),
        vec!["K".into(), "K".into()],
        true,
        CONSTITUENT,
        constituent_definition(),
        true,
    )
    .unwrap();
    assert_eq!(made.key_columns(), ["K"]);
    assert!(made.unique_keys());
    // A static table cannot change, whatever the caller says.
    assert!(!made.constituent_changes_permitted());
    assert_eq!(rows(&*made.merge().unwrap()), rows(&*pt.merge().unwrap()));

    // The order of the columns of the definition is the caller's.
    let reordered = definition(&[("V", ColumnType::Double), ("K", ColumnType::Int)]);
    let made = PartitionedTable::of(pt.table(), vec![], false, CONSTITUENT, reordered, false);
    assert_eq!(names(made.unwrap().constituent_definition()), ["V", "K"]);
}

#[test]
fn of_refuses_with_javas_texts() {
    let pt = partitioned();
    let of = |keys: &[&str], column: &str, definition: TableDefinition| {
        error(PartitionedTable::of(
            pt.table(),
            keys.iter().map(|key| key.to_string()).collect(),
            true,
            column,
            definition,
            true,
        ))
    };
    assert_eq!(
        of(&["K", "Nope"], CONSTITUENT, constituent_definition()),
        "Partitioned table QueryTable does not have all key columns in [K, Nope]; instead \
         has K,__CONSTITUENT__"
    );
    assert_eq!(
        of(&["K"], "C", constituent_definition()),
        "Partitioned table QueryTable has no column named C"
    );
    assert_eq!(
        of(&[], "K", constituent_definition()),
        "Constituent column K has unsupported data type int"
    );
    assert_eq!(
        of(
            &["K"],
            CONSTITUENT,
            definition(&[("K", ColumnType::Int), ("W", ColumnType::Long)])
        ),
        "Table definition incompatibilities: \n\texpected column 'W' is missing in \
         constituent\n\tconstituent column 'V' is missing in expected"
    );
    assert_eq!(
        of(
            &["K"],
            CONSTITUENT,
            definition(&[("K", ColumnType::Int), ("V", ColumnType::Long)])
        ),
        "Table definition incompatibilities: \n\tcolumn 'V' is different ...\n\t    V: \
         expected dataType 'long' does not match constituent dataType 'double'"
    );
}

/// The underlying table of `partitioned` naturally joined to a table of the keys `keys`:
/// a key it has no constituent for has a null cell.
fn with_null_cells(partitioned: &PartitionedTable, keys: &Arc<dyn Table>) -> Arc<dyn Table> {
    natural_join(
        keys,
        partitioned.table(),
        &[JoinMatch::same("K")],
        &[JoinAddition::same(CONSTITUENT)],
        NaturalJoinType::ErrorOnDuplicate,
    )
    .unwrap()
}

#[test]
fn a_null_constituent_is_refused_and_a_first_null_is_no_constituent() {
    let pt = partitioned();
    let key_table = |values: &[i32]| -> Arc<dyn Table> {
        oxhaven_testkit::ReferenceTable::new_static(
            definition(&[("K", ColumnType::Int)]),
            RowSet::flat(values.len() as u64),
            vec![ints(values.iter().copied())],
        )
    };
    let nulls = with_null_cells(&pt, &key_table(&[0, 9]));
    let explicit = PartitionedTable::of(
        &nulls,
        vec!["K".into()],
        true,
        CONSTITUENT,
        constituent_definition(),
        false,
    );
    assert_eq!(error(explicit), "Encountered null constituent");
    assert_eq!(
        error(PartitionedTable::of_table(&nulls)),
        "Encountered null constituent"
    );
    let first_null = with_null_cells(&pt, &key_table(&[9, 0]));
    assert_eq!(
        error(PartitionedTable::of_table(&first_null)),
        "Underlying table QueryTable has no constituent tables in column: __CONSTITUENT__"
    );
}

#[test]
fn of_table_infers_the_rest() {
    let pt = partitioned();
    let made = PartitionedTable::of_table(pt.table()).unwrap();
    assert_eq!(made.key_columns(), ["K"]);
    assert!(!made.unique_keys());
    assert_eq!(made.constituent_column(), CONSTITUENT);
    assert_eq!(names(made.constituent_definition()), ["K", "V"]);
    assert!(!made.constituent_changes_permitted());
    assert_eq!(keys(&made), ["0", "1", "2"]);

    let empty = PartitionedTable::of_table(&crate::ops::sort::head(pt.table(), 0).unwrap());
    assert_eq!(
        error(empty),
        "Underlying table QueryTable has no constituent tables in column: __CONSTITUENT__"
    );
}

#[test]
fn of_table_needs_exactly_one_column_of_tables() {
    let pt = partitioned();
    let underlying = pt.table();
    let column = |name: &str, index: usize| {
        (
            ColumnDefinition::new(
                name,
                underlying.definition().columns()[index].column_type.clone(),
            ),
            underlying.column(index),
        )
    };
    let build = |columns: Vec<(ColumnDefinition, Arc<dyn ColumnSource>)>| -> Arc<dyn Table> {
        let (definitions, sources): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        crate::table::QueryTable::new_static(crate::table::TableSpec::new(
            TableDefinition::new(definitions).unwrap(),
            (*underlying.row_set()).clone(),
            sources,
        ))
    };
    let two = build(vec![
        column("K", 0),
        column(CONSTITUENT, 1),
        column("C2", 1),
    ]);
    assert_eq!(
        error(PartitionedTable::of_table(&two)),
        "Underlying table QueryTable has multiple possible constituent columns: \
         __CONSTITUENT__, C2"
    );
    let none = build(vec![column("K", 0)]);
    assert_eq!(
        error(PartitionedTable::of_table(&none)),
        "Underlying table QueryTable has multiple possible constituent columns: "
    );
    let made = PartitionedTable::of(
        &two,
        vec!["K".into()],
        true,
        "C2",
        constituent_definition(),
        true,
    );
    assert_eq!(made.unwrap().constituent_column(), "C2");
}

#[test]
fn of_tables_makes_one_row_per_table() {
    let first = seven_rows();
    let second = crate::ops::sort::head(&seven_rows(), 2).unwrap();
    let made =
        PartitionedTable::of_tables(&[Some(first.clone()), Some(second.clone())], None).unwrap();
    assert!(made.key_columns().is_empty());
    assert!(!made.unique_keys());
    assert!(!made.constituent_changes_permitted());
    assert_eq!(made.constituent_column(), CONSTITUENT);
    assert_eq!(names(made.table().definition()), [CONSTITUENT]);
    assert_eq!(
        made.table().definition().columns()[0].column_type,
        ColumnType::Table(oxhaven_contracts::schema::TableClass::Table)
    );
    assert!(made.table().is_flat());
    assert!(!made.table().is_refreshing());
    let mut expected = rows(&*first);
    expected.extend(rows(&*second));
    assert_eq!(rows(&*made.merge().unwrap()), expected);

    assert_eq!(
        error(PartitionedTable::of_tables(&[], None)),
        "No non-null constituents provided"
    );
    assert_eq!(
        error(PartitionedTable::of_tables(&[None], None)),
        "No non-null constituents provided"
    );
    assert!(error(PartitionedTable::of_tables(
        &[None, Some(first.clone())],
        None
    ))
    .starts_with("Cannot invoke \"io.deephaven.engine.table.Table.getUpdateGraph("));
    let other = definition(&[("W", ColumnType::Int)]);
    assert_eq!(
        error(PartitionedTable::of_tables(&[Some(first)], Some(other))),
        "Table definition incompatibilities: \n\texpected column 'W' is missing in \
         constituent\n\tconstituent column 'K' is missing in expected\n\tconstituent column \
         'V' is missing in expected"
    );
}

/// Seen on Java 42.9: a refreshing constituent makes the underlying table refreshing,
/// though it never changes, and its constituents still cannot change.
#[test]
fn of_tables_with_a_refreshing_table_is_refreshing() {
    let Probe {
        graph,
        source,
        partitioned: _,
    } = probe(false);
    let ticking: Arc<dyn Table> = source.table();
    let made = PartitionedTable::of_tables(&[Some(ticking)], None).unwrap();
    assert!(made.table().is_refreshing());
    assert!(!made.constituent_changes_permitted());
    let merged = made.merge().unwrap();
    source.push(TableChange::new().add([3], probe_columns(&[3], &[13], &[4.0])));
    graph.run_cycle();
    assert_eq!(
        rows(&*merged),
        ["1|10|1.0", "2|11|2.0", "1|12|3.0", "3|13|4.0"]
    );
}

/// A refreshing table is checked again as rows are added: a null constituent fails it.
#[test]
fn a_null_constituent_in_a_cycle_fails_the_table() {
    let Probe {
        graph, partitioned, ..
    } = probe(false);
    let key_source = ScriptedTable::new(
        graph.clone(),
        definition(&[("K", ColumnType::Int)]),
        RowSet::flat(1),
        vec![ints([1])],
    );
    let key_table: Arc<dyn Table> = key_source.table();
    let nulls = with_null_cells(&partitioned, &key_table);
    let definition = partitioned.constituent_definition().clone();
    let made = PartitionedTable::of(
        &nulls,
        vec!["K".into()],
        true,
        CONSTITUENT,
        definition,
        true,
    )
    .unwrap();
    assert!(made.constituent_changes_permitted());
    assert!(!made.table().is_failed());
    key_source.push(TableChange::new().add([1], vec![ints([2])]));
    graph.run_cycle();
    assert!(!made.table().is_failed());
    key_source.push(TableChange::new().add([2], vec![ints([9])]));
    graph.run_cycle();
    assert!(made.table().is_failed());
}

/// Seen on Java 42.9: keys that are not unique pair each row with every row of the
/// other's key, in the other's order.
#[test]
fn partitioned_transform_of_keys_that_are_not_unique_pairs_every_row() {
    let pt = partitioned();
    let doubled = merge_tables(&[pt.table().clone(), pt.table().clone()]).unwrap();
    let not_unique = PartitionedTable::of_table(&doubled).unwrap();
    assert!(!not_unique.unique_keys());
    let first: Transformer = Arc::new(|tables: &[Arc<dyn Table>]| Ok(Some(tables[0].clone())));
    let paired = not_unique
        .partitioned_transform(&not_unique, first.clone(), &[])
        .unwrap();
    assert!(!paired.unique_keys());
    assert_eq!(
        keys(&paired),
        ["0", "0", "1", "1", "2", "2", "0", "0", "1", "1", "2", "2"]
    );
    // A partitioned table with unique keys and another without: Java's natural join
    // refuses the duplicate.
    let refused = pt.partitioned_transform(&not_unique, first, &[]);
    assert!(
        error(refused).contains("duplicate right key"),
        "the natural join refuses"
    );
}
