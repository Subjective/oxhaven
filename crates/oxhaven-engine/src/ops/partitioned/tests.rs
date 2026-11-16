//! Partitioned tables: what `partition_by` makes, how constituents follow their groups,
//! and `merge`. The scripted case replays what Java 42.9 was observed to do
//! (`docs/PARTITIONED.md`).

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::Pair;
use crate::ops::agg::{aggregate, AggSpec};
use arrow::array::{Float64Array, Int32Array};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::TableClass;
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{
    at_least_for, definition, doubles, ints, test_seeds, ConsistencyChecker, RandomUpdates,
    Recorder, ReferenceTable, ScriptedTable, TableChange,
};
use std::collections::HashMap;

/// The rows of `table`, each as the text of its cells joined by `|`.
pub(super) fn rows(table: &dyn Table) -> Vec<String> {
    let row_set = table.row_set();
    let columns: Vec<ArrayRef> = (0..table.definition().columns().len())
        .map(|index| {
            table
                .column(index)
                .chunk(&row_set)
                .expect("a readable column")
        })
        .collect();
    (0..row_set.size() as usize)
        .map(|row| {
            columns
                .iter()
                .map(|column| array_value_to_string(column, row).expect("a printable cell"))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// The key columns of the underlying table of `partitioned`, as [`rows`] gives them.
pub(super) fn keys(partitioned: &PartitionedTable) -> Vec<String> {
    let table = partitioned.table();
    let row_set = table.row_set();
    let columns: Vec<ArrayRef> = partitioned
        .key_columns()
        .iter()
        .map(|name| {
            let index = table.definition().index_of(name).expect("a key column");
            table
                .column(index)
                .chunk(&row_set)
                .expect("a readable column")
        })
        .collect();
    (0..row_set.size() as usize)
        .map(|row| {
            columns
                .iter()
                .map(|column| array_value_to_string(column, row).expect("a printable cell"))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

pub(super) fn names(definition: &TableDefinition) -> Vec<&str> {
    definition
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

/// `K = i % 3`, `V = i * 1.5` for seven rows: the table of the Java probe.
pub(super) fn seven_rows() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[("K", ColumnType::Int), ("V", ColumnType::Double)]),
        RowSet::flat(7),
        vec![
            ints((0..7).map(|i| i % 3)),
            doubles((0..7).map(|i| i as f64 * 1.5)),
        ],
    )
}

#[test]
fn a_static_partition_has_a_constituent_per_key_in_first_seen_order() {
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    assert_eq!(keys(&partitioned), ["0", "1", "2"]);
    assert_eq!(partitioned.key_columns(), ["K"]);
    assert!(partitioned.unique_keys());
    assert!(!partitioned.constituent_changes_permitted());
    assert_eq!(partitioned.constituent_column(), "__CONSTITUENT__");
    assert_eq!(names(partitioned.constituent_definition()), ["K", "V"]);
    let underlying = partitioned.table();
    assert!(!underlying.is_refreshing());
    assert_eq!(
        underlying.definition().columns()[1].column_type,
        ColumnType::Table(TableClass::QueryTable)
    );

    let constituents = partitioned.constituents().unwrap();
    assert_eq!(constituents.len(), 3);
    assert_eq!(rows(&*constituents[0]), ["0|0.0", "0|4.5", "0|9.0"]);
    assert_eq!(rows(&*constituents[1]), ["1|1.5", "1|6.0"]);
    assert_eq!(rows(&*constituents[2]), ["2|3.0", "2|7.5"]);
    assert!(constituents.iter().all(|table| !table.is_refreshing()));

    let merged = partitioned.merge().unwrap();
    assert_eq!(
        rows(&*merged),
        ["0|0.0", "0|4.5", "0|9.0", "1|1.5", "1|6.0", "2|3.0", "2|7.5"]
    );
    // The merge is kept while someone has it.
    assert!(Arc::ptr_eq(&merged, &partitioned.merge().unwrap()));
}

/// Behavior: `BaseTable.CopyAttributeOperation#PartitionBy` and
/// `PartitionedTableImpl#computeSharedAttributes`: the constituents and the merge of a
/// static table have the add-only and append-only attributes it has, which the result of
/// `head` or `where` of a static table does and an in-memory table does not
/// (`tasks/4.20-fuzz-findings-2.md`, D2, observed on Java 42.9).
#[test]
fn constituents_and_merge_of_a_static_table_keep_its_attributes() {
    let first = crate::ops::sort::head(&seven_rows(), 5).unwrap();
    let of_head = first.attributes();
    assert!(of_head.append_only, "head of a static table is append-only");
    for (parent, expected) in [(first, of_head), (seven_rows(), TableAttributes::default())] {
        let partitioned = partition_by(&parent, &["K"], false).unwrap();
        for constituent in partitioned.constituents().unwrap() {
            let attributes = constituent.attributes();
            assert_eq!(attributes.add_only, expected.add_only);
            assert_eq!(attributes.append_only, expected.append_only);
        }
        let merged = partitioned.merge().unwrap().attributes();
        assert_eq!(merged.add_only, expected.add_only);
        assert_eq!(merged.append_only, expected.append_only);
        // The merge of the constituents a static key table asks for keeps them too:
        // constituent changes are permitted only where the underlying table refreshes.
        let asked = partitioned
            .with_table(partitioned.table().clone())
            .unwrap()
            .with_constituent_changes_permitted();
        assert!(!asked.constituent_changes_permitted());
        let asked = asked.merge().unwrap().attributes();
        assert_eq!(asked.add_only, expected.add_only);
        assert_eq!(asked.append_only, expected.append_only);
    }
}

/// The merge of the constituents a refreshing key table asks for takes no attribute from
/// them, as Java 42.9 was seen to answer (`partitions` seeds 413, 606 and 967 of the
/// second generator, tasks/4.20-fuzz-findings-2.md, D2).
#[test]
fn a_merge_for_a_refreshing_key_table_takes_no_attribute_of_the_constituents() {
    let first = crate::ops::sort::head(&seven_rows(), 5).unwrap();
    let partitioned = partition_by(&first, &["K"], false).unwrap();
    let graph = SteppedUpdateGraph::new();
    let keys = ScriptedTable::new(
        graph.clone(),
        definition(&[("K", ColumnType::Int)]),
        RowSet::flat(2),
        vec![ints([0, 1])],
    );
    let key_table: Arc<dyn Table> = keys.table();
    let rows = partitioned.rows_for_keys(&key_table).unwrap();
    assert!(rows.is_refreshing());
    let kept = partitioned.with_table(rows.clone()).unwrap();
    assert!(kept.merge().unwrap().attributes().append_only);
    let asked = partitioned
        .with_table(rows)
        .unwrap()
        .with_constituent_changes_permitted();
    assert!(asked.constituent_changes_permitted());
    assert!(!asked.merge().unwrap().attributes().append_only);
}

#[test]
fn dropped_keys_are_not_columns_of_the_constituents() {
    let partitioned = partition_by(&seven_rows(), &["K"], true).unwrap();
    assert_eq!(names(partitioned.constituent_definition()), ["V"]);
    assert_eq!(keys(&partitioned), ["0", "1", "2"]);
    let constituents = partitioned.constituents().unwrap();
    assert_eq!(rows(&*constituents[1]), ["1.5", "6.0"]);
    assert_eq!(names(partitioned.merge().unwrap().definition()), ["V"]);
}

#[test]
fn no_key_column_gives_one_constituent_with_every_row() {
    let partitioned = partition_by(&seven_rows(), &[], false).unwrap();
    assert_eq!(names(partitioned.table().definition()), ["__CONSTITUENT__"]);
    let constituents = partitioned.constituents().unwrap();
    assert_eq!(constituents.len(), 1);
    assert_eq!(constituents[0].row_set().size(), 7);
}

#[test]
fn an_unknown_key_and_a_second_partition_are_refused() {
    let parent = seven_rows();
    assert!(matches!(
        partition_by(&parent, &["Nope"], false),
        Err(PartitionedError::Aggregate(AggregateError::UnknownColumn(name))) if name == "Nope"
    ));
    let partition = |column: &str| Aggregation::Partition {
        column: column.to_string(),
        include_group_by_columns: true,
    };
    let twice = aggregate(&parent, &[partition("A"), partition("B")], false, &["K"]);
    assert_eq!(
        twice.err(),
        Some(AggregateError::Unsupported(
            "Only one AggPartition is permitted per aggregation".into()
        ))
    );
}

#[test]
fn a_partition_is_an_aggregation_beside_others() {
    let parent = seven_rows();
    let result = aggregate(
        &parent,
        &[
            Aggregation::columns(AggSpec::Sum, [Pair::new("Sum", "V")]),
            Aggregation::Partition {
                column: "Rows".to_string(),
                include_group_by_columns: false,
            },
        ],
        false,
        &["K"],
    )
    .unwrap();
    assert_eq!(names(result.definition()), ["K", "Sum", "Rows"]);
    let handles = result.column(2).chunk(&result.row_set()).unwrap();
    let tables = tables_of(&handles).unwrap();
    let second = tables[1].as_ref().expect("a constituent");
    assert_eq!(names(second.definition()), ["V"]);
    assert_eq!(rows(&**second), ["1.5", "6.0"]);
}

#[test]
fn a_handle_names_nothing_once_its_partition_is_gone() {
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    let underlying = partitioned.table().clone();
    let handles = underlying.column(1).chunk(&underlying.row_set()).unwrap();
    assert!(tables_of(&handles).unwrap().iter().all(Option::is_some));
    drop(partitioned);
    drop(underlying);
    // A chunk read from a column keeps nothing; a column that holds the cells does.
    let error = tables_of(&handles).err().expect("the tables are gone");
    assert!(error.message.contains("is gone"), "{error}");
}

/// The rows of each constituent of `copy`, a copy of an underlying table whose
/// constituent column is its second column.
fn copied_constituents(copy: &Arc<dyn Table>) -> Vec<Vec<String>> {
    let handles = copy.column(1).chunk(&copy.row_set()).unwrap();
    tables_of(&handles)
        .unwrap()
        .into_iter()
        .map(|table| rows(&*table.expect("a constituent")))
        .collect()
}

/// Seen on Java 42.9: `select`, `snapshot` and `update` of the underlying table of a
/// `partition_by` read the constituents after the partitioned table is gone.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ObjectArraySource#get, whose cells
/// refer to their tables
#[test]
fn a_copied_cell_keeps_its_table_when_the_partition_is_gone() {
    type Copy = fn(&Arc<dyn Table>) -> Arc<dyn Table>;
    let copies: [(&str, Copy); 3] = [
        ("select", |table| {
            crate::ops::select::select(
                table,
                &["K", "C = __CONSTITUENT__"],
                &oxhaven_expr::Compiler,
            )
            .unwrap()
        }),
        ("update", |table| {
            crate::ops::select::update(table, &["X = 1"], &oxhaven_expr::Compiler).unwrap()
        }),
        ("snapshot", |table| {
            crate::ops::snapshot::snapshot(table).unwrap()
        }),
    ];
    for (name, copy) in copies {
        let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
        let expected = copied_constituents(partitioned.table());
        let copied = copy(partitioned.table());
        drop(partitioned);
        assert_eq!(copied_constituents(&copied), expected, "{name}");
    }
}

/// A snapshot of a refreshing partition is a copy that does not keep the partition.
#[test]
fn a_snapshot_of_a_refreshing_partition_keeps_its_tables() {
    let Probe {
        graph, partitioned, ..
    } = probe(false);
    let snapshot = locked(&*graph, || {
        crate::ops::snapshot::snapshot(partitioned.table())
    })
    .unwrap();
    let expected = copied_constituents(partitioned.table());
    drop(partitioned);
    assert_eq!(copied_constituents(&snapshot), expected);
}

/// The keyed input table of the Java probe: `K`, `N`, `V`, rows keyed by `N`.
pub(super) struct Probe {
    pub(super) graph: Arc<SteppedUpdateGraph>,
    pub(super) source: Arc<ScriptedTable>,
    pub(super) partitioned: PartitionedTable,
}

pub(super) fn probe_columns(k: &[i32], n: &[i32], v: &[f64]) -> Vec<ArrayRef> {
    vec![
        Arc::new(Int32Array::from(k.to_vec())),
        Arc::new(Int32Array::from(n.to_vec())),
        Arc::new(Float64Array::from(v.to_vec())),
    ]
}

pub(super) fn probe(drop_keys: bool) -> Probe {
    let graph = SteppedUpdateGraph::new();
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&[
            ("K", ColumnType::Int),
            ("N", ColumnType::Int),
            ("V", ColumnType::Double),
        ]),
        RowSet::flat(3),
        probe_columns(&[1, 2, 1], &[10, 11, 12], &[1.0, 2.0, 3.0]),
    );
    let parent: Arc<dyn Table> = source.table();
    let partitioned = partition_by(&parent, &["K"], drop_keys).unwrap();
    Probe {
        graph,
        source,
        partitioned,
    }
}

#[test]
fn constituents_follow_their_groups_as_java_was_observed_to() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    assert!(partitioned.constituent_changes_permitted());
    assert!(partitioned.table().is_refreshing());
    let merged = partitioned.merge().unwrap();
    let first = partitioned.constituents().unwrap().remove(0);
    assert!(first.is_refreshing());
    let checks = [
        ConsistencyChecker::attach(partitioned.table().clone()),
        ConsistencyChecker::attach(merged.clone()),
        ConsistencyChecker::attach(first.clone()),
    ];
    let first_updates = Recorder::attach(first.clone());
    let verify = || checks.iter().for_each(|check| check.assert_consistent());
    assert_eq!(keys(&partitioned), ["1", "2"]);
    assert_eq!(rows(&*merged), ["1|10|1.0", "1|12|3.0", "2|11|2.0"]);
    assert_eq!(rows(&*first), ["1|10|1.0", "1|12|3.0"]);

    // A new key, and the row N=12 moves from key 1 to key 2.
    source.push(
        TableChange::new()
            .add([3], probe_columns(&[3], &[13], &[4.0]))
            .modify([2], [(0, ints([2])), (2, doubles([5.0]))]),
    );
    graph.run_cycle();
    verify();
    assert_eq!(keys(&partitioned), ["1", "2", "3"]);
    assert_eq!(
        rows(&*merged),
        ["1|10|1.0", "2|11|2.0", "2|12|5.0", "3|13|4.0"]
    );
    assert_eq!(rows(&*first), ["1|10|1.0"]);

    // The last row of key 1 goes: its row of the underlying table goes, the constituent
    // stays and is empty.
    source.push(TableChange::new().remove([0]));
    graph.run_cycle();
    verify();
    assert_eq!(keys(&partitioned), ["2", "3"]);
    assert_eq!(rows(&*merged), ["2|11|2.0", "2|12|5.0", "3|13|4.0"]);
    assert!(rows(&*first).is_empty());
    assert_eq!(partitioned.constituents().unwrap().len(), 2);

    // Key 1 has a row again: the same table, at its old place.
    source.push(TableChange::new().add([7], probe_columns(&[1], &[20], &[9.0])));
    graph.run_cycle();
    verify();
    assert_eq!(keys(&partitioned), ["1", "2", "3"]);
    assert_eq!(
        rows(&*merged),
        ["1|20|9.0", "2|11|2.0", "2|12|5.0", "3|13|4.0"]
    );
    assert_eq!(rows(&*first), ["1|20|9.0"]);

    // A modification of a value column reaches the constituent as one; a shift moves it.
    source.push(
        TableChange::new()
            .shift(7, 7, 2)
            .modify([9], [(2, doubles([9.5]))]),
    );
    graph.run_cycle();
    verify();
    assert_eq!(rows(&*first), ["1|20|9.5"]);
    assert_eq!(*first.row_set(), RowSet::from_keys([9]));
    let updates: Vec<TableUpdate> = first_updates
        .events()
        .into_iter()
        .map(|event| event.expect("no failure"))
        .collect();
    assert_eq!(updates.len(), 4);
    assert_eq!(updates[0].removed, RowSet::from_keys([2]));
    assert_eq!(updates[1].removed, RowSet::from_keys([0]));
    assert_eq!(updates[2].added, RowSet::from_keys([7]));
    assert_eq!(updates[3].modified, RowSet::from_keys([9]));
    assert_eq!(updates[3].shifted.ranges().len(), 1);
    // `V` is the constituent's third column.
    assert!(updates[3].modified_columns.contains(2));
    assert!(!updates[3].modified_columns.contains(0));
}

#[test]
fn a_constituent_that_is_kept_keeps_being_updated() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(true);
    let second = partitioned.constituents().unwrap().remove(1);
    assert_eq!(names(second.definition()), ["N", "V"]);
    drop(partitioned);
    source.push(TableChange::new().add([5], probe_columns(&[2], &[30], &[7.0])));
    graph.run_cycle();
    assert_eq!(rows(&*second), ["11|2.0", "30|7.0"]);
}

#[test]
fn a_failure_of_the_parent_fails_the_constituents_and_the_merge() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    let merged = partitioned.merge().unwrap();
    let first = partitioned.constituents().unwrap().remove(0);
    source.push_failure("the parent broke");
    graph.run_cycle();
    assert!(partitioned.table().is_failed());
    assert!(first.is_failed());
    assert!(merged.is_failed());
}

#[test]
fn a_merge_of_the_rows_for_some_keys_follows_the_key_table() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    let wanted = ScriptedTable::new(
        graph.clone(),
        definition(&[("K", ColumnType::Int)]),
        RowSet::flat(1),
        vec![ints([2])],
    );
    let key_table: Arc<dyn Table> = wanted.table();
    let some = partitioned
        .with_table(partitioned.rows_for_keys(&key_table).unwrap())
        .unwrap();
    let merged = some.merge().unwrap();
    let check = ConsistencyChecker::attach(merged.clone());
    assert_eq!(rows(&*merged), ["2|11|2.0"]);

    // Key 1 is wanted too, on the cycle in which its constituent gets a row.
    wanted.push(TableChange::new().add([1], vec![ints([1])]));
    source.push(TableChange::new().add([4], probe_columns(&[1], &[14], &[6.0])));
    graph.run_cycle();
    check.assert_consistent();
    assert_eq!(
        rows(&*merged),
        ["1|10|1.0", "1|12|3.0", "1|14|6.0", "2|11|2.0"]
    );

    wanted.push(TableChange::new().remove([0]));
    graph.run_cycle();
    check.assert_consistent();
    assert_eq!(rows(&*merged), ["1|10|1.0", "1|12|3.0", "1|14|6.0"]);
}

#[test]
fn head_by_refuses_a_column_of_tables_it_would_have_to_cast() {
    use crate::ops::head_by::{head_by, HeadByError};
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    // Observed: Java fails with "Cannot use view or updateView to produce results of type
    // class io.deephaven.engine.table.impl.QueryTable; use select or update instead".
    let refused = head_by(partitioned.table(), 1, &["K".to_string()]);
    assert_eq!(
        refused.err(),
        Some(HeadByError::TableColumn(TableClass::QueryTable))
    );
    // grouped by, the column is a key and is not cast
    let by_tables = head_by(partitioned.table(), 1, &[CONSTITUENT.to_string()]).unwrap();
    assert_eq!(by_tables.row_set().size(), 3);
}

/// The rows of `parent` by the text of their `by` columns, in the parent's order.
pub(super) fn groups(parent: &dyn Table, by: &[usize]) -> HashMap<String, Vec<String>> {
    let row_set = parent.row_set();
    let columns: Vec<ArrayRef> = (0..parent.definition().columns().len())
        .map(|index| parent.column(index).chunk(&row_set).unwrap())
        .collect();
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for row in 0..row_set.size() as usize {
        let text = |column: &ArrayRef| array_value_to_string(column, row).unwrap();
        let key = by
            .iter()
            .map(|&index| text(&columns[index]))
            .collect::<Vec<_>>();
        let cells = columns.iter().map(text).collect::<Vec<_>>();
        groups
            .entry(key.join("|"))
            .or_default()
            .push(cells.join("|"));
    }
    groups
}

/// Random changes of the parent; after every cycle each constituent holds the rows of its
/// key in the parent's order, the merge is the constituents one after the other, and
/// every update of the underlying table, of the merge and of the constituents that
/// existed at the start is a valid change of what they held.
fn random_partition(seed: u64, by: &[&str], kinds: [bool; 4]) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    [random.adds, random.removes, random.modifies, random.shifts] = kinds;
    let columns = [
        ("K", ColumnType::String),
        ("G", ColumnType::Int),
        ("X", ColumnType::Double),
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
    let partitioned = partition_by(&parent, by, false).unwrap();
    let merged = partitioned.merge().unwrap();
    let by: Vec<usize> = by
        .iter()
        .map(|name| parent.definition().index_of(name).unwrap())
        .collect();
    let mut checks = vec![
        ConsistencyChecker::attach(partitioned.table().clone()),
        ConsistencyChecker::attach(merged.clone()),
    ];
    for constituent in partitioned.constituents().unwrap() {
        checks.push(ConsistencyChecker::attach(constituent));
    }
    for _ in 0..30 {
        source.push(random.next_change(&*parent, 6));
        graph.run_cycle();
        for check in &checks {
            check.assert_consistent();
        }
        let expected = groups(&*parent, &by);
        let constituents = partitioned.constituents().unwrap();
        let keys = keys(&partitioned);
        assert_eq!(keys.len(), expected.len(), "seed {seed}");
        let mut all = Vec::new();
        for (key, constituent) in keys.iter().zip(&constituents) {
            let own = rows(&**constituent);
            assert_eq!(Some(&own), expected.get(key), "seed {seed}, key {key}");
            all.extend(own);
        }
        assert_eq!(rows(&*merged), all, "seed {seed}");
    }
    checks.iter().map(|check| check.updates_seen()).sum()
}

#[test]
fn random_changes_keep_constituents_and_merge_right() {
    let mut updates = 0;
    for seed in test_seeds(0..40) {
        updates += random_partition(seed, &["K"], [true; 4]);
        updates += random_partition(seed, &["K", "G"], [true; 4]);
        updates += random_partition(seed, &[], [true; 4]);
        updates += random_partition(seed, &["G"], [true, true, false, true]);
        updates += random_partition(seed, &["K"], [true, false, true, false]);
    }
    assert!(
        updates >= at_least_for(4000, 0..40),
        "only {updates} updates"
    );
}
