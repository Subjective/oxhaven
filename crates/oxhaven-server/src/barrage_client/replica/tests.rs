//! The messages of a subscription applied to the local copy of a table.
//!
//! A scripted table plays the server's table. Each change it makes is written as the
//! message a server would send for it, and the copy that the messages are applied to
//! must hold what the scripted table holds, and tell its listeners changes that are true.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{new_empty_array, Array, ArrayRef, Int32Array};
use arrow::datatypes::Schema;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::ModifiedColumnSet;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_testkit::{
    assert_tables_equal, definition, ints, strings, test_seeds, ConsistencyChecker, RandomUpdates,
    Recorder, ScriptedTable, TableChange, TableSnapshot,
};

use super::*;
use crate::barrage_client::message::ModifiedColumn;
use crate::barrage_client::schema::RemoteAttribute;

fn schema_of(definition: &TableDefinition, attributes: &[&str]) -> RemoteSchema {
    RemoteSchema {
        definition: definition.clone(),
        attributes: attributes
            .iter()
            .map(|name| {
                (
                    name.to_string(),
                    RemoteAttribute {
                        value: "true".to_string(),
                        java_type: Some("java.lang.Boolean".to_string()),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
        arrow: Schema::empty(),
    }
}

fn empty_of(definition: &TableDefinition) -> Vec<ArrayRef> {
    definition
        .columns()
        .iter()
        .map(|column| new_empty_array(&column.column_type.arrow_type()))
        .collect()
}

/// A message that changes nothing.
fn nothing(definition: &TableDefinition) -> RemoteMessage {
    RemoteMessage {
        first_seq: 0,
        last_seq: 0,
        is_snapshot: false,
        effective_viewport: None,
        effective_reverse_viewport: false,
        effective_columns: None,
        rows_added: RowSet::empty(),
        rows_removed: RowSet::empty(),
        shifted: RowSetShiftData::empty(),
        rows_included: RowSet::empty(),
        add_columns: empty_of(definition),
        mod_columns: empty_of(definition)
            .into_iter()
            .map(|data| ModifiedColumn {
                rows_modified: RowSet::empty(),
                data,
            })
            .collect(),
        table_size: 0,
    }
}

/// The message a server sends a full subscriber for `change`.
fn message_of(definition: &TableDefinition, change: &TableChange) -> RemoteMessage {
    let mut message = nothing(definition);
    message.rows_added = change.update.added.clone();
    message.rows_included = change.update.added.clone();
    message.rows_removed = change.update.removed.clone();
    message.shifted = change.update.shifted.clone();
    if !change.update.added.is_empty() {
        message.add_columns = change.added_values.clone();
    }
    for (index, values) in &change.modified_values {
        message.mod_columns[*index] = ModifiedColumn {
            rows_modified: change.update.modified.clone(),
            data: values.clone(),
        };
    }
    message
}

/// The snapshot a server answers a full subscription to `table` with.
fn snapshot_of(table: &dyn Table) -> RemoteMessage {
    let snapshot = TableSnapshot::current(table).unwrap();
    let mut message = nothing(table.definition());
    message.is_snapshot = true;
    message.rows_added = snapshot.rows.clone();
    message.rows_included = snapshot.rows;
    message.add_columns = snapshot.columns;
    message
}

/// A copy of `source` on `graph`, and the inbox its messages go to.
fn copy_of(
    graph: &Arc<SteppedUpdateGraph>,
    source: &dyn Table,
    attributes: &[&str],
) -> (Arc<QueryTable>, Arc<Inbox>) {
    let schema = schema_of(source.definition(), attributes);
    let mut replica = Replica::new(&schema);
    replica.apply(&[snapshot_of(source)]).unwrap();
    let inbox = Inbox::new(None);
    let graph: Arc<dyn UpdateGraph> = graph.clone();
    let table = oxhaven_contracts::graph::locked(graph.as_ref(), || {
        live_table(graph.clone(), &schema, replica, inbox.clone())
    });
    (table, inbox)
}

fn every_type() -> TableDefinition {
    definition(&[
        ("I", ColumnType::Int),
        ("S", ColumnType::String),
        ("D", ColumnType::Double),
        ("B", ColumnType::Boolean),
        ("T", ColumnType::Instant),
        ("L", ColumnType::vector(ColumnType::Int)),
    ])
}

/// Random changes of the server's table, sent one or several per local cycle, leave the
/// copy with the server's rows, and every update the copy tells is true of it.
#[test]
fn the_copy_follows_random_changes_whether_one_or_several_arrive_per_cycle() {
    for seed in test_seeds(0..60) {
        let definition = every_type();
        let remote = SteppedUpdateGraph::new();
        let local = SteppedUpdateGraph::new();
        let scripted = ScriptedTable::new(
            remote.clone(),
            definition.clone(),
            RowSet::empty(),
            empty_of(&definition),
        );
        let source = scripted.table();
        let (copy, inbox) = copy_of(&local, source.as_ref(), &[]);
        let checker = ConsistencyChecker::attach(copy.clone());
        let mut random = RandomUpdates::new(seed);
        // some seeds grow the table past the block of a column source
        let target = if seed.is_multiple_of(10) { 2000 } else { 12 };
        for cycle in 0..20 {
            let messages = 1 + (seed + cycle) % 3;
            for _ in 0..messages {
                let change = random.next_change(source.as_ref(), target);
                assert!(inbox.deliver(message_of(&definition, &change)));
                scripted.push(change);
                remote.run_cycle();
            }
            local.run_cycle();
            checker.assert_consistent();
            assert!(!copy.is_failed(), "seed {seed} cycle {cycle}");
            assert_tables_equal(source.as_ref(), copy.as_ref());
            assert_eq!(
                *source.row_set(),
                *copy.row_set(),
                "seed {seed} cycle {cycle}"
            );
        }
    }
}

fn two_ints() -> TableDefinition {
    definition(&[("A", ColumnType::Int), ("B", ColumnType::Int)])
}

fn cells(table: &dyn Table, column: usize) -> Vec<(u64, i32)> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let values = snapshot.columns[column]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .clone();
    snapshot
        .rows
        .iter()
        .zip(values.iter())
        .map(|(key, value)| (key as u64, value.unwrap()))
        .collect()
}

/// Rows 10, 20, 30 with A = key and B = key + 1, copied.
fn three_rows() -> (
    Arc<SteppedUpdateGraph>,
    TableDefinition,
    Arc<QueryTable>,
    Arc<Inbox>,
) {
    let definition = two_ints();
    let remote = SteppedUpdateGraph::new();
    let local = SteppedUpdateGraph::new();
    let scripted = ScriptedTable::new(
        remote,
        definition.clone(),
        RowSet::from_keys([10, 20, 30]),
        vec![ints([10, 20, 30]), ints([11, 21, 31])],
    );
    let (copy, inbox) = copy_of(&local, scripted.table().as_ref(), &[]);
    (local, definition, copy, inbox)
}

/// A message whose columns are modified at different rows writes each column at its own
/// rows, and the update names the union of the rows and exactly the columns modified.
#[test]
fn each_column_is_modified_at_its_own_rows() {
    let (local, definition, copy, inbox) = three_rows();
    let recorder = Recorder::attach(copy.clone());
    let mut message = nothing(&definition);
    message.mod_columns[0] = ModifiedColumn {
        rows_modified: RowSet::from_keys([10, 20]),
        data: ints([-10, -20]),
    };
    message.mod_columns[1] = ModifiedColumn {
        rows_modified: RowSet::from_keys([20, 30]),
        data: ints([-21, -31]),
    };
    inbox.deliver(message);
    local.run_cycle();
    assert_eq!(cells(copy.as_ref(), 0), [(10, -10), (20, -20), (30, 30)]);
    assert_eq!(cells(copy.as_ref(), 1), [(10, 11), (20, -21), (30, -31)]);
    let update = recorder.events().pop().unwrap().unwrap();
    assert_eq!(update.modified, RowSet::from_keys([10, 20, 30]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0, 1]));

    // a column with no modified rows is not a modified column
    let mut message = nothing(&definition);
    message.mod_columns[1] = ModifiedColumn {
        rows_modified: RowSet::from_keys([10]),
        data: ints([0]),
    };
    inbox.deliver(message);
    local.run_cycle();
    let update = recorder.events().pop().unwrap().unwrap();
    assert_eq!(update.modified, RowSet::from_keys([10]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([1]));
    assert_eq!(cells(copy.as_ref(), 0), [(10, -10), (20, -20), (30, 30)]);
}

/// The modified rows of a message are in the key space after its shift: the cells move
/// first and are written second.
#[test]
fn a_shift_moves_the_cells_before_the_modified_rows_are_written() {
    let (local, definition, copy, inbox) = three_rows();
    let recorder = Recorder::attach(copy.clone());
    let checker = ConsistencyChecker::attach(copy.clone());
    let change = TableChange::new()
        .remove([10])
        .shift(20, 30, 5)
        .add([20], vec![ints([1]), ints([2])])
        .modify([35], [(1, ints([-31]))]);
    inbox.deliver(message_of(&definition, &change));
    local.run_cycle();
    checker.assert_consistent();
    assert_eq!(cells(copy.as_ref(), 0), [(20, 1), (25, 20), (35, 30)]);
    assert_eq!(cells(copy.as_ref(), 1), [(20, 2), (25, 21), (35, -31)]);
    let update = recorder.events().pop().unwrap().unwrap();
    assert_eq!(update.shifted, change.update.shifted);
    assert_eq!(update.modified, RowSet::from_keys([35]));
    assert_eq!(update.removed, RowSet::from_keys([10]));
    assert_eq!(update.added, RowSet::from_keys([20]));
}

/// Two messages in one cycle are one update: a row modified in column A by the first and
/// shifted by the second is modified at its new key, in column A only; a row added by
/// the first and removed by the second was never there.
#[test]
fn messages_of_one_cycle_are_told_as_one_update() {
    let (local, definition, copy, inbox) = three_rows();
    let recorder = Recorder::attach(copy.clone());
    let checker = ConsistencyChecker::attach(copy.clone());
    let first = TableChange::new()
        .add([40], vec![ints([40]), ints([41])])
        .modify([20], [(0, ints([-20]))]);
    let second = TableChange::new().remove([40]).shift(20, 30, 100);
    inbox.deliver(message_of(&definition, &first));
    inbox.deliver(message_of(&definition, &second));
    local.run_cycle();
    checker.assert_consistent();
    assert_eq!(cells(copy.as_ref(), 0), [(10, 10), (120, -20), (130, 30)]);
    assert_eq!(cells(copy.as_ref(), 1), [(10, 11), (120, 21), (130, 31)]);
    let events = recorder.events();
    assert_eq!(events.len(), 1);
    let update = events[0].clone().unwrap();
    assert!(update.added.is_empty(), "{update:?}");
    assert!(update.removed.is_empty(), "{update:?}");
    assert_eq!(update.modified, RowSet::from_keys([120]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0]));
    assert_eq!(
        update
            .shifted
            .apply(&RowSet::from_keys([10, 20, 30]))
            .unwrap(),
        RowSet::from_keys([10, 120, 130])
    );
}

/// A message that changes nothing, and a cycle with no message, tell the listeners
/// nothing.
#[test]
fn an_empty_message_is_no_update() {
    let (local, definition, copy, inbox) = three_rows();
    let recorder = Recorder::attach(copy.clone());
    inbox.deliver(nothing(&definition));
    local.run_cycle();
    local.run_cycle();
    inbox.deliver(nothing(&definition));
    inbox.deliver(nothing(&definition));
    local.run_cycle();
    assert!(recorder.events().is_empty());
    assert_eq!(cells(copy.as_ref(), 0), [(10, 10), (20, 20), (30, 30)]);

    // the copy itself says that nothing changed, so the table is told nothing
    let mut replica = Replica::new(&schema_of(&definition, &[]));
    let quiet = [nothing(&definition), nothing(&definition)];
    assert!(replica.apply(&quiet).unwrap().is_none());
    assert!(replica.apply(&[]).unwrap().is_none());
}

/// A table that grows by many more rows than a column source keeps in one block, in
/// several messages, then is modified and shifted across the blocks.
#[test]
fn a_table_grows_past_the_blocks_of_its_columns() {
    let definition = definition(&[("A", ColumnType::Int), ("S", ColumnType::String)]);
    let remote = SteppedUpdateGraph::new();
    let local = SteppedUpdateGraph::new();
    let scripted = ScriptedTable::new(
        remote.clone(),
        definition.clone(),
        RowSet::empty(),
        empty_of(&definition),
    );
    let source = scripted.table();
    let (copy, inbox) = copy_of(&local, source.as_ref(), &[]);
    let checker = ConsistencyChecker::attach(copy.clone());
    let step = |change: TableChange| {
        inbox.deliver(message_of(&definition, &change));
        scripted.push(change);
        remote.run_cycle();
    };
    const ROWS: i64 = 70_000;
    let names: Vec<String> = (0..ROWS).map(|row| format!("s{row}")).collect();
    for part in 0..7 {
        let rows = part * 10_000..(part + 1) * 10_000;
        step(TableChange::new().add(
            rows.clone().map(|row| row * 2),
            vec![
                ints(rows.clone().map(|row| row as i32)),
                strings(rows.map(|row| names[row as usize].as_str())),
            ],
        ));
        if part % 2 == 0 {
            local.run_cycle();
            checker.assert_consistent();
        }
    }
    local.run_cycle();
    checker.assert_consistent();
    assert_tables_equal(source.as_ref(), copy.as_ref());
    assert_eq!(copy.row_set().size(), ROWS as u64);

    // every 1000th row goes, the rows from key 60000 on move far away, and every 7th
    // row that stays where it is changes
    let removed: Vec<i64> = (0..ROWS).step_by(1000).map(|row| row * 2).collect();
    let modified: Vec<i64> = (0..ROWS)
        .step_by(7)
        .map(|row| row * 2)
        .filter(|key| *key < 60_000 && !removed.contains(key))
        .collect();
    step(
        TableChange::new()
            .remove(removed)
            .shift(60_000, 2 * ROWS, 1 << 20)
            .modify(
                modified.clone(),
                [(0, ints(modified.iter().map(|key| -(*key as i32))))],
            ),
    );
    local.run_cycle();
    checker.assert_consistent();
    assert_tables_equal(source.as_ref(), copy.as_ref());
}

fn blink_rows(definition: &TableDefinition, first: i32, count: i32) -> RemoteMessage {
    let mut message = nothing(definition);
    // a server numbers the rows of a blink table as it likes; the copy does not keep
    // the numbers
    let keys = RowSet::from_range(1000 + first as i64, 1000 + (first + count) as i64 - 1);
    message.rows_added = keys.clone();
    message.rows_included = keys;
    message.add_columns = vec![
        ints(first..first + count),
        ints((first..first + count).map(|value| value + 1)),
    ];
    message
}

/// The rows of a blink table are there for one cycle, at `0..count`, whatever keys the
/// server gave them; the rows of all messages of a cycle are there together.
#[test]
fn the_rows_of_a_blink_table_last_one_cycle() {
    let definition = two_ints();
    let local = SteppedUpdateGraph::new();
    let schema = schema_of(&definition, &["BlinkTable"]);
    let mut replica = Replica::new(&schema);
    let mut first = blink_rows(&definition, 0, 2);
    first.is_snapshot = true;
    replica.apply(&[first]).unwrap();
    let inbox = Inbox::new(None);
    let graph: Arc<dyn UpdateGraph> = local.clone();
    let copy = oxhaven_contracts::graph::locked(graph.as_ref(), || {
        live_table(graph.clone(), &schema, replica, inbox.clone())
    });
    assert!(copy.attributes().blink);
    assert!(copy.is_flat());
    let checker = ConsistencyChecker::attach(copy.clone());
    assert_eq!(cells(copy.as_ref(), 0), [(0, 0), (1, 1)]);

    // the first rows go on the next cycle, with nothing arriving
    local.run_cycle();
    checker.assert_consistent();
    assert!(copy.row_set().is_empty());

    inbox.deliver(blink_rows(&definition, 5, 3));
    inbox.deliver(blink_rows(&definition, 8, 1));
    local.run_cycle();
    checker.assert_consistent();
    assert_eq!(cells(copy.as_ref(), 0), [(0, 5), (1, 6), (2, 7), (3, 8)]);
    assert_eq!(cells(copy.as_ref(), 1), [(0, 6), (1, 7), (2, 8), (3, 9)]);

    inbox.deliver(blink_rows(&definition, 20, 1));
    local.run_cycle();
    checker.assert_consistent();
    assert_eq!(cells(copy.as_ref(), 0), [(0, 20)]);

    local.run_cycle();
    checker.assert_consistent();
    assert!(copy.row_set().is_empty());
    let updates = checker.updates_seen();
    local.run_cycle();
    assert_eq!(checker.updates_seen(), updates);
}

/// A subscription that ends fails the table on the next cycle, before anything that
/// arrived with the end, and the network side is told the table wants nothing more.
#[test]
fn an_error_fails_the_table_and_releases_the_subscription() {
    let (local, definition, copy, inbox) = three_rows();
    let recorder = Recorder::attach(copy.clone());
    let mut message = nothing(&definition);
    message.rows_removed = RowSet::from_keys([10]);
    assert!(inbox.deliver(message.clone()));
    inbox.fail(TableFailure {
        message: "Barrage subscription closed by server".to_string(),
    });
    assert!(!inbox.deliver(message));
    local.run_cycle();
    assert!(copy.is_failed());
    let events = recorder.events();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].clone().unwrap_err().message,
        "Barrage subscription closed by server"
    );
    assert!(lock(&inbox.arrived).closed);
}

/// A message that does not fit the table fails the table and does not panic.
#[test]
fn a_message_that_does_not_fit_fails_the_table() {
    type Spoil = Box<dyn Fn(&mut RemoteMessage)>;
    let cases: Vec<(&str, Spoil)> = vec![
        (
            "removes rows the table does not have",
            Box::new(|message| message.rows_removed = RowSet::from_keys([11])),
        ),
        (
            "adds rows the table already has",
            Box::new(|message| {
                message.rows_added = RowSet::from_keys([10]);
                message.rows_included = RowSet::from_keys([10]);
                message.add_columns = vec![ints([1]), ints([1])];
            }),
        ),
        (
            "cells of column 1 for 1 rows",
            Box::new(|message| {
                message.rows_added = RowSet::from_keys([11]);
                message.rows_included = RowSet::from_keys([11]);
                message.add_columns = vec![ints([1]), ints([1, 2])];
            }),
        ),
        (
            "modifies rows of column 0 the table does not have",
            Box::new(|message| {
                message.mod_columns[0] = ModifiedColumn {
                    rows_modified: RowSet::from_keys([11]),
                    data: ints([1]),
                }
            }),
        ),
    ];
    for (expected, spoil) in cases {
        let (local, definition, copy, inbox) = three_rows();
        let recorder = Recorder::attach(copy.clone());
        let mut message = nothing(&definition);
        spoil(&mut message);
        inbox.deliver(message);
        local.run_cycle();
        assert!(copy.is_failed(), "{expected}");
        let told = recorder.events().pop().unwrap().unwrap_err().message;
        assert!(told.contains(expected), "{told}");
        assert!(lock(&inbox.arrived).closed, "{expected}");
    }
}

/// Dropping the table tells the network side that nobody is listening.
#[test]
fn dropping_the_table_releases_the_subscription() {
    let (_local, definition, copy, inbox) = three_rows();
    assert!(!lock(&inbox.arrived).closed);
    drop(copy);
    assert!(lock(&inbox.arrived).closed);
    assert!(!inbox.deliver(nothing(&definition)));
}

/// While the server is growing the viewport of a full subscription, the copy has cells
/// only for the rows inside it, and the snapshot that covers the whole table completes it.
#[test]
fn a_subscription_that_grows_is_whole_at_its_last_snapshot() {
    let definition = two_ints();
    let schema = schema_of(&definition, &[]);
    let mut replica = Replica::new(&schema);
    let rows = RowSet::from_keys([10, 20, 30, 40]);

    let mut first = nothing(&definition);
    first.is_snapshot = true;
    first.effective_viewport = Some(RowSet::from_range(0, 1));
    first.rows_added = rows.clone();
    first.rows_included = RowSet::from_keys([10, 20]);
    first.add_columns = vec![ints([10, 20]), ints([11, 21])];
    assert!(!first.is_whole_table(2));
    replica.apply(&[first]).unwrap();
    assert_eq!(replica.rows(), &rows);

    // an update in between: a row inside the viewport is modified
    let mut update = nothing(&definition);
    update.mod_columns[0] = ModifiedColumn {
        rows_modified: RowSet::from_keys([20]),
        data: ints([-20]),
    };
    replica.apply(&[update]).unwrap();

    let mut last = nothing(&definition);
    last.is_snapshot = true;
    last.rows_included = RowSet::from_keys([30, 40]);
    last.add_columns = vec![ints([30, 40]), ints([31, 41])];
    assert!(last.is_whole_table(2));
    replica.apply(&[last]).unwrap();

    let local = SteppedUpdateGraph::new();
    let graph: Arc<dyn UpdateGraph> = local.clone();
    let copy = oxhaven_contracts::graph::locked(graph.as_ref(), || {
        live_table(graph.clone(), &schema, replica, Inbox::new(None))
    });
    assert_eq!(
        cells(copy.as_ref(), 0),
        [(10, 10), (20, -20), (30, 30), (40, 40)]
    );
    assert_eq!(
        cells(copy.as_ref(), 1),
        [(10, 11), (20, 21), (30, 31), (40, 41)]
    );
}
