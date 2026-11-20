//! The reader against the messages this crate's own writer makes.

use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_testkit::{definition, test_seeds, RandomUpdates, ScriptedTable, TableChange};

use super::*;
use crate::barrage::writer::{
    subscription_messages, BarrageMessage, ModColumn, SubView, WireOptions,
};
use crate::barrage_client::schema::RemoteSchema;
use crate::flight::BatchOptions;
use crate::schema::{arrow_schema, SchemaContext};

fn types() -> TableDefinition {
    definition(&[
        ("Bo", ColumnType::Boolean),
        ("By", ColumnType::Byte),
        ("Ch", ColumnType::Char),
        ("Sh", ColumnType::Short),
        ("In", ColumnType::Int),
        ("Lo", ColumnType::Long),
        ("Fl", ColumnType::Float),
        ("Do", ColumnType::Double),
        ("St", ColumnType::String),
        ("Ti", ColumnType::Instant),
        ("Ints", ColumnType::vector(ColumnType::Int)),
        ("Strings", ColumnType::array(ColumnType::String)),
    ])
}

fn empty_of(definition: &TableDefinition) -> Vec<ArrayRef> {
    definition
        .columns()
        .iter()
        .map(|column| new_empty_array(&column.column_type.arrow_type()))
        .collect()
}

/// `change` as the server holds it before it writes it for a subscriber.
fn server_message(definition: &TableDefinition, change: &TableChange) -> BarrageMessage {
    let empty = empty_of(definition);
    BarrageMessage {
        first_seq: 3,
        last_seq: 4,
        is_snapshot: false,
        rows_added: change.update.added.clone(),
        rows_removed: change.update.removed.clone(),
        shifted: change.update.shifted.clone(),
        rows_included: change.update.added.clone(),
        add_columns: if change.update.added.is_empty() {
            empty.clone()
        } else {
            change.added_values.clone()
        },
        mod_columns: empty
            .iter()
            .enumerate()
            .map(|(index, none)| match change.modified_values.get(&index) {
                Some(values) => ModColumn {
                    rows_modified: change.update.modified.clone(),
                    data: values.clone(),
                },
                None => ModColumn {
                    rows_modified: RowSet::empty(),
                    data: none.clone(),
                },
            })
            .collect(),
        table_size: 0,
    }
}

struct Wire {
    schema: RemoteSchema,
    options: WireOptions,
}

impl Wire {
    fn new(definition: &TableDefinition, seed: u64) -> Self {
        let schema =
            RemoteSchema::of(arrow_schema(definition, &SchemaContext::default())).unwrap();
        assert_eq!(&schema.definition, definition);
        Wire {
            schema,
            options: WireOptions {
                use_deephaven_nulls: seed % 2 == 0,
                columns_as_list: seed % 3 == 0,
                preview_list_length_limit: 0,
                batch: BatchOptions {
                    batch_size: [0, 1, 3, 1000][(seed % 4) as usize],
                    ..BatchOptions::default()
                },
            },
        }
    }

    fn write(&self, message: &BarrageMessage) -> Vec<FlightData> {
        let nullable: Vec<bool> = self
            .schema
            .arrow
            .fields()
            .iter()
            .map(|field| field.is_nullable())
            .collect();
        subscription_messages(
            message,
            &nullable,
            &SubView {
                options: self.options,
                is_initial_snapshot: message.is_snapshot,
                is_full_subscription: true,
                viewport: None,
                reverse_viewport: false,
                keyspace_viewport_prev: None,
                keyspace_viewport: None,
                subscribed_columns: None,
            },
        )
        .unwrap()
    }

    fn reader(&self) -> MessageReader {
        MessageReader::new(
            self.schema.definition.clone(),
            &self.schema.arrow,
            ReadOptions {
                columns_as_list: self.options.columns_as_list,
            },
        )
        .unwrap()
    }
}

fn assert_same(sent: &BarrageMessage, read: &RemoteMessage, context: &str) {
    assert_eq!(read.first_seq, sent.first_seq, "{context}");
    assert_eq!(read.last_seq, sent.last_seq, "{context}");
    assert_eq!(read.is_snapshot, sent.is_snapshot, "{context}");
    assert_eq!(read.rows_added, sent.rows_added, "{context}");
    assert_eq!(read.rows_removed, sent.rows_removed, "{context}");
    assert_eq!(read.shifted, sent.shifted, "{context}");
    assert_eq!(read.rows_included, sent.rows_included, "{context}");
    assert_eq!(read.add_columns.len(), sent.add_columns.len(), "{context}");
    for (index, (read, sent)) in read.add_columns.iter().zip(&sent.add_columns).enumerate() {
        assert_eq!(read.as_ref(), sent.as_ref(), "{context} added column {index}");
    }
    assert_eq!(read.mod_columns.len(), sent.mod_columns.len(), "{context}");
    for (index, (read, sent)) in read.mod_columns.iter().zip(&sent.mod_columns).enumerate() {
        assert_eq!(
            read.rows_modified, sent.rows_modified,
            "{context} column {index}"
        );
        assert_eq!(
            read.data.as_ref(),
            sent.data.as_ref(),
            "{context} modified column {index}"
        );
    }
}

/// What the writer of this crate sends a full subscriber for a change is read back as
/// that change, whatever the batch size, the way nulls are sent and whether columns are
/// wrapped in lists; a change of nothing is read as a message of nothing.
#[test]
fn what_the_server_writes_for_a_change_is_read_back() {
    for seed in test_seeds(0..96) {
        let definition = types();
        let wire = Wire::new(&definition, seed);
        let graph = SteppedUpdateGraph::new();
        let handle: Arc<dyn UpdateGraph> = graph.clone();
        let scripted = ScriptedTable::new(
            handle,
            definition.clone(),
            RowSet::empty(),
            empty_of(&definition),
        );
        let source = scripted.table();
        let mut random = RandomUpdates::new(seed);
        let mut reader = wire.reader();
        let mut changes = vec![TableChange::new()];
        for _ in 0..8 {
            let change = random.next_change(source.as_ref(), 9);
            scripted.push(change.clone());
            graph.run_cycle();
            changes.push(change);
        }
        for (step, change) in changes.iter().enumerate() {
            let sent = server_message(&definition, change);
            let frames = wire.write(&sent);
            let mut read = Vec::new();
            for frame in &frames {
                read.extend(reader.accept(frame).unwrap());
            }
            assert!(!reader.is_mid_message());
            assert_eq!(read.len(), 1, "seed {seed} step {step}");
            assert_same(&sent, &read[0], &format!("seed {seed} step {step}"));
        }
    }
}

/// A small deterministic generator for the damage below.
struct Dice(u64);

impl Dice {
    fn roll(&mut self, sides: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % sides.max(1)
    }
}

/// A stream that was cut, repeated, reordered or changed in a byte is refused with an
/// error or read as some message; it is never a panic.
#[test]
fn a_damaged_stream_is_refused_and_does_not_panic() {
    let definition = types();
    let mut refused = 0;
    for seed in test_seeds(0..400) {
        let wire = Wire::new(&definition, seed);
        let mut random = RandomUpdates::new(seed);
        let graph = SteppedUpdateGraph::new();
        let handle: Arc<dyn UpdateGraph> = graph.clone();
        let scripted = ScriptedTable::new(
            handle,
            definition.clone(),
            RowSet::empty(),
            empty_of(&definition),
        );
        let mut frames = Vec::new();
        for _ in 0..3 {
            let change = random.next_change(scripted.table().as_ref(), 6);
            frames.extend(wire.write(&server_message(&definition, &change)));
            scripted.push(change);
            graph.run_cycle();
        }
        let mut dice = Dice(seed);
        let victim = dice.roll(frames.len());
        match dice.roll(6) {
            0 => {
                frames.remove(victim);
            }
            1 => {
                let again = frames[victim].clone();
                frames.insert(victim, again);
            }
            2 => {
                let mut body = frames[victim].data_body.to_vec();
                body.truncate(dice.roll(body.len()));
                frames[victim].data_body = body.into();
            }
            kind => {
                let frame = &mut frames[victim];
                let part = match kind {
                    3 => &mut frame.data_header,
                    4 => &mut frame.app_metadata,
                    _ => &mut frame.data_body,
                };
                let mut bytes = part.to_vec();
                if !bytes.is_empty() {
                    let at = dice.roll(bytes.len());
                    bytes[at] ^= 1 << dice.roll(8);
                }
                *part = bytes.into();
            }
        }
        let mut reader = wire.reader();
        for frame in &frames {
            if reader.accept(frame).is_err() {
                refused += 1;
                break;
            }
        }
    }
    assert!(refused > 0, "no damage was noticed");
}

/// What a stream may not hold is refused by name.
#[test]
fn messages_out_of_place_are_refused_by_name() {
    let definition = definition(&[("A", ColumnType::Int)]);
    let wire = Wire::new(&definition, 1);
    let change = TableChange::new().add([1, 2], vec![oxhaven_testkit::ints([1, 2])]);
    let frames = wire.write(&server_message(&definition, &change));

    // a record batch that no metadata announced
    let mut bare = frames[0].clone();
    bare.app_metadata = Default::default();
    let refused = wire.reader().accept(&bare).unwrap_err();
    assert!(
        refused.to_string().contains("Missing app metadata tag"),
        "{refused}"
    );

    // a message that begins before the last one ended
    let mut reader = wire.reader();
    let two_rows = Wire {
        options: WireOptions {
            batch: BatchOptions {
                batch_size: 1,
                ..BatchOptions::default()
            },
            ..wire.options
        },
        schema: wire.schema.clone(),
    };
    let halves = two_rows.write(&server_message(&definition, &change));
    assert!(halves.len() > 1);
    assert!(reader.accept(&halves[0]).unwrap().is_none());
    assert!(reader.is_mid_message());
    let refused = reader.accept(&halves[0]).unwrap_err();
    assert!(
        refused.to_string().contains("Previous message was not complete"),
        "{refused}"
    );

    let mut nothing = frames[0].clone();
    nothing.data_header = Default::default();
    nothing.app_metadata = Default::default();
    let refused = wire.reader().accept(&nothing).unwrap_err();
    assert!(
        refused.to_string().contains("Missing metadata header"),
        "{refused}"
    );
}
