//! The client against this crate's own server, over a socket.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, DurationNanosecondArray, Float64Array,
    Int32Array, Int8Array, StringArray, StructArray, Time64NanosecondArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use oxhaven_column::java_values::{big_decimal_cell, big_integer_cell};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::table::{
    append_only_input_table, blink_input_table, in_memory_table, keyed_input_table,
};
use oxhaven_testkit::{
    assert_tables_equal, definition, diff_tables, ints, list_of, strings, test_seeds,
    ConsistencyChecker, RandomUpdates, ScriptedTable, TableChange, TableSnapshot,
};
use tokio::io::copy_bidirectional;
use tokio::net::TcpListener;

use super::*;
use crate::session::{export_id_to_bytes, ExportState, ExportValue};
use crate::test_support::{TestServer, PSK};

/// Options that make the server send each change as soon as it has it.
const PROMPT: SubscriptionOptions = SubscriptionOptions {
    use_deephaven_nulls: true,
    columns_as_list: false,
    min_update_interval_ms: 1,
    batch_size: 0,
    max_message_size: 0,
};

const PATIENCE: Duration = Duration::from_secs(20);

fn config(server: &TestServer) -> ClientConfig {
    ClientConfig::new("127.0.0.1", server.address.port()).with_psk(PSK)
}

async fn connect(server: &TestServer) -> (BarrageSession, Arc<SteppedUpdateGraph>) {
    let local = SteppedUpdateGraph::new();
    let session = BarrageSession::connect(config(server), local.clone(), None)
        .await
        .unwrap();
    (session, local)
}

fn publish(server: &TestServer, name: &str, table: Arc<dyn Table>) -> Vec<u8> {
    server
        .state
        .scope
        .put(name, ExportValue::Table(table))
        .unwrap();
    scope_ticket(name)
}

/// The barrier of these tests: runs cycles of the local graph until `done`, and panics
/// with what `describe` says if that takes longer than anything here should.
async fn until(
    local: &SteppedUpdateGraph,
    mut done: impl FnMut() -> bool,
    describe: impl Fn() -> String,
) {
    let start = tokio::time::Instant::now();
    loop {
        local.run_cycle();
        if done() {
            return;
        }
        assert!(start.elapsed() < PATIENCE, "{}", describe());
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Waits until `copy` has what `source` has, row for row and key for key.
async fn caught_up(local: &SteppedUpdateGraph, source: &dyn Table, copy: &dyn Table) {
    until(
        local,
        || diff_tables(source, copy).is_none() && *source.row_set() == *copy.row_set(),
        || {
            format!(
                "the copy never caught up: {:?}; rows {:?} and {:?}",
                diff_tables(source, copy),
                source.row_set(),
                copy.row_set()
            )
        },
    )
    .await;
    assert!(!copy.is_failed());
}

fn rows_of(table: &dyn Table) -> Vec<Vec<String>> {
    let snapshot = TableSnapshot::current(table).unwrap();
    (0..snapshot.rows.size() as usize)
        .map(|row| {
            snapshot
                .columns
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        "null".to_string()
                    } else {
                        array_value_to_string(column, row).unwrap()
                    }
                })
                .collect()
        })
        .collect()
}

fn batch_of(definition: &TableDefinition, columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Three rows, the second null in every column, of every type the client reads that the
/// random tables below do not have.
fn rarer_types() -> Arc<dyn Table> {
    fn cells<T>(value: impl Fn(i64) -> T) -> Vec<Option<T>> {
        (0..3).map(|i| (i != 1).then(|| value(i))).collect()
    }
    let valid = arrow::buffer::NullBuffer::from(vec![true, false, true]);
    let period = ColumnType::Period.arrow_type();
    let DataType::Struct(period_fields) = &period else {
        unreachable!()
    };
    // Java sends a Period as its months alone
    let periods = StructArray::new(
        period_fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 0])),
            Arc::new(Int32Array::from(vec![14, 0, -3])),
            Arc::new(Int32Array::from(vec![0, 0, 0])),
        ],
        Some(valid.clone()),
    );
    let zoned = ColumnType::ZonedDateTime.arrow_type();
    let DataType::Struct(zoned_fields) = &zoned else {
        unreachable!()
    };
    // Java sends a ZonedDateTime as its instant, which arrives in UTC
    let zoned = StructArray::new(
        zoned_fields.clone(),
        vec![
            Arc::new(
                TimestampNanosecondArray::from(vec![1_700_000_000_000_000_001, 0, -5])
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["UTC", "UTC", "UTC"])),
        ],
        Some(valid),
    );
    let byte_array = ColumnType::array(ColumnType::Byte);
    let bool_array = ColumnType::array(ColumnType::PrimitiveBoolean);
    let nested = ColumnType::array(ColumnType::vector(ColumnType::Int));
    let inner = ColumnType::vector(ColumnType::Int);
    let columns: Vec<(&str, ColumnType, ArrayRef)> = vec![
        (
            "Date",
            ColumnType::LocalDate,
            Arc::new(Date32Array::from(cells(|i| 19_000 + i as i32))),
        ),
        (
            "Time",
            ColumnType::LocalTime,
            Arc::new(Time64NanosecondArray::from(cells(|i| {
                3_600_000_000_001 + i
            }))),
        ),
        (
            "Dur",
            ColumnType::Duration,
            Arc::new(DurationNanosecondArray::from(cells(|i| {
                1_000_000_007 * (i - 1)
            }))),
        ),
        ("Per", ColumnType::Period, Arc::new(periods)),
        ("Zdt", ColumnType::ZonedDateTime, Arc::new(zoned)),
        (
            "Dec",
            ColumnType::BigDecimal,
            Arc::new(BinaryArray::from_iter(cells(|i| {
                big_decimal_cell(2 + i as i32, &big_integer_cell(-12345 * (i as i128 + 1)))
            }))),
        ),
        (
            "Big",
            ColumnType::BigInteger,
            Arc::new(BinaryArray::from_iter(cells(|i| {
                big_integer_cell((1i128 << 70) * (i as i128 - 1) + 3)
            }))),
        ),
        (
            "Bytes",
            byte_array.clone(),
            list_of(
                &byte_array,
                [Some(2), None, Some(0)],
                Arc::new(Int8Array::from(vec![1, -1])),
            ),
        ),
        (
            "Flags",
            bool_array.clone(),
            list_of(
                &bool_array,
                [Some(1), None, Some(2)],
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ),
        ),
        (
            "Nested",
            nested.clone(),
            list_of(
                &nested,
                [Some(2), None, Some(1)],
                list_of(&inner, [Some(2), None, Some(1)], ints([1, 2, 3])),
            ),
        ),
    ];
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let definition = definition(&names);
    let batch = batch_of(
        &definition,
        columns.into_iter().map(|(_, _, array)| array).collect(),
    );
    in_memory_table(definition, &[batch]).unwrap()
}

fn random_types() -> TableDefinition {
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
        ("Doubles", ColumnType::array(ColumnType::Double)),
        ("Strings", ColumnType::vector(ColumnType::String)),
    ])
}

fn empty_of(definition: &TableDefinition) -> Vec<ArrayRef> {
    definition
        .columns()
        .iter()
        .map(|column| arrow::array::new_empty_array(&column.column_type.arrow_type()))
        .collect()
}

/// A static table of the server is the same table here, snapshotted or subscribed, with
/// either way of sending nulls, for every type the client reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_static_table_arrives_whole_as_a_snapshot_and_as_a_subscription() {
    let server = TestServer::start().await;
    let (session, _local) = connect(&server).await;
    let mut random = RandomUpdates::new(7);
    let common = random_types();
    let values: Vec<ArrayRef> = common
        .columns()
        .iter()
        .map(|column| random.values(&column.column_type, 500))
        .collect();
    let common: Arc<dyn Table> =
        in_memory_table(common.clone(), &[batch_of(&common, values)]).unwrap();
    for (name, table) in [("common", common), ("rarer", rarer_types())] {
        let ticket = publish(&server, name, table.clone());
        for nulls in [false, true] {
            let snapshot = session
                .snapshot(
                    &ticket,
                    SnapshotOptions {
                        use_deephaven_nulls: nulls,
                        ..SnapshotOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(!snapshot.is_refreshing());
            assert_tables_equal(table.as_ref(), snapshot.as_ref());
            for as_list in [false, true] {
                let subscribed = session
                    .subscribe(
                        &ticket,
                        SubscriptionOptions {
                            use_deephaven_nulls: nulls,
                            columns_as_list: as_list,
                            ..PROMPT
                        },
                    )
                    .await
                    .unwrap();
                assert!(subscribed.is_refreshing());
                assert_tables_equal(table.as_ref(), subscribed.as_ref());
                assert_eq!(*table.row_set(), *subscribed.row_set(), "{name}");
            }
        }
    }
    session.close().await.unwrap();
}

/// A ticking table of the server, changed at random, is followed change for change: after
/// every barrier the copy has the server's rows at the server's keys, and each update it
/// told its listeners was true.
#[tokio::test(flavor = "multi_thread")]
async fn a_ticking_table_is_followed_row_for_row() {
    let server = TestServer::start().await;
    let (session, local) = connect(&server).await;
    for seed in test_seeds(0..12) {
        let definition = random_types();
        let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
        let scripted = ScriptedTable::new(
            graph,
            definition.clone(),
            RowSet::empty(),
            empty_of(&definition),
        );
        let source = scripted.table();
        let mut random = RandomUpdates::new(seed);
        // rows before the subscription, which arrive in its snapshot
        for _ in 0..3 {
            scripted.push(random.next_change(source.as_ref(), 40));
            server.state.graph.run_cycle();
        }
        let ticket = publish(&server, "ticking", source.clone());
        let copy = session
            .subscribe(
                &ticket,
                SubscriptionOptions {
                    use_deephaven_nulls: seed.is_multiple_of(2),
                    // a long interval makes the server send several cycles as one message
                    min_update_interval_ms: if seed.is_multiple_of(3) { 30 } else { 1 },
                    batch_size: if seed % 4 == 1 { 7 } else { 0 },
                    ..PROMPT
                },
            )
            .await
            .unwrap();
        assert!(copy.is_refreshing());
        let checker = ConsistencyChecker::attach(copy.clone());
        caught_up(&local, source.as_ref(), copy.as_ref()).await;
        for round in 0..12 {
            for _ in 0..1 + (seed + round) % 3 {
                scripted.push(random.next_change(source.as_ref(), 25));
                server.state.graph.run_cycle();
            }
            caught_up(&local, source.as_ref(), copy.as_ref()).await;
            checker.assert_consistent();
        }
        assert!(checker.updates_seen() > 0, "seed {seed}");
    }
    session.close().await.unwrap();
}

/// A table larger than one snapshot message, and larger than the blocks of a column,
/// arrives whole, and its later changes arrive too.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_table_arrives_whole_and_keeps_changing() {
    const ROWS: i64 = 150_000;
    let server = TestServer::start().await;
    let (session, local) = connect(&server).await;
    let definition = definition(&[("K", ColumnType::Int), ("S", ColumnType::String)]);
    let names: Vec<String> = (0..ROWS).map(|row| format!("row {row}")).collect();
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let scripted = ScriptedTable::new(
        graph,
        definition.clone(),
        RowSet::from_keys((0..ROWS).map(|row| row * 3)),
        vec![
            ints(0..ROWS as i32),
            strings(names.iter().map(String::as_str)),
        ],
    );
    let source = scripted.table();
    let ticket = publish(&server, "large", source.clone());
    for batch_size in [0, 1000] {
        let copy = session
            .subscribe(
                &ticket,
                SubscriptionOptions {
                    batch_size,
                    ..PROMPT
                },
            )
            .await
            .unwrap();
        assert_eq!(copy.row_set().size(), source.row_set().size());
        assert_tables_equal(source.as_ref(), copy.as_ref());
        let checker = ConsistencyChecker::attach(copy.clone());
        let last = source.row_set().iter().last().unwrap();
        scripted.push(
            TableChange::new()
                .remove([0, 3, last])
                .shift(300, last - 3, 1 << 30)
                .add([1, 2], vec![ints([-1, -2]), strings(["a", "b"])])
                .modify([6, (1 << 30) + 300], [(1, strings(["six", "moved"]))]),
        );
        server.state.graph.run_cycle();
        caught_up(&local, source.as_ref(), copy.as_ref()).await;
        checker.assert_consistent();
    }
    let snapshot = session
        .snapshot(&ticket, SnapshotOptions::resolver())
        .await
        .unwrap();
    assert_tables_equal(source.as_ref(), snapshot.as_ref());
    session.close().await.unwrap();
}

fn input_definition() -> TableDefinition {
    definition(&[
        ("K", ColumnType::Int),
        ("S", ColumnType::String),
        ("V", ColumnType::Double),
    ])
}

fn input_rows(keys: &[i32], tag: &str) -> RecordBatch {
    batch_of(
        &input_definition(),
        vec![
            Arc::new(Int32Array::from(keys.to_vec())),
            Arc::new(StringArray::from(
                keys.iter()
                    .map(|key| (*key != 4).then(|| format!("{tag}{key}")))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                keys.iter()
                    .map(|key| f64::from(*key) + 0.5)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn input_keys(keys: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("K", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(keys.to_vec()))],
    )
    .unwrap()
}

/// Input tables of the server, keyed and append-only, are followed through adds, changes
/// and deletes, and the append-only table says here what it says there.
#[tokio::test(flavor = "multi_thread")]
async fn input_tables_are_followed_through_adds_changes_and_deletes() {
    let server = TestServer::start().await;
    let (session, local) = connect(&server).await;
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();

    let keyed: Arc<dyn Table> = keyed_input_table(
        graph.clone(),
        input_definition(),
        &["K"],
        Some(&input_rows(&[1, 2, 3, 4, 5], "s")),
    )
    .unwrap();
    let ticket = publish(&server, "keyed", keyed.clone());
    let snapshot = session
        .snapshot(&ticket, SnapshotOptions::default())
        .await
        .unwrap();
    let before = rows_of(keyed.as_ref());
    assert_eq!(rows_of(snapshot.as_ref()), before);
    // a keyed input table is not flat, and neither is its snapshot, whose rows are at
    // 0..size all the same
    assert!(!keyed.is_flat() && !snapshot.is_flat());
    let copy = session.subscribe(&ticket, PROMPT).await.unwrap();
    let checker = ConsistencyChecker::attach(copy.clone());
    assert!(!copy.attributes().add_only && !copy.attributes().blink);
    let input = keyed.input_table().unwrap();
    let steps: Vec<Box<dyn Fn()>> = vec![
        Box::new(|| input.add(input_rows(&[7, 8], "s")).unwrap()),
        Box::new(|| input.add(input_rows(&[2], "m")).unwrap()),
        Box::new(|| input.delete(input_keys(&[1])).unwrap()),
        Box::new(|| {
            input.add(input_rows(&[9], "s")).unwrap();
            input.add(input_rows(&[3, 4], "m")).unwrap();
            input.delete(input_keys(&[5])).unwrap();
        }),
    ];
    for step in &steps {
        step();
        server.state.graph.run_cycle();
        caught_up(&local, keyed.as_ref(), copy.as_ref()).await;
        checker.assert_consistent();
    }
    assert!(checker.updates_seen() >= steps.len() as u64);
    // the snapshot is of the moment it was taken
    assert_eq!(rows_of(snapshot.as_ref()), before);

    let appended: Arc<dyn Table> =
        append_only_input_table(graph, input_definition(), Some(&input_rows(&[1, 2], "a")))
            .unwrap();
    let ticket = publish(&server, "appended", appended.clone());
    let copy = session.subscribe(&ticket, PROMPT).await.unwrap();
    assert!(copy.attributes().append_only && copy.attributes().add_only);
    let checker = ConsistencyChecker::attach(copy.clone());
    for keys in [[3, 4], [1, 5]] {
        appended
            .input_table()
            .unwrap()
            .add(input_rows(&keys, "b"))
            .unwrap();
        server.state.graph.run_cycle();
        caught_up(&local, appended.as_ref(), copy.as_ref()).await;
        checker.assert_consistent();
    }
    assert_eq!(copy.row_set().size(), 6);
    session.close().await.unwrap();
}

/// A snapshot of a blink table has the rows the table shows at that moment.
#[tokio::test(flavor = "multi_thread")]
async fn a_blink_table_is_snapshotted_and_its_subscription_answers_as_the_server_does() {
    let server = TestServer::start().await;
    let (session, local) = connect(&server).await;
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let blink: Arc<dyn Table> = blink_input_table(graph, input_definition());
    let ticket = publish(&server, "blink", blink.clone());

    // rows the table shows when it is snapshotted are the snapshot
    blink
        .input_table()
        .unwrap()
        .add(input_rows(&[100, 101], "x"))
        .unwrap();
    server.state.graph.run_cycle();
    let snapshot = session
        .snapshot(&ticket, SnapshotOptions::default())
        .await
        .unwrap();
    assert_eq!(rows_of(snapshot.as_ref()), rows_of(blink.as_ref()));
    assert_eq!(snapshot.row_set().size(), 2);
    // flat, as the remote table says it is; not a blink table (owner's item 7.25)
    assert!(snapshot.is_flat() && !snapshot.attributes().blink);
    server.state.graph.run_cycle();

    // This crate's server does not serve subscriptions to blink tables yet; what it
    // answers is what the caller is told, and the export is let go. The copy of a blink
    // table is tested on messages in `replica::tests` and against Deephaven by hand.
    let refused = session.subscribe(&ticket, PROMPT).await.err().unwrap();
    let ClientError::Status(status) = &refused else {
        panic!("{refused}");
    };
    assert_eq!(status.code(), tonic::Code::Unimplemented, "{status:?}");
    assert!(status.message().contains("blink tables"), "{status:?}");
    let on_server = server.session(&session.bearer_for_tests());
    until(
        &local,
        || {
            (1..=2).all(|id| {
                on_server
                    .get_export_if_exists(id)
                    .unwrap()
                    .is_none_or(|export| export.state() == ExportState::Released)
            })
        },
        || "the export of a refused subscription was not released".to_string(),
    )
    .await;
    session.close().await.unwrap();
}

/// A snapshot of some rows and some columns has those rows, and nulls in the columns
/// that were not asked for.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_takes_a_viewport_and_a_choice_of_columns() {
    let server = TestServer::start().await;
    let (session, _local) = connect(&server).await;
    let keys: Vec<i32> = (0..50).collect();
    let table: Arc<dyn Table> =
        in_memory_table(input_definition(), &[input_rows(&keys, "s")]).unwrap();
    let ticket = publish(&server, "t", table.clone());
    let all = rows_of(table.as_ref());

    let head = session
        .snapshot(
            &ticket,
            SnapshotOptions {
                viewport: Some(RowSet::from_range(10, 14)),
                ..SnapshotOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows_of(head.as_ref()), all[10..15]);

    let tail = session
        .snapshot(
            &ticket,
            SnapshotOptions {
                viewport: Some(RowSet::from_range(0, 2)),
                reverse_viewport: true,
                ..SnapshotOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows_of(tail.as_ref()), all[47..50]);

    let some = session
        .snapshot(
            &ticket,
            SnapshotOptions {
                viewport: Some(RowSet::from_range(1, 2)),
                columns: Some(vec!["V".to_string(), "K".to_string()]),
                ..SnapshotOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(some.definition(), table.definition());
    let expected: Vec<Vec<String>> = all[1..3]
        .iter()
        .map(|row| vec![row[0].clone(), "null".to_string(), row[2].clone()])
        .collect();
    assert_eq!(rows_of(some.as_ref()), expected);

    let refused = session
        .snapshot(
            &ticket,
            SnapshotOptions {
                columns: Some(vec!["Nope".to_string()]),
                ..SnapshotOptions::default()
            },
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&refused, ClientError::Config(text) if text.contains("Nope")),
        "{refused}"
    );
    session.close().await.unwrap();
}

/// The ways of naming a table: a variable, a field of an application, an export shared
/// under an id; and what a ticket that names nothing answers.
#[tokio::test(flavor = "multi_thread")]
async fn tables_are_fetched_by_scope_application_and_shared_tickets() {
    let table: Arc<dyn Table> =
        in_memory_table(input_definition(), &[input_rows(&[1, 2], "s")]).unwrap();
    let server = TestServer::start().await;
    let (session, _local) = connect(&server).await;
    let ticket = publish(&server, "t", table.clone());

    let fetched = session.fetch(&ticket).await.unwrap();
    assert_eq!(&fetched.schema.definition, table.definition());
    assert!(fetched.is_static);
    assert_eq!(fetched.size, 2);
    assert_eq!(fetched.export_id, 1);
    session.release(fetched.export_id).await.unwrap();

    let field = application_field_ticket(
        crate::app::demo::APPLICATION_ID,
        crate::app::demo::STATIC_FIELD,
    );
    match session.fetch(&field).await {
        Ok(fetched) => {
            assert!(fetched.is_static);
            session.release(fetched.export_id).await.unwrap();
        }
        Err(problem) => {
            // the test server has no application unless one was installed
            assert!(matches!(problem, ClientError::Status(_)), "{problem}");
        }
    }

    let missing = session
        .snapshot(&scope_ticket("nothing"), SnapshotOptions::default())
        .await
        .err()
        .unwrap();
    let ClientError::Status(status) = &missing else {
        panic!("{missing}");
    };
    assert_eq!(status.code(), tonic::Code::NotFound, "{status:?}");
    let missing = session
        .subscribe(&shared_id_to_bytes(b"nobody shared this"), PROMPT)
        .await
        .err()
        .unwrap();
    assert!(matches!(missing, ClientError::Status(_)), "{missing}");
    // a fetch that failed leaves no export that holds anything
    let bearer = session.bearer_for_tests();
    let on_server = server.session(&bearer);
    until(
        &SteppedUpdateGraph::new(),
        || {
            (1..=4).all(|id| {
                on_server
                    .get_export_if_exists(id)
                    .unwrap()
                    .is_none_or(|export| crate::session::is_terminal(export.state()))
            })
        },
        || {
            format!(
                "exports of failed fetches were not released: {:?}",
                (1..=4)
                    .map(|id| {
                        on_server
                            .get_export_if_exists(id)
                            .unwrap()
                            .map(|export| export.state())
                    })
                    .collect::<Vec<_>>()
            )
        },
    )
    .await;
    session.close().await.unwrap();
}

/// Each way of authenticating the test server takes works, a wrong key is refused with
/// the server's status, TLS is refused before anything is sent, and extra headers arrive.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_authenticates_or_says_why_not() {
    let server = TestServer::start().await;
    let local = SteppedUpdateGraph::new();
    let port = server.address.port();

    let anonymous =
        BarrageSession::connect(ClientConfig::new("127.0.0.1", port), local.clone(), None)
            .await
            .unwrap();
    assert!(!anonymous.is_closed());
    anonymous.close().await.unwrap();

    let mut with_headers = config(&server);
    with_headers.extra_headers = vec![("X-Test".to_string(), "yes".to_string())];
    BarrageSession::connect(with_headers, local.clone(), None)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

    let refused = BarrageSession::connect(
        ClientConfig::new("127.0.0.1", port).with_psk("not the key"),
        local.clone(),
        None,
    )
    .await
    .err()
    .unwrap();
    let ClientError::Status(status) = &refused else {
        panic!("{refused}");
    };
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let mut tls = config(&server);
    tls.use_tls = true;
    let refused = BarrageSession::connect(tls, local.clone(), None)
        .await
        .err()
        .unwrap();
    assert!(matches!(refused, ClientError::TlsNotAvailable), "{refused}");
    assert!(refused.to_string().contains("ChannelHelper#channelBuilder"));

    let mut certs = config(&server);
    certs.tls_root_certs = Some(b"pem".to_vec());
    let refused = BarrageSession::connect(certs, local.clone(), None)
        .await
        .err()
        .unwrap();
    assert!(matches!(refused, ClientError::Config(_)), "{refused}");

    let mut bad = config(&server);
    bad.extra_headers = vec![("a header".to_string(), "x".to_string())];
    let refused = BarrageSession::connect(bad, local.clone(), None)
        .await
        .err()
        .unwrap();
    assert!(matches!(refused, ClientError::Config(_)), "{refused}");

    // nothing listens on the port of a server that is gone
    drop(server);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let refused = BarrageSession::connect(ClientConfig::new("127.0.0.1", port), local, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(refused, ClientError::Connect(_)), "{refused}");
}

/// A session outlives its first token: the client pings the server often enough to be
/// given the next one, and uses it.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_is_kept_alive_past_its_first_token() {
    // the shortest a server lets a token last
    const TOKEN_MS: i64 = 10_000;
    let server = TestServer::start_expiring(TOKEN_MS, |_| {}).await;
    let (session, _local) = connect(&server).await;
    let table: Arc<dyn Table> =
        in_memory_table(input_definition(), &[input_rows(&[1], "s")]).unwrap();
    let ticket = publish(&server, "t", table);
    let first = session.bearer_for_tests();
    tokio::time::sleep(Duration::from_millis(TOKEN_MS as u64 + 1500)).await;
    assert_ne!(session.bearer_for_tests(), first);
    session
        .snapshot(&ticket, SnapshotOptions::default())
        .await
        .unwrap();
    session.close().await.unwrap();
}

/// Closing the session releases its exports on the server and fails the tables it
/// subscribed; a closed session refuses further calls. A table that is dropped while the
/// session is open releases its own export.
#[tokio::test(flavor = "multi_thread")]
async fn closing_a_session_releases_its_exports_and_fails_its_tables() {
    let server = TestServer::start().await;
    let (session, local) = connect(&server).await;
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let definition = definition(&[("K", ColumnType::Int)]);
    let scripted = ScriptedTable::new(
        graph,
        definition.clone(),
        RowSet::from_keys([1, 2]),
        vec![ints([1, 2])],
    );
    let ticket = publish(&server, "t", scripted.table());
    let on_server = server.session(&session.bearer_for_tests());
    let state_of = |id: i32| {
        on_server
            .get_export_if_exists(id)
            .unwrap()
            .map(|export| export.state())
    };

    let dropped = session.subscribe(&ticket, PROMPT).await.unwrap();
    let kept = session.subscribe(&ticket, PROMPT).await.unwrap();
    let fetched = session.fetch(&ticket).await.unwrap();
    assert_eq!(fetched.export_id, 3);
    for id in 1..=3 {
        assert_eq!(state_of(id), Some(ExportState::Exported), "export {id}");
    }
    // the subscriptions use the exports
    assert_eq!(export_id_to_bytes(fetched.export_id), [b'e', 3, 0, 0, 0]);

    drop(dropped);
    until(
        &local,
        || state_of(1) == Some(ExportState::Released),
        || format!("the export of a dropped table is {:?}", state_of(1)),
    )
    .await;
    assert_eq!(state_of(2), Some(ExportState::Exported));

    assert!(!on_server.is_expired());
    session.close().await.unwrap();
    assert!(session.is_closed());
    assert!(on_server.is_expired());
    until(
        &local,
        || kept.is_failed(),
        || "the table of a closed session did not fail".to_string(),
    )
    .await;
    let refused = session
        .snapshot(&ticket, SnapshotOptions::default())
        .await
        .err()
        .unwrap();
    assert!(matches!(refused, ClientError::Closed), "{refused}");
    let refused = session.subscribe(&ticket, PROMPT).await.err().unwrap();
    assert!(matches!(refused, ClientError::Closed), "{refused}");
    // closing twice is closing once
    session.close().await.unwrap();
}

/// A TCP relay to `to` that can be cut.
async fn relay(to: std::net::SocketAddr) -> (u16, tokio::sync::watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cut, is_cut) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let mut stop = is_cut.clone();
        loop {
            let accepted = tokio::select! {
                _ = stop.wait_for(|cut| *cut) => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((mut near, _)) = accepted else {
                return;
            };
            let mut is_cut = is_cut.clone();
            tokio::spawn(async move {
                let Ok(mut far) = tokio::net::TcpStream::connect(to).await else {
                    return;
                };
                tokio::select! {
                    _ = is_cut.wait_for(|cut| *cut) => {}
                    _ = copy_bidirectional(&mut near, &mut far) => {}
                }
            });
        }
    });
    (port, cut)
}

/// When the connection is lost, every table the session subscribed fails, with an error
/// that says the subscription ended; the tables it snapshotted stay.
#[tokio::test(flavor = "multi_thread")]
async fn losing_the_connection_fails_every_subscribed_table() {
    let server = TestServer::start().await;
    let (port, cut) = relay(server.address).await;
    let local = SteppedUpdateGraph::new();
    let session = BarrageSession::connect(
        ClientConfig::new("127.0.0.1", port).with_psk(PSK),
        local.clone(),
        None,
    )
    .await
    .unwrap();
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let definition = definition(&[("K", ColumnType::Int)]);
    let scripted = ScriptedTable::new(
        graph,
        definition.clone(),
        RowSet::from_keys([1, 2]),
        vec![ints([1, 2])],
    );
    let source = scripted.table();
    let ticket = publish(&server, "t", source.clone());
    let first = session.subscribe(&ticket, PROMPT).await.unwrap();
    let second = session.subscribe(&ticket, PROMPT).await.unwrap();
    let snapshot = session
        .snapshot(&ticket, SnapshotOptions::default())
        .await
        .unwrap();
    let recorder = oxhaven_testkit::Recorder::attach(first.clone());
    scripted.push(TableChange::new().add([5], vec![ints([5])]));
    server.state.graph.run_cycle();
    caught_up(&local, source.as_ref(), first.as_ref()).await;
    caught_up(&local, source.as_ref(), second.as_ref()).await;

    cut.send_replace(true);
    until(
        &local,
        || first.is_failed() && second.is_failed(),
        || "the tables did not fail when the connection was cut".to_string(),
    )
    .await;
    let failure = recorder.events().pop().unwrap().unwrap_err();
    assert!(
        failure
            .message
            .starts_with("Barrage subscription error for 127.0.0.1:"),
        "{}",
        failure.message
    );
    assert_eq!(rows_of(snapshot.as_ref()), [["1"], ["2"]]);
    assert!(!snapshot.is_failed());
}

/// A wake is called when something has arrived for a table, so that a graph that runs
/// on a timer can run sooner.
#[tokio::test(flavor = "multi_thread")]
async fn the_graph_is_woken_when_a_change_arrives() {
    let server = TestServer::start().await;
    let local = SteppedUpdateGraph::new();
    let woken = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let wake: Wake = {
        let woken = woken.clone();
        Arc::new(move || {
            woken.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
    };
    let session = BarrageSession::connect(config(&server), local.clone(), Some(wake))
        .await
        .unwrap();
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let definition = definition(&[("K", ColumnType::Int)]);
    let scripted = ScriptedTable::new(graph, definition, RowSet::empty(), vec![ints([])]);
    let ticket = publish(&server, "t", scripted.table());
    let copy = session.subscribe(&ticket, PROMPT).await.unwrap();
    let before = woken.load(std::sync::atomic::Ordering::SeqCst);
    scripted.push(TableChange::new().add([5], vec![ints([5])]));
    server.state.graph.run_cycle();
    let start = tokio::time::Instant::now();
    while woken.load(std::sync::atomic::Ordering::SeqCst) == before {
        assert!(start.elapsed() < PATIENCE, "no wake");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    local.run_cycle();
    assert_eq!(copy.row_set().size(), 1);
    session.close().await.unwrap();
}
