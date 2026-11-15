//! Tests of input tables: what Java answers for one (`../flight/fixtures/input_*`), and the
//! service over the wire.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::create_input_table_request::input_table_kind::{
    Blink, InMemoryAppendOnly, InMemoryKeyBacked, Kind,
};
use oxhaven_proto::grpc::create_input_table_request::{Definition, InputTableKind};
use oxhaven_proto::grpc::input_table_service_client::InputTableServiceClient;
use oxhaven_proto::grpc::{AddTableRequest, CreateInputTableRequest, DeleteTableRequest};
use tonic::{Code, Status};

use super::*;
use crate::flight::tests::support::{
    assert_do_get_matches, code_of, export_descriptor, fixture_lines, fixture_schema, Server,
};
use crate::flight::{do_get_messages, flight_info, wire_schema};
use crate::schema::schema_ipc_bytes;
use crate::session::{export_id_to_bytes, export_id_to_ticket};

fn columns() -> Vec<(&'static str, ColumnType)> {
    vec![
        ("K", ColumnType::Int),
        ("S", ColumnType::String),
        ("V", ColumnType::Double),
    ]
}

fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    TableDefinition::new(
        columns
            .iter()
            .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
            .collect(),
    )
    .unwrap()
}

fn arrow_schema(columns: &[(&str, ColumnType)]) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
            .collect::<Vec<_>>(),
    )
}

fn request(kind: Kind) -> CreateInputTableRequest {
    CreateInputTableRequest {
        result_id: None,
        kind: Some(InputTableKind { kind: Some(kind) }),
        definition: Some(Definition::Schema(schema_ipc_bytes(&arrow_schema(
            &columns(),
        )))),
    }
}

fn keyed(keys: &[&str]) -> Kind {
    Kind::InMemoryKeyBacked(InMemoryKeyBacked {
        key_columns: keys.iter().map(|key| (*key).to_owned()).collect(),
    })
}

fn append_only() -> Kind {
    Kind::InMemoryAppendOnly(InMemoryAppendOnly {})
}

fn graph() -> Arc<dyn UpdateGraph> {
    SteppedUpdateGraph::new()
}

/// A static table of `K`, `S` and `V`.
fn rows(k: &[i32], s: &[Option<&str>], v: &[Option<f64>]) -> Arc<dyn Table> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(k.to_vec())),
        Arc::new(StringArray::from(s.to_vec())),
        Arc::new(Float64Array::from(v.to_vec())),
    ];
    let batch = RecordBatch::try_new(Arc::new(arrow_schema(&self::columns())), columns).unwrap();
    in_memory_table(definition(&self::columns()), &[batch]).unwrap()
}

/// A static table of the key column `K` alone.
fn keys(k: &[i32]) -> Arc<dyn Table> {
    let schema = Arc::new(Schema::new(vec![Field::new("K", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(k.to_vec()))]).unwrap();
    in_memory_table(definition(&[("K", ColumnType::Int)]), &[batch]).unwrap()
}

/// The rows `capture.py` adds to its input tables.
fn captured_rows() -> Arc<dyn Table> {
    rows(&[1, 2], &[Some("a"), None], &[Some(1.5), None])
}

fn value(table: &Arc<dyn Table>) -> ExportValue {
    ExportValue::Table(table.clone())
}

/// `K` and `S` of every row, in row order.
fn contents(table: &Arc<dyn Table>) -> Vec<(i32, Option<String>)> {
    let snapshot = snapshot(table).unwrap();
    let k = snapshot.columns[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let s = snapshot.columns[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..snapshot.rows)
        .map(|row| {
            (
                k.value(row),
                s.is_valid(row).then(|| s.value(row).to_owned()),
            )
        })
        .collect()
}

#[test]
fn input_tables_are_described_and_sent_as_java_does() {
    let keyed_table = create_input_table(graph(), &request(keyed(&["K"])), None).unwrap();
    let appended = create_input_table(graph(), &request(append_only()), None).unwrap();
    assert!(keyed_table.is_refreshing() && appended.is_refreshing());

    // without rows a refreshing table still gets one record batch
    let java = fixture_schema("input_keyed_empty");
    assert_eq!(wire_schema(&keyed_table), java);
    let messages = do_get_messages(&keyed_table).unwrap();
    assert_eq!(messages.len(), 2);
    assert_do_get_matches("input_keyed_empty", &messages, &java);

    for (case, table) in [
        ("input_keyed", &keyed_table),
        ("input_append_only", &appended),
    ] {
        add_table(&value(table), &value(&captured_rows())).unwrap();
        let java = fixture_schema(case);
        assert_eq!(wire_schema(table), java, "{case}");
        assert_do_get_matches(case, &do_get_messages(table).unwrap(), &java);
        // the size of a refreshing table is not known
        let info = flight_info(table, export_descriptor(1), export_id_to_bytes(1));
        assert_eq!((info.total_records, info.total_bytes), (-1, -1), "{case}");
    }
}

/// The `deephaven:inputtable.*` keys of each field, and whether the schema says the table
/// has an input table.
fn input_roles(table: &Arc<dyn Table>) -> (Vec<(String, Vec<String>)>, bool) {
    let schema = crate::schema::table_schema(table.as_ref());
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let mut roles: Vec<String> = field
                .metadata()
                .keys()
                .filter_map(|key| key.strip_prefix("deephaven:inputtable."))
                .map(str::to_owned)
                .collect();
            roles.sort_unstable();
            (field.name().clone(), roles)
        })
        .collect();
    let marked = schema
        .metadata()
        .contains_key("deephaven:unsent.attribute.InputTable");
    assert_eq!(
        marked,
        schema.metadata().contains_key("deephaven:tableMetadata")
    );
    (fields, marked)
}

/// Observed on Java 42.9 for `input_table(K int key, S String, V double)` and the tables
/// below: the roles are those of the input table's own columns, by name.
#[test]
fn derived_tables_mark_only_the_columns_of_their_input_table() {
    use oxhaven_engine::ops::select::{update, update_view};
    use oxhaven_engine::ops::sort::{sort, SortColumn};
    use oxhaven_expr::Compiler;

    let table = create_input_table(graph(), &request(keyed(&["K"])), None).unwrap();
    let roles = |names: &[(&str, &[&str])]| -> Vec<(String, Vec<String>)> {
        names
            .iter()
            .map(|(name, roles)| {
                let roles = roles.iter().map(|&role| role.to_owned()).collect();
                ((*name).to_owned(), roles)
            })
            .collect()
    };
    let own: [(&str, &[&str]); 3] = [("K", &["isKey"]), ("S", &["isValue"]), ("V", &["isValue"])];
    assert_eq!(input_roles(&table), (roles(&own), true));

    // an added column is neither; a replaced one keeps the role of its name
    let added = update_view(&table, &["X = V + 1", "S = 1"], &Compiler).unwrap();
    let mut with_added = own.to_vec();
    with_added.push(("X", &[]));
    assert_eq!(input_roles(&added), (roles(&with_added), true));
    let sorted = sort(&added, &[SortColumn::descending("X")]).unwrap();
    assert_eq!(input_roles(&sorted), (roles(&with_added), true));

    // these results no longer have the input table
    let plain: Vec<(&str, &[&str])> = with_added
        .iter()
        .map(|(name, _)| (*name, &[][..]))
        .collect();
    let stored = update(&table, &["X = V + 1"], &Compiler).unwrap();
    assert_eq!(input_roles(&stored), (roles(&plain), false));
    let by_size = sort(&table, &[SortColumn::ascending("V").absolute()]).unwrap();
    assert_eq!(input_roles(&by_size), (roles(&plain[..3]), false));
}

#[test]
fn keyed_table_replaces_deletes_and_reuses_the_place_of_a_key() {
    let table = create_input_table(graph(), &request(keyed(&["K"])), None).unwrap();
    let target = value(&table);
    add_table(&target, &value(&captured_rows())).unwrap();
    assert_eq!(contents(&table), [(1, Some("a".into())), (2, None)]);

    // the same key again replaces the row; a new key goes to the end
    let more = rows(&[1, 3], &[Some("b"), Some("c")], &[None, None]);
    add_table(&target, &value(&more)).unwrap();
    assert_eq!(
        contents(&table),
        [(1, Some("b".into())), (2, None), (3, Some("c".into()))]
    );

    // deleting ignores a key that is not there
    delete_table(&target, &value(&keys(&[1, 9]))).unwrap();
    assert_eq!(contents(&table), [(2, None), (3, Some("c".into()))]);

    // a deleted key that comes back takes its old place
    let back = rows(&[1], &[Some("again")], &[None]);
    add_table(&target, &value(&back)).unwrap();
    assert_eq!(
        contents(&table),
        [(1, Some("again".into())), (2, None), (3, Some("c".into()))]
    );

    // nothing to add or delete is not an error and changes nothing
    add_table(&target, &value(&rows(&[], &[], &[]))).unwrap();
    delete_table(&target, &value(&keys(&[]))).unwrap();
    assert_eq!(contents(&table).len(), 3);
}

#[test]
fn append_only_table_appends_and_refuses_deletes() {
    let table = create_input_table(graph(), &request(append_only()), None).unwrap();
    let target = value(&table);
    add_table(&target, &value(&captured_rows())).unwrap();
    add_table(&target, &value(&captured_rows())).unwrap();
    assert_eq!(
        contents(&table),
        [
            (1, Some("a".into())),
            (2, None),
            (1, Some("a".into())),
            (2, None)
        ]
    );
    let status = status_of(delete_table(&target, &value(&keys(&[1]))).unwrap_err());
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Provided input table does not support delete."
    );
    assert_eq!(contents(&table).len(), 4);
}

fn status_of(error: ExportError) -> Status {
    match error {
        ExportError::Status(status) => status,
        ExportError::Internal(text) => panic!("an internal error: {text}"),
    }
}

#[test]
fn changes_that_do_not_fit_are_refused() {
    let table = create_input_table(graph(), &request(keyed(&["K"])), None).unwrap();
    let target = value(&table);

    // not an input table
    let status =
        status_of(add_table(&value(&captured_rows()), &value(&captured_rows())).unwrap_err());
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "Table can't be used as an input table");
    let status =
        status_of(delete_table(&value(&captured_rows()), &value(&keys(&[1]))).unwrap_err());
    assert_eq!(status.code(), Code::InvalidArgument);

    // other columns than the input table's
    let status = status_of(add_table(&target, &value(&keys(&[1]))).unwrap_err());
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status
        .message()
        .starts_with("Provided tables's columns are not compatible"));

    // keys with a column that is not a key column: an exception in Java, behind an error id
    let error = delete_table(&target, &value(&captured_rows())).unwrap_err();
    assert!(matches!(error, ExportError::Internal(_)), "{error:?}");
    // not tables at all
    let error = add_table(&ExportValue::object(1_i32), &target).unwrap_err();
    assert!(matches!(error, ExportError::Internal(_)), "{error:?}");

    assert!(contents(&table).is_empty());
}

#[test]
fn create_input_table_checks_the_request_and_says_what_is_not_ported() {
    let from_source = |kind: Kind, source: &Arc<dyn Table>| -> Result<Arc<dyn Table>, Status> {
        let request = CreateInputTableRequest {
            definition: Some(Definition::SourceTableId(
                oxhaven_proto::grpc::TableReference {
                    r#ref: Some(oxhaven_proto::grpc::table_reference::Ref::Ticket(
                        export_id_to_ticket(1),
                    )),
                },
            )),
            ..request(kind)
        };
        create_input_table(graph(), &request, Some(source))
    };
    // the columns of a source table, not its rows
    let table = from_source(keyed(&["K"]), &captured_rows()).unwrap();
    assert_eq!(table.definition(), captured_rows().definition());
    assert_eq!(table.row_set().size(), 0);
    assert_eq!(table.input_table().unwrap().key_columns(), ["K"]);
    let table = create_input_table(graph(), &request(append_only()), None).unwrap();
    assert!(table.input_table().unwrap().key_columns().is_empty());
    let attributes = table.attributes();
    assert!(attributes.add_only && attributes.append_only);

    let refused = |request: CreateInputTableRequest| {
        create_input_table(graph(), &request, None)
            .map(|_| ())
            .unwrap_err()
    };
    let no_definition = CreateInputTableRequest {
        definition: None,
        ..request(append_only())
    };
    let status = refused(no_definition);
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Must specify one of schema and source_table_id"
    );
    let no_kind = CreateInputTableRequest {
        kind: None,
        ..request(append_only())
    };
    let status = refused(no_kind);
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "Unrecognized InputTableKind");
    let unknown_key = refused(request(keyed(&["Nope"])));
    assert_eq!(unknown_key.code(), Code::InvalidArgument);

    for not_ported in [
        refused(request(Kind::Blink(Blink {}))),
        refused(request(keyed(&[]))),
        refused(request(keyed(&["V"]))),
    ] {
        assert_eq!(not_ported.code(), Code::Unimplemented, "{not_ported:?}");
        assert!(not_ported.message().starts_with("not ported yet: "));
    }
}

fn add_request(input: i32, rows: i32) -> AddTableRequest {
    AddTableRequest {
        input_table: Some(export_id_to_ticket(input)),
        table_to_add: Some(export_id_to_ticket(rows)),
    }
}

fn delete_request(input: i32, keys: i32) -> DeleteTableRequest {
    DeleteTableRequest {
        input_table: Some(export_id_to_ticket(input)),
        table_to_remove: Some(export_id_to_ticket(keys)),
    }
}

#[tokio::test]
async fn service_calls_answer_what_java_answers() {
    let server = &Server::start().await;
    let table =
        create_input_table(server.state.graph.clone(), &request(keyed(&["K"])), None).unwrap();
    let appended =
        create_input_table(server.state.graph.clone(), &request(append_only()), None).unwrap();
    // as in `capture.py`: the keyed table already has the rows it is given again
    add_table(&value(&table), &value(&captured_rows())).unwrap();
    let (keyed, rows, only_key, append_only) = (1, 2, 3, 4);
    server.export(keyed, table.clone());
    server.export(rows, captured_rows());
    server.export(only_key, keys(&[2]));
    server.export(append_only, appended.clone());
    let service = || InputTableServiceClient::new(server.channel.clone());

    let unauthenticated = service()
        .add_table_to_input_table(add_request(keyed, rows))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let add = |request: AddTableRequest| async {
        service()
            .add_table_to_input_table(server.authorized(request))
            .await
    };
    let delete = |input: i32, keys: i32| async move {
        service()
            .delete_table_from_input_table(server.authorized(delete_request(input, keys)))
            .await
    };
    let no_input_table = AddTableRequest {
        input_table: None,
        ..add_request(keyed, rows)
    };
    let ours = [
        (
            "Add to a keyed input table",
            code_of(add(add_request(keyed, rows))).await,
        ),
        (
            "Add to a table that is not an input table",
            code_of(add(add_request(rows, rows))).await,
        ),
        (
            "Add a table of other columns",
            code_of(add(add_request(keyed, only_key))).await,
        ),
        (
            "Add without an input table",
            code_of(add(no_input_table)).await,
        ),
        (
            "Add an export that does not exist",
            code_of(add(add_request(keyed, 999_999))).await,
        ),
        (
            "Delete from a keyed input table",
            code_of(delete(keyed, only_key)).await,
        ),
        (
            "Delete from an append-only input table",
            code_of(delete(append_only, only_key)).await,
        ),
        (
            "Delete with a column that is not a key",
            code_of(delete(keyed, rows)).await,
        ),
        (
            "Delete from a table that is not an input table",
            code_of(delete(rows, only_key)).await,
        ),
    ];
    let ours: Vec<String> = ours
        .iter()
        .map(|(call, code)| format!("{call}: {code}"))
        .collect();
    assert_eq!(ours, fixture_lines("input_calls.status"));

    // what the two calls that were accepted left behind, as a client reads it
    assert_eq!(contents(&table), [(1, Some("a".into()))]);
    let java = fixture_schema("input_keyed_after_delete");
    let sent = server.do_get(keyed).await.unwrap();
    assert_do_get_matches("input_keyed_after_delete", &sent, &java);
    assert!(contents(&appended).is_empty());
}

#[tokio::test]
async fn service_change_is_visible_when_the_call_answers() {
    let server = Server::start().await;
    // the table comes from the table service, on the server's own update graph
    let create = CreateInputTableRequest {
        result_id: Some(export_id_to_ticket(1)),
        ..request(append_only())
    };
    let created =
        oxhaven_proto::grpc::table_service_client::TableServiceClient::new(server.channel.clone())
            .create_input_table(server.authorized(create))
            .await
            .unwrap()
            .into_inner();
    assert!(created.success && !created.is_static, "{created:?}");
    assert_eq!(
        crate::flight::tests::support::schema_of_ipc(&created.schema_header),
        fixture_schema("input_append_only")
    );
    let table = server.session().get_export(1).unwrap().value().unwrap();
    let table = table.as_table().unwrap().clone();
    let mut service = InputTableServiceClient::new(server.channel.clone());

    // the call waits for the table it adds
    let pending = tokio::spawn({
        let (mut service, request) = (service.clone(), server.authorized(add_request(1, 10)));
        async move { service.add_table_to_input_table(request).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    server.export(10, captured_rows());
    pending.await.unwrap().unwrap();
    // no cycle has to be awaited after the answer
    assert_eq!(contents(&table).len(), 2);

    service
        .add_table_to_input_table(server.authorized(add_request(1, 10)))
        .await
        .unwrap();
    assert_eq!(contents(&table).len(), 4);
    let java = fixture_schema("input_append_only");
    let sent = server.do_get(1).await.unwrap();
    assert_eq!(
        crate::flight::tests::support::schema_of_message(&sent[0]),
        java
    );
    assert_eq!(crate::flight::tests::support::batch_of(&sent[1]).rows, 4);
}
