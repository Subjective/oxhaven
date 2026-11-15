//! The partitioned table service over the wire. The statuses and texts are what the Java
//! reference (42.9) answered to the same calls; `docs/PARTITIONED.md` lists them.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::{append_only_input_table, in_memory_table};
use oxhaven_proto::grpc::partitioned_table_service_client::PartitionedTableServiceClient;
use tonic::{Code, Status};

use super::*;
use crate::flight::snapshot;
use crate::flight::tests::support::Server;
use crate::session::export_id_to_ticket;

fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    TableDefinition::new(
        columns
            .iter()
            .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
            .collect(),
    )
    .unwrap()
}

fn batch(k: &[i32], v: &[f64]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("K", DataType::Int32, true),
        Field::new("V", DataType::Float64, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(k.to_vec())),
        Arc::new(Float64Array::from(v.to_vec())),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn kv() -> TableDefinition {
    definition(&[("K", ColumnType::Int), ("V", ColumnType::Double)])
}

/// A static table of `K` and `V`.
fn rows(k: &[i32], v: &[f64]) -> Arc<dyn Table> {
    in_memory_table(kv(), &[batch(k, v)]).unwrap()
}

/// A static table of the key column `K` alone.
fn keys(k: &[i32]) -> Arc<dyn Table> {
    let schema = Arc::new(Schema::new(vec![Field::new("K", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(k.to_vec()))]).unwrap();
    in_memory_table(definition(&[("K", ColumnType::Int)]), &[batch]).unwrap()
}

/// `K` and `V` of every row, in row order.
fn contents(table: &Arc<dyn Table>) -> Vec<(i32, f64)> {
    let snapshot = snapshot(table).unwrap();
    let k = snapshot.columns[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let v = snapshot.columns[1]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..snapshot.rows)
        .map(|row| (k.value(row), v.value(row)))
        .collect()
}

fn partition_request(table: i32, result: i32, keys: &[&str]) -> PartitionByRequest {
    PartitionByRequest {
        table_id: Some(export_id_to_ticket(table)),
        result_id: Some(export_id_to_ticket(result)),
        key_column_names: keys.iter().map(|key| (*key).to_owned()).collect(),
        drop_keys: false,
    }
}

fn get_request(partitioned: i32, keys: i32, result: i32, behavior: UniqueBehavior) -> GetTableRequest {
    GetTableRequest {
        partitioned_table: Some(export_id_to_ticket(partitioned)),
        key_table_ticket: Some(export_id_to_ticket(keys)),
        result_id: Some(export_id_to_ticket(result)),
        unique_behavior: behavior as i32,
    }
}

fn merge_request(partitioned: i32, result: i32) -> MergeRequest {
    MergeRequest {
        partitioned_table: Some(export_id_to_ticket(partitioned)),
        result_id: Some(export_id_to_ticket(result)),
    }
}

fn refused(status: Status, code: Code, text: &str) {
    assert_eq!(status.code(), code, "{status:?}");
    assert!(
        status.message().starts_with(text),
        "{:?} does not start with {text:?}",
        status.message()
    );
}

const LOGGED: &str = "Details Logged w/ID '";

#[tokio::test]
async fn service_calls_answer_what_java_answers() {
    let server = &Server::start().await;
    server.export(1, rows(&[1, 2, 1, 3, 2], &[1.0, 2.0, 3.0, 4.0, 5.0]));
    server.export(2, keys(&[2]));
    server.export(3, keys(&[1, 3]));
    server.export(4, keys(&[9]));
    server.export(5, rows(&[2], &[0.0]));
    let service = || PartitionedTableServiceClient::new(server.channel.clone());
    let table = |id: i32| {
        let value = server.session().get_export(id).unwrap().value().unwrap();
        value.as_table().unwrap().clone()
    };

    // PartitionBy
    let made = service()
        .partition_by(server.authorized(partition_request(1, 10, &["K"])))
        .await;
    assert_eq!(made.unwrap().into_inner(), PartitionByResponse {});
    let value = server.session().get_export(10).unwrap().value().unwrap();
    let partitioned = partitioned_of(&value).unwrap();
    assert_eq!(partitioned.key_columns(), ["K"]);
    assert_eq!(partitioned.table().row_set().size(), 3);

    let unknown = service()
        .partition_by(server.authorized(partition_request(1, 11, &["Nope"])))
        .await;
    refused(unknown.unwrap_err(), Code::InvalidArgument, LOGGED);
    let of_partitioned = service()
        .partition_by(server.authorized(partition_request(10, 12, &["K"])))
        .await;
    refused(of_partitioned.unwrap_err(), Code::InvalidArgument, LOGGED);
    let mut no_result = partition_request(1, 13, &["K"]);
    no_result.result_id = None;
    let no_result = service().partition_by(server.authorized(no_result)).await;
    refused(
        no_result.unwrap_err(),
        Code::FailedPrecondition,
        "Could not resolve ticket 'resultId': ticket was not provided",
    );
    let mut missing = partition_request(1, 14, &["K"]);
    missing.table_id = Some(Ticket {
        ticket: b"s/nope".to_vec(),
    });
    let missing = service().partition_by(server.authorized(missing)).await;
    refused(
        missing.unwrap_err(),
        Code::NotFound,
        "Could not resolve 'tableId: variable 'nope' not found",
    );

    // Merge: the constituents one after the other, in the order of their keys' first rows
    let merged = service()
        .merge(server.authorized(merge_request(10, 20)))
        .await
        .unwrap()
        .into_inner();
    assert!(merged.success && merged.is_static);
    assert_eq!(merged.size, 5);
    assert_eq!(
        contents(&table(20)),
        [(1, 1.0), (1, 3.0), (2, 2.0), (2, 5.0), (3, 4.0)]
    );
    let of_table = service().merge(server.authorized(merge_request(1, 21))).await;
    refused(of_table.unwrap_err(), Code::InvalidArgument, LOGGED);

    // GetTable
    let unique = UniqueBehavior::RequireUniqueResultsStaticSingleKey;
    for (result, behavior) in [(30, UniqueBehavior::NotSetUniqueBehavior), (31, unique)] {
        let one = service()
            .get_table(server.authorized(get_request(10, 2, result, behavior)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((one.size, one.is_static), (2, true));
        assert_eq!(contents(&table(result)), [(2, 2.0), (2, 5.0)]);
    }
    let two_keys = service()
        .get_table(server.authorized(get_request(10, 3, 32, unique)))
        .await;
    refused(
        two_keys.unwrap_err(),
        Code::InvalidArgument,
        "Provided key table does not have one row, instead has 2",
    );
    let no_match = service()
        .get_table(server.authorized(get_request(10, 4, 33, unique)))
        .await;
    refused(
        no_match.unwrap_err(),
        Code::InvalidArgument,
        "Filtered PartitionedTable has more than one constituent, 0 constituents found",
    );
    // a key table needs the key columns only; another column of the same name is ignored
    let wider = service()
        .get_table(server.authorized(get_request(10, 5, 34, unique)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(wider.size, 2);

    let permit = UniqueBehavior::PermitMultipleKeys;
    let several = service()
        .get_table(server.authorized(get_request(10, 3, 35, permit)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(several.size, 3);
    assert_eq!(contents(&table(35)), [(1, 1.0), (1, 3.0), (3, 4.0)]);
    let none = service()
        .get_table(server.authorized(get_request(10, 4, 36, permit)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(none.size, 0);
    let mut invalid = get_request(10, 2, 37, permit);
    invalid.unique_behavior = 7;
    let invalid = service().get_table(server.authorized(invalid)).await;
    refused(
        invalid.unwrap_err(),
        Code::InvalidArgument,
        "Invalid unique behavior 7",
    );
    let keys_are_partitioned = service()
        .get_table(server.authorized(get_request(10, 10, 38, unique)))
        .await;
    refused(keys_are_partitioned.unwrap_err(), Code::InvalidArgument, LOGGED);
}

#[tokio::test]
async fn a_constituent_of_a_ticking_table_is_found_once_its_key_has_rows() {
    let server = &Server::start().await;
    let input = append_only_input_table(server.state.graph.clone(), kv(), None).unwrap();
    let writer = input.input_table().unwrap();
    writer.add(batch(&[1, 2], &[1.0, 2.0])).unwrap();
    server.export(1, input.clone());
    server.export(2, keys(&[5]));
    let service = || PartitionedTableServiceClient::new(server.channel.clone());
    service()
        .partition_by(server.authorized(partition_request(1, 10, &["K"])))
        .await
        .unwrap();

    let unique = UniqueBehavior::RequireUniqueResultsStaticSingleKey;
    let early = service()
        .get_table(server.authorized(get_request(10, 2, 20, unique)))
        .await;
    refused(
        early.unwrap_err(),
        Code::InvalidArgument,
        "Filtered PartitionedTable has more than one constituent, 0 constituents found",
    );

    writer.add(batch(&[5, 1], &[5.0, 6.0])).unwrap();
    let found = service()
        .get_table(server.authorized(get_request(10, 2, 21, unique)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((found.size, found.is_static), (1, false));
    let merged = service()
        .merge(server.authorized(merge_request(10, 22)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((merged.size, merged.is_static), (4, false));

    // the constituent and the merge go on following the table
    writer.add(batch(&[5], &[7.0])).unwrap();
    let table = |id: i32| {
        let value = server.session().get_export(id).unwrap().value().unwrap();
        value.as_table().unwrap().clone()
    };
    assert_eq!(contents(&table(21)), [(5, 5.0), (5, 7.0)]);
    assert_eq!(
        contents(&table(22)),
        [(1, 1.0), (1, 6.0), (2, 2.0), (5, 5.0), (5, 7.0)]
    );
}
