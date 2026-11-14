use super::ops::{bind, op_name, BoundOp, IMPLEMENTED};
use super::*;
use crate::session::{export_id_to_ticket, ExportState};
use crate::test_support::{with_auth, TestServer};
use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_engine::table::{empty_table, time_table, TimeTableOptions, WallClock};
use oxhaven_proto::grpc::batch_table_request::Operation;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::time_table_request::{Period, StartTime};
use std::collections::HashSet;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Code;

mod handlers;

type Client = TableServiceClient<Channel>;

fn ticket(id: i32) -> Option<Ticket> {
    Some(export_id_to_ticket(id))
}

fn by_ticket(id: i32) -> Option<TableReference> {
    Some(ticket_reference(export_id_to_ticket(id).ticket))
}

fn by_offset(offset: i32) -> Option<TableReference> {
    Some(TableReference {
        r#ref: Some(Ref::BatchOffset(offset)),
    })
}

fn empty(result: Option<Ticket>, size: i64) -> EmptyTableRequest {
    EmptyTableRequest {
        result_id: result,
        size,
    }
}

/// A time table that has one row for the rest of the century: it started just after the
/// epoch and its period is longer than the time since.
fn one_row_time_table(result: Option<Ticket>) -> TimeTableRequest {
    TimeTableRequest {
        result_id: result,
        blink_table: false,
        start_time: Some(StartTime::StartTimeNanos(1)),
        period: Some(Period::PeriodNanos(4_000_000_000_000_000_000)),
    }
}

fn ticking_time_table(result: Option<Ticket>) -> TimeTableRequest {
    TimeTableRequest {
        result_id: result,
        blink_table: false,
        start_time: None,
        period: Some(Period::PeriodNanos(1_000_000)),
    }
}

fn schema_of(response: &ExportedTableCreationResponse) -> Schema {
    let reader =
        StreamReader::try_new(std::io::Cursor::new(response.schema_header.clone()), None).unwrap();
    reader.schema().as_ref().clone()
}

fn schema_metadata(response: &ExportedTableCreationResponse) -> Vec<(String, String)> {
    let mut pairs: Vec<_> = schema_of(response).metadata().clone().into_iter().collect();
    pairs.sort();
    pairs
}

fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

const FLAT: [(&str, &str); 2] = [
    ("deephaven:attribute.IsFlat", "true"),
    ("deephaven:attribute_type.IsFlat", "java.lang.Boolean"),
];

struct Connected {
    server: TestServer,
    client: Client,
    bearer: String,
}

impl Connected {
    async fn start() -> Self {
        let server = TestServer::start().await;
        let bearer = server.login().await;
        let client = TableServiceClient::new(server.channel.clone());
        Self {
            server,
            client,
            bearer,
        }
    }

    fn auth<T>(&self, message: T) -> Request<T> {
        with_auth(message, &self.bearer)
    }

    /// The table exported under `id`.
    fn table(&self, id: i32) -> Arc<dyn Table> {
        let export = self.server.session(&self.bearer).get_export(id).unwrap();
        export.value().unwrap().as_table().unwrap().clone()
    }

    /// Runs one update graph cycle off the async thread.
    async fn cycle(&self) {
        let graph = self.server.state.graph.clone();
        tokio::task::spawn_blocking(move || graph.run_cycle())
            .await
            .unwrap();
    }

    async fn batch(
        &mut self,
        ops: Vec<Op>,
    ) -> Result<(Vec<ExportedTableCreationResponse>, Option<Status>), Status> {
        let ops = ops
            .into_iter()
            .map(|op| Operation { op: Some(op) })
            .collect();
        let request = self.auth(BatchTableRequest { ops });
        let mut stream = self.client.batch(request).await?.into_inner();
        let mut responses = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(response) => responses.push(response),
                Err(status) => return Ok((responses, Some(status))),
            }
        }
        Ok((responses, None))
    }
}

// ---------------------------------------------------------------- the registry

/// Every operation of the protocol, each with an empty request.
fn every_operation() -> Vec<Op> {
    vec![
        Op::EmptyTable(Default::default()),
        Op::TimeTable(Default::default()),
        Op::DropColumns(Default::default()),
        Op::Update(Default::default()),
        Op::LazyUpdate(Default::default()),
        Op::View(Default::default()),
        Op::UpdateView(Default::default()),
        Op::Select(Default::default()),
        Op::SelectDistinct(Default::default()),
        Op::Filter(Default::default()),
        Op::UnstructuredFilter(Default::default()),
        Op::Sort(Default::default()),
        Op::Head(Default::default()),
        Op::Tail(Default::default()),
        Op::HeadBy(Default::default()),
        Op::TailBy(Default::default()),
        Op::Ungroup(Default::default()),
        Op::Merge(Default::default()),
        Op::ComboAggregate(Default::default()),
        Op::Flatten(Default::default()),
        Op::RunChartDownsample(Default::default()),
        Op::CrossJoin(Default::default()),
        Op::NaturalJoin(Default::default()),
        Op::ExactJoin(Default::default()),
        Op::LeftJoin(Default::default()),
        Op::AsOfJoin(Default::default()),
        Op::FetchTable(Default::default()),
        Op::ApplyPreviewColumns(Default::default()),
        Op::CreateInputTable(Default::default()),
        Op::UpdateBy(Default::default()),
        Op::WhereIn(Default::default()),
        Op::AggregateAll(Default::default()),
        Op::Aggregate(Default::default()),
        Op::Snapshot(Default::default()),
        Op::SnapshotWhen(Default::default()),
        Op::MetaTable(Default::default()),
        Op::RangeJoin(Default::default()),
        Op::Aj(Default::default()),
        Op::Raj(Default::default()),
        Op::ColumnStatistics(Default::default()),
        Op::MultiJoin(Default::default()),
        Op::Slice(Default::default()),
    ]
}

/// `EmptyTable` as Java's `OpCase` names it: `EMPTY_TABLE`.
fn java_name(variant: &str) -> String {
    let mut name = String::new();
    for c in variant.chars() {
        if c.is_ascii_uppercase() && !name.is_empty() {
            name.push('_');
        }
        name.push(c.to_ascii_uppercase());
    }
    name
}

#[test]
fn every_operation_has_a_handler_or_answers_unimplemented_with_its_name() {
    let operations = every_operation();
    let names: HashSet<&str> = operations.iter().map(op_name).collect();
    assert_eq!(names.len(), 42, "operation names must be distinct");

    let implemented: HashSet<String> = IMPLEMENTED.iter().map(|v| java_name(v)).collect();
    assert_eq!(
        implemented,
        [
            "AGGREGATE",
            "AGGREGATE_ALL",
            "CREATE_INPUT_TABLE",
            "EMPTY_TABLE",
            "TIME_TABLE",
            "FETCH_TABLE",
            "META_TABLE",
            "UPDATE",
            "LAZY_UPDATE",
            "VIEW",
            "UPDATE_VIEW",
            "SELECT",
            "DROP_COLUMNS",
            "SORT",
            "HEAD",
            "TAIL",
            "SLICE",
            "MERGE",
            "FLATTEN",
            "UNSTRUCTURED_FILTER",
            "FILTER",
        ]
        .into_iter()
        .map(String::from)
        .collect::<HashSet<_>>(),
        "a new handler belongs in this list"
    );
    for op in operations {
        let name = op_name(&op);
        match bind(op) {
            Ok(_) => assert!(implemented.contains(name), "{name} is bound but not listed"),
            Err(status) => {
                assert!(
                    !implemented.contains(name),
                    "{name} is listed but not bound"
                );
                assert_eq!(status.code(), Code::Unimplemented);
                assert_eq!(
                    status.message(),
                    format!("TableService operation {name} is not implemented")
                );
            }
        }
    }
}

#[tokio::test]
async fn an_operation_without_a_handler_is_unimplemented_in_both_forms() {
    let mut c = Connected::start().await;
    let request = SelectDistinctRequest {
        result_id: ticket(1),
        source_id: by_ticket(2),
        column_names: Vec::new(),
    };
    let refused = c
        .client
        .select_distinct(c.auth(request.clone()))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "TableService operation SELECT_DISTINCT is not implemented"
    );

    // In a batch the whole call is refused and nothing before it is created.
    let refused = c
        .batch(vec![
            Op::EmptyTable(empty(ticket(3), 1)),
            Op::SelectDistinct(request),
        ])
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "TableService operation SELECT_DISTINCT is not implemented"
    );
    let session = c.server.session(&c.bearer);
    let unused = session.get_export(3).unwrap();
    assert_ne!(unused.state(), ExportState::Exported);
}

// ---------------------------------------------------------------- one-shot operations

#[tokio::test]
async fn empty_table_is_exported_and_described() {
    let mut c = Connected::start().await;
    let response = c
        .client
        .empty_table(c.auth(empty(ticket(1), 5)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.result_id, by_ticket(1));
    assert!(response.success);
    assert_eq!(response.error_info, "");
    assert!(response.is_static);
    assert_eq!(response.size, 5);
    assert_eq!(schema_of(&response).fields().len(), 0);
    assert_eq!(schema_metadata(&response), pairs(&FLAT));

    let table = c.table(1);
    assert_eq!(*table.row_set(), RowSet::flat(5));
    // an export that did not fail has no failure to report
    let export = c.server.session(&c.bearer).get_export(1).unwrap();
    assert!(export.failure().is_none());
}

#[tokio::test]
async fn a_call_without_a_session_is_unauthenticated() {
    let server = TestServer::start().await;
    let mut client = TableServiceClient::new(server.channel.clone());
    let refused = client.empty_table(empty(ticket(1), 5)).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
    let refused = client
        .batch(BatchTableRequest {
            ops: vec![Operation {
                op: Some(Op::EmptyTable(empty(None, 1))),
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn a_request_that_fails_validation_does_not_use_up_its_ticket() {
    let mut c = Connected::start().await;
    let refused = c
        .client
        .empty_table(c.auth(empty(ticket(1), -1)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(refused.message(), "Size must be greater than zero");

    let refused = c
        .client
        .empty_table(c.auth(empty(None, 1)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "No result ticket supplied");

    // zero is a size, and ticket 1 is still free
    let response = c
        .client
        .empty_table(c.auth(empty(ticket(1), 0)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.size, 0);

    // a second definition of the same export is refused
    let refused = c
        .client
        .empty_table(c.auth(empty(ticket(1), 3)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn time_table_is_refreshing_and_grows_with_the_graph() {
    let mut c = Connected::start().await;
    let response = c
        .client
        .time_table(c.auth(ticking_time_table(ticket(1))))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success);
    assert!(!response.is_static);
    assert_eq!(response.size, 0);
    let schema = schema_of(&response);
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "Timestamp");
    assert_eq!(
        schema.field(0).metadata()["deephaven:type"],
        "java.time.Instant"
    );
    assert_eq!(
        schema_metadata(&response),
        pairs(&[
            ("deephaven:attribute.AddOnly", "true"),
            ("deephaven:attribute.AppendOnly", "true"),
            ("deephaven:attribute.IsFlat", "true"),
            ("deephaven:attribute_type.AddOnly", "java.lang.Boolean"),
            ("deephaven:attribute_type.AppendOnly", "java.lang.Boolean"),
            ("deephaven:attribute_type.IsFlat", "java.lang.Boolean"),
        ])
    );

    c.cycle().await;
    let described = c
        .client
        .get_exported_table_creation_response(c.auth(export_id_to_ticket(1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(described.size, 1);
    assert_eq!(described.result_id, by_ticket(1));
    assert!(!described.is_static);
}

#[tokio::test]
async fn a_blink_time_table_with_a_start_has_its_rows_at_once() {
    let mut c = Connected::start().await;
    let mut request = one_row_time_table(ticket(1));
    request.blink_table = true;
    let response = c
        .client
        .time_table(c.auth(request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.size, 1);
    assert!(!response.is_static);
    assert_eq!(
        schema_metadata(&response),
        pairs(&[
            ("deephaven:attribute.BlinkTable", "true"),
            ("deephaven:attribute_type.BlinkTable", "java.lang.Boolean"),
        ])
    );
}

#[tokio::test]
async fn time_table_period_and_start_are_checked_as_java_checks_them() {
    let mut c = Connected::start().await;
    let mut request = ticking_time_table(ticket(1));
    request.period = Some(Period::PeriodNanos(0));
    let refused = c.client.time_table(c.auth(request)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "periodNanos must be >= 0 (found: 0)");

    let mut request = ticking_time_table(ticket(1));
    request.period = Some(Period::PeriodString("nope".to_string()));
    let refused = c.client.time_table(c.auth(request)).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(refused.message().starts_with("Details Logged w/ID '"));

    // The start is parsed by the export, so this failure uses up the ticket.
    let mut request = ticking_time_table(ticket(1));
    request.start_time = Some(StartTime::StartTimeString("nope".to_string()));
    let refused = c.client.time_table(c.auth(request)).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(refused.message().starts_with("Details Logged w/ID '"));
    let session = c.server.session(&c.bearer);
    assert_eq!(session.get_export(1).unwrap().state(), ExportState::Failed);

    let mut request = ticking_time_table(ticket(2));
    request.period = Some(Period::PeriodString("PT1M".to_string()));
    request.start_time = Some(StartTime::StartTimeString(
        "2024-01-01T00:00:00 UTC".to_string(),
    ));
    let response = c
        .client
        .time_table(c.auth(request))
        .await
        .unwrap()
        .into_inner();
    // one row per minute since the start, which was a while ago
    assert!(response.size > 1_000_000, "{}", response.size);
}

#[tokio::test]
async fn fetch_table_exports_the_same_table_under_another_ticket() {
    let mut c = Connected::start().await;
    c.client
        .empty_table(c.auth(empty(ticket(1), 4)))
        .await
        .unwrap();
    let response = c
        .client
        .fetch_table(c.auth(FetchTableRequest {
            source_id: by_ticket(1),
            result_id: ticket(2),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.result_id, by_ticket(2));
    assert_eq!(response.size, 4);
    assert!(Arc::ptr_eq(&c.table(1), &c.table(2)));
}

#[tokio::test]
async fn one_shot_sources_must_be_tickets_of_tables() {
    let mut c = Connected::start().await;
    for source in [None, by_offset(0)] {
        let refused = c
            .client
            .fetch_table(c.auth(FetchTableRequest {
                source_id: source,
                result_id: ticket(2),
            }))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::FailedPrecondition);
        assert_eq!(
            refused.message(),
            "One-shot operations must use ticket references"
        );
    }

    let refused = c
        .client
        .fetch_table(c.auth(FetchTableRequest {
            source_id: by_ticket(-5),
            result_id: ticket(2),
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "Export id -5 does not exist and cannot be used out-of-order!"
    );

    // An export that is not a table: an internal failure, as Java's failed cast is.
    let session = c.server.session(&c.bearer);
    session
        .new_export(7)
        .unwrap()
        .submit(|_| Ok(ExportValue::unit()))
        .unwrap();
    let refused = c
        .client
        .fetch_table(c.auth(FetchTableRequest {
            source_id: by_ticket(7),
            result_id: ticket(3),
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(refused.message().starts_with("Details Logged w/ID '"));
}

#[tokio::test]
async fn an_operation_waits_for_a_source_that_is_exported_later() {
    let mut c = Connected::start().await;
    let mut early = c.client.clone();
    let request = c.auth(FetchTableRequest {
        source_id: by_ticket(1),
        result_id: ticket(2),
    });
    let waiting = tokio::spawn(async move { early.fetch_table(request).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());

    c.client
        .empty_table(c.auth(empty(ticket(1), 6)))
        .await
        .unwrap();
    let response = waiting.await.unwrap().unwrap().into_inner();
    assert_eq!(response.size, 6);
}

fn strings(table: &dyn Table, column: usize) -> Vec<String> {
    let values = table.column(column).chunk(&table.row_set()).unwrap();
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    values.iter().map(|v| v.unwrap().to_string()).collect()
}

#[tokio::test]
async fn meta_table_describes_the_columns_of_its_source() {
    let mut c = Connected::start().await;
    c.client
        .time_table(c.auth(one_row_time_table(ticket(1))))
        .await
        .unwrap();
    let response = c
        .client
        .meta_table(c.auth(MetaTableRequest {
            result_id: ticket(2),
            source_id: by_ticket(1),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.is_static);
    assert_eq!(response.size, 1);
    // an in-memory table is not marked flat in Java
    assert_eq!(schema_metadata(&response), Vec::new());
    let schema = schema_of(&response);
    let fields: Vec<(&str, &str)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.metadata()["deephaven:type"].as_str()))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("Name", "java.lang.String"),
            ("DataType", "java.lang.String"),
            ("ColumnType", "java.lang.String"),
            ("IsPartitioning", "java.lang.Boolean"),
        ]
    );

    let meta = c.table(2);
    assert_eq!(strings(meta.as_ref(), 0), vec!["Timestamp"]);
    assert_eq!(strings(meta.as_ref(), 1), vec!["java.time.Instant"]);
    assert_eq!(strings(meta.as_ref(), 2), vec!["Normal"]);
    let partitioning = meta.column(3).chunk(&meta.row_set()).unwrap();
    let partitioning = partitioning
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(partitioning.iter().collect::<Vec<_>>(), vec![Some(false)]);
}

#[tokio::test]
async fn creation_response_of_a_ticket_needs_a_table() {
    let mut c = Connected::start().await;
    let refused = c
        .client
        .get_exported_table_creation_response(c.auth(Ticket { ticket: Vec::new() }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "No request ticket supplied");

    let refused = c
        .client
        .get_exported_table_creation_response(c.auth(Ticket {
            ticket: b"zz".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);

    let session = c.server.session(&c.bearer);
    session
        .new_export(7)
        .unwrap()
        .submit(|_| Ok(ExportValue::unit()))
        .unwrap();
    let refused = c
        .client
        .get_exported_table_creation_response(c.auth(export_id_to_ticket(7)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "Ticket is not a table");

    c.client
        .empty_table(c.auth(empty(ticket(1), 5)))
        .await
        .unwrap();
    let described = c
        .client
        .get_exported_table_creation_response(c.auth(export_id_to_ticket(1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(described.result_id, by_ticket(1));
    assert!(described.success && described.is_static);
    assert_eq!(described.size, 5);
    assert_eq!(schema_metadata(&described), pairs(&FLAT));
}

// ---------------------------------------------------------------- batch

fn fetch(source: Option<TableReference>, result: Option<Ticket>) -> Op {
    Op::FetchTable(FetchTableRequest {
        source_id: source,
        result_id: result,
    })
}

fn by_result(
    mut responses: Vec<ExportedTableCreationResponse>,
) -> Vec<ExportedTableCreationResponse> {
    let key = |r: &ExportedTableCreationResponse| match r.result_id.as_ref().unwrap().r#ref {
        Some(Ref::BatchOffset(offset)) => (0, offset),
        Some(Ref::Ticket(ref ticket)) => (1, ticket.ticket[1] as i32),
        None => (2, 0),
    };
    responses.sort_by_key(key);
    responses
}

#[tokio::test]
async fn a_batch_resolves_references_by_offset_in_either_direction_and_by_ticket() {
    let mut c = Connected::start().await;
    let (responses, end) = c
        .batch(vec![
            // refers forward to an operation without a ticket
            fetch(by_offset(1), None),
            Op::EmptyTable(empty(None, 3)),
            // refers back, and is exported
            Op::MetaTable(MetaTableRequest {
                result_id: ticket(9),
                source_id: by_offset(1),
            }),
            // refers by ticket to an export made in this batch
            fetch(by_ticket(9), None),
        ])
        .await
        .unwrap();
    assert!(end.is_none(), "{end:?}");
    let responses = by_result(responses);
    assert_eq!(responses.len(), 4);
    assert!(responses.iter().all(|r| r.success && r.is_static));
    assert_eq!(
        responses
            .iter()
            .map(|r| r.result_id.clone())
            .collect::<Vec<_>>(),
        vec![by_offset(0), by_offset(1), by_offset(3), by_ticket(9)]
    );
    // the empty table and its fetch have three rows; its meta table has none
    assert_eq!(
        responses.iter().map(|r| r.size).collect::<Vec<_>>(),
        vec![3, 3, 0, 0]
    );
    assert_eq!(schema_metadata(&responses[0]), pairs(&FLAT));
    assert_eq!(schema_of(&responses[3]).fields().len(), 4);

    // only the operation with a result ticket is an export of the session
    assert_eq!(*c.table(9).row_set(), RowSet::flat(0));
}

#[tokio::test]
async fn a_malformed_batch_is_refused_as_a_whole() {
    let mut c = Connected::start().await;
    let refused = c.batch(Vec::new()).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "io.deephaven.proto.backplane.grpc.BatchTableRequest must have at least one ops (1)"
    );

    let request = c.auth(BatchTableRequest {
        ops: vec![
            Operation {
                op: Some(Op::EmptyTable(empty(ticket(1), 1))),
            },
            Operation { op: None },
        ],
    });
    let refused = c.client.batch(request).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(
        refused.message().starts_with(
            "io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation must have oneof op."
        ),
        "{}",
        refused.message()
    );

    for (source, message) in [
        (by_offset(7), "invalid table reference: batch_offset: 7\n"),
        (by_offset(-1), "invalid table reference: batch_offset: -1\n"),
        (None, "invalid table reference: "),
    ] {
        let refused = c.batch(vec![fetch(source, None)]).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), message);
    }

    // one request that fails validation refuses the batch, whatever else is in it
    let refused = c
        .batch(vec![
            Op::EmptyTable(empty(None, 2)),
            Op::EmptyTable(empty(None, -1)),
        ])
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(refused.message(), "Size must be greater than zero");
    let refused = c
        .batch(vec![Op::TimeTable(TimeTableRequest::default())])
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn a_failed_batch_operation_fails_its_dependents_and_ends_the_stream_with_its_status() {
    let mut c = Connected::start().await;
    let (responses, end) = c
        .batch(vec![
            fetch(by_ticket(-7), None),
            fetch(by_offset(0), ticket(4)),
            Op::EmptyTable(empty(None, 1)),
        ])
        .await
        .unwrap();
    let responses = by_result(responses);
    assert_eq!(
        responses
            .iter()
            .map(|r| (r.result_id.clone(), r.success))
            .collect::<Vec<_>>(),
        vec![
            (by_offset(0), false),
            (by_offset(2), true),
            (by_ticket(4), false)
        ]
    );
    let cause = " cause: FAILED_PRECONDITION: Export id -7 does not exist and cannot be used \
                 out-of-order!";
    let failed = &responses[0];
    let dependent = &responses[2];
    assert!(failed.error_info.ends_with(cause), "{}", failed.error_info);
    assert!(failed.schema_header.is_empty() && failed.size == 0 && !failed.is_static);
    // the dependent reports the error of the operation that failed it, under the same id
    assert_eq!(dependent.error_info, failed.error_info);
    let error_id = failed.error_info.strip_suffix(cause).unwrap();
    assert_eq!(error_id.len(), 36, "{error_id}");

    let end = end.expect("the stream ends with the failure");
    assert_eq!(end.code(), Code::FailedPrecondition);
    assert_eq!(
        end.message(),
        "Export id -7 does not exist and cannot be used out-of-order!"
    );
}

#[tokio::test]
async fn an_internal_failure_in_a_batch_is_reported_by_id_and_not_passed_to_dependents() {
    let mut c = Connected::start().await;
    let session = c.server.session(&c.bearer);
    session
        .new_export(7)
        .unwrap()
        .submit(|_| Ok(ExportValue::unit()))
        .unwrap();
    // fetching something that is not a table fails inside the export
    let (responses, end) = c
        .batch(vec![fetch(by_ticket(7), None), fetch(by_offset(0), None)])
        .await
        .unwrap();
    let responses = by_result(responses);
    assert_eq!(responses.len(), 2);
    assert!(responses.iter().all(|r| !r.success));
    let (error_id, cause) = responses[0].error_info.split_once(' ').unwrap();
    assert_eq!(
        cause,
        format!("cause: INVALID_ARGUMENT: Details Logged w/ID '{error_id}'")
    );
    // the dependent knows the id and nothing else
    assert_eq!(responses[1].error_info, error_id);

    let end = end.expect("the stream ends with the failure");
    assert_eq!(end.code(), Code::InvalidArgument);
    assert_eq!(end.message(), format!("Details Logged w/ID '{error_id}'"));
}

#[tokio::test]
async fn a_batch_operation_that_depends_on_a_released_export_names_it() {
    let mut c = Connected::start().await;
    c.client
        .empty_table(c.auth(empty(ticket(1), 1)))
        .await
        .unwrap();
    let session = c.server.session(&c.bearer);
    session.get_export(1).unwrap().cancel();

    let (responses, end) = c.batch(vec![fetch(by_ticket(1), None)]).await.unwrap();
    assert_eq!(responses.len(), 1);
    assert!(!responses[0].success);
    let (error_id, dependency) = responses[0].error_info.split_once(' ').unwrap();
    assert_eq!(error_id.len(), 36);
    assert_eq!(dependency, "dependency: 1");
    // a release is not a status, so the stream just ends
    assert!(end.is_none(), "{end:?}");
}

// ---------------------------------------------------------------- the graph lock

/// An operation that asks for an update cycle while it is creating and reports whether
/// the cycle ran before it gave up waiting.
struct Probe {
    graph: Arc<crate::update_graph::PeriodicUpdateGraph>,
    patience: Duration,
    cycles_during_create: Mutex<Option<u64>>,
}

impl BoundOp for Probe {
    fn result_id(&self) -> &[u8] {
        &[]
    }
    fn sources(&self) -> Vec<TableReference> {
        Vec::new()
    }
    fn validate(&self) -> Result<(), Status> {
        Ok(())
    }
    fn create(
        &self,
        _context: &OpContext,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let before = self.graph.completed_cycles();
        self.graph.request_cycle();
        let deadline = std::time::Instant::now() + self.patience;
        while self.graph.completed_cycles() == before && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        *self.cycles_during_create.lock().unwrap() = Some(self.graph.completed_cycles() - before);
        Ok(sources[0].clone())
    }
}

#[tokio::test]
async fn an_operation_on_a_refreshing_source_runs_under_the_shared_lock() {
    let server = TestServer::start().await;
    let context = server.state.op_context();
    let graph = server.state.graph.clone();
    let refreshing: Arc<dyn Table> = time_table(
        context.graph.clone(),
        Arc::new(WallClock),
        TimeTableOptions {
            start_nanos: None,
            period_nanos: 1_000_000,
            blink: false,
        },
    )
    .unwrap();
    let fixed: Arc<dyn Table> = empty_table(1);

    let cycles_while_creating = |source: Arc<dyn Table>, patience: Duration| {
        let probe = Probe {
            graph: graph.clone(),
            patience,
            cycles_during_create: Mutex::new(None),
        };
        let context = context.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                run_operation(&probe, &context, &[ExportValue::Table(source)]).unwrap();
                let cycles = probe.cycles_during_create.lock().unwrap().take();
                cycles.unwrap()
            })
            .await
            .unwrap()
        }
    };
    // No cycle runs while an operation reads a refreshing table; the requested one runs
    // after. A static source needs no lock, and the cycle runs meanwhile.
    assert_eq!(
        cycles_while_creating(refreshing, Duration::from_millis(200)).await,
        0
    );
    // At least one: the cycle the first probe asked for may also finish in this window.
    assert!(cycles_while_creating(fixed, Duration::from_secs(20)).await >= 1);
}

// ---------------------------------------------------------------- exported table updates

async fn next_update(
    stream: &mut tonic::Streaming<ExportedTableUpdateMessage>,
) -> (i32, i64, String) {
    let message = tokio::time::timeout(Duration::from_secs(20), stream.next())
        .await
        .expect("an update message in time")
        .expect("the stream is open")
        .unwrap();
    let id = crate::session::ticket_to_export_id(&message.export_id.unwrap().ticket, "t").unwrap();
    (id, message.size, message.update_failure_message)
}

#[tokio::test]
async fn exported_table_updates_send_sizes_at_export_and_after_each_change() {
    let mut c = Connected::start().await;
    c.client
        .empty_table(c.auth(empty(ticket(1), 5)))
        .await
        .unwrap();
    // an export that is not a table is not mentioned
    let session = c.server.session(&c.bearer);
    session
        .new_export(2)
        .unwrap()
        .submit(|_| Ok(ExportValue::unit()))
        .unwrap();

    let mut stream = c
        .client
        .exported_table_updates(c.auth(ExportedTableUpdatesRequest {}))
        .await
        .unwrap()
        .into_inner();
    // what was exported before the call
    assert_eq!(next_update(&mut stream).await, (1, 5, String::new()));

    // a static table: once, when it is exported
    c.client
        .empty_table(c.auth(empty(ticket(3), 9)))
        .await
        .unwrap();
    assert_eq!(next_update(&mut stream).await, (3, 9, String::new()));

    // a refreshing table: when it is exported, then after every cycle that changes it
    c.client
        .time_table(c.auth(ticking_time_table(ticket(4))))
        .await
        .unwrap();
    assert_eq!(next_update(&mut stream).await, (4, 0, String::new()));
    c.cycle().await;
    assert_eq!(next_update(&mut stream).await, (4, 1, String::new()));
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let (id, size, failure) = next_update(&mut stream).await;
    assert_eq!((id, failure), (4, String::new()));
    assert!(size > 1, "{size}");

    // after its release the table is no longer followed
    session.get_export(4).unwrap().cancel();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    c.client
        .empty_table(c.auth(empty(ticket(5), 2)))
        .await
        .unwrap();
    assert_eq!(next_update(&mut stream).await, (5, 2, String::new()));
}

#[tokio::test]
async fn exported_table_updates_end_with_the_session() {
    let mut c = Connected::start().await;
    let mut stream = c
        .client
        .exported_table_updates(c.auth(ExportedTableUpdatesRequest {}))
        .await
        .unwrap()
        .into_inner();
    let session = c.server.session(&c.bearer);
    session.on_expired();
    let end = tokio::time::timeout(Duration::from_secs(20), stream.next())
        .await
        .expect("the stream to end in time");
    assert!(end.is_none(), "{end:?}");

    let refused = Connected::start().await;
    let mut client = TableServiceClient::new(refused.server.channel.clone());
    let status = client
        .exported_table_updates(ExportedTableUpdatesRequest {})
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
}
