//! The hierarchical table service, the object a client opens and the subscription to a
//! view, over the wire. The statuses and texts are what the Java reference (42.9) answered
//! to the same calls; `docs/HIERARCHICAL.md` describes them.

// `FetchObject` is deprecated in favour of `MessageStream`; clients still call it.
#![allow(deprecated)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use flatbuffers::FlatBufferBuilder;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::append_only_input_table;
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageSubscriptionRequest, BarrageSubscriptionRequestArgs,
    BarrageUpdateMetadata, MAGIC,
};
use oxhaven_proto::flight::FlightData;
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::agg_spec::{AggSpecSum, Type as SpecType};
use oxhaven_proto::grpc::aggregation::{AggregationColumns, AggregationCount, Type};
use oxhaven_proto::grpc::hierarchical_table_service_client::HierarchicalTableServiceClient;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::{
    AggSpec as SpecMessage, Aggregation as AggregationMessage, FetchObjectRequest,
    HierarchicalTableViewKeyTableDescriptor, SortDescriptor, TypedTicket,
};
use oxhaven_testkit::{definition, ints, strings, ReferenceTable};
use tonic::{Code, Status};

use super::*;
use crate::barrage::codec::{decode_row_set, encode_row_set};
use crate::barrage::tests::{cells, columns_of, metadata_of, schema_of, snapshot_request, wrapped};
use crate::barrage::tests::{Call, Cell};
use crate::flight::tests::support::{schema_of_ipc, Server};
use crate::session::export_id_to_ticket;

fn trades_definition() -> oxhaven_contracts::schema::TableDefinition {
    definition(&[
        ("Sym", ColumnType::String),
        ("Ex", ColumnType::String),
        ("Qty", ColumnType::Int),
    ])
}

/// The table of the IDE's rollup route.
fn trades() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        trades_definition(),
        RowSet::flat(6),
        vec![
            strings(["AAPL", "AAPL", "MSFT", "AAPL", "MSFT", "GOOG"]),
            strings(["N", "Q", "N", "N", "Q", "Q"]),
            ints([10, 20, 30, 40, 50, 60]),
        ],
    )
}

fn trade_rows(rows: &[(&str, &str, i32)]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("Sym", DataType::Utf8, true),
        Field::new("Ex", DataType::Utf8, true),
        Field::new("Qty", DataType::Int32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.2))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// A key table that expands the node of `sym`, with the action column `Action`; without
/// `depth`, a table that is no key table.
fn expand(sym: &str, depth: bool) -> Arc<dyn Table> {
    let mut columns = vec![
        ("Sym", ColumnType::String),
        ("Ex", ColumnType::String),
        ("Action", ColumnType::Byte),
    ];
    let mut data: Vec<ArrayRef> = vec![
        strings([sym]),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(Int8Array::from(vec![1])),
    ];
    if depth {
        columns.push(("__DEPTH__", ColumnType::Int));
        data.push(ints([2]));
    }
    ReferenceTable::new_static(definition(&columns), RowSet::flat(1), data)
}

fn sum(pair: &str) -> AggregationMessage {
    AggregationMessage {
        r#type: Some(Type::Columns(AggregationColumns {
            spec: Some(SpecMessage {
                r#type: Some(SpecType::Sum(AggSpecSum {})),
            }),
            match_pairs: vec![pair.to_string()],
        })),
    }
}

fn count(column: &str) -> AggregationMessage {
    AggregationMessage {
        r#type: Some(Type::Count(AggregationCount {
            column_name: column.to_string(),
        })),
    }
}

fn rollup_request(source: i32, result: i32, by: &[&str]) -> RollupRequest {
    RollupRequest {
        result_rollup_table_id: Some(export_id_to_ticket(result)),
        source_table_id: Some(export_id_to_ticket(source)),
        aggregations: vec![sum("Qty"), count("Rows")],
        include_constituents: false,
        group_by_columns: by.iter().map(|column| column.to_string()).collect(),
    }
}

fn view_request(table: i32, result: i32, keys: Option<i32>) -> HierarchicalTableViewRequest {
    HierarchicalTableViewRequest {
        result_view_id: Some(export_id_to_ticket(result)),
        expansions: keys.map(|keys| HierarchicalTableViewKeyTableDescriptor {
            key_table_id: Some(export_id_to_ticket(keys)),
            key_table_action_column: Some("Action".to_string()),
        }),
        target: Some(Target::HierarchicalTableId(export_id_to_ticket(table))),
    }
}

fn refused<T: std::fmt::Debug>(answer: Result<T, Status>, code: Code, text: &str) {
    let status = answer.unwrap_err();
    assert_eq!(status.code(), code, "{status:?}");
    assert!(
        status.message().starts_with(text),
        "{:?} does not start with {text:?}",
        status.message()
    );
}

const LOGGED: &str = "Details Logged w/ID '";
const ONE_OF: &str = "Note: this may also indicate that the server is older than the client and \
                      doesn't know about this new oneof option.";

/// A server whose session has the trades as export 1, their rollup by `Sym` and `Ex` as
/// export 10 and key tables as exports 2 (expands AAPL) and 3 (has no depth column).
async fn server() -> Server {
    let server = Server::start().await;
    server.export(1, trades());
    server.export(2, expand("AAPL", true));
    server.export(3, expand("AAPL", false));
    HierarchicalTableServiceClient::new(server.channel.clone())
        .rollup(server.authorized(rollup_request(1, 10, &["Sym", "Ex"])))
        .await
        .unwrap();
    server
}

#[tokio::test]
async fn rollup_answers_what_java_answers() {
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    let rollup = |request: RollupRequest| async move {
        service()
            .rollup(server.authorized(request))
            .await
            .map(tonic::Response::into_inner)
    };

    let value = server.session().get_export(10).unwrap().value().unwrap();
    let HierarchicalTable::Rollup(made) = hierarchical_of(&value).unwrap().as_ref();
    assert_eq!(made.group_by(), ["Sym", "Ex"]);
    // Java makes a rollup without aggregations and without group-by columns
    let bare = RollupRequest {
        aggregations: Vec::new(),
        ..rollup_request(1, 11, &[])
    };
    assert_eq!(rollup(bare).await.unwrap(), RollupResponse {});

    let message = "io.deephaven.proto.backplane.grpc.RollupRequest must have field";
    // a result of its own for every call: a refused export keeps its id
    let next = std::cell::Cell::new(20);
    let fresh = || next.replace(next.get() + 1);
    let request = || rollup_request(1, fresh(), &["Sym"]);
    for (request, code, text) in [
        (
            RollupRequest {
                result_rollup_table_id: None,
                ..request()
            },
            Code::InvalidArgument,
            format!("{message} result_rollup_table_id (1)"),
        ),
        (
            RollupRequest {
                source_table_id: None,
                ..request()
            },
            Code::InvalidArgument,
            format!("{message} source_table_id (2)"),
        ),
        (
            RollupRequest {
                result_rollup_table_id: Some(Ticket::default()),
                ..request()
            },
            Code::InvalidArgument,
            "Ticket is empty".to_string(),
        ),
        (
            RollupRequest {
                source_table_id: Some(Ticket::default()),
                ..request()
            },
            Code::InvalidArgument,
            "Ticket is empty".to_string(),
        ),
        (
            RollupRequest {
                source_table_id: Some(Ticket {
                    ticket: b"s/nope".to_vec(),
                }),
                ..request()
            },
            Code::NotFound,
            "Could not resolve 'sourceTableId: variable 'nope' not found".to_string(),
        ),
        (
            RollupRequest {
                aggregations: vec![AggregationMessage { r#type: None }],
                ..request()
            },
            Code::InvalidArgument,
            format!("io.deephaven.proto.backplane.grpc.Aggregation must have oneof type. {ONE_OF}"),
        ),
        // a rollup of a rollup, columns that are not there, a result that is there
        (
            rollup_request(10, fresh(), &["Sym"]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            rollup_request(1, fresh(), &["Nope"]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            RollupRequest {
                aggregations: vec![sum("Nope")],
                ..request()
            },
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            rollup_request(1, 10, &["Sym"]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        // not ported; Java makes this rollup
        (
            RollupRequest {
                include_constituents: true,
                ..request()
            },
            Code::Unimplemented,
            "not ported: io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#makeRollup"
                .to_string(),
        ),
    ] {
        refused(rollup(request).await, code, &text);
    }
}

#[tokio::test]
async fn view_and_export_source_answer_what_java_answers() {
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    let view = |request: HierarchicalTableViewRequest| async move {
        service()
            .view(server.authorized(request))
            .await
            .map(tonic::Response::into_inner)
    };
    let existing = |id: i32, result: i32| HierarchicalTableViewRequest {
        target: Some(Target::ExistingViewId(export_id_to_ticket(id))),
        ..view_request(10, result, None)
    };

    assert_eq!(
        view(view_request(10, 11, None)).await.unwrap(),
        HierarchicalTableViewResponse {}
    );
    view(view_request(10, 12, Some(2))).await.unwrap();
    // a view of the table of another view, with its own expansions
    view(existing(12, 13)).await.unwrap();
    let rows = |id: i32| {
        let value = server.session().get_export(id).unwrap().value().unwrap();
        let view = view_of(&value).unwrap().clone();
        view.view.snapshot(None, 0, 100).unwrap().size
    };
    assert_eq!((rows(11), rows(12), rows(13)), (4, 6, 4));
    // a key table that cannot be read is found by the first snapshot
    view(view_request(10, 14, Some(3))).await.unwrap();

    let message = "io.deephaven.proto.backplane.grpc.HierarchicalTableViewRequest must have";
    // a result of its own for every call: a refused export keeps its id
    let next = std::cell::Cell::new(20);
    let fresh = || next.replace(next.get() + 1);
    for (request, code, text) in [
        (
            HierarchicalTableViewRequest {
                result_view_id: None,
                ..view_request(10, fresh(), None)
            },
            Code::InvalidArgument,
            format!("{message} field result_view_id (1)"),
        ),
        (
            HierarchicalTableViewRequest {
                target: None,
                ..view_request(10, fresh(), None)
            },
            Code::InvalidArgument,
            format!("{message} oneof target. {ONE_OF}"),
        ),
        (
            HierarchicalTableViewRequest {
                target: Some(Target::HierarchicalTableId(Ticket::default())),
                ..view_request(10, fresh(), None)
            },
            Code::InvalidArgument,
            "Ticket is empty".to_string(),
        ),
        (
            HierarchicalTableViewRequest {
                target: Some(Target::HierarchicalTableId(Ticket {
                    ticket: b"s/nope".to_vec(),
                })),
                ..view_request(10, fresh(), None)
            },
            Code::NotFound,
            "Could not resolve 'targetTableId: variable 'nope' not found".to_string(),
        ),
        // a table, a view and a rollup where the other kind is asked for
        (
            view_request(1, fresh(), None),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            view_request(11, fresh(), None),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            existing(10, fresh()),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            view_request(10, fresh(), Some(10)),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            HierarchicalTableViewRequest {
                expansions: Some(HierarchicalTableViewKeyTableDescriptor::default()),
                ..view_request(10, fresh(), None)
            },
            Code::FailedPrecondition,
            "could not resolve 'expansions.keyTableId' it's an empty ticket".to_string(),
        ),
    ] {
        refused(view(request).await, code, &text);
    }

    // ExportSource
    let source = |table: Option<i32>, result: Option<i32>| async move {
        let request = HierarchicalTableSourceExportRequest {
            result_table_id: result.map(export_id_to_ticket),
            hierarchical_table_id: table.map(export_id_to_ticket),
        };
        service()
            .export_source(server.authorized(request))
            .await
            .map(tonic::Response::into_inner)
    };
    let exported = source(Some(10), Some(40)).await.unwrap();
    assert!(exported.success && exported.is_static);
    assert_eq!(exported.size, 6);
    assert_eq!(
        exported.result_id.unwrap().r#ref,
        Some(Ref::Ticket(export_id_to_ticket(40)))
    );
    let names: Vec<String> = schema_of_ipc(&exported.schema_header)
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["Sym", "Ex", "Qty"]);
    let value = server.session().get_export(40).unwrap().value().unwrap();
    assert_eq!(value.as_table().unwrap().row_set().size(), 6);

    let message = "io.deephaven.proto.backplane.grpc.HierarchicalTableSourceExportRequest must \
                   have field";
    refused(
        source(Some(10), None).await,
        Code::InvalidArgument,
        &format!("{message} result_table_id (1)"),
    );
    refused(
        source(None, Some(41)).await,
        Code::InvalidArgument,
        &format!("{message} hierarchical_table_id (2)"),
    );
    // a table and a view have no source
    refused(
        source(Some(1), Some(42)).await,
        Code::InvalidArgument,
        LOGGED,
    );
    refused(
        source(Some(11), Some(43)).await,
        Code::InvalidArgument,
        LOGGED,
    );
}

#[tokio::test]
async fn apply_reads_its_request_and_tree_is_refused_by_name() {
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    let apply = |request: HierarchicalTableApplyRequest| async move {
        service()
            .apply(server.authorized(request))
            .await
            .map(tonic::Response::into_inner)
    };
    let next = std::cell::Cell::new(20);
    let request = || HierarchicalTableApplyRequest {
        result_hierarchical_table_id: Some(export_id_to_ticket(next.replace(next.get() + 1))),
        input_hierarchical_table_id: Some(export_id_to_ticket(10)),
        ..Default::default()
    };
    // what Java answers
    let message = "io.deephaven.proto.backplane.grpc.HierarchicalTableApplyRequest must have field";
    refused(
        apply(HierarchicalTableApplyRequest {
            result_hierarchical_table_id: None,
            ..request()
        })
        .await,
        Code::InvalidArgument,
        &format!("{message} result_hierarchical_table_id (1)"),
    );
    refused(
        apply(HierarchicalTableApplyRequest {
            input_hierarchical_table_id: None,
            ..request()
        })
        .await,
        Code::InvalidArgument,
        &format!("{message} input_hierarchical_table_id (2)"),
    );
    refused(
        apply(request()).await,
        Code::InvalidArgument,
        "No operations specified",
    );
    refused(
        apply(HierarchicalTableApplyRequest {
            input_hierarchical_table_id: Some(Ticket {
                ticket: b"s/nope".to_vec(),
            }),
            ..request()
        })
        .await,
        Code::NotFound,
        "Could not resolve 'inputHierarchicalTableId: variable 'nope' not found",
    );
    // what Java does and this server does not yet
    let tree = service()
        .tree(server.authorized(TreeRequest {
            result_tree_table_id: Some(export_id_to_ticket(40)),
            source_table_id: Some(export_id_to_ticket(1)),
            identifier_column: "Sym".to_string(),
            parent_identifier_column: "Ex".to_string(),
            promote_orphans: false,
        }))
        .await;
    refused(
        tree,
        Code::Unimplemented,
        "not ported: io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl#tree",
    );
}

fn sort_by(column: &str, direction: proto::sort_descriptor::SortDirection) -> SortDescriptor {
    SortDescriptor {
        column_name: column.to_string(),
        is_absolute: false,
        direction: direction as i32,
    }
}

/// `column <operation> value`, as the IDE sends a filter.
fn compared(
    column: &str,
    operation: proto::compare_condition::CompareOperation,
    value: proto::literal::Value,
) -> proto::Condition {
    let reference = proto::Value {
        data: Some(proto::value::Data::Reference(proto::Reference {
            column_name: column.to_string(),
        })),
    };
    let literal = proto::Value {
        data: Some(proto::value::Data::Literal(proto::Literal {
            value: Some(value),
        })),
    };
    proto::Condition {
        data: Some(proto::condition::Data::Compare(proto::CompareCondition {
            operation: operation as i32,
            case_sensitivity: 0,
            lhs: Some(reference),
            rhs: Some(literal),
        })),
    }
}

fn update_view(formula: &str) -> proto::UpdateViewRequest {
    proto::UpdateViewRequest {
        column_spec: Some(proto::Selectable {
            r#type: Some(proto::selectable::Type::Raw(formula.to_string())),
        }),
        node_type: proto::RollupNodeType::Aggregated as i32,
    }
}

fn text_value(value: &str) -> proto::literal::Value {
    proto::literal::Value::StringValue(value.to_string())
}

/// The statuses are those Java 42.9 answered to the same calls; the texts that are not
/// Java's own are of errors that Java logs and does not send.
///
/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl#apply`
#[tokio::test]
async fn apply_sorts_and_filters_a_rollup_and_refuses_what_java_refuses() {
    use proto::compare_condition::CompareOperation;
    use proto::sort_descriptor::SortDirection;
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    // the rollup by `Sym` alone, in which `Ex` is a column no aggregation reads
    service()
        .rollup(server.authorized(rollup_request(1, 15, &["Sym"])))
        .await
        .unwrap();
    let apply = |request: HierarchicalTableApplyRequest| async move {
        service()
            .apply(server.authorized(request))
            .await
            .map(tonic::Response::into_inner)
    };
    let request = |input: i32, result: i32| HierarchicalTableApplyRequest {
        result_hierarchical_table_id: Some(export_id_to_ticket(result)),
        input_hierarchical_table_id: Some(export_id_to_ticket(input)),
        ..Default::default()
    };
    let sorted =
        |input: i32, result: i32, sorts: Vec<SortDescriptor>| HierarchicalTableApplyRequest {
            sorts,
            ..request(input, result)
        };
    let filtered =
        |input: i32, result: i32, filters: Vec<proto::Condition>| HierarchicalTableApplyRequest {
            filters,
            ..request(input, result)
        };
    // the node of AAPL expanded: the key column and the sum of each row
    let rows = |id: i32| {
        let value = server.session().get_export(id).unwrap().value().unwrap();
        let view = View::new(
            hierarchical_of(&value).unwrap().hierarchy(),
            Some(expand("AAPL", true)),
            Some("Action".to_string()),
        );
        let snapshot = view.snapshot(None, 0, 100).unwrap();
        let names = arrow::array::AsArray::as_string::<i32>(snapshot.columns[2].as_ref());
        let column = snapshot.columns.len() - 2;
        let sums = arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(
            snapshot.columns[column].as_ref(),
        );
        (0..snapshot.rows as usize)
            .map(|row| {
                let name = if arrow::array::Array::is_null(names, row) {
                    "-"
                } else {
                    names.value(row)
                };
                format!("{name} {}", sums.value(row))
            })
            .collect::<Vec<_>>()
    };
    let source_size = |table: i32, result: i32| async move {
        let request = HierarchicalTableSourceExportRequest {
            result_table_id: Some(export_id_to_ticket(result)),
            hierarchical_table_id: Some(export_id_to_ticket(table)),
        };
        let exported = service()
            .export_source(server.authorized(request))
            .await
            .unwrap();
        exported.into_inner().size
    };

    assert_eq!(
        rows(10),
        ["- 210", "AAPL 70", "AAPL 50", "AAPL 20", "MSFT 80", "GOOG 60"]
    );
    // a sort orders the rows of every node
    assert_eq!(
        apply(sorted(
            10,
            20,
            vec![sort_by("Qty", SortDirection::Descending)]
        ))
        .await
        .unwrap(),
        HierarchicalTableApplyResponse {}
    );
    assert_eq!(
        rows(20),
        ["- 210", "MSFT 80", "AAPL 70", "AAPL 50", "AAPL 20", "GOOG 60"]
    );
    assert_eq!(rows(10)[1], "AAPL 70", "the input is as it was");
    // a later sort decides first, and what was sorted by before decides among its equals
    apply(sorted(
        20,
        21,
        vec![sort_by("Rows", SortDirection::Ascending)],
    ))
    .await
    .unwrap();
    assert_eq!(
        rows(21),
        ["- 210", "GOOG 60", "MSFT 80", "AAPL 70", "AAPL 20", "AAPL 50"]
    );
    let absolute = SortDescriptor {
        is_absolute: true,
        ..sort_by("Qty", SortDirection::Ascending)
    };
    apply(sorted(10, 22, vec![absolute.clone()])).await.unwrap();
    assert_eq!(rows(22)[1], "GOOG 60");

    // a filter by a group-by column leaves the source as it is
    let not_msft = compared("Sym", CompareOperation::NotEquals, text_value("MSFT"));
    apply(filtered(10, 23, vec![not_msft.clone()]))
        .await
        .unwrap();
    assert_eq!(
        rows(23),
        ["- 130", "AAPL 70", "AAPL 50", "AAPL 20", "GOOG 60"]
    );
    assert_eq!(source_size(23, 40).await, 6);
    // a filter by a column that is neither grouped by nor aggregated filters the source
    let on_n = compared("Ex", CompareOperation::Equals, text_value("N"));
    apply(filtered(15, 24, vec![on_n.clone()])).await.unwrap();
    assert_eq!(rows(24), ["- 80", "AAPL 50", "MSFT 30"]);
    assert_eq!(source_size(24, 41).await, 3);
    // filters first, then sorts
    apply(HierarchicalTableApplyRequest {
        filters: vec![not_msft.clone()],
        sorts: vec![sort_by("Qty", SortDirection::Ascending)],
        ..request(10, 25)
    })
    .await
    .unwrap();
    assert_eq!(
        rows(25),
        ["- 130", "GOOG 60", "AAPL 70", "AAPL 20", "AAPL 50"]
    );

    // what Java refuses
    let next = std::cell::Cell::new(50);
    let fresh = || next.replace(next.get() + 1);
    let positive = compared(
        "Qty",
        CompareOperation::GreaterThan,
        proto::literal::Value::DoubleValue(0.0),
    );
    let unknown = "Unsupported or unknown sort direction:";
    for (request, code, text) in [
        (
            sorted(10, fresh(), vec![sort_by("Qty", SortDirection::Unknown)]),
            Code::InvalidArgument,
            format!("{unknown} UNKNOWN"),
        ),
        (
            sorted(10, fresh(), vec![sort_by("Qty", SortDirection::Reverse)]),
            Code::InvalidArgument,
            format!("{unknown} REVERSE"),
        ),
        (
            sorted(
                10,
                fresh(),
                vec![SortDescriptor {
                    direction: 7,
                    ..sort_by("Qty", SortDirection::Ascending)
                }],
            ),
            Code::InvalidArgument,
            format!("{unknown} UNRECOGNIZED"),
        ),
        // a direction is read before a column is looked for
        (
            sorted(
                10,
                fresh(),
                vec![
                    sort_by("Nope", SortDirection::Ascending),
                    sort_by("Qty", SortDirection::Unknown),
                ],
            ),
            Code::InvalidArgument,
            format!("{unknown} UNKNOWN"),
        ),
        // a column the nodes do not have, a structural column, the absolute value of text
        (
            sorted(10, fresh(), vec![sort_by("Nope", SortDirection::Ascending)]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            sorted(
                10,
                fresh(),
                vec![sort_by("__DEPTH__", SortDirection::Ascending)],
            ),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            sorted(
                10,
                fresh(),
                vec![SortDescriptor {
                    column_name: "Sym".to_string(),
                    ..absolute
                }],
            ),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        // a filter reads no column that an aggregation reads
        (
            filtered(10, fresh(), vec![positive.clone()]),
            Code::InvalidArgument,
            "Invalid filter found:".to_string(),
        ),
        (
            filtered(10, fresh(), vec![not_msft.clone(), positive]),
            Code::InvalidArgument,
            "Invalid filter found:".to_string(),
        ),
        // nor a result of the rollup, which the source does not have, nor no column
        (
            filtered(
                10,
                fresh(),
                vec![compared(
                    "Rows",
                    CompareOperation::GreaterThan,
                    proto::literal::Value::DoubleValue(1.0),
                )],
            ),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            filtered(10, fresh(), vec![proto::Condition { data: None }]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        // a table and a view are no hierarchical table, and the result is there
        (
            sorted(1, fresh(), vec![sort_by("Qty", SortDirection::Ascending)]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        (
            sorted(10, 20, vec![sort_by("Qty", SortDirection::Ascending)]),
            Code::InvalidArgument,
            LOGGED.to_string(),
        ),
        // what Java does and this server does not yet
        (
            HierarchicalTableApplyRequest {
                update_views: vec![update_view("Twice = Qty * 2")],
                ..request(10, fresh())
            },
            Code::Unimplemented,
            "not ported: io.deephaven.engine.table.impl.hierarchical.\
             RollupNodeOperationsRecorder#updateView"
                .to_string(),
        ),
        (
            HierarchicalTableApplyRequest {
                format_views: vec![update_view("Qty = RED")],
                ..request(10, fresh())
            },
            Code::Unimplemented,
            "not ported: io.deephaven.engine.table.impl.hierarchical.\
             RollupNodeOperationsRecorder#formatColumns"
                .to_string(),
        ),
    ] {
        refused(apply(request).await, code, &text);
    }
}

/// A rollup of a table whose sorts are restricted says so where a client reads it, and
/// `Apply` refuses a sort by another column.
///
/// Behavior: `HierarchicalTableGrpcHelper#translateAndValidateSorts` and
/// `io.deephaven.engine.table.impl.BaseGridAttributes#restrictSortTo`
#[tokio::test]
async fn a_rollup_of_a_table_with_restricted_sorts_sorts_by_those_columns() {
    use proto::sort_descriptor::SortDirection;
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    let restricted =
        oxhaven_engine::ops::sort::restrict_sort_to(&trades(), &["Sym", "Qty"]).unwrap();
    server.export(4, restricted);
    service()
        .rollup(server.authorized(rollup_request(4, 16, &["Sym", "Ex"])))
        .await
        .unwrap();
    let fetched = ObjectServiceClient::new(server.channel.clone())
        .fetch_object(server.authorized(FetchObjectRequest {
            source_id: Some(TypedTicket {
                ticket: Some(export_id_to_ticket(16)),
                r#type: "HierarchicalTable".to_string(),
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    let descriptor = HierarchicalTableDescriptor::decode(fetched.data.as_ref()).unwrap();
    let schema = schema_of_ipc(&descriptor.snapshot_schema);
    let attribute = |key: &str| schema.metadata().get(key).map(String::as_str);
    assert_eq!(
        attribute("deephaven:attribute.SortableColumns"),
        Some("Sym,Qty")
    );
    assert_eq!(
        attribute("deephaven:attribute_type.SortableColumns"),
        Some("java.lang.String")
    );
    let sortable: Vec<&str> = schema
        .fields()
        .iter()
        .filter(|field| field.metadata().contains_key("deephaven:isSortable"))
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(sortable, ["Sym", "Qty"]);

    let apply = |result: i32, column: &str| {
        let request = HierarchicalTableApplyRequest {
            result_hierarchical_table_id: Some(export_id_to_ticket(result)),
            input_hierarchical_table_id: Some(export_id_to_ticket(16)),
            sorts: vec![sort_by(column, SortDirection::Descending)],
            ..Default::default()
        };
        async move {
            service()
                .apply(server.authorized(request))
                .await
                .map(tonic::Response::into_inner)
        }
    };
    apply(20, "Qty").await.unwrap();
    refused(
        apply(21, "Rows").await,
        Code::InvalidArgument,
        "Sorting attempted on restricted column(s): [Rows], available column(s) for sorting \
         are: [Sym, Qty]",
    );
    // the restriction is read before the column is looked for
    refused(
        apply(22, "Nope").await,
        Code::InvalidArgument,
        "Sorting attempted on restricted column(s): [Nope]",
    );
    // and what is made of the rollup keeps it
    refused(
        async {
            service()
                .apply(server.authorized(HierarchicalTableApplyRequest {
                    result_hierarchical_table_id: Some(export_id_to_ticket(23)),
                    input_hierarchical_table_id: Some(export_id_to_ticket(20)),
                    sorts: vec![sort_by("Rows", SortDirection::Ascending)],
                    ..Default::default()
                }))
                .await
        }
        .await,
        Code::InvalidArgument,
        "Sorting attempted on restricted column(s): [Rows]",
    );
}

/// The metadata of a field without the prefix every key has.
fn metadata(field: &Field) -> Vec<(&str, &str)> {
    let mut all: Vec<(&str, &str)> = field
        .metadata()
        .iter()
        .map(|(key, value)| (key.strip_prefix("deephaven:").unwrap(), value.as_str()))
        .collect();
    all.sort();
    all
}

#[tokio::test]
async fn a_rollup_is_opened_as_java_describes_it() {
    let server = &server().await;
    let fetch = |id: i32, type_name: &str| {
        let request = FetchObjectRequest {
            source_id: Some(TypedTicket {
                ticket: Some(export_id_to_ticket(id)),
                r#type: type_name.to_string(),
            }),
        };
        async move {
            ObjectServiceClient::new(server.channel.clone())
                .fetch_object(server.authorized(request))
                .await
                .map(tonic::Response::into_inner)
        }
    };
    let fetched = fetch(10, "HierarchicalTable").await.unwrap();
    assert_eq!(fetched.r#type, "HierarchicalTable");
    assert!(fetched.typed_export_ids.is_empty());
    let descriptor = HierarchicalTableDescriptor::decode(fetched.data.as_ref()).unwrap();
    assert!(descriptor.is_static);
    let schema = schema_of_ipc(&descriptor.snapshot_schema);
    assert_eq!(
        schema.metadata(),
        &HashMap::from([(
            "deephaven:unsent.attribute.ColumnDescriptions".to_string(),
            String::new()
        )])
    );
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type(), metadata(field)))
        .collect();
    let group_by = |java_type: &'static str| {
        vec![
            ("hierarchicalTable.isExpandByColumn", "true"),
            ("isSortable", "true"),
            ("rollupTable.isAggregatedNodeColumn", "true"),
            ("rollupTable.isGroupByColumn", "true"),
            ("type", java_type),
        ]
    };
    let aggregated = |description: &'static str, input: &'static str| {
        vec![
            ("description", description),
            ("isSortable", "true"),
            ("rollupTable.aggregationInputColumnName", input),
            ("rollupTable.isAggregatedNodeColumn", "true"),
            ("type", "long"),
        ]
    };
    assert_eq!(
        fields,
        [
            (
                "__DEPTH__",
                &DataType::Int32,
                vec![
                    ("hierarchicalTable.isExpandByColumn", "true"),
                    ("hierarchicalTable.isRowDepthColumn", "true"),
                    ("hierarchicalTable.isStructuralColumn", "true"),
                    ("isSortable", "true"),
                    ("type", "int"),
                ]
            ),
            (
                "__EXPANDED__",
                &DataType::Boolean,
                vec![
                    ("hierarchicalTable.isRowExpandedColumn", "true"),
                    ("hierarchicalTable.isStructuralColumn", "true"),
                    ("isSortable", "true"),
                    ("type", "java.lang.Boolean"),
                ]
            ),
            ("Sym", &DataType::Utf8, group_by("java.lang.String")),
            ("Ex", &DataType::Utf8, group_by("java.lang.String")),
            (
                "Qty",
                &DataType::Int64,
                aggregated("Qty aggregated with sum", "Qty")
            ),
            ("Rows", &DataType::Int64, aggregated("count", "")),
        ]
    );

    refused(
        fetch(10, "Table").await,
        Code::FailedPrecondition,
        "Unexpected ObjectType, expected type 'Table', actual type 'HierarchicalTable'",
    );
    refused(
        fetch(1, "HierarchicalTable").await,
        Code::NotFound,
        "No ObjectType found, expected type 'HierarchicalTable'",
    );
    // a view is not an object a client opens
    HierarchicalTableServiceClient::new(server.channel.clone())
        .view(server.authorized(view_request(10, 11, None)))
        .await
        .unwrap();
    refused(
        fetch(11, "HierarchicalTable").await,
        Code::NotFound,
        "No ObjectType found, expected type 'HierarchicalTable'",
    );
}

/// A subscription request for export `id`: `columns` as the bytes of a bit set, and a
/// viewport.
fn subscription(id: i32, columns: Option<&[u8]>, viewport: &RowSet, reverse: bool) -> FlightData {
    let signed = |bytes: &[u8]| bytes.iter().map(|byte| *byte as i8).collect::<Vec<i8>>();
    let mut builder = FlatBufferBuilder::new();
    let ticket = Some(builder.create_vector(&signed(&export_id_to_ticket(id).ticket)));
    let columns = columns.map(|columns| builder.create_vector(&signed(columns)));
    let viewport = Some(builder.create_vector(&signed(&encode_row_set(viewport))));
    let request = BarrageSubscriptionRequest::create(
        &mut builder,
        &BarrageSubscriptionRequestArgs {
            ticket,
            columns,
            viewport,
            reverse_viewport: reverse,
            ..Default::default()
        },
    );
    builder.finish(request, None);
    wrapped(
        BarrageMessageType::BarrageSubscriptionRequest,
        Some(builder.finished_data()),
        MAGIC,
    )
}

/// What a snapshot message says: the size of the view, the rows it has, and those rows.
#[derive(Debug, PartialEq)]
struct Sent {
    size: i64,
    viewport: Option<RowSet>,
    rows: Vec<String>,
}

fn sent(message: &FlightData, schema: &Schema) -> Sent {
    let bytes = metadata_of(message).expect("update metadata");
    let update = flatbuffers::root::<BarrageUpdateMetadata>(&bytes).unwrap();
    assert!(update.is_snapshot());
    let rows = |bytes: Option<flatbuffers::Vector<'_, i8>>| {
        bytes.map(|bytes| decode_row_set(bytes.bytes()).unwrap())
    };
    let size = update.table_size();
    let columns: Vec<Vec<Cell>> = columns_of(message, schema).iter().map(cells).collect();
    let count = columns.iter().map(Vec::len).max().unwrap_or(0);
    let text = |row: usize| {
        let cell = |column: &Vec<Cell>| match column.get(row) {
            Some(Some(value)) => value.clone(),
            Some(None) => "-".to_string(),
            None => String::new(),
        };
        columns.iter().map(cell).collect::<Vec<_>>().join("|")
    };
    Sent {
        size,
        viewport: rows(update.effective_viewport()),
        rows: (0..count).map(text).collect(),
    }
}

#[tokio::test]
async fn a_subscription_to_a_view_is_sent_a_snapshot_of_its_viewport_when_it_changes() {
    let server = &Server::start().await;
    let input =
        append_only_input_table(server.state.graph.clone(), trades_definition(), None).unwrap();
    let writer = input.input_table().unwrap();
    writer
        .add(trade_rows(&[
            ("AAPL", "N", 10),
            ("AAPL", "Q", 20),
            ("MSFT", "N", 30),
        ]))
        .unwrap();
    server.state.graph.run_cycle();
    server.export(1, input.clone());
    server.export(2, expand("AAPL", true));
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    service()
        .rollup(server.authorized(rollup_request(1, 10, &["Sym", "Ex"])))
        .await
        .unwrap();
    service()
        .view(server.authorized(view_request(10, 11, Some(2))))
        .await
        .unwrap();
    let value = server.session().get_export(10).unwrap().value().unwrap();
    let descriptor =
        HierarchicalTableDescriptor::decode(describe(hierarchical_of(&value).unwrap()).as_slice())
            .unwrap();
    assert!(!descriptor.is_static);

    let mut call = Call::open(server).await;
    let first_ten = RowSet::from_range(0, 9);
    call.send(subscription(11, None, &first_ten, false));
    let messages = call.collect().await;
    let [schema, snapshot] = messages.as_slice() else {
        panic!("a schema and a snapshot, not {} messages", messages.len());
    };
    let schema = schema_of(schema).unwrap();
    assert_eq!(schema, schema_of_ipc(&descriptor.snapshot_schema));
    assert_eq!(
        sent(snapshot, &schema),
        Sent {
            size: 5,
            viewport: Some(first_ten.clone()),
            rows: [
                "1|true|-|-|60|3",
                "2|true|AAPL|-|30|2",
                "3|-|AAPL|N|10|1",
                "3|-|AAPL|Q|20|1",
                "2|false|MSFT|-|30|1"
            ]
            .map(String::from)
            .to_vec(),
        }
    );

    // the source changes: the whole viewport again
    writer
        .add(trade_rows(&[("GOOG", "Q", 60), ("AAPL", "N", 40)]))
        .unwrap();
    server.state.graph.run_cycle();
    let messages = call.collect().await;
    let [snapshot] = messages.as_slice() else {
        panic!("one snapshot, not {} messages", messages.len());
    };
    assert_eq!(
        sent(snapshot, &schema).rows,
        [
            "1|true|-|-|160|5",
            "2|true|AAPL|-|70|3",
            "3|-|AAPL|N|50|2",
            "3|-|AAPL|Q|20|1",
            "2|false|MSFT|-|30|1",
            "2|false|GOOG|-|60|1"
        ]
    );
    // a cycle that changes nothing sends nothing
    server.state.graph.run_cycle();
    assert!(call.collect().await.is_empty());

    // another viewport and fewer columns: rows 1 and 2 without Ex and Rows
    let two = RowSet::from_range(1, 2);
    call.send(subscription(11, Some(&[0b010111]), &two, false));
    let messages = call.collect().await;
    let [snapshot] = messages.as_slice() else {
        panic!("one snapshot, not {} messages", messages.len());
    };
    assert_eq!(
        sent(snapshot, &schema),
        Sent {
            size: 6,
            viewport: Some(two),
            rows: ["2|true|AAPL||70|", "3|-|AAPL||50|"]
                .map(String::from)
                .to_vec(),
        }
    );
    // a viewport beyond the rows has none
    let beyond = RowSet::from_range(50, 59);
    call.send(subscription(11, Some(&[0b111111]), &beyond, false));
    let messages = call.collect().await;
    assert_eq!(sent(&messages[0], &schema).rows, Vec::<String>::new());
    assert_eq!(sent(&messages[0], &schema).size, 6);

    call.close().await;
    assert_eq!(call.ended.as_deref(), Some("OK: "));
}

#[tokio::test]
async fn a_subscription_to_a_view_is_refused_what_java_refuses() {
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    for (keys, result) in [(None, 11), (Some(3), 12)] {
        service()
            .view(server.authorized(view_request(10, result, keys)))
            .await
            .unwrap();
    }
    let ten = RowSet::from_range(0, 9);
    let mut two_ranges = RowSet::from_range(0, 3);
    two_ranges.insert(&RowSet::from_range(6, 9));
    let contiguous = "INVALID_ARGUMENT: HierarchicalTableView subscriptions only support \
                      contiguous viewports";
    for (requests, ended) in [
        (
            vec![subscription(11, None, &two_ranges, false)],
            contiguous.to_string(),
        ),
        // also in a later request of a subscription
        (
            vec![
                subscription(11, None, &ten, false),
                subscription(11, None, &two_ranges, false),
            ],
            contiguous.to_string(),
        ),
        (
            vec![subscription(11, None, &RowSet::from_range(0, 70000), false)],
            "INVALID_ARGUMENT: HierarchicalTableView subscriptions only support viewport size up \
             to 65536 rows, requested 70001 rows"
                .to_string(),
        ),
        (
            vec![subscription(11, None, &ten, true)],
            "INVALID_ARGUMENT: HierarchicalTableView subscriptions do not support reverse \
             viewports"
                .to_string(),
        ),
        (
            vec![subscription(11, Some(&[0xff, 0xff, 0xff]), &ten, false)],
            "INVALID_ARGUMENT: Requested columns out of range: length=24, available length=6"
                .to_string(),
        ),
        // without the depth and expanded columns; and a key table without a depth column
        (
            vec![subscription(11, Some(&[0b000101]), &ten, false)],
            format!("INVALID_ARGUMENT: {LOGGED}"),
        ),
        (
            vec![subscription(12, None, &ten, false)],
            format!("INVALID_ARGUMENT: {LOGGED}"),
        ),
        (
            vec![snapshot_request(&export_id_to_ticket(11).ticket)],
            "FAILED_PRECONDITION: Ticket (export/11) is a HierarchicalTableView and does not \
             support snapshots."
                .to_string(),
        ),
    ] {
        let mut call = Call::open(server).await;
        for request in requests {
            call.send(request);
            call.collect().await;
        }
        let ending = call.ended.clone().unwrap_or_default();
        assert!(ending.starts_with(&ended), "{ending:?}, not {ended:?}");
    }
    // the largest viewport there is
    let mut call = Call::open(server).await;
    call.send(subscription(11, None, &RowSet::from_range(0, 65535), false));
    assert_eq!(call.collect().await.len(), 2);
    assert_eq!(call.ended, None);
}
