//! The update views and format views of `Apply` (task 7.8.2). The fields, the rows and
//! the refusals are what the Java reference (42.9) answered to the same calls for a
//! rollup of the same shape.

// `FetchObject` is deprecated in favour of `MessageStream`; clients still call it.
#![allow(deprecated)]

use arrow::array::Int8Array;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_proto::grpc::hierarchical_table_service_client::HierarchicalTableServiceClient;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::{FetchObjectRequest, Selectable, TypedTicket};
use oxhaven_testkit::{definition, ints, ReferenceTable};
use tonic::Code;

use super::tests::{metadata, refused, rollup_request, server, sort_by, LOGGED};
use super::*;
use crate::flight::tests::support::{schema_of_ipc, Server};
use crate::session::export_id_to_ticket;

/// What `ColorUtil.toLong(RED)` and `ColorUtil.toLong(BLUE)` are.
const RED: &str = "143833713130660064";
const BLUE: &str = "72058689286103264";

fn view(formula: &str, node_type: RollupNodeType) -> UpdateViewRequest {
    UpdateViewRequest {
        column_spec: Some(Selectable {
            r#type: Some(oxhaven_proto::grpc::selectable::Type::Raw(
                formula.to_string(),
            )),
        }),
        node_type: node_type as i32,
    }
}

fn aggregated(formula: &str) -> UpdateViewRequest {
    view(formula, RollupNodeType::Aggregated)
}

fn constituent(formula: &str) -> UpdateViewRequest {
    view(formula, RollupNodeType::Constituent)
}

fn request(input: i32, result: i32) -> HierarchicalTableApplyRequest {
    HierarchicalTableApplyRequest {
        result_hierarchical_table_id: Some(export_id_to_ticket(result)),
        input_hierarchical_table_id: Some(export_id_to_ticket(input)),
        ..Default::default()
    }
}

/// A server with the rollup of the trades by `Sym` with constituents as export 30 and
/// without as export 40.
async fn rollups() -> Server {
    let server = server().await;
    let mut service = HierarchicalTableServiceClient::new(server.channel.clone());
    service
        .rollup(server.authorized(RollupRequest {
            include_constituents: true,
            ..rollup_request(1, 30, &["Sym"])
        }))
        .await
        .unwrap();
    service
        .rollup(server.authorized(rollup_request(1, 40, &["Sym"])))
        .await
        .unwrap();
    server
}

async fn apply(server: &Server, request: HierarchicalTableApplyRequest) -> Result<(), Status> {
    HierarchicalTableServiceClient::new(server.channel.clone())
        .apply(server.authorized(request))
        .await
        .map(|_| ())
}

/// The fields of export `id` after the two structural ones: name, type and metadata.
async fn fields(server: &Server, id: i32) -> Vec<(String, DataType, Vec<(String, String)>)> {
    let request = FetchObjectRequest {
        source_id: Some(TypedTicket {
            ticket: Some(export_id_to_ticket(id)),
            r#type: "HierarchicalTable".to_string(),
        }),
    };
    let fetched = ObjectServiceClient::new(server.channel.clone())
        .fetch_object(server.authorized(request))
        .await
        .unwrap()
        .into_inner();
    let descriptor = HierarchicalTableDescriptor::decode(fetched.data.as_ref()).unwrap();
    let schema = schema_of_ipc(&descriptor.snapshot_schema);
    schema
        .fields()
        .iter()
        .skip(2)
        .map(|field| {
            let metadata = metadata(field)
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect();
            (field.name().clone(), field.data_type().clone(), metadata)
        })
        .collect()
}

/// Every row of export `id` with everything expanded, without the two structural
/// columns: the cells with `|` between them, `-` for null.
fn rows(server: &Server, id: i32) -> Vec<String> {
    let value = server.session().get_export(id).unwrap().value().unwrap();
    let everything = ReferenceTable::new_static(
        definition(&[
            ("__DEPTH__", ColumnType::Int),
            ("Sym", ColumnType::String),
            ("Action", ColumnType::Byte),
        ]),
        RowSet::flat(1),
        vec![
            ints([0]),
            oxhaven_testkit::strings([""]),
            std::sync::Arc::new(Int8Array::from(vec![3])),
        ],
    );
    let view = View::new(
        hierarchical_of(&value).unwrap().hierarchy(),
        Some(everything),
        Some("Action".to_string()),
    );
    let snapshot = view.snapshot(None, 0, 100).unwrap();
    (0..snapshot.rows as usize)
        .map(|row| {
            let cells: Vec<String> = snapshot.columns[2..]
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        "-".to_string()
                    } else {
                        array_value_to_string(column, row).unwrap()
                    }
                })
                .collect();
            cells.join("|")
        })
        .collect()
}

fn pairs(metadata: &[(&str, &str)]) -> Vec<(String, String)> {
    metadata
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[tokio::test]
async fn update_views_add_columns_to_the_nodes_of_their_type() {
    let server = &rollups().await;
    apply(
        server,
        HierarchicalTableApplyRequest {
            update_views: vec![aggregated("Twice = Qty * 2"), constituent("Neg = -Qty")],
            ..request(30, 31)
        },
    )
    .await
    .unwrap();
    let fields = fields(server, 31).await;
    let names: Vec<&str> = fields.iter().map(|field| field.0.as_str()).collect();
    assert_eq!(
        names,
        ["Sym", "Qty", "Rows", "Twice", "Sym", "Ex", "Qty", "Neg"]
    );
    assert_eq!(
        fields[3],
        (
            "Twice".to_string(),
            DataType::Int64,
            pairs(&[
                ("isSortable", "true"),
                ("rollupTable.aggregationInputColumnName", ""),
                ("rollupTable.isAggregatedNodeColumn", "true"),
                ("type", "long"),
            ])
        )
    );
    assert_eq!(
        fields[7],
        (
            "Neg".to_string(),
            DataType::Int32,
            pairs(&[
                ("isSortable", "true"),
                ("rollupTable.isConstituentNodeColumn", "true"),
                ("type", "int"),
            ])
        )
    );
    assert_eq!(
        rows(server, 31)[..4],
        [
            "-|210|6|420|-|-|-|-",
            "AAPL|70|3|140|-|-|-|-",
            "-|-|-|-|AAPL|N|10|-10",
            "-|-|-|-|AAPL|Q|20|-20",
        ]
    );
    // a sort by the column, and a filter, of a later request
    apply(
        server,
        HierarchicalTableApplyRequest {
            sorts: vec![sort_by("Twice", SortDirection::Descending)],
            ..request(31, 32)
        },
    )
    .await
    .unwrap();
    assert_eq!(rows(server, 32)[1], "MSFT|80|2|160|-|-|-|-");
    // and of the same request
    apply(
        server,
        HierarchicalTableApplyRequest {
            update_views: vec![aggregated("Twice = Qty * 2")],
            sorts: vec![sort_by("Twice", SortDirection::Descending)],
            ..request(40, 41)
        },
    )
    .await
    .unwrap();
    assert_eq!(
        rows(server, 41),
        [
            "-|210|6|420",
            "MSFT|80|2|160",
            "AAPL|70|3|140",
            "GOOG|60|1|120"
        ]
    );
}

#[tokio::test]
async fn format_views_add_the_hidden_columns_of_the_formats() {
    let server = &rollups().await;
    apply(
        server,
        HierarchicalTableApplyRequest {
            format_views: vec![constituent("Qty = BLUE"), aggregated("Qty = RED")],
            ..request(30, 31)
        },
    )
    .await
    .unwrap();
    let fields = fields(server, 31).await;
    let names: Vec<&str> = fields.iter().map(|field| field.0.as_str()).collect();
    assert_eq!(
        names,
        [
            "Sym",
            "Qty",
            "Rows",
            "Qty__TABLE_STYLE_FORMAT",
            "Sym",
            "Ex",
            "Qty",
            "Qty__TABLE_STYLE_FORMAT"
        ]
    );
    assert_eq!(
        fields[1].2,
        pairs(&[
            ("description", "Qty aggregated with sum"),
            ("isSortable", "true"),
            ("rollupTable.aggregationInputColumnName", "Qty"),
            ("rollupTable.isAggregatedNodeColumn", "true"),
            ("styleColumn", "Qty__TABLE_STYLE_FORMAT"),
            ("type", "long"),
        ])
    );
    assert_eq!(
        fields[3],
        (
            "Qty__TABLE_STYLE_FORMAT".to_string(),
            DataType::Int64,
            pairs(&[
                ("isSortable", "true"),
                ("isStyle", "true"),
                ("rollupTable.aggregationInputColumnName", ""),
                ("rollupTable.isAggregatedNodeColumn", "true"),
                ("type", "long"),
            ])
        )
    );
    assert_eq!(
        fields[6].2,
        pairs(&[
            ("isSortable", "true"),
            ("rollupTable.isConstituentNodeColumn", "true"),
            ("styleColumn", "Qty__TABLE_STYLE_FORMAT"),
            ("type", "int"),
        ])
    );
    assert_eq!(
        fields[7],
        (
            "Qty__TABLE_STYLE_FORMAT".to_string(),
            DataType::Int64,
            pairs(&[
                ("isSortable", "true"),
                ("isStyle", "true"),
                ("rollupTable.isConstituentNodeColumn", "true"),
                ("type", "long"),
            ])
        )
    );
    assert_eq!(
        rows(server, 31)[..3],
        [
            format!("-|210|6|{RED}|-|-|-|-"),
            format!("AAPL|70|3|{RED}|-|-|-|-"),
            format!("-|-|-|-|AAPL|N|10|{BLUE}"),
        ]
    );
    // update views of one node type and formats of the other, in one request
    apply(
        server,
        HierarchicalTableApplyRequest {
            update_views: vec![aggregated("Twice = Qty * 2")],
            format_views: vec![constituent("Qty = Qty > 15 ? RED : NO_FORMATTING")],
            ..request(30, 32)
        },
    )
    .await
    .unwrap();
    assert_eq!(
        rows(server, 32)[1..4],
        [
            "AAPL|70|3|140|-|-|-|-".to_string(),
            "-|-|-|-|AAPL|N|10|0".to_string(),
            format!("-|-|-|-|AAPL|Q|20|{RED}"),
        ]
    );
    // the views of the rows of the source of a rollup that shows none are dropped
    apply(
        server,
        HierarchicalTableApplyRequest {
            update_views: vec![constituent("Neg = -Qty")],
            format_views: vec![constituent("Qty = RED")],
            ..request(40, 41)
        },
    )
    .await
    .unwrap();
    assert_eq!(rows(server, 41)[..2], ["-|210|6", "AAPL|70|3"]);
}

#[tokio::test]
async fn apply_refuses_the_views_java_refuses() {
    let server = &rollups().await;
    apply(
        server,
        HierarchicalTableApplyRequest {
            update_views: vec![aggregated("Twice = Qty * 2")],
            ..request(30, 31)
        },
    )
    .await
    .unwrap();
    let mut next = 50;
    let mut fresh = || {
        next += 1;
        next
    };
    let updates =
        |input: i32, result: i32, views: Vec<UpdateViewRequest>| HierarchicalTableApplyRequest {
            update_views: views,
            ..request(input, result)
        };
    let formats =
        |input: i32, result: i32, views: Vec<UpdateViewRequest>| HierarchicalTableApplyRequest {
            format_views: views,
            ..request(input, result)
        };
    for (request, text) in [
        // nodes with columns added take no more: not in one request, not in a later one
        (
            HierarchicalTableApplyRequest {
                update_views: vec![aggregated("Twice = Qty * 2")],
                format_views: vec![aggregated("Qty = RED")],
                ..request(30, fresh())
            },
            LOGGED,
        ),
        (formats(31, fresh(), vec![aggregated("Qty = RED")]), LOGGED),
        (
            updates(31, fresh(), vec![aggregated("Thrice = Qty * 3")]),
            LOGGED,
        ),
        // an expression is checked against the columns of the source: the count is none
        (
            updates(30, fresh(), vec![aggregated("More = Rows + 1")]),
            LOGGED,
        ),
        (updates(30, fresh(), vec![aggregated("Qty = 1")]), LOGGED),
        (updates(30, fresh(), vec![aggregated("Qty")]), LOGGED),
        (
            updates(30, fresh(), vec![aggregated("A = Nope + 1")]),
            LOGGED,
        ),
        (updates(30, fresh(), vec![aggregated("R = i")]), LOGGED),
        (
            updates(
                30,
                fresh(),
                vec![UpdateViewRequest {
                    column_spec: None,
                    node_type: RollupNodeType::Aggregated as i32,
                }],
            ),
            LOGGED,
        ),
        (
            updates(
                30,
                fresh(),
                vec![view("Twice = Qty * 2", RollupNodeType::TypeNotSpecified)],
            ),
            "Unsupported or unknown node type: TYPE_NOT_SPECIFIED",
        ),
        (
            formats(
                30,
                fresh(),
                vec![UpdateViewRequest {
                    node_type: 7,
                    ..aggregated("Qty = RED")
                }],
            ),
            "Unsupported or unknown node type: UNRECOGNIZED",
        ),
        // a column the nodes do not have, the row, a hidden column and a pattern
        (formats(30, fresh(), vec![aggregated("Nope = RED")]), LOGGED),
        (formats(30, fresh(), vec![aggregated("* = RED")]), LOGGED),
        (
            formats(
                30,
                fresh(),
                vec![aggregated("Qty__TABLE_STYLE_FORMAT = RED")],
            ),
            LOGGED,
        ),
        (
            formats(30, fresh(), vec![aggregated("Qty = Decimal(`0.0`)")]),
            LOGGED,
        ),
    ] {
        let described = format!("{request:?}");
        let answer = apply(server, request).await;
        assert!(answer.is_err(), "{described}");
        refused(answer, Code::InvalidArgument, text);
    }
}
