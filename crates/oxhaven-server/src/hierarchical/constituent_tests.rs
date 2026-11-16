//! A rollup with constituents over the wire: its snapshot schema and the sorts of `Apply`.
//! The fields and the orders are what the Java reference (42.9) answered to the same
//! calls for the same table.

// `FetchObject` is deprecated in favour of `MessageStream`; clients still call it.
#![allow(deprecated)]

use arrow::array::{Array, AsArray, Int8Array};
use arrow::datatypes::{DataType, Int32Type};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_proto::grpc::hierarchical_table_service_client::HierarchicalTableServiceClient;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::{FetchObjectRequest, SortDescriptor, TypedTicket};
use oxhaven_testkit::{definition, ints, ReferenceTable};
use tonic::Code;

use super::tests::{metadata, refused, rollup_request, server, sort_by, LOGGED};
use super::*;
use crate::flight::tests::support::{schema_of_ipc, Server};
use crate::session::export_id_to_ticket;

/// The rollup of the trades by `by` with constituents, as export `result`.
async fn rollup_with_constituents(server: &Server, result: i32, by: &[&str]) {
    HierarchicalTableServiceClient::new(server.channel.clone())
        .rollup(server.authorized(RollupRequest {
            include_constituents: true,
            ..rollup_request(1, result, by)
        }))
        .await
        .unwrap();
}

/// Every row of export `id` with everything expanded: for a row of a level its key and
/// its sum, for a row of the source its `Ex` and its `Qty`.
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
    // __DEPTH__, __EXPANDED__, Sym, Qty, Rows, then Sym, Ex, Qty of the source
    let text = |column: usize, row: usize| {
        let values = snapshot.columns[column].as_string::<i32>();
        if values.is_null(row) {
            "-".to_string()
        } else {
            values.value(row).to_string()
        }
    };
    let sums = snapshot.columns[3].as_primitive::<arrow::datatypes::Int64Type>();
    let quantities = snapshot.columns[7].as_primitive::<Int32Type>();
    (0..snapshot.rows as usize)
        .map(|row| {
            if quantities.is_null(row) {
                format!("{} {}", text(2, row), sums.value(row))
            } else {
                assert!(sums.is_null(row) && snapshot.columns[1].is_null(row));
                format!("  {} {}", text(6, row), quantities.value(row))
            }
        })
        .collect()
}

#[tokio::test]
async fn a_rollup_with_constituents_is_opened_as_java_describes_it() {
    let server = &server().await;
    rollup_with_constituents(server, 30, &["Sym"]).await;
    let request = FetchObjectRequest {
        source_id: Some(TypedTicket {
            ticket: Some(export_id_to_ticket(30)),
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
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .skip(2)
        .map(|field| (field.name().as_str(), field.data_type(), metadata(field)))
        .collect();
    let aggregated = |description: &'static str, input: &'static str| {
        vec![
            ("description", description),
            ("isSortable", "true"),
            ("rollupTable.aggregationInputColumnName", input),
            ("rollupTable.isAggregatedNodeColumn", "true"),
            ("type", "long"),
        ]
    };
    let constituent = |java_type: &'static str| {
        vec![
            ("isSortable", "true"),
            ("rollupTable.isConstituentNodeColumn", "true"),
            ("type", java_type),
        ]
    };
    assert_eq!(
        fields,
        [
            (
                "Sym",
                &DataType::Utf8,
                vec![
                    ("hierarchicalTable.isExpandByColumn", "true"),
                    ("isSortable", "true"),
                    ("rollupTable.isAggregatedNodeColumn", "true"),
                    ("rollupTable.isGroupByColumn", "true"),
                    ("type", "java.lang.String"),
                ]
            ),
            (
                "Qty",
                &DataType::Int64,
                aggregated("Qty aggregated with sum", "Qty")
            ),
            ("Rows", &DataType::Int64, aggregated("count", "")),
            // the columns of the source, the group-by column among them
            (
                "Sym",
                &DataType::Utf8,
                vec![
                    ("isSortable", "true"),
                    ("rollupTable.isConstituentNodeColumn", "true"),
                    ("rollupTable.isGroupByColumn", "true"),
                    ("type", "java.lang.String"),
                ]
            ),
            ("Ex", &DataType::Utf8, constituent("java.lang.String")),
            ("Qty", &DataType::Int32, constituent("int")),
        ]
    );
}

#[tokio::test]
async fn apply_sorts_the_rows_of_the_source_by_the_inputs_of_the_results() {
    let server = &server().await;
    rollup_with_constituents(server, 30, &["Sym"]).await;
    let apply = |input: i32, result: i32, sort: SortDescriptor| async move {
        HierarchicalTableServiceClient::new(server.channel.clone())
            .apply(server.authorized(HierarchicalTableApplyRequest {
                result_hierarchical_table_id: Some(export_id_to_ticket(result)),
                input_hierarchical_table_id: Some(export_id_to_ticket(input)),
                sorts: vec![sort],
                ..Default::default()
            }))
            .await
    };
    assert_eq!(
        rows(server, 30),
        [
            "- 210", "AAPL 70", "  N 10", "  Q 20", "  N 40", "MSFT 80", "  N 30", "  Q 50",
            "GOOG 60", "  Q 60",
        ]
    );
    // by the sum: the levels by the sum, the rows of the source by what is summed
    apply(30, 31, sort_by("Qty", SortDirection::Descending))
        .await
        .unwrap();
    assert_eq!(
        rows(server, 31),
        [
            "- 210", "MSFT 80", "  Q 50", "  N 30", "AAPL 70", "  N 40", "  Q 20", "  N 10",
            "GOOG 60", "  Q 60",
        ]
    );
    // by the count, which has no input: the rows of the source stay as they were sorted
    apply(31, 32, sort_by("Rows", SortDirection::Ascending))
        .await
        .unwrap();
    assert_eq!(
        rows(server, 32),
        [
            "- 210", "GOOG 60", "  Q 60", "MSFT 80", "  Q 50", "  N 30", "AAPL 70", "  N 40",
            "  Q 20", "  N 10",
        ]
    );
    // by the group-by column: the levels alone
    apply(30, 33, sort_by("Sym", SortDirection::Ascending))
        .await
        .unwrap();
    assert_eq!(
        rows(server, 33),
        [
            "- 210", "AAPL 70", "  N 10", "  Q 20", "  N 40", "GOOG 60", "  Q 60", "MSFT 80",
            "  N 30", "  Q 50",
        ]
    );
    // a column of the source alone is no column of the levels, which is what `Apply`
    // sorts
    refused(
        apply(30, 34, sort_by("Ex", SortDirection::Ascending)).await,
        Code::InvalidArgument,
        LOGGED,
    );
}
