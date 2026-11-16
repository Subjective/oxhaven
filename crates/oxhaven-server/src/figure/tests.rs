//! A figure as a value and over the wire. That a fetch of a figure answers the description
//! and then the tables and the partitioned tables as references is what Java 42.9 answered
//! (`docs/FIGURES.md`); the refusals of [`FigureWidget::new`] are Oxhaven's own.

// FetchObject is deprecated in the protocol and still what clients without streams call
#![allow(deprecated)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::{FetchObjectRequest, TypedTicket};
use oxhaven_proto::script::figure_descriptor::{
    ChartDescriptor, MultiSeriesDescriptor, MultiSeriesSourceDescriptor, SeriesDescriptor,
    SourceDescriptor,
};
use oxhaven_proto::script::FigureDescriptor;
use prost::Message;
use tonic::Code;

use super::*;
use crate::flight::tests::support::Server;
use crate::partitioned::{partition, partitioned_of};
use crate::session::{export_id_to_ticket, ticket_to_export_id};

fn rows() -> Arc<dyn Table> {
    let schema = Schema::new(vec![
        Field::new("K", DataType::Int32, true),
        Field::new("V", DataType::Int32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1, 2, 1])),
        Arc::new(Int32Array::from(vec![10, 20, 30])),
    ];
    let definition = TableDefinition::new(vec![
        ColumnDefinition::new("K", ColumnType::Int),
        ColumnDefinition::new("V", ColumnType::Int),
    ])
    .unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
    in_memory_table(definition, &[batch]).unwrap()
}

/// The partition of [`rows`] by `K`: its constituents have `V` only.
fn parts() -> Partitioned {
    partition(&rows(), &["K".to_string()], true).unwrap()
}

/// A figure of one chart: a series that reads `column` of table `table`, and, when
/// `multi` is given, a multi series that reads that column of that partitioned table.
fn descriptor(table: i32, column: &str, multi: Option<(i32, &str)>) -> FigureDescriptor {
    let series = SeriesDescriptor {
        name: "one".to_string(),
        data_sources: vec![SourceDescriptor {
            axis_id: "X0".to_string(),
            table_id: table,
            partitioned_table_id: -1,
            column_name: column.to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let multi_series = multi
        .into_iter()
        .map(|(id, column)| MultiSeriesDescriptor {
            name: "many".to_string(),
            data_sources: vec![MultiSeriesSourceDescriptor {
                axis_id: "X0".to_string(),
                partitioned_table_id: id,
                column_name: column.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .collect();
    FigureDescriptor {
        cols: 1,
        rows: 1,
        charts: vec![ChartDescriptor {
            series: vec![series],
            multi_series,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn refused(descriptor: FigureDescriptor, tables: usize, partitioned: usize) -> String {
    let tables = (0..tables).map(|_| rows()).collect();
    let partitioned = (0..partitioned).map(|_| parts()).collect();
    match FigureWidget::new(descriptor, tables, partitioned) {
        Ok(_) => "accepted".to_string(),
        Err(text) => text,
    }
}

#[test]
fn a_figure_whose_source_names_nothing_is_refused() {
    assert_eq!(
        refused(descriptor(0, "V", Some((0, "V"))), 1, 1),
        "accepted"
    );
    assert_eq!(
        refused(descriptor(1, "V", None), 1, 0),
        "series one: there is no table 1"
    );
    assert_eq!(
        refused(descriptor(-1, "V", None), 1, 0),
        "series one: there is no table -1"
    );
    assert_eq!(
        refused(descriptor(0, "W", None), 1, 0),
        "series one: table 0 has no column W"
    );
    assert_eq!(
        refused(descriptor(0, "V", Some((1, "V"))), 1, 1),
        "series many: there is no partitioned table 1"
    );
    assert_eq!(
        refused(descriptor(0, "V", Some((-1, "V"))), 1, 1),
        "series many: there is no partitioned table -1"
    );
    // a key column is not a column of the constituents
    assert_eq!(
        refused(descriptor(0, "V", Some((0, "K"))), 1, 1),
        "series many: the constituents have no column K"
    );
    // a source without a column is one Java could not describe: it reads nothing
    assert_eq!(refused(descriptor(7, "", None), 0, 0), "accepted");
}

#[test]
fn only_a_figure_is_of_the_figure_type() {
    let figure: Figure =
        Arc::new(FigureWidget::new(descriptor(0, "V", None), vec![rows()], vec![]).unwrap());
    let value = export_value(figure);
    assert_eq!(FigureType.name(), "Figure");
    assert!(FigureType.is_type(&value).unwrap());
    assert!(!FigureType.is_type(&ExportValue::Table(rows())).unwrap());
    assert!(!FigureType
        .is_type(&crate::partitioned::export_value(parts()))
        .unwrap());
    assert_eq!(
        crate::object::types::registry()
            .type_name(&value)
            .unwrap()
            .as_deref(),
        Some("Figure")
    );
}

#[tokio::test]
async fn a_fetched_figure_is_its_description_with_tables_then_partitioned_tables() {
    let server = &Server::start().await;
    let table = rows();
    let described = descriptor(0, "V", Some((0, "V")));
    let figure: Figure =
        Arc::new(FigureWidget::new(described.clone(), vec![table.clone()], vec![parts()]).unwrap());
    server.export(1, table.clone());
    server
        .session()
        .new_export(2)
        .unwrap()
        .submit(move |_| Ok(export_value(figure)))
        .unwrap();
    let fetch = |id: i32, type_name: &str| {
        let request = FetchObjectRequest {
            source_id: Some(TypedTicket {
                ticket: Some(export_id_to_ticket(id)),
                r#type: type_name.to_string(),
            }),
        };
        let mut service = ObjectServiceClient::new(server.channel.clone());
        async move { service.fetch_object(server.authorized(request)).await }
    };

    let fetched = fetch(2, "Figure").await.unwrap().into_inner();
    assert_eq!(fetched.r#type, "Figure");
    assert_eq!(
        FigureDescriptor::decode(&fetched.data[..]).unwrap(),
        described
    );
    assert_eq!(fetched.data, described.encode_to_vec());
    let kinds: Vec<&str> = fetched
        .typed_export_ids
        .iter()
        .map(|reference| reference.r#type.as_str())
        .collect();
    assert_eq!(kinds, ["Table", "PartitionedTable"]);
    let value = |place: usize| {
        let ticket = fetched.typed_export_ids[place].ticket.as_ref().unwrap();
        let id = ticket_to_export_id(&ticket.ticket, "reference").unwrap();
        assert!(id < 0, "an export the server chose, not {id}");
        server.session().get_export(id).unwrap().value().unwrap()
    };
    assert!(Arc::ptr_eq(value(0).as_table().unwrap(), &table));
    let partitioned = value(1);
    let partitioned = partitioned_of(&partitioned).expect("a partitioned table");
    assert_eq!(partitioned.key_columns(), ["K"]);

    // a figure is no table, and a table no figure
    let status = fetch(2, "Table").await.unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (
            Code::FailedPrecondition,
            "Unexpected ObjectType, expected type 'Table', actual type 'Figure'"
        )
    );
    let status = fetch(1, "Figure").await.unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (
            Code::NotFound,
            "No ObjectType found, expected type 'Figure'"
        )
    );
}
