//! `HierarchicalTableService.Tree`, and a tree in the calls a rollup is in, over the wire.
//! The statuses, texts and rows are what the Java reference (42.9) answered to the same
//! calls; `docs/HIERARCHICAL.md` describes them.

// `FetchObject` is deprecated in favour of `MessageStream`; clients still call it.
#![allow(deprecated)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::append_only_input_table;
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::hierarchical_table_service_client::HierarchicalTableServiceClient;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::{FetchObjectRequest, SortDescriptor, TypedTicket};
use oxhaven_testkit::{definition, ints, strings, ReferenceTable};
use tonic::Code;

use super::tests::{
    compared, metadata, refused, sent, sort_by, subscription, update_view, view_request, Sent,
    LOGGED,
};
use super::*;
use crate::barrage::tests::{schema_of, Call};
use crate::flight::tests::support::{schema_of_ipc, Server};
use crate::session::export_id_to_ticket;

fn nodes_definition() -> oxhaven_contracts::schema::TableDefinition {
    definition(&[
        ("Id", ColumnType::Int),
        ("Parent", ColumnType::Int),
        ("Name", ColumnType::String),
        ("V", ColumnType::Int),
    ])
}

fn numbers(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

/// Row 1 with the children 2 and 3, and row 4, whose parent 9 is no row.
fn nodes() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        nodes_definition(),
        RowSet::flat(4),
        vec![
            ints([1, 2, 3, 4]),
            numbers(&[None, Some(1), Some(1), Some(9)]),
            strings(["a", "b", "c", "d"]),
            ints([4, 3, 2, 1]),
        ],
    )
}

fn node_rows(rows: &[(i32, Option<i32>, &str, i32)]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("Id", DataType::Int32, true),
        Field::new("Parent", DataType::Int32, true),
        Field::new("Name", DataType::Utf8, true),
        Field::new("V", DataType::Int32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
        Arc::new(Int32Array::from_iter(rows.iter().map(|row| row.1))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.3))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// A key table that expands every node: the root, which is the row without an
/// identifier, with Java's action `ExpandAll`.
fn everything() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[("Id", ColumnType::Int), ("Action", ColumnType::Byte)]),
        RowSet::flat(1),
        vec![numbers(&[None]), Arc::new(Int8Array::from(vec![3]))],
    )
}

fn tree_request(source: i32, result: i32) -> TreeRequest {
    TreeRequest {
        result_tree_table_id: Some(export_id_to_ticket(result)),
        source_table_id: Some(export_id_to_ticket(source)),
        identifier_column: "Id".to_string(),
        parent_identifier_column: "Parent".to_string(),
        promote_orphans: false,
    }
}

/// A server whose session has the nodes as export 1, the key table that expands every
/// node as export 2, the tree of the nodes as export 10 and that with its orphans
/// promoted as export 11.
async fn server() -> Server {
    let server = Server::start().await;
    server.export(1, nodes());
    server.export(2, everything());
    let mut service = HierarchicalTableServiceClient::new(server.channel.clone());
    service
        .tree(server.authorized(tree_request(1, 10)))
        .await
        .unwrap();
    let promoted = TreeRequest {
        promote_orphans: true,
        ..tree_request(1, 11)
    };
    service.tree(server.authorized(promoted)).await.unwrap();
    server
}

/// The rows of the hierarchical table that is export `id`, every node expanded or with
/// the default expansions: depth, `+` expanded, `-` not, `.` without children, and the
/// identifier.
fn rows(server: &Server, id: i32, expanded: bool) -> String {
    let value = server.session().get_export(id).unwrap().value().unwrap();
    let view = View::new(
        hierarchical_of(&value).unwrap().hierarchy(),
        expanded.then(everything),
        expanded.then(|| "Action".to_string()),
    );
    let snapshot = view.snapshot(None, 0, 100).unwrap();
    let depths = snapshot.columns[0].as_primitive::<Int32Type>();
    let marks = snapshot.columns[1].as_boolean();
    let ids = snapshot.columns[2].as_primitive::<Int32Type>();
    (0..snapshot.rows as usize)
        .map(|row| {
            let mark = match (marks.is_null(row), marks.value(row)) {
                (true, _) => '.',
                (false, true) => '+',
                (false, false) => '-',
            };
            format!("{}{mark}{}", depths.value(row), ids.value(row))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl#tree`
#[tokio::test]
async fn tree_answers_what_java_answers() {
    let server = &server().await;
    let tree = |request: TreeRequest| async move {
        HierarchicalTableServiceClient::new(server.channel.clone())
            .tree(server.authorized(request))
            .await
            .map(tonic::Response::into_inner)
    };
    assert_eq!(rows(server, 10, false), "1-1");
    assert_eq!(rows(server, 10, true), "1+1 2.2 2.3");
    assert_eq!(rows(server, 11, true), "1+1 2.2 2.3 1.4");

    let must_have = "io.deephaven.proto.backplane.grpc.TreeRequest must have field ";
    let without = |change: fn(&mut TreeRequest)| {
        let mut request = tree_request(1, 20);
        change(&mut request);
        request
    };
    refused(
        tree(without(|request| request.result_tree_table_id = None)).await,
        Code::InvalidArgument,
        &format!("{must_have}result_tree_table_id (1)"),
    );
    refused(
        tree(without(|request| request.source_table_id = None)).await,
        Code::InvalidArgument,
        &format!("{must_have}source_table_id (2)"),
    );
    refused(
        tree(without(|request| request.identifier_column.clear())).await,
        Code::InvalidArgument,
        &format!("{must_have}identifier_column (3)"),
    );
    refused(
        tree(without(|request| request.parent_identifier_column.clear())).await,
        Code::InvalidArgument,
        &format!("{must_have}parent_identifier_column (4)"),
    );
    // an export a refused call was to make is not made again
    let next = std::cell::Cell::new(30);
    let unused = || next.replace(next.get() + 1);
    // what the engine refuses, Java logs: with the orphans promoted and without
    for promote_orphans in [false, true] {
        for (identifier, parent) in [("Nope", "Parent"), ("Id", "Nope"), ("Id", "Name")] {
            let request = TreeRequest {
                identifier_column: identifier.to_string(),
                parent_identifier_column: parent.to_string(),
                promote_orphans,
                ..tree_request(1, unused())
            };
            refused(tree(request).await, Code::InvalidArgument, LOGGED);
        }
    }
    // two rows with one identifier
    server.export(
        3,
        ReferenceTable::new_static(
            nodes_definition(),
            RowSet::flat(2),
            vec![
                ints([1, 1]),
                numbers(&[None, None]),
                strings(["a", "b"]),
                ints([1, 2]),
            ],
        ),
    );
    for promote_orphans in [false, true] {
        let request = TreeRequest {
            promote_orphans,
            ..tree_request(3, unused())
        };
        refused(tree(request).await, Code::InvalidArgument, LOGGED);
    }
}

/// Behavior: `io.deephaven.extensions.barrage.util.HierarchicalTableSchemaUtil#makeSchemaPayload`
#[tokio::test]
async fn a_tree_is_opened_as_java_describes_it() {
    let server = &server().await;
    let fetched = ObjectServiceClient::new(server.channel.clone())
        .fetch_object(server.authorized(FetchObjectRequest {
            source_id: Some(TypedTicket {
                ticket: Some(export_id_to_ticket(10)),
                r#type: "HierarchicalTable".to_string(),
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.r#type, "HierarchicalTable");
    let descriptor = HierarchicalTableDescriptor::decode(fetched.data.as_ref()).unwrap();
    assert!(descriptor.is_static);
    let schema = schema_of_ipc(&descriptor.snapshot_schema);
    assert_eq!(schema.metadata(), &HashMap::new());
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type(), metadata(field)))
        .collect();
    assert_eq!(
        fields,
        [
            (
                "__DEPTH__",
                &DataType::Int32,
                vec![
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
            (
                "Id",
                &DataType::Int32,
                vec![
                    ("hierarchicalTable.isExpandByColumn", "true"),
                    ("isSortable", "true"),
                    ("treeTable.isIdentifierColumn", "true"),
                    ("treeTable.isNodeColumn", "true"),
                    ("type", "int"),
                ]
            ),
            (
                "Parent",
                &DataType::Int32,
                vec![
                    ("isSortable", "true"),
                    ("treeTable.isNodeColumn", "true"),
                    ("treeTable.isParentIdentifierColumn", "true"),
                    ("type", "int"),
                ]
            ),
            (
                "Name",
                &DataType::Utf8,
                vec![
                    ("isSortable", "true"),
                    ("treeTable.isNodeColumn", "true"),
                    ("type", "java.lang.String"),
                ]
            ),
            (
                "V",
                &DataType::Int32,
                vec![
                    ("isSortable", "true"),
                    ("treeTable.isNodeColumn", "true"),
                    ("type", "int"),
                ]
            ),
        ]
    );
}

/// The statuses are those Java 42.9 answered to the same calls, but for the update views
/// and format views, which Java does not read.
///
/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl#apply`
#[tokio::test]
async fn apply_sorts_and_filters_a_tree_and_refuses_what_java_refuses() {
    use proto::compare_condition::CompareOperation;
    use proto::sort_descriptor::SortDirection;
    let server = &server().await;
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
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
    let sorted = |result: i32, sorts: Vec<SortDescriptor>| HierarchicalTableApplyRequest {
        sorts,
        ..request(10, result)
    };
    let number = |value: i64| proto::literal::Value::LongValue(value);
    let ascending = |column: &str| sort_by(column, SortDirection::Ascending);
    // an export a refused call was to make is not made again
    let next = std::cell::Cell::new(30);
    let unused = || next.replace(next.get() + 1);

    refused(
        apply(request(10, unused())).await,
        Code::InvalidArgument,
        "No operations specified",
    );
    // a sort orders the children of every node
    apply(sorted(20, vec![ascending("V")])).await.unwrap();
    assert_eq!(rows(server, 20, true), "1+1 2.3 2.2");
    assert_eq!(
        rows(server, 10, true),
        "1+1 2.2 2.3",
        "the input is as it was"
    );
    let absolute = SortDescriptor {
        is_absolute: true,
        ..sort_by("V", SortDirection::Descending)
    };
    apply(sorted(21, vec![absolute])).await.unwrap();
    assert_eq!(rows(server, 21, true), "1+1 2.2 2.3");
    for column in ["Nope", "__DEPTH__"] {
        refused(
            apply(sorted(unused(), vec![ascending(column)])).await,
            Code::InvalidArgument,
            LOGGED,
        );
    }
    refused(
        apply(sorted(unused(), vec![sort_by("V", SortDirection::Reverse)])).await,
        Code::InvalidArgument,
        "Unsupported or unknown sort direction: REVERSE",
    );
    let absolute_name = SortDescriptor {
        is_absolute: true,
        ..ascending("Name")
    };
    refused(
        apply(sorted(unused(), vec![absolute_name])).await,
        Code::InvalidArgument,
        LOGGED,
    );

    // a filter keeps the rows that pass and their ancestors
    let small = compared("V", CompareOperation::LessThan, number(3));
    apply(HierarchicalTableApplyRequest {
        filters: vec![small],
        ..request(10, 22)
    })
    .await
    .unwrap();
    assert_eq!(rows(server, 22, true), "1+1 2.3");
    // filters first, then sorts
    let large = compared("V", CompareOperation::GreaterThan, number(1));
    apply(HierarchicalTableApplyRequest {
        filters: vec![large],
        sorts: vec![ascending("V")],
        ..request(10, 23)
    })
    .await
    .unwrap();
    assert_eq!(rows(server, 23, true), "1+1 2.3 2.2");
    // of the tree with its orphans promoted, the one row that passes is an orphan
    let smallest = compared("V", CompareOperation::LessThan, number(2));
    apply(HierarchicalTableApplyRequest {
        filters: vec![smallest],
        ..request(11, 24)
    })
    .await
    .unwrap();
    assert_eq!(rows(server, 24, true), "1.4");
    let unknown = compared("Nope", CompareOperation::LessThan, number(3));
    refused(
        apply(HierarchicalTableApplyRequest {
            filters: vec![unknown.clone()],
            ..request(10, unused())
        })
        .await,
        Code::InvalidArgument,
        LOGGED,
    );
    // the sorts are read before a filter is applied
    refused(
        apply(HierarchicalTableApplyRequest {
            filters: vec![unknown],
            sorts: vec![sort_by("V", SortDirection::Reverse)],
            ..request(10, unused())
        })
        .await,
        Code::InvalidArgument,
        "Unsupported or unknown sort direction: REVERSE",
    );

    // Java answers these as if the request had neither
    let not_read = "not ported: \
                    io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl#apply";
    refused(
        apply(HierarchicalTableApplyRequest {
            update_views: vec![update_view("W = V + 1")],
            ..request(10, unused())
        })
        .await,
        Code::Unimplemented,
        not_read,
    );
    refused(
        apply(HierarchicalTableApplyRequest {
            format_views: vec![update_view("W = V + 1")],
            sorts: vec![ascending("V")],
            ..request(10, unused())
        })
        .await,
        Code::Unimplemented,
        not_read,
    );

    // a tree of a table that restricts its sorts sorts by those columns
    let restricted = oxhaven_engine::ops::sort::restrict_sort_to(&nodes(), &["V", "Name"]).unwrap();
    server.export(4, restricted);
    service()
        .tree(server.authorized(tree_request(4, 12)))
        .await
        .unwrap();
    let of_restricted = |result: i32, column: &str| HierarchicalTableApplyRequest {
        sorts: vec![ascending(column)],
        ..request(12, result)
    };
    refused(
        apply(of_restricted(unused(), "Id")).await,
        Code::InvalidArgument,
        "Sorting attempted on restricted column(s): [Id], available column(s) for sorting are: \
         [V, Name]",
    );
    apply(of_restricted(25, "V")).await.unwrap();
    assert_eq!(rows(server, 25, true), "1+1 2.3 2.2");
    let value = server.session().get_export(25).unwrap().value().unwrap();
    let schema = snapshot_schema(hierarchical_of(&value).unwrap());
    assert_eq!(
        schema
            .metadata()
            .get("deephaven:attribute.SortableColumns")
            .map(String::as_str),
        Some("V,Name")
    );
    let sortable: Vec<&str> = schema
        .fields()
        .iter()
        .filter(|field| field.metadata().contains_key("deephaven:isSortable"))
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(sortable, ["Name", "V"]);
}

/// Behavior: `HierarchicalTableServiceGrpcImpl#view` and
/// `io.deephaven.server.hierarchicaltable.HierarchicalTableViewSubscription`
#[tokio::test]
async fn a_subscription_to_a_view_of_a_tree_follows_its_source() {
    let server = &Server::start().await;
    let input =
        append_only_input_table(server.state.graph.clone(), nodes_definition(), None).unwrap();
    let writer = input.input_table().unwrap();
    writer
        .add(node_rows(&[
            (1, None, "a", 4),
            (2, Some(1), "b", 3),
            (3, Some(1), "c", 2),
            (4, Some(9), "d", 1),
        ]))
        .unwrap();
    server.state.graph.run_cycle();
    server.export(1, input.clone());
    server.export(2, everything());
    let service = || HierarchicalTableServiceClient::new(server.channel.clone());
    service()
        .tree(server.authorized(tree_request(1, 10)))
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
            size: 3,
            viewport: Some(first_ten.clone()),
            rows: ["1|true|1|-|a|4", "2|-|2|1|b|3", "2|-|3|1|c|2"]
                .map(String::from)
                .to_vec(),
        }
    );

    // the parent of the orphan arrives, and a child of a row that had none
    writer
        .add(node_rows(&[(5, Some(2), "e", 0), (9, None, "i", 7)]))
        .unwrap();
    server.state.graph.run_cycle();
    let messages = call.collect().await;
    let [snapshot] = messages.as_slice() else {
        panic!("one snapshot, not {} messages", messages.len());
    };
    assert_eq!(
        sent(snapshot, &schema).rows,
        [
            "1|true|1|-|a|4",
            "2|true|2|1|b|3",
            "3|-|5|2|e|0",
            "2|-|3|1|c|2",
            "1|true|9|-|i|7",
            "2|-|4|9|d|1"
        ]
    );
    // a cycle that changes nothing sends nothing
    server.state.graph.run_cycle();
    assert!(call.collect().await.is_empty());

    call.close().await;
    assert_eq!(call.ended.as_deref(), Some("OK: "));
}
