//! The `Ungroup` handler, called as a client calls it. No operation of the server makes a
//! vector or array column yet, so the sources with such columns are engine tables that the
//! tests export into the session.

use super::*;
use arrow::array::{ArrayRef, Float64Array, Int32Array, RecordBatch};
use arrow::datatypes::Field;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::UngroupRequest;
use oxhaven_testkit::{definition, list_of, ScriptedTable, TableChange};

fn ungrouped(source: i32, result: i32, null_fill: bool, columns: &[&str]) -> UngroupRequest {
    UngroupRequest {
        result_id: ticket(result),
        source_id: by_ticket(source),
        null_fill,
        columns_to_ungroup: texts(columns),
    }
}

async fn call(
    c: &mut Connected,
    request: UngroupRequest,
) -> Result<ExportedTableCreationResponse, Status> {
    let response = c.client.ungroup(c.auth(request)).await;
    response.map(Response::into_inner)
}

/// Makes `table` export `id` of the session.
fn export(c: &Connected, id: i32, table: Arc<dyn Table>) {
    let session = c.server.session(&c.bearer);
    let export = session.new_export(id).unwrap();
    export
        .submit(move |_| Ok(ExportValue::Table(table)))
        .unwrap();
}

fn vector() -> ColumnType {
    ColumnType::vector(ColumnType::Int)
}

fn array() -> ColumnType {
    ColumnType::array(ColumnType::Double)
}

fn doubles(values: &[Option<f64>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<Float64Array>())
}

/// `K` = a, b, c, d, the vector column `A` = `[1, 2]`, null, `[]`, `[3, null, 5]` and the
/// array column `B` = `[1.5]`, null, `[]`, `[3.0, null, 5.0]`: what the Java server was
/// sent for the raw requests this file follows, where `A` was an array too.
fn lists() -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let a: Int32Array = [Some(1), Some(2), Some(3), None, Some(5)]
        .into_iter()
        .collect();
    let a: ArrayRef = Arc::new(a);
    let b = doubles(&[Some(1.5), Some(3.0), None, Some(5.0)]);
    vec![
        (
            "K",
            ColumnType::String,
            oxhaven_testkit::strings(["a", "b", "c", "d"]),
        ),
        (
            "A",
            vector(),
            list_of(&vector(), [Some(2), None, Some(0), Some(3)], a),
        ),
        (
            "B",
            array(),
            list_of(&array(), [Some(1), None, Some(0), Some(3)], b),
        ),
    ]
}

/// Exports the static table of [`lists`] as `id`.
fn export_lists(c: &Connected, id: i32) {
    let columns = lists();
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns.iter().map(|(_, _, cells)| cells.clone()).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    export(
        c,
        id,
        in_memory_table(definition(&names), &[batch]).unwrap(),
    );
}

fn types(table: &dyn Table) -> Vec<ColumnType> {
    let columns = table.definition().columns().iter();
    columns.map(|column| column.column_type.clone()).collect()
}

#[tokio::test]
async fn ungroup_gives_a_row_for_every_element_of_the_named_or_of_all_array_columns() {
    let mut c = Connected::start().await;
    export_lists(&c, 1);

    // every array column, the shorter arrays filled with nulls
    let response = call(&mut c, ungrouped(1, 2, true, &[])).await.unwrap();
    assert!(response.success);
    assert_eq!(response.size, 5);
    assert!(response.is_static);
    let table = c.table(2);
    assert_eq!(column_names(&*table), ["K", "A", "B"]);
    assert_eq!(
        types(&*table),
        [ColumnType::String, ColumnType::Int, ColumnType::Double]
    );
    assert_eq!(values(&*table, "K"), texts(&["a", "a", "d", "d", "d"]));
    assert_eq!(values(&*table, "A"), texts(&["1", "2", "3", "", "5"]));
    assert_eq!(values(&*table, "B"), texts(&["1.5", "", "3.0", "", "5.0"]));

    // one column by name: the other stays an array
    let response = call(&mut c, ungrouped(1, 3, false, &["A"])).await.unwrap();
    assert_eq!(response.size, 5);
    let table = c.table(3);
    assert_eq!(
        types(&*table),
        [ColumnType::String, ColumnType::Int, array()]
    );
    assert_eq!(values(&*table, "A"), texts(&["1", "2", "3", "", "5"]));
    assert_eq!(
        values(&*table, "B"),
        texts(&[
            "[1.5]",
            "[1.5]",
            "[3.0, , 5.0]",
            "[3.0, , 5.0]",
            "[3.0, , 5.0]"
        ])
    );

    // a column named twice is ungrouped once, as Java answers
    for (result, null_fill) in [(4, false), (5, true)] {
        let request = ungrouped(1, result, null_fill, &["A", "A"]);
        let response = call(&mut c, request).await.unwrap();
        assert_eq!(response.size, 5);
        assert_eq!(types(&*c.table(result)), types(&*table));
        assert_eq!(values(&*c.table(result), "A"), values(&*table, "A"));
    }

    // in a batch, from a table of the batch and without a result ticket
    let ops = vec![
        Op::Flatten(proto::FlattenRequest {
            result_id: None,
            source_id: by_ticket(1),
        }),
        Op::Ungroup(UngroupRequest {
            result_id: None,
            source_id: by_offset(0),
            null_fill: false,
            columns_to_ungroup: texts(&["B"]),
        }),
        Op::Ungroup(UngroupRequest {
            source_id: by_offset(1),
            ..ungrouped(0, 6, true, &[])
        }),
    ];
    let (responses, status) = c.batch(ops).await.unwrap();
    assert!(status.is_none(), "{status:?}");
    assert!(responses.iter().all(|response| response.success));
    let sizes: Vec<i64> = responses.iter().map(|response| response.size).collect();
    // `B` has four elements, and each of those rows the two or three elements of `A`.
    assert_eq!(sizes, [4, 4, 11]);
    let table = c.table(6);
    assert_eq!(
        types(&*table),
        [ColumnType::String, ColumnType::Int, ColumnType::Double]
    );
    assert_eq!(
        values(&*table, "A"),
        texts(&["1", "2", "3", "", "5", "3", "", "5", "3", "", "5"])
    );
    assert_eq!(
        values(&*table, "B"),
        texts(&["1.5", "1.5", "3.0", "3.0", "3.0", "", "", "", "5.0", "5.0", "5.0"])
    );
}

#[tokio::test]
async fn ungroup_of_a_table_without_arrays_is_the_table() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    for (result, null_fill) in [(2, false), (3, true)] {
        let response = call(&mut c, ungrouped(1, result, null_fill, &[]))
            .await
            .unwrap();
        assert!(response.success && response.is_static);
        assert_eq!(response.size, 6);
        let table = c.table(result);
        assert_eq!(column_names(&*table), ["I", "N", "S"]);
        assert_eq!(values(&*table, "I"), ints(&[0, 1, 2, 3, 4, 5]));
    }
}

#[tokio::test]
async fn ungroup_refuses_as_java_does() {
    let mut c = Connected::start().await;
    export_lists(&c, 1);
    numbers(&mut c, 2).await;

    // Whatever Java's `ungroup` or `ColumnName.of` throws is an exception: the arrays of
    // a row that differ in size, a column that is not an array, alone or after one that
    // is, a column the table does not have, and a string that cannot name a column.
    let refused: [(i32, &[&str]); 9] = [
        (1, &[]),
        (1, &["A", "B"]),
        (1, &["K"]),
        (1, &["A", "K"]),
        (2, &["N"]),
        (2, &["Nope"]),
        (2, &["1 bad"]),
        (2, &[""]),
        (1, &[" A "]),
    ];
    for (index, (source, columns)) in refused.into_iter().enumerate() {
        let result = 10 + i32::try_from(index).unwrap();
        let status = call(&mut c, ungrouped(source, result, false, columns)).await;
        match status {
            Ok(response) => panic!("{columns:?} of {source}: {response:?}"),
            Err(status) => assert_internal(&status),
        }
        // and in a batch, which answers for the operation and then ends with the failure
        let request = ungrouped(source, result + 100, false, columns);
        let (responses, status) = c.batch(vec![Op::Ungroup(request)]).await.unwrap();
        assert_eq!(responses.len(), 1, "{columns:?}: {responses:?}");
        assert!(!responses[0].success, "{columns:?}: {responses:?}");
        let info = &responses[0].error_info;
        let cause = " cause: INVALID_ARGUMENT: Details Logged w/ID '";
        assert!(info.contains(cause), "{columns:?}: {info}");
        let status = status.unwrap_or_else(|| panic!("{columns:?} of {source} in a batch"));
        assert_internal(&status);
    }
    // With null fill the sizes may differ; the other refusals stay.
    assert!(call(&mut c, ungrouped(1, 30, true, &["A", "B"]))
        .await
        .is_ok());
    let status = call(&mut c, ungrouped(1, 31, true, &["A", "K"])).await;
    assert_internal(&status.unwrap_err());

    // What every one-shot operation needs.
    let request = UngroupRequest {
        result_id: None,
        ..ungrouped(1, 0, false, &["Nope"])
    };
    let status = call(&mut c, request).await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(status.message(), "No result ticket supplied");
    let request = UngroupRequest {
        source_id: None,
        ..ungrouped(1, 40, false, &[])
    };
    let status = call(&mut c, request).await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "One-shot operations must use ticket references"
    );
}

#[tokio::test]
async fn ungroup_of_a_refreshing_table_follows_it() {
    let mut c = Connected::start().await;
    let graph: Arc<dyn UpdateGraph> = c.server.state.graph.clone();
    let columns = lists();
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let cells = columns.iter().map(|(_, _, cells)| cells.clone()).collect();
    let rows = RowSet::from_keys([10, 20, 30, 40]);
    let source = ScriptedTable::new(graph, definition(&names), rows, cells);
    export(&c, 1, source.table());

    let response = call(&mut c, ungrouped(1, 2, true, &[])).await.unwrap();
    assert!(!response.is_static);
    assert_eq!(response.size, 5);
    let table = c.table(2);
    assert!(table.is_refreshing());

    // Row `b` gets arrays, row `a` goes, and a row comes.
    let row = |k: &str, a: &[i32], b: &[f64]| -> Vec<ArrayRef> {
        let b: Vec<Option<f64>> = b.iter().copied().map(Some).collect();
        vec![
            oxhaven_testkit::strings([k]),
            list_of(
                &vector(),
                [Some(a.len())],
                oxhaven_testkit::ints(a.iter().copied()),
            ),
            list_of(&array(), [Some(b.len())], doubles(&b)),
        ]
    };
    let modified = row("b", &[7, 8], &[7.5]);
    let change = TableChange::new()
        .remove([10])
        .add([50], row("e", &[9], &[9.5, 9.75]))
        .modify([20], [(1, modified[1].clone()), (2, modified[2].clone())]);
    source.push(change);
    c.cycle().await;
    assert!(!table.is_failed());
    assert_eq!(
        values(&*table, "K"),
        texts(&["b", "b", "d", "d", "d", "e", "e"])
    );
    assert_eq!(
        values(&*table, "A"),
        texts(&["7", "8", "3", "", "5", "9", ""])
    );
    assert_eq!(
        values(&*table, "B"),
        texts(&["7.5", "", "3.0", "", "5.0", "9.5", "9.75"])
    );
}
