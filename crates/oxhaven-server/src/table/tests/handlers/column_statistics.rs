//! The `ComputeColumnStatistics` handler, called as a client calls it. What the statistics
//! of each column type are is tested beside the handler.

use super::*;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Field;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::ColumnStatisticsRequest;
use oxhaven_testkit::{definition, ints, list_of, strings};

fn statistics(
    source: i32,
    result: i32,
    column: &str,
    limit: Option<i32>,
) -> ColumnStatisticsRequest {
    ColumnStatisticsRequest {
        result_id: ticket(result),
        source_id: by_ticket(source),
        column_name: column.to_string(),
        unique_value_limit: limit,
    }
}

async fn call(
    c: &mut Connected,
    request: ColumnStatisticsRequest,
) -> Result<ExportedTableCreationResponse, Status> {
    let response = c.client.compute_column_statistics(c.auth(request)).await;
    response.map(Response::into_inner)
}

/// Exports, as `id`, a static table of these columns.
fn export(c: &Connected, id: i32, columns: Vec<(&str, ColumnType, ArrayRef)>) {
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
    let table: Arc<dyn Table> = in_memory_table(definition(&names), &[batch]).unwrap();
    let export = c.server.session(&c.bearer).new_export(id).unwrap();
    export
        .submit(move |_| Ok(ExportValue::Table(table)))
        .unwrap();
}

/// The number of elements of the one cell of the array column `name`.
fn listed(table: &dyn Table, name: &str) -> usize {
    use arrow::array::AsArray;
    let index = table.definition().index_of(name).unwrap();
    let cells = table.column(index).chunk(&table.row_set()).unwrap();
    cells.as_list::<i32>().value(0).len()
}

#[tokio::test]
async fn column_statistics_describes_the_column_in_a_static_table_of_one_row() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;

    let response = call(&mut c, statistics(1, 2, "N", None)).await.unwrap();
    assert!(response.success);
    assert!(response.is_static);
    assert_eq!(response.size, 1);
    let table = c.table(2);
    assert_eq!(
        column_names(&*table),
        [
            "COUNT", "SIZE", "SUM", "SUM_ABS", "SUM_SQRD", "MIN", "MAX", "MIN_ABS", "MAX_ABS",
            "AVG", "AVG_ABS", "STD_DEV"
        ]
    );
    // N = 3, 2, 1, 0, -1, -2
    assert_eq!(values(&*table, "COUNT"), ints_text(&[6]));
    assert_eq!(values(&*table, "SUM"), ints_text(&[3]));
    assert_eq!(values(&*table, "SUM_ABS"), ints_text(&[9]));
    assert_eq!(values(&*table, "SUM_SQRD"), ints_text(&[19]));
    assert_eq!(values(&*table, "MIN"), ints_text(&[-2]));
    assert_eq!(values(&*table, "MAX_ABS"), ints_text(&[3]));
    assert_eq!(values(&*table, "AVG"), texts(&["0.5"]));

    let response = call(&mut c, statistics(1, 3, "S", None)).await.unwrap();
    assert!(response.is_static);
    let table = c.table(3);
    assert_eq!(
        column_names(&*table),
        [
            "COUNT",
            "SIZE",
            "UNIQUE_VALUES",
            "UNIQUE_KEYS",
            "UNIQUE_COUNTS"
        ]
    );
    assert_eq!(values(&*table, "UNIQUE_VALUES"), ints_text(&[2]));
    assert_eq!(values(&*table, "UNIQUE_KEYS"), texts(&["[a, b]"]));
    assert_eq!(values(&*table, "UNIQUE_COUNTS"), texts(&["[3, 3]"]));
}

fn ints_text(values: &[i64]) -> Vec<String> {
    super::ints(values)
}

#[tokio::test]
async fn column_statistics_lists_twenty_values_unless_asked_and_never_more_than_two_hundred() {
    let mut c = Connected::start().await;
    let names: Vec<String> = (0..250).map(|i| format!("v{i}")).collect();
    let cells = strings(names.iter().map(String::as_str));
    export(&c, 1, vec![("S", ColumnType::String, cells)]);

    for (result, limit, shown) in [
        (2, None, 20),
        (3, Some(7), 7),
        (4, Some(0), 0),
        (5, Some(200), 200),
        (6, Some(201), 200),
        (7, Some(i32::MAX), 200),
    ] {
        call(&mut c, statistics(1, result, "S", limit))
            .await
            .unwrap();
        let table = c.table(result);
        assert_eq!(values(&*table, "UNIQUE_VALUES"), ints_text(&[250]));
        assert_eq!(listed(&*table, "UNIQUE_KEYS"), shown, "limit {limit:?}");
        assert_eq!(listed(&*table, "UNIQUE_COUNTS"), shown, "limit {limit:?}");
    }
}

#[tokio::test]
async fn column_statistics_refuses_as_java_does() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;

    for (result, column) in [(2, "Nope"), (3, ""), (4, "s")] {
        let status = call(&mut c, statistics(1, result, column, None))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "Table doesn't have a column with the specified name"
        );
    }

    // Java fails on the array it allocates for the values, and only where it lists them
    let status = call(&mut c, statistics(1, 5, "S", Some(-1)))
        .await
        .unwrap_err();
    assert_internal(&status);
    let response = call(&mut c, statistics(1, 6, "N", Some(-1))).await.unwrap();
    assert!(response.success);

    // a vector is counted by its elements and listed by its text in Java
    let vector = ColumnType::vector(ColumnType::Int);
    let cells = list_of(&vector, [Some(1), Some(1)], ints([1, 2]));
    export(&c, 7, vec![("V", vector, cells)]);
    let status = call(&mut c, statistics(7, 8, "V", None)).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(
        status
            .message()
            .contains("io.deephaven.server.table.stats.ObjectChunkedStats#processChunks"),
        "{status:?}"
    );
}

#[tokio::test]
async fn column_statistics_of_a_refreshing_table_does_not_follow_it() {
    let mut c = Connected::start().await;
    super::call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;

    let response = call(&mut c, statistics(1, 2, "Timestamp", None))
        .await
        .unwrap();
    assert!(response.is_static);
    let (parent, result) = (c.table(1), c.table(2));
    assert!(!result.is_refreshing());
    let counted = values(&*result, "COUNT");
    let graph = c.server.state.graph.clone();
    let size = || locked(graph.as_ref(), || parent.row_set().size());
    let before = size();
    assert_ne!(counted, ints_text(&[0]));

    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    assert!(size() > before);
    assert_eq!(values(&*result, "COUNT"), counted);
    assert_eq!(values(&*result, "SIZE"), counted);
}
