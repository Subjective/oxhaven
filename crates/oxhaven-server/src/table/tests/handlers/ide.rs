//! The `RunChartDownsample` handler and the `SeekRow` call, called as a client calls them.
//! Which rows a chart needs and which row a seek finds is tested in the engine.

use super::*;
use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::Field;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::ops::ide::downsample::{run_chart_downsample, Downsample};
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::literal::Value;
use oxhaven_proto::grpc::run_chart_downsample_request::ZoomRange;
use oxhaven_proto::grpc::{Literal, RunChartDownsampleRequest, SeekRowRequest};
use oxhaven_testkit::definition;

const T0: i64 = 1_700_000_000_000_000_000;

/// Exports, as `id`, a static table of 3000 rows: `X` = 0 to 2999, `T` that many seconds
/// after `T0`, and `Y` = 0, 5, 3, 1, 6, 4, 2 and so on.
fn export(c: &Connected, id: i32) -> Arc<dyn Table> {
    let times: TimestampNanosecondArray = (0..3000).map(|x| Some(T0 + x * 1_000_000_000)).collect();
    let columns: Vec<(&str, ColumnType, ArrayRef)> = vec![
        (
            "X",
            ColumnType::Long,
            Arc::new(Int64Array::from_iter_values(0..3000)),
        ),
        (
            "T",
            ColumnType::Instant,
            Arc::new(times.with_timezone("UTC")),
        ),
        (
            "Y",
            ColumnType::Int,
            Arc::new(Int32Array::from_iter_values((0..3000).map(|x| x * 5 % 7))),
        ),
    ];
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
    let exported = table.clone();
    export
        .submit(move |_| Ok(ExportValue::Table(exported)))
        .unwrap();
    table
}

fn downsample(source: i32, result: i32, x: &str, pixels: i32) -> RunChartDownsampleRequest {
    RunChartDownsampleRequest {
        result_id: ticket(result),
        source_id: by_ticket(source),
        pixel_count: pixels,
        zoom_range: None,
        x_column_name: x.to_string(),
        y_column_names: vec!["Y".to_string()],
    }
}

async fn chart(
    c: &mut Connected,
    request: RunChartDownsampleRequest,
) -> Result<ExportedTableCreationResponse, Status> {
    let response = c.client.run_chart_downsample(c.auth(request)).await;
    response.map(Response::into_inner)
}

/// The values of `X` in the rows the engine picks for the same request.
fn picked(table: &Arc<dyn Table>, x: &str, pixels: i32, zoom: Option<(i64, i64)>) -> Vec<String> {
    let request = Downsample {
        pixel_count: pixels,
        zoom_range: zoom,
        x_column: x.to_string(),
        y_columns: vec!["Y".to_string()],
    };
    values(&*run_chart_downsample(table, &request).unwrap(), "X")
}

#[tokio::test]
async fn run_chart_downsample_exports_the_rows_a_chart_needs() {
    let mut c = Connected::start().await;
    let table = export(&c, 1);

    let response = chart(&mut c, downsample(1, 2, "X", 10)).await.unwrap();
    assert!(response.success);
    assert!(response.is_static);
    let rows = values(&*c.table(2), "X");
    // 500 buckets of six rows, of which at most the first, the last, a 6 and a 0
    assert_eq!(response.size, rows.len() as i64);
    assert!((1000..=2000).contains(&rows.len()), "{}", rows.len());
    assert_eq!(rows, picked(&table, "X", 10, None));
    assert_eq!(column_names(&*c.table(2)), ["X", "T", "Y"]);

    // the pixel count and the X column are the request's
    let response = chart(&mut c, downsample(1, 3, "T", 900)).await.unwrap();
    assert!(response.success);
    let wide = values(&*c.table(3), "X");
    assert_eq!(wide, picked(&table, "T", 900, None));
    assert_ne!(wide, rows);

    // and in a batch
    let (responses, status) = c
        .batch(vec![Op::RunChartDownsample(downsample(1, 4, "X", 10))])
        .await
        .unwrap();
    assert!(status.is_none(), "{status:?}");
    assert!(responses[0].success);
    assert_eq!(values(&*c.table(4), "X"), rows);
}

#[tokio::test]
async fn an_end_of_a_zoom_range_that_is_not_set_is_zero() {
    let mut c = Connected::start().await;
    let table = export(&c, 1);
    let zoomed = |result: i32, min: Option<i64>, max: Option<i64>| RunChartDownsampleRequest {
        zoom_range: Some(ZoomRange {
            min_date_nanos: min,
            max_date_nanos: max,
        }),
        ..downsample(1, result, "X", 10)
    };

    let response = chart(&mut c, zoomed(2, Some(100), Some(700)))
        .await
        .unwrap();
    assert!(response.success);
    let rows = values(&*c.table(2), "X");
    assert_eq!(rows, picked(&table, "X", 10, Some((100, 700))));
    assert_ne!(rows, picked(&table, "X", 10, None));

    let response = chart(&mut c, zoomed(3, None, Some(700))).await.unwrap();
    assert!(response.success);
    let from_zero = values(&*c.table(3), "X");
    assert_eq!(from_zero, picked(&table, "X", 10, Some((0, 700))));
    assert_ne!(from_zero, rows);

    // a range from 0 to 0 is one Java refuses
    let refused = chart(&mut c, zoomed(4, None, None)).await.unwrap_err();
    assert_internal(&refused);
    let refused = chart(&mut c, zoomed(5, Some(100), None)).await.unwrap_err();
    assert_internal(&refused);
}

#[tokio::test]
async fn a_downsample_the_engine_refuses_is_an_internal_error_or_not_ported() {
    let mut c = Connected::start().await;
    export(&c, 1);
    for (result, request) in [
        downsample(1, 0, "X", 0),
        downsample(1, 0, "Q", 10),
        downsample(1, 0, "Y", 10),
        RunChartDownsampleRequest {
            y_column_names: Vec::new(),
            ..downsample(1, 0, "X", 10)
        },
    ]
    .into_iter()
    .enumerate()
    {
        let request = RunChartDownsampleRequest {
            result_id: ticket(10 + result as i32),
            ..request
        };
        assert_internal(&chart(&mut c, request).await.unwrap_err());
    }

    // a sorted table reads its columns through a redirection, and of such a `long` column
    // it is not known whether Java takes it
    let sorted = sorted(2, vec![sort_by(SortDirection::Descending, "X")]);
    let Op::Sort(mut sort) = sorted else {
        unreachable!()
    };
    sort.source_id = by_ticket(1);
    assert!(call(&mut c, Op::Sort(sort)).await.unwrap().success);
    let refused = chart(&mut c, downsample(2, 3, "X", 10)).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "not ported: io.deephaven.engine.table.ColumnSource#allowsReinterpret: downsampling by \
         the long column X, which is not held in memory"
    );
    // an Instant column is taken wherever it is: here its values fall, which Java refuses
    assert_internal(&chart(&mut c, downsample(2, 4, "T", 10)).await.unwrap_err());
    let zoomed = RunChartDownsampleRequest {
        zoom_range: Some(ZoomRange {
            min_date_nanos: Some(T0),
            max_date_nanos: Some(T0 + 1_000_000_000_000),
        }),
        ..downsample(2, 5, "T", 10)
    };
    assert!(chart(&mut c, zoomed).await.unwrap().success);
}

fn seek(source: i32, column: &str, value: Value, starting_row: i64) -> SeekRowRequest {
    SeekRowRequest {
        source_id: ticket(source),
        starting_row,
        column_name: column.to_string(),
        seek_value: Some(Literal { value: Some(value) }),
        insensitive: false,
        contains: false,
        is_backward: false,
    }
}

async fn found(c: &mut Connected, request: SeekRowRequest) -> Result<i64, Status> {
    let response = c.client.seek_row(c.auth(request)).await;
    response.map(|response| response.into_inner().result_row)
}

#[tokio::test]
async fn seek_row_answers_the_position_of_the_next_row_with_the_value() {
    let mut c = Connected::start().await;
    export(&c, 1);
    // Y is 6 in the rows 4, 11, 18 and so on
    assert_eq!(
        found(&mut c, seek(1, "Y", Value::LongValue(6), 0))
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        found(&mut c, seek(1, "Y", Value::LongValue(6), 4))
            .await
            .unwrap(),
        11
    );
    let backward = SeekRowRequest {
        is_backward: true,
        ..seek(1, "Y", Value::LongValue(6), 11)
    };
    assert_eq!(found(&mut c, backward).await.unwrap(), 4);
    assert_eq!(
        found(&mut c, seek(1, "Y", Value::LongValue(7), 0))
            .await
            .unwrap(),
        -1
    );
    // the literal becomes a value of the column's class
    assert_eq!(
        found(&mut c, seek(1, "Y", Value::DoubleValue(6.9), 0))
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        found(&mut c, seek(1, "X", Value::LongValue(2999), 0))
            .await
            .unwrap(),
        2999
    );
    let time = Value::NanoTimeValue(T0 + 17_000_000_000);
    assert_eq!(found(&mut c, seek(1, "T", time, 0)).await.unwrap(), 17);

    // of a sorted table, too
    let Op::Sort(mut sort) = sorted(2, vec![sort_by(SortDirection::Ascending, "Y")]) else {
        unreachable!()
    };
    sort.source_id = by_ticket(1);
    assert!(call(&mut c, Op::Sort(sort)).await.unwrap().success);
    // 429 rows each of 0, 1 and 3 and 428 of 2, and so the first 4 is the row 1715
    assert_eq!(
        found(&mut c, seek(2, "Y", Value::LongValue(4), 0))
            .await
            .unwrap(),
        1715
    );
}

#[tokio::test]
async fn seek_row_refuses_as_java_does() {
    let mut c = Connected::start().await;
    export(&c, 1);

    let refused = found(&mut c, seek(1, "X", Value::StringValue("7".to_string()), 0)).await;
    let refused = refused.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(refused.message(), "Invalid String type for seek: long");

    let without_value = SeekRowRequest {
        seek_value: None,
        ..seek(1, "T", Value::LongValue(0), 0)
    };
    let refused = found(&mut c, without_value).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "Invalid column type for seek: class java.time.Instant"
    );

    // a column the table does not have, and a search Java ends with an exception
    let missing = found(&mut c, seek(1, "Q", Value::LongValue(7), 0)).await;
    assert_internal(&missing.unwrap_err());
    let contains = SeekRowRequest {
        contains: true,
        ..seek(1, "X", Value::LongValue(7), 0)
    };
    assert_internal(&found(&mut c, contains).await.unwrap_err());

    let without_source = SeekRowRequest {
        source_id: None,
        ..seek(1, "X", Value::LongValue(7), 0)
    };
    let refused = found(&mut c, without_source).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "No consoleId supplied");
}
