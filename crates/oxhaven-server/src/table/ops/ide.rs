//! What the web IDE asks of a table besides the relational operations: `RunChartDownsample`,
//! the rows a line chart needs, and `SeekRow`, the grid's "go to row". The other two,
//! `ComputeColumnStatistics` and `ApplyPreviewColumns`, have files of their own.

use super::prelude::*;
use crate::schema::java_type_name;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::ops::ide::downsample::{run_chart_downsample, Downsample};
use oxhaven_engine::ops::ide::seek::{seek_row as seek, Seek, SeekValue};
use oxhaven_proto::grpc::literal::Value;
use oxhaven_proto::grpc::{Literal, RunChartDownsampleRequest, SeekRowRequest};

/// Behavior: `io.deephaven.server.table.ops.RunChartDownsampleGrpcImpl`
pub struct RunChartDownsample;

impl TableOp for RunChartDownsample {
    type Request = RunChartDownsampleRequest;

    fn result_id(request: &RunChartDownsampleRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &RunChartDownsampleRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Every refusal is the engine's, so the client is told `INVALID_ARGUMENT` and an
    /// error id whatever is wrong with the request.
    fn create(
        _context: &OpContext,
        request: &RunChartDownsampleRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        // an end of the range that is not set is 0
        let zoom_range = request.zoom_range.as_ref().map(|range| {
            (
                range.min_date_nanos.unwrap_or_default(),
                range.max_date_nanos.unwrap_or_default(),
            )
        });
        let downsample = Downsample {
            pixel_count: request.pixel_count,
            zoom_range,
            x_column: request.x_column_name.clone(),
            y_columns: request.y_column_names.clone(),
        };
        Ok(run_chart_downsample(&sources[0], &downsample)?)
    }
}

/// The class of a column as Java prints it in a message: `long`, `class java.lang.String`.
fn class_text(column_type: &ColumnType) -> String {
    let name = java_type_name(column_type);
    match column_type {
        ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double => name.into_owned(),
        _ => format!("class {name}"),
    }
}

/// The value a `SeekRow` request looks for in a column of `column_type`: the literal as
/// the Java object of the column's class, with Java's narrowing of a number that does not
/// fit. A literal of a kind the column cannot be compared with is `INVALID_ARGUMENT`,
/// except that a boolean is taken for any column, where it then equals nothing.
///
/// Behavior: `io.deephaven.server.table.ops.TableServiceGrpcImpl#getSeekValue`
fn seek_value(literal: Option<&Literal>, column_type: &ColumnType) -> Result<SeekValue, Status> {
    let refused = |kind: &str| {
        Status::invalid_argument(format!(
            "Invalid {kind} type for seek: {}",
            class_text(column_type)
        ))
    };
    Ok(match literal.and_then(|literal| literal.value.as_ref()) {
        Some(Value::StringValue(text)) => match column_type {
            ColumnType::String | ColumnType::Char => SeekValue::String(text.clone()),
            _ => return Err(refused("String")),
        },
        Some(Value::NanoTimeValue(nanos)) => match column_type {
            ColumnType::Instant => SeekValue::Instant(*nanos),
            _ => return Err(refused("date")),
        },
        Some(Value::LongValue(value)) => match column_type {
            ColumnType::Byte => SeekValue::Byte(*value as i8),
            ColumnType::Short => SeekValue::Short(*value as i16),
            ColumnType::Int => SeekValue::Int(*value as i32),
            ColumnType::Long => SeekValue::Long(*value),
            ColumnType::Float => SeekValue::Float(*value as f32),
            ColumnType::Double => SeekValue::Double(*value as f64),
            _ => return Err(refused("column")),
        },
        // Java narrows a double to an int first and that to a byte or a short
        Some(Value::DoubleValue(value)) => match column_type {
            ColumnType::Byte => SeekValue::Byte(*value as i32 as i8),
            ColumnType::Short => SeekValue::Short(*value as i32 as i16),
            ColumnType::Int => SeekValue::Int(*value as i32),
            ColumnType::Long => SeekValue::Long(*value as i64),
            ColumnType::Float => SeekValue::Float(*value as f32),
            ColumnType::Double => SeekValue::Double(*value),
            _ => return Err(refused("column")),
        },
        Some(Value::BoolValue(value)) => SeekValue::Boolean(*value),
        None => return Err(refused("column")),
    })
}

/// The answer to a `SeekRow` request for `table`: the position of the row found, or `-1`.
/// A refreshing table is read under its update graph's lock.
///
/// A column that does not exist is, as every exception of Java's search, an internal
/// failure: `INVALID_ARGUMENT` with an error id.
///
/// Behavior: the work `TableServiceGrpcImpl#seekRow` submits
pub(in crate::table) fn seek_row(
    table: &Arc<dyn Table>,
    request: &SeekRowRequest,
) -> Result<i64, ExportError> {
    let definition = table.definition();
    let Some(column) = definition.index_of(&request.column_name) else {
        // Java reads the type of a column definition that is null
        return Err(ExportError::Internal(
            "java.lang.NullPointerException".to_string(),
        ));
    };
    let column_type = &definition.columns()[column].column_type;
    let value = seek_value(request.seek_value.as_ref(), column_type)?;
    let seek_for = Seek {
        starting_row: request.starting_row,
        column: request.column_name.clone(),
        value,
        insensitive: request.insensitive,
        contains: request.contains,
        backward: request.is_backward,
    };
    let found = match table.update_graph() {
        Some(graph) if table.is_refreshing() => {
            locked(graph.as_ref(), || seek(table.as_ref(), &seek_for))
        }
        _ => seek(table.as_ref(), &seek_for),
    };
    Ok(found?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn literal(value: Value) -> Literal {
        Literal { value: Some(value) }
    }

    #[test]
    fn a_literal_becomes_the_object_of_the_columns_class() {
        let value = |value, column_type| seek_value(Some(&literal(value)), &column_type).unwrap();
        assert_eq!(
            value(Value::LongValue(263), ColumnType::Byte),
            SeekValue::Byte(7)
        );
        assert_eq!(
            value(Value::LongValue((1 << 32) + 7), ColumnType::Int),
            SeekValue::Int(7)
        );
        assert_eq!(
            value(Value::LongValue(3), ColumnType::Double),
            SeekValue::Double(3.0)
        );
        assert_eq!(
            value(Value::DoubleValue(7.9), ColumnType::Long),
            SeekValue::Long(7)
        );
        // (byte) 300.7 is (byte) 300
        assert_eq!(
            value(Value::DoubleValue(300.7), ColumnType::Byte),
            SeekValue::Byte(44)
        );
        assert_eq!(
            value(Value::DoubleValue(1e30), ColumnType::Short),
            SeekValue::Short(-1)
        );
        assert_eq!(
            value(Value::DoubleValue(3.5), ColumnType::Float),
            SeekValue::Float(3.5)
        );
        assert_eq!(
            value(Value::StringValue("H".to_string()), ColumnType::Char),
            SeekValue::String("H".to_string())
        );
        assert_eq!(
            value(Value::NanoTimeValue(5), ColumnType::Instant),
            SeekValue::Instant(5)
        );
        // a boolean is taken for any column
        assert_eq!(
            value(Value::BoolValue(true), ColumnType::String),
            SeekValue::Boolean(true)
        );
    }

    #[test]
    fn a_literal_the_column_cannot_be_compared_with_is_invalid() {
        let refused = |literal: Option<Literal>, column_type| {
            let status = seek_value(literal.as_ref(), &column_type).unwrap_err();
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            status.message().to_string()
        };
        assert_eq!(
            refused(Some(literal(Value::LongValue(7))), ColumnType::String),
            "Invalid column type for seek: class java.lang.String"
        );
        assert_eq!(
            refused(Some(literal(Value::LongValue(72))), ColumnType::Char),
            "Invalid column type for seek: char"
        );
        assert_eq!(
            refused(
                Some(literal(Value::StringValue("true".to_string()))),
                ColumnType::Boolean
            ),
            "Invalid String type for seek: class java.lang.Boolean"
        );
        assert_eq!(
            refused(Some(literal(Value::NanoTimeValue(1))), ColumnType::Long),
            "Invalid date type for seek: long"
        );
        assert_eq!(
            refused(Some(literal(Value::DoubleValue(1.0))), ColumnType::Instant),
            "Invalid column type for seek: class java.time.Instant"
        );
        // no literal, and a literal without a value
        assert_eq!(
            refused(None, ColumnType::Long),
            "Invalid column type for seek: long"
        );
        assert_eq!(
            refused(Some(Literal { value: None }), ColumnType::Long),
            "Invalid column type for seek: long"
        );
    }
}
