//! `Sort`: a table ordered by columns, reversed, or both.

use super::prelude::*;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::ops::sort::{reverse, sort, SortColumn};
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::{SortDescriptor, SortTableRequest};

/// Behavior: `io.deephaven.server.table.ops.SortTableGrpcImpl`
pub struct Handler;

fn direction(descriptor: &SortDescriptor) -> Option<SortDirection> {
    SortDirection::try_from(descriptor.direction).ok()
}

/// Java computes `abs(Column)` for every descriptor marked absolute before it looks at the
/// directions, so the mark fails a `REVERSE` descriptor whose column has no `abs`, though
/// such a descriptor sorts by nothing. The engine makes this check for the columns it
/// sorts by; this makes it for the ones it is not given.
///
/// Behavior: `SortTableGrpcImpl#create`, the `updateView` of
/// `io.deephaven.engine.table.impl.AbsoluteSortColumnConventions#makeSelectable`
fn check_unused_absolute(parent: &dyn Table, column: &str) -> Result<(), ExportError> {
    let definition = parent.definition();
    let Some(index) = definition.index_of(column) else {
        return Err(ExportError::Internal(format!(
            "abs({column}): no such column"
        )));
    };
    match &definition.columns()[index].column_type {
        ColumnType::Byte
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double => Ok(()),
        ColumnType::Boolean | ColumnType::String | ColumnType::Instant => Err(
            ExportError::Internal(format!("abs({column}): no abs for the column's type")),
        ),
        other => Err(not_ported(
            "io.deephaven.engine.table.impl.lang.QueryLanguageParser#visit(MethodCallExpr)",
            format!("abs of a {other:?} column for an absolute sort"),
        )
        .into()),
    }
}

impl TableOp for Handler {
    type Request = SortTableRequest;

    fn result_id(request: &SortTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &SortTableRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// The descriptors are read in order. `REVERSE` names no column: it reverses the
    /// table, and each one inverts the direction of the descriptors after it, so that the
    /// reversal at the end leaves those columns in the direction the client asked for.
    fn create(
        _context: &OpContext,
        request: &SortTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let parent = &sources[0];
        // Java skips an empty name here and fails on it below, as an internal error.
        require_columns(
            parent.definition(),
            request
                .sorts
                .iter()
                .filter(|sort| direction(sort) != Some(SortDirection::Reverse))
                .map(|sort| &sort.column_name)
                .filter(|name| !name.is_empty()),
        )?;
        for unused in request.sorts.iter().filter(|sort| {
            sort.is_absolute && !matches!(direction(sort), Some(SortDirection::Ascending | SortDirection::Descending))
        }) {
            check_unused_absolute(parent.as_ref(), &unused.column_name)?;
        }

        let mut columns = Vec::new();
        let mut reversed = false;
        for descriptor in &request.sorts {
            let descending = match direction(descriptor) {
                Some(SortDirection::Reverse) => {
                    reversed = !reversed;
                    continue;
                }
                Some(SortDirection::Descending) => true,
                Some(SortDirection::Ascending) => false,
                // Java's message prints a variable that is still zero.
                Some(SortDirection::Unknown) | None => {
                    return Err(
                        Status::invalid_argument("Unexpected sort direction: 0").into()
                    );
                }
            };
            let name = descriptor.column_name.as_str();
            let column = if descending != reversed {
                SortColumn::descending(name)
            } else {
                SortColumn::ascending(name)
            };
            columns.push(if descriptor.is_absolute {
                column.absolute()
            } else {
                column
            });
        }

        let mut result = parent.clone();
        if !columns.is_empty() {
            result = sort(&result, &columns)?;
        }
        if reversed {
            result = reverse(&result)?;
        }
        Ok(result)
    }
}
