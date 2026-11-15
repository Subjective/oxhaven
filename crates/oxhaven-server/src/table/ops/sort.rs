//! `Sort`: a table ordered by columns, reversed, or both.

use super::prelude::*;
use oxhaven_engine::ops::select::{drop_columns, update_view};
use oxhaven_engine::ops::sort::{reverse, sort, SortColumn};
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::{SortDescriptor, SortTableRequest};

/// Behavior: `io.deephaven.server.table.ops.SortTableGrpcImpl`
pub struct Handler;

fn direction(descriptor: &SortDescriptor) -> Option<SortDirection> {
    SortDirection::try_from(descriptor.direction).ok()
}

/// The name of the column Java adds for a sort by the absolute value of `column`.
///
/// Behavior: `io.deephaven.engine.table.impl.AbsoluteSortColumnConventions#baseColumnNameToAbsoluteName`
fn absolute_name(column: &str) -> String {
    format!("__ABS__{column}")
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
    ///
    /// The engine has no sort by an absolute value. For every descriptor marked absolute,
    /// whatever its direction, a column `__ABS__<name>` with `abs(<name>)` is added with
    /// `updateView`; the table is sorted by that column in place of the one named, and
    /// reversed; and the added columns are dropped. So the result of such a request is the
    /// result of `dropColumns`, with the attributes that keeps, a marked column that has
    /// no `abs` fails the request even when nothing is sorted by it, and a column of the
    /// source that has the name of an added one is lost.
    ///
    /// Java then gives the result the key column attributes of the source, which
    /// `dropColumns` does not keep. No table has them here.
    fn create(
        context: &OpContext,
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
        // Java adds them last descriptor first.
        let absolute: Vec<&SortDescriptor> = request
            .sorts
            .iter()
            .rev()
            .filter(|sort| sort.is_absolute)
            .collect();
        let added: Vec<String> = absolute
            .iter()
            .map(|sort| absolute_name(&sort.column_name))
            .collect();
        let mut result = parent.clone();
        if !absolute.is_empty() {
            let formulas: Vec<String> = absolute
                .iter()
                .map(|sort| format!("{} = abs({})", absolute_name(&sort.column_name), sort.column_name))
                .collect();
            result = update_view(&result, &formulas, context.compiler.as_ref())?;
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
            let name = if descriptor.is_absolute {
                absolute_name(&descriptor.column_name)
            } else {
                descriptor.column_name.clone()
            };
            columns.push(if descending != reversed {
                SortColumn::descending(name)
            } else {
                SortColumn::ascending(name)
            });
        }

        if !columns.is_empty() {
            result = sort(&result, &columns)?;
        }
        if reversed {
            result = reverse(&result)?;
        }
        if !added.is_empty() {
            result = drop_columns(&result, &added)?;
        }
        Ok(result)
    }
}
