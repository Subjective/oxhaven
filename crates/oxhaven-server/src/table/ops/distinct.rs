//! `SelectDistinct`: the distinct combinations of values of some columns, in the order in
//! which they first appear.

use super::prelude::*;
use oxhaven_engine::ops::distinct::{select_distinct, DistinctError};
use oxhaven_proto::grpc::SelectDistinctRequest;

/// The handler checks the columns itself, so the engine's `NoSuchColumn` is not expected;
/// it gets the handler's status all the same. Anything else is an exception out of the
/// engine.
///
/// Behavior: `io.deephaven.server.table.ops.SelectDistinctGrpcImpl#create`
impl From<DistinctError> for ExportError {
    fn from(error: DistinctError) -> Self {
        match error {
            DistinctError::NoSuchColumn(_) => {
                ExportError::Status(Status::failed_precondition(error.to_string()))
            }
            DistinctError::Table(_) => ExportError::Internal(error.to_string()),
        }
    }
}

/// Behavior: `io.deephaven.server.table.ops.SelectDistinctGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = SelectDistinctRequest;

    fn result_id(request: &SelectDistinctRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &SelectDistinctRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Every name must be a column of the source: Java refuses expressions such as `X = I`
    /// here, with the names that are not columns, before the engine is asked.
    ///
    /// Behavior: `SelectDistinctGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &SelectDistinctRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let parent = &sources[0];
        require_columns(parent.definition(), &request.column_names)?;
        Ok(select_distinct(parent, &request.column_names)?)
    }
}
