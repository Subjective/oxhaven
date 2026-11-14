//! `EmptyTable`: a static table of a given size with no columns.

use super::prelude::*;
use oxhaven_engine::table::empty_table;
use oxhaven_proto::grpc::EmptyTableRequest;

/// Behavior: `io.deephaven.server.table.ops.EmptyTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = EmptyTableRequest;

    fn result_id(request: &EmptyTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn validate(request: &EmptyTableRequest) -> Result<(), Status> {
        if request.size < 0 {
            // Java's message; zero is accepted.
            return Err(Status::invalid_argument("Size must be greater than zero"));
        }
        Ok(())
    }

    fn create(
        _context: &OpContext,
        request: &EmptyTableRequest,
        _sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(empty_table(request.size as u64))
    }
}
