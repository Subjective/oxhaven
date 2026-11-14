//! `FetchTable`: exports a table that some ticket names under a new export ticket.

use super::prelude::*;
use oxhaven_proto::grpc::FetchTableRequest;

/// Behavior: `io.deephaven.server.table.ops.FetchTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = FetchTableRequest;

    fn result_id(request: &FetchTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &FetchTableRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        _context: &OpContext,
        _request: &FetchTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(sources[0].clone())
    }
}
