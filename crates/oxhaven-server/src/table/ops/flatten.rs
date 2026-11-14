//! `Flatten`: the same rows with the row keys 0, 1, 2 and so on.

use super::prelude::*;
use oxhaven_engine::ops::merge::flatten;
use oxhaven_proto::grpc::FlattenRequest;

/// Behavior: `io.deephaven.server.table.ops.FlattenTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = FlattenRequest;

    fn result_id(request: &FlattenRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &FlattenRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        _context: &OpContext,
        _request: &FlattenRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(flatten(&sources[0])?)
    }
}
