//! `DropColumns`: a table without the named columns.

use super::prelude::*;
use oxhaven_engine::ops::select::drop_columns;
use oxhaven_proto::grpc::DropColumnsRequest;

/// Behavior: `io.deephaven.server.table.ops.DropColumnsGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = DropColumnsRequest;

    fn result_id(request: &DropColumnsRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &DropColumnsRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        _context: &OpContext,
        request: &DropColumnsRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        // Java leaves a column that does not exist to the engine, unlike `Sort`.
        Ok(drop_columns(&sources[0], &request.column_names)?)
    }
}
