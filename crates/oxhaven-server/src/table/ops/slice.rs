//! `Slice`: the rows of a table between two positions, each counted from the start or,
//! when negative, from the end.

use super::prelude::*;
use oxhaven_engine::ops::sort::slice;
use oxhaven_proto::grpc::SliceRequest;

/// Behavior: `io.deephaven.server.table.ops.SliceGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = SliceRequest;

    fn result_id(request: &SliceRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &SliceRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Behavior: `SliceGrpcImpl#validateRequest`
    fn validate(request: &SliceRequest) -> Result<(), Status> {
        if request.source_id.is_none() {
            return Err(Status::invalid_argument(
                "io.deephaven.proto.backplane.grpc.SliceRequest must have field source_id (2)",
            ));
        }
        let first = request.first_position_inclusive;
        let last = request.last_position_exclusive;
        if first >= 0 && last >= 0 && last < first {
            return Err(Status::invalid_argument(
                "Cannot slice with a non-negative start position that is after a non-negative end position.",
            ));
        }
        if first < 0 && last < 0 && last < first {
            return Err(Status::invalid_argument(
                "Cannot slice with a negative start position that is after a negative end position.",
            ));
        }
        Ok(())
    }

    fn create(
        _context: &OpContext,
        request: &SliceRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(slice(
            &sources[0],
            request.first_position_inclusive,
            request.last_position_exclusive,
        )?)
    }
}
