//! `Head` and `Tail`: the first or last rows of a table.

use super::prelude::*;
use oxhaven_engine::ops::sort;
use oxhaven_proto::grpc::HeadOrTailRequest;

/// Behavior: `io.deephaven.server.table.ops.HeadOrTailGrpcImpl#validateRequest`
fn validate(request: &HeadOrTailRequest) -> Result<(), Status> {
    if request.num_rows < 0 {
        return Err(Status::invalid_argument(format!(
            "numRows must be >= 0 (found: {})",
            request.num_rows
        )));
    }
    Ok(())
}

macro_rules! head_or_tail {
    ($(#[$doc:meta])* $handler:ident => $operation:path) => {
        $(#[$doc])*
        pub struct $handler;

        impl TableOp for $handler {
            type Request = HeadOrTailRequest;

            fn result_id(request: &HeadOrTailRequest) -> Option<&Ticket> {
                request.result_id.as_ref()
            }

            fn sources(request: &HeadOrTailRequest) -> Vec<TableReference> {
                vec![source(&request.source_id)]
            }

            fn validate(request: &HeadOrTailRequest) -> Result<(), Status> {
                validate(request)
            }

            fn create(
                _context: &OpContext,
                request: &HeadOrTailRequest,
                sources: &[Arc<dyn Table>],
            ) -> Result<Arc<dyn Table>, ExportError> {
                Ok($operation(&sources[0], request.num_rows)?)
            }
        }
    };
}

head_or_tail! {
    /// Behavior: `io.deephaven.server.table.ops.HeadOrTailGrpcImpl.HeadGrpcImpl`
    Head => sort::head
}
head_or_tail! {
    /// Behavior: `io.deephaven.server.table.ops.HeadOrTailGrpcImpl.TailGrpcImpl`
    Tail => sort::tail
}
