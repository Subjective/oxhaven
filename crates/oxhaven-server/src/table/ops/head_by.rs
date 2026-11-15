//! `HeadBy` and `TailBy`: the first or last rows of every group of a table.
//!
//! Java reads every group-by column as a formula and checks it against the source before
//! it calls the engine, and then gives the engine the strings as they came. The engine
//! takes a string that is not, character for character, the name of a column for a column
//! that does not exist, and refuses. So a request succeeds only when every string is a
//! column name, which is also a formula that passes Java's checks, and the checks never
//! decide an answer: whatever they refuse the engine refuses too, and the client is told
//! the same either way, `INVALID_ARGUMENT` with an error id. They are not made here.

use super::prelude::*;
use oxhaven_engine::ops::head_by::{self, HeadByError};
use oxhaven_proto::grpc::HeadOrTailByRequest;

/// What Java's `headBy` and `tailBy` refuse is an exception out of the engine, whatever
/// its class: the `RequirementFailure` for no rows, the `ArrayIndexOutOfBoundsException`
/// and `NegativeArraySizeException` for group-by columns that are not distinct columns,
/// the `IllegalArgumentException` for no other column and for a column of arrays, the
/// `UnsupportedOperationException` for a blink table. All are `INVALID_ARGUMENT` with an
/// error id.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<HeadByError> for ExportError {
    fn from(error: HeadByError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// A negative number of rows is refused with a status of its own. Zero is not: it is the
/// engine that refuses it.
///
/// Behavior: `io.deephaven.server.table.ops.HeadOrTailByGrpcImpl#validateRequest`
fn validate(request: &HeadOrTailByRequest) -> Result<(), Status> {
    if request.num_rows < 0 {
        return Err(Status::invalid_argument(format!(
            "numRows must be >= 0 (found: {})",
            request.num_rows
        )));
    }
    Ok(())
}

macro_rules! head_or_tail_by {
    ($(#[$doc:meta])* $handler:ident => $operation:path) => {
        $(#[$doc])*
        pub struct $handler;

        impl TableOp for $handler {
            type Request = HeadOrTailByRequest;

            fn result_id(request: &HeadOrTailByRequest) -> Option<&Ticket> {
                request.result_id.as_ref()
            }

            fn sources(request: &HeadOrTailByRequest) -> Vec<TableReference> {
                vec![source(&request.source_id)]
            }

            fn validate(request: &HeadOrTailByRequest) -> Result<(), Status> {
                validate(request)
            }

            /// Behavior: `io.deephaven.server.table.ops.HeadOrTailByGrpcImpl#create`
            fn create(
                _context: &OpContext,
                request: &HeadOrTailByRequest,
                sources: &[Arc<dyn Table>],
            ) -> Result<Arc<dyn Table>, ExportError> {
                Ok($operation(
                    &sources[0],
                    request.num_rows,
                    &request.group_by_column_specs,
                )?)
            }
        }
    };
}

head_or_tail_by! {
    /// Behavior: `io.deephaven.server.table.ops.HeadOrTailByGrpcImpl.HeadByGrpcImpl`
    HeadBy => head_by::head_by
}
head_or_tail_by! {
    /// Behavior: `io.deephaven.server.table.ops.HeadOrTailByGrpcImpl.TailByGrpcImpl`
    TailBy => head_by::tail_by
}
