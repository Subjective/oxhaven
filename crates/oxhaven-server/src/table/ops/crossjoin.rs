//! `CrossJoinTables`: every pair of a left row and a right row with the same key, or every
//! pair when the request names no match columns.
//!
//! The request is read as the other joins read theirs (see [`join`](super::join)): every
//! match and every addition must be `Name` or `Left=Right` (also `Left==Right`).

use super::join::parse;
use super::prelude::*;
use oxhaven_engine::ops::crossjoin::{cross_join, CrossJoinError, DEFAULT_RESERVE_BITS};
use oxhaven_proto::grpc::CrossJoinTablesRequest;

/// What Java's cross join refuses is an exception out of the engine, the lack of row key
/// space (`OutOfKeySpaceException`) and a number of bits out of range (`AssertionFailure`)
/// as much as the column refusals it shares with the natural join: `INVALID_ARGUMENT` with
/// an error id.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<CrossJoinError> for ExportError {
    fn from(error: CrossJoinError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// The number of bits to reserve for the right rows of one left row: the request's when it
/// is positive, the configured default otherwise. A positive number is passed on unchecked,
/// as Java passes it; the engine refuses one it cannot use.
///
/// Behavior: `CrossJoinTablesGrpcImpl#doJoin`
fn reserve_bits(request: &CrossJoinTablesRequest) -> i32 {
    if request.reserve_bits <= 0 {
        DEFAULT_RESERVE_BITS
    } else {
        request.reserve_bits
    }
}

/// Behavior: `io.deephaven.server.table.ops.JoinTablesGrpcImpl.CrossJoinTablesGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = CrossJoinTablesRequest;

    fn result_id(request: &CrossJoinTablesRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &CrossJoinTablesRequest) -> Vec<TableReference> {
        vec![source(&request.left_id), source(&request.right_id)]
    }

    /// Behavior: `JoinTablesGrpcImpl#validateRequest`
    fn validate(request: &CrossJoinTablesRequest) -> Result<(), Status> {
        parse(&request.columns_to_match, &request.columns_to_add).map(|_| ())
    }

    /// Behavior: `JoinTablesGrpcImpl#create`, `CrossJoinTablesGrpcImpl#doJoin`
    fn create(
        _context: &OpContext,
        request: &CrossJoinTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let (on, add) = parse(&request.columns_to_match, &request.columns_to_add)?;
        Ok(cross_join(
            &sources[0],
            &sources[1],
            &on,
            &add,
            reserve_bits(request),
        )?)
    }
}
