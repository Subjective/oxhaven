//! `MultiJoinTables`: one row per key that any of several tables has, with the columns of
//! the row of each table that has the key.
//!
//! The request is checked for its shape alone: at least one input, and for every input at
//! least one match column and a table reference that names something. The columns are
//! read when the join is made, by the parsers of `io.deephaven.api` that `Aj` and `Raj`
//! use (see [`asof`](super::asof)), so a column that cannot be read is an exception out of
//! Java's handler like every refusal of the engine, not a status of its own.

use super::asof::{join_addition, join_match};
use super::prelude::*;
use oxhaven_engine::ops::multi_join::{multi_join, MultiJoinError, MultiJoinInput};
use oxhaven_proto::grpc::MultiJoinTablesRequest;

/// What Java's multi join refuses is an exception out of the engine: the recorded results
/// have `INVALID_ARGUMENT` for a duplicate key, a column added twice, key columns of other
/// types and an unknown column.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<MultiJoinError> for ExportError {
    fn from(error: MultiJoinError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// Behavior: `io.deephaven.server.table.ops.MultiJoinGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = MultiJoinTablesRequest;

    fn result_id(request: &MultiJoinTablesRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &MultiJoinTablesRequest) -> Vec<TableReference> {
        let inputs = request.multi_join_inputs.iter();
        inputs.map(|input| source(&input.source_id)).collect()
    }

    /// Java also refuses unknown fields, of the request and of each input; prost drops
    /// them, so that check cannot be made here.
    ///
    /// Behavior: `MultiJoinGrpcImpl#validateRequest`,
    /// `io.deephaven.server.grpc.GrpcErrorHelper#checkRepeatedFieldNonEmpty`
    fn validate(request: &MultiJoinTablesRequest) -> Result<(), Status> {
        if request.multi_join_inputs.is_empty() {
            return Err(Status::invalid_argument("Cannot join zero source tables."));
        }
        for input in &request.multi_join_inputs {
            if input.columns_to_match.is_empty() {
                return Err(Status::invalid_argument(
                    "io.deephaven.proto.backplane.grpc.MultiJoinInput must have at least one \
                     columns_to_match (2)",
                ));
            }
            validate_reference(&source(&input.source_id))?;
        }
        Ok(())
    }

    /// The columns of each input are read in turn, its matches before its additions. One
    /// that cannot be read is an `IllegalArgumentException` or an `InvalidNameException`
    /// in Java, thrown while the export is made.
    ///
    /// Behavior: `MultiJoinGrpcImpl#create`,
    /// `io.deephaven.engine.table.MultiJoinInput#of(Table, String[], String[])`
    fn create(
        _context: &OpContext,
        request: &MultiJoinTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let mut inputs = Vec::with_capacity(sources.len());
        for (input, table) in request.multi_join_inputs.iter().zip(sources) {
            let on = input.columns_to_match.iter().map(|text| join_match(text));
            let on = on.collect::<Result<Vec<_>, _>>();
            let add = input.columns_to_add.iter().map(|text| join_addition(text));
            let add = add.collect::<Result<Vec<_>, _>>();
            inputs.push(MultiJoinInput {
                table: table.clone(),
                on: on.map_err(ExportError::Internal)?,
                add: add.map_err(ExportError::Internal)?,
            });
        }
        Ok(multi_join(&inputs)?)
    }
}
