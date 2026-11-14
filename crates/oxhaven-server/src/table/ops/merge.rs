//! `Merge`: the rows of several tables with the same columns, one table after another.

use super::prelude::*;
use oxhaven_engine::ops::merge::merge;
use oxhaven_engine::ops::sort::{sort, SortColumn};
use oxhaven_proto::grpc::MergeTablesRequest;

/// Behavior: `io.deephaven.server.table.ops.MergeTablesGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = MergeTablesRequest;

    fn result_id(request: &MergeTablesRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &MergeTablesRequest) -> Vec<TableReference> {
        request.source_ids.clone()
    }

    /// Behavior: `MergeTablesGrpcImpl#validateRequest`
    fn validate(request: &MergeTablesRequest) -> Result<(), Status> {
        if request.source_ids.is_empty() {
            return Err(Status::invalid_argument("Cannot merge zero source tables."));
        }
        Ok(())
    }

    /// With a key column Java merges static tables with `mergeSorted`, which takes the
    /// least next row of the tables in turn, and sorts the merge of refreshing tables.
    fn create(
        _context: &OpContext,
        request: &MergeTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let key_column = &request.key_column;
        if key_column.is_empty() {
            return Ok(merge(sources)?);
        }
        if !sources.iter().any(|table| table.is_refreshing()) {
            return Err(not_ported(
                "io.deephaven.engine.util.TableTools#mergeSorted",
                "a merge of static tables with a key column",
            )
            .into());
        }
        let merged = merge(sources)?;
        Ok(sort(&merged, &[SortColumn::ascending(key_column.as_str())])?)
    }
}
