//! `UnstructuredFilter`: the rows that satisfy every filter string.
//!
//! Java checks a filter that is a formula with its `ColumnExpressionValidator`; see
//! `select.rs` for why the formula compiler makes the same refusals here.

use super::prelude::*;
use oxhaven_engine::ops::filter::unstructured_filter;
use oxhaven_proto::grpc::UnstructuredFilterTableRequest;

/// Behavior: `io.deephaven.server.table.ops.UnstructuredFilterTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = UnstructuredFilterTableRequest;

    fn result_id(request: &UnstructuredFilterTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &UnstructuredFilterTableRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        context: &OpContext,
        request: &UnstructuredFilterTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(unstructured_filter(
            &sources[0],
            &request.filters,
            context.compiler.as_ref(),
        )?)
    }
}
