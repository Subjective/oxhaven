//! `Update`, `LazyUpdate`, `View`, `UpdateView` and `Select`: columns computed by formulas.
//!
//! The five differ only in the engine operation they call.
//!
//! Java checks every formula with its `ColumnExpressionValidator` before the engine sees
//! it, and refuses a call to a method or constructor outside the server's allow list. Here
//! the formula compiler knows only the function library, so such a call does not compile,
//! and both servers answer it the same way: `INVALID_ARGUMENT` with an error id.

use super::prelude::*;
use oxhaven_engine::ops::select;
use oxhaven_proto::grpc::SelectOrUpdateRequest;

macro_rules! formula_operation {
    ($(#[$doc:meta])* $handler:ident => $operation:path) => {
        $(#[$doc])*
        pub struct $handler;

        impl TableOp for $handler {
            type Request = SelectOrUpdateRequest;

            fn result_id(request: &SelectOrUpdateRequest) -> Option<&Ticket> {
                request.result_id.as_ref()
            }

            fn sources(request: &SelectOrUpdateRequest) -> Vec<TableReference> {
                vec![source(&request.source_id)]
            }

            fn create(
                context: &OpContext,
                request: &SelectOrUpdateRequest,
                sources: &[Arc<dyn Table>],
            ) -> Result<Arc<dyn Table>, ExportError> {
                Ok($operation(
                    &sources[0],
                    &request.column_specs,
                    context.compiler.as_ref(),
                )?)
            }
        }
    };
}

formula_operation! {
    /// Behavior: `io.deephaven.server.table.ops.UpdateOrSelectGrpcImpl.UpdateGrpcImpl`
    Update => select::update
}
formula_operation! {
    /// Behavior: `io.deephaven.server.table.ops.UpdateOrSelectGrpcImpl.LazyUpdateGrpcImpl`
    LazyUpdate => select::lazy_update
}
formula_operation! {
    /// Behavior: `io.deephaven.server.table.ops.UpdateOrSelectGrpcImpl.ViewGrpcImpl`
    View => select::view
}
formula_operation! {
    /// Behavior: `io.deephaven.server.table.ops.UpdateOrSelectGrpcImpl.UpdateViewGrpcImpl`
    UpdateView => select::update_view
}
formula_operation! {
    /// Behavior: `io.deephaven.server.table.ops.UpdateOrSelectGrpcImpl.SelectGrpcImpl`
    Select => select::select
}
