//! `CreateInputTable`: makes an empty input table from a schema or from the columns of
//! another table. Rows are added and deleted through the input table service.

use super::prelude::*;
use crate::inputtable::{create_input_table, validate_create_input_table};
use oxhaven_proto::grpc::create_input_table_request::Definition;
use oxhaven_proto::grpc::CreateInputTableRequest;

/// Behavior: `io.deephaven.server.table.ops.CreateInputTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = CreateInputTableRequest;

    fn result_id(request: &CreateInputTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    /// Behavior: `CreateInputTableGrpcImpl#getTableReferences`
    fn sources(request: &CreateInputTableRequest) -> Vec<TableReference> {
        match &request.definition {
            Some(Definition::SourceTableId(reference)) => vec![reference.clone()],
            _ => Vec::new(),
        }
    }

    fn validate(request: &CreateInputTableRequest) -> Result<(), Status> {
        validate_create_input_table(request)
    }

    fn create(
        context: &OpContext,
        request: &CreateInputTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(create_input_table(
            context.graph.clone(),
            request,
            sources.first(),
        )?)
    }
}
