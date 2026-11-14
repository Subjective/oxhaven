//! The `ApplicationService` gRPC service.

use std::sync::Arc;

use oxhaven_proto::grpc::application_service_server::ApplicationService;
use oxhaven_proto::grpc::{FieldsChangeUpdate, ListFieldsRequest};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use super::fields::Applications;
use crate::session::current_session;

/// Behavior: `io.deephaven.server.appmode.ApplicationServiceGrpcImpl`
pub struct ApplicationGrpc {
    applications: Arc<Applications>,
}

impl ApplicationGrpc {
    pub fn new(applications: Arc<Applications>) -> Self {
        Self { applications }
    }
}

#[tonic::async_trait]
impl ApplicationService for ApplicationGrpc {
    type ListFieldsStream = UnboundedReceiverStream<Result<FieldsChangeUpdate, Status>>;

    /// Every field as created, then the changes, until the client or its session goes.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl#listFields`
    async fn list_fields(
        &self,
        request: Request<ListFieldsRequest>,
    ) -> Result<Response<Self::ListFieldsStream>, Status> {
        let session = current_session(&request)?;
        let updates = self.applications.subscribe(&session)?;
        Ok(Response::new(UnboundedReceiverStream::new(updates)))
    }
}
