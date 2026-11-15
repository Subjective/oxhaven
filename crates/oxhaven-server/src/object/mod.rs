//! The object service, for the one kind of object the server has besides tables: a
//! partitioned table. `FetchObject` and `MessageStream` send what its Java plugin sends.
//!
//! Object types of plugins, and the two calls a browser without streams uses, belong to
//! task 7.10; until then they are refused.
//!
//! Behavior: `io.deephaven.server.object.ObjectServiceGrpcImpl`

#[cfg(test)]
mod tests;

use std::sync::Arc;

use oxhaven_proto::grpc::object_service_server::ObjectService;
use oxhaven_proto::grpc::stream_request::Message as ClientMessage;
use oxhaven_proto::grpc::stream_response::Message as ServerMessage;
use oxhaven_proto::grpc::{
    BrowserNextResponse, FetchObjectRequest, FetchObjectResponse, ServerData, StreamRequest,
    StreamResponse, TypedTicket,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::partitioned::{describe, partitioned_of, TYPE_NAME};
use crate::session::{current_session, ExportError, ExportValue, Reply, Session, TicketRouter};
use crate::table::support::not_ported;

const TABLE_TYPE_NAME: &str = "Table";

/// The message a client that opens `value` as an object of type `expected` is sent: the
/// payload, and the objects it refers to, exported to `session` under ids the server
/// chooses.
///
/// Behavior: `ObjectServiceGrpcImpl#getObjectTypeInstance`, the
/// `compatibleClientConnection` of `io.deephaven.plugin.type.ObjectTypeBase.FetchOnly`,
/// and `ObjectServiceGrpcImpl.PluginMessageSender#onData`
fn open(
    session: &Arc<Session>,
    expected: &str,
    value: &ExportValue,
) -> Result<ServerData, ExportError> {
    let Some(partitioned) = partitioned_of(value) else {
        return Err(Status::not_found(format!(
            "No ObjectType found, expected type '{expected}'"
        ))
        .into());
    };
    if expected != TYPE_NAME {
        return Err(Status::failed_precondition(format!(
            "Unexpected ObjectType, expected type '{expected}', actual type '{TYPE_NAME}'"
        ))
        .into());
    }
    let (payload, table) = describe(partitioned)?;
    let export = session.new_server_side_export(ExportValue::Table(table))?;
    Ok(ServerData {
        payload,
        exported_references: vec![TypedTicket {
            ticket: Some(export.ticket()),
            r#type: TABLE_TYPE_NAME.to_string(),
        }],
    })
}

/// The type and ticket bytes of the object a request names.
///
/// Behavior: the checks at the start of `ObjectServiceGrpcImpl#fetchObject` and of a
/// connect request in `SendMessageObserver#onNext`
fn named(source: Option<TypedTicket>) -> Result<(String, Vec<u8>), Status> {
    let source = source.unwrap_or_default();
    if source.r#type.is_empty() {
        return Err(Status::invalid_argument("No type supplied"));
    }
    let ticket = source.ticket.map(|ticket| ticket.ticket).unwrap_or_default();
    if ticket.is_empty() {
        return Err(Status::invalid_argument("No ticket supplied"));
    }
    Ok((source.r#type, ticket))
}

pub struct ObjectGrpc {
    router: Arc<TicketRouter>,
}

impl ObjectGrpc {
    pub fn new(router: Arc<TicketRouter>) -> Self {
        Self { router }
    }
}

type Responses = mpsc::UnboundedSender<Result<StreamResponse, Status>>;

/// One call of `MessageStream`: reads the client's messages until the client ends the
/// stream or a message is refused.
async fn converse(
    router: Arc<TicketRouter>,
    session: Arc<Session>,
    mut requests: Streaming<StreamRequest>,
    responses: Responses,
) {
    // set by the connect request; resolves when the object's first message has been sent
    let mut connected: Option<oneshot::Receiver<()>> = None;
    while let Some(request) = requests.next().await {
        let Ok(request) = request else {
            // the client is gone
            return;
        };
        let handled = match request.message {
            None => Err(Status::invalid_argument(
                "io.deephaven.proto.backplane.grpc.StreamRequest must have oneof message. Note: \
                 this may also indicate that the server is older than the client and doesn't \
                 know about this new oneof option.",
            )),
            Some(ClientMessage::Connect(_)) if connected.is_some() => Err(
                Status::failed_precondition("Already sent a connect request, cannot send another"),
            ),
            Some(ClientMessage::Connect(connect)) => {
                connect_to(&router, &session, connect.source_id, &responses)
                    .map(|sent| connected = Some(sent))
            }
            Some(ClientMessage::Data(_)) if connected.is_none() => Err(Status::invalid_argument(
                "Data message sent before Connect message",
            )),
            // Behavior: `ObjectType.MessageStream#NOOP`, the stream of an object that is
            // only fetched
            Some(ClientMessage::Data(_)) => Ok(()),
        };
        if let Err(status) = handled {
            let _ = responses.send(Err(status));
            return;
        }
    }
    // Behavior: `SendMessageObserver#onCompleted`, which closes the stream after the
    // messages before it have been handled
    if let Some(sent) = connected {
        let _ = sent.await;
    }
}

/// Starts the work of a connect request and returns what resolves when it is done.
fn connect_to(
    router: &TicketRouter,
    session: &Arc<Session>,
    source: Option<TypedTicket>,
    responses: &Responses,
) -> Result<oneshot::Receiver<()>, Status> {
    let (expected, ticket) = named(source)?;
    let object = router.resolve(Some(session), &ticket, "sourceId")?;
    let (done, sent) = oneshot::channel();
    let (failed, opened) = (responses.clone(), responses.clone());
    let owner = session.clone();
    session
        .non_export()?
        .require([object])
        .on_error(move |status| {
            let _ = failed.send(Err(status));
        })
        .submit(move |inputs| {
            let data = open(&owner, &expected, &inputs[0])?;
            let _ = opened.send(Ok(StreamResponse {
                message: Some(ServerMessage::Data(data)),
            }));
            let _ = done.send(());
            Ok(ExportValue::unit())
        })?;
    Ok(sent)
}

#[tonic::async_trait]
impl ObjectService for ObjectGrpc {
    /// Behavior: `ObjectServiceGrpcImpl#fetchObject`
    async fn fetch_object(
        &self,
        request: Request<FetchObjectRequest>,
    ) -> Result<Response<FetchObjectResponse>, Status> {
        let session = current_session(&request)?;
        let (expected, ticket) = named(request.into_inner().source_id)?;
        self.router.log_name(&ticket, "sourceId")?;
        let object = self.router.resolve(Some(&session), &ticket, "sourceId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let owner = session.clone();
        session
            .non_export()?
            .require([object])
            .on_error(move |status| fail(Err(status)))
            .submit(move |inputs| {
                let data = open(&owner, &expected, &inputs[0])?;
                succeed(Ok(FetchObjectResponse {
                    r#type: expected,
                    data: data.payload,
                    typed_export_ids: data.exported_references,
                }));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }

    type MessageStreamStream = UnboundedReceiverStream<Result<StreamResponse, Status>>;

    /// Behavior: `ObjectServiceGrpcImpl#messageStream`
    async fn message_stream(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Self::MessageStreamStream>, Status> {
        let session = current_session(&request)?;
        let (responses, stream) = mpsc::unbounded_channel();
        tokio::spawn(converse(
            self.router.clone(),
            session,
            request.into_inner(),
            responses,
        ));
        Ok(Response::new(UnboundedReceiverStream::new(stream)))
    }

    type OpenMessageStreamStream = UnboundedReceiverStream<Result<StreamResponse, Status>>;

    async fn open_message_stream(
        &self,
        _request: Request<StreamRequest>,
    ) -> Result<Response<Self::OpenMessageStreamStream>, Status> {
        Err(not_ported(
            "io.deephaven.server.object.ObjectServiceGrpcImpl#openMessageStream",
            "a message stream in two calls",
        ))
    }

    async fn next_message_stream(
        &self,
        _request: Request<StreamRequest>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        Err(not_ported(
            "io.deephaven.server.object.ObjectServiceGrpcImpl#nextMessageStream",
            "a message stream in two calls",
        ))
    }
}
