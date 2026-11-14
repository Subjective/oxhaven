//! What Java answers to `GetConsoleTypes` is replayed in `crate::ide`, where the refusals
//! of the other calls are checked too; the bidirectional call cannot be recorded that way.

use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{AutoCompleteRequest, GetConsoleTypesRequest};
use tonic::Code;

use crate::test_support::{with_auth, TestServer};

#[tokio::test]
async fn there_is_no_console_type_and_no_console_call() {
    let server = TestServer::start().await;
    let bearer = server.login().await;
    let mut console = ConsoleServiceClient::new(server.channel.clone());

    // without a session, as the web IDE asks before it logs in
    let types = console
        .get_console_types(GetConsoleTypesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(types.console_types.is_empty());

    let requests = tokio_stream::iter(vec![AutoCompleteRequest::default()]);
    let refused = console
        .auto_complete_stream(with_auth(requests, &bearer))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "ConsoleService/AutoCompleteStream is not implemented: this server has no script session"
    );
}
