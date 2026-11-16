//! `DoPut` as the calls of a browser. The upload is one that Java 42.9 was sent as a
//! `DoPut` and whose table it then answered (`fixtures/put_three_batches.*`); what a
//! browser stream answers to its headers is tested with the stream (`console::browser`).

use oxhaven_proto::browser_flight::browser_flight_service_client::BrowserFlightServiceClient;
use oxhaven_proto::flight::{FlightData, PutResult};
use tonic::{Code, Request, Status};

use super::tests::support::{assert_do_get_matches, fixture_messages, Server};

const CASE: &str = "put_three_batches";

/// A message of a browser stream: the stream is named by an export id of the client,
/// `sequence` is the place of the message, and the message that ends the client's side
/// carries nothing.
fn part(
    server: &Server,
    message: FlightData,
    stream: i32,
    sequence: i32,
    last: bool,
) -> Request<FlightData> {
    let mut request = server.authorized(message);
    let headers = request.metadata_mut();
    headers.insert(
        "x-deephaven-stream-ticket",
        stream.to_string().parse().unwrap(),
    );
    headers.insert(
        "x-deephaven-stream-sequence",
        sequence.to_string().parse().unwrap(),
    );
    if last {
        headers.insert("x-deephaven-stream-halfclose", "true".parse().unwrap());
    }
    request
}

async fn results(mut stream: tonic::Streaming<PutResult>) -> Result<Vec<PutResult>, Status> {
    let mut results = Vec::new();
    while let Some(result) = stream.message().await? {
        results.push(result);
    }
    Ok(results)
}

/// The export the upload of [`CASE`] makes: its descriptor is the path `export/<id>`.
fn target(messages: &[FlightData]) -> i32 {
    let descriptor = messages[0].flight_descriptor.as_ref().unwrap();
    assert_eq!(descriptor.path[0], "export");
    descriptor.path[1].parse().unwrap()
}

#[tokio::test]
async fn an_upload_sent_as_calls_makes_the_table_of_the_same_do_put() {
    let server = &Server::start().await;
    let mut client = BrowserFlightServiceClient::new(server.channel.clone());
    let mut messages = fixture_messages(&format!("{CASE}.put"));
    let id = target(&messages);
    let stream = id + 1000;
    let batches = messages.len() - 1;
    assert_eq!(batches, 3);

    let first = messages.remove(0);
    let opened = client
        .open_do_put(part(server, first, stream, 0, false))
        .await
        .unwrap()
        .into_inner();
    // the last batch overtakes the others: it is taken, and waits for them
    let numbered: Vec<(i32, FlightData)> = (1..).zip(messages).collect();
    for (sequence, message) in numbered.into_iter().rev() {
        client
            .next_do_put(part(server, message, stream, sequence, false))
            .await
            .unwrap();
    }
    // nothing is made until the client ends its side: a read of the table waits for it
    let early = tokio::time::timeout(std::time::Duration::from_millis(300), server.do_get(id));
    assert!(early.await.is_err());
    let end = batches as i32 + 1;
    client
        .next_do_put(part(server, FlightData::default(), stream, end, true))
        .await
        .unwrap();

    // one empty result per record batch, as for the DoPut
    let acknowledged = results(opened).await.unwrap();
    assert_eq!(acknowledged.len(), batches);
    assert!(acknowledged
        .iter()
        .all(|result| result.app_metadata.is_empty()));
    let schema = server.get_schema(id).await.unwrap();
    let sent = server.do_get(id).await.unwrap();
    // the rows are in the order of the sequence, not of arrival
    assert_do_get_matches(CASE, &sent, &schema);
    // the stream is over, and its export with it
    let late = client
        .next_do_put(part(server, FlightData::default(), stream, end + 1, false))
        .await
        .unwrap_err();
    assert_eq!(late.code(), Code::Cancelled, "{late:?}");
}

#[tokio::test]
async fn an_upload_needs_the_headers_of_a_stream_and_the_other_calls_are_not_ported() {
    let server = &Server::start().await;
    let mut client = BrowserFlightServiceClient::new(server.channel.clone());
    let messages = fixture_messages(&format!("{CASE}.put"));
    let id = target(&messages);

    let status = client
        .open_do_put(server.authorized(messages[0].clone()))
        .await
        .unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (
            Code::InvalidArgument,
            "no x-deephaven-stream headers, cannot handle open request"
        )
    );
    let status = client
        .open_do_put(part(server, messages[0].clone(), id + 1000, 1, false))
        .await
        .unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (
            Code::InvalidArgument,
            "open request must have sequence 0, received 1"
        )
    );
    // a call without a session is refused
    let mut anonymous = part(server, messages[0].clone(), id + 1000, 0, false);
    anonymous.metadata_mut().remove("authorization");
    let status = client.open_do_put(anonymous).await.unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");

    let status = client
        .open_do_exchange(part(server, FlightData::default(), id + 1000, 0, false))
        .await
        .unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (
            Code::Unimplemented,
            "not ported yet: io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding: OpenDoExchange"
        )
    );
    let status = client
        .open_handshake(server.authorized(Default::default()))
        .await
        .unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (
            Code::Unimplemented,
            "not ported yet: io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding: OpenHandshake"
        )
    );
}

#[tokio::test]
async fn a_client_that_gives_up_the_open_call_ends_the_stream() {
    let server = &Server::start().await;
    let mut client = BrowserFlightServiceClient::new(server.channel.clone());
    let mut messages = fixture_messages(&format!("{CASE}.put"));
    let stream = target(&messages) + 1000;
    let first = messages.remove(0);
    let opened = client
        .open_do_put(part(server, first, stream, 0, false))
        .await
        .unwrap();
    // the stream takes messages while its call is open
    client
        .next_do_put(part(server, messages[0].clone(), stream, 1, false))
        .await
        .unwrap();
    drop(opened);
    // the server learns of it after a moment; from then on a message is refused
    let mut refused = None;
    for sequence in 2..200 {
        let next = part(server, messages[1].clone(), stream, sequence, false);
        if let Err(status) = client.next_do_put(next).await {
            refused = Some(status.code());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(refused, Some(Code::Cancelled));
}
