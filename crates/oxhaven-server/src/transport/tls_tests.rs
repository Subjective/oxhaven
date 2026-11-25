//! The port over TLS, with certificates made for each run (`tests/tls/support.rs`) and
//! written to a directory of the run: gRPC from the Barrage client, the web IDE's files over
//! HTTP/1.1 and HTTP/2, gRPC-web over HTTP/2, what ALPN agrees, the host check, client
//! certificates, and the certificates and keys the server refuses to start with.

use std::path::{Path, PathBuf};
use std::pin::Pin;

use super::*;
use crate::barrage_client::{BarrageSession, ClientConfig, SnapshotOptions};
use crate::session::{AuthHandler, AuthHandlers, ExportValue, DEFAULT_TOKEN_EXPIRE_MS};
use crate::ServerConfig;
use bytes::Bytes;
use openssl::pkey::{PKey, Private};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::X509;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_openssl::SslStream;

use crate::test_support::tls::Authority;

const PSK: &str = "test-key";

/// The files a server is started with, in a directory of their own.
struct Files {
    dir: PathBuf,
}

impl Files {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("oxhaven-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("web/ide")).unwrap();
        std::fs::write(dir.join("web/ide/index.html"), "<html>the IDE</html>").unwrap();
        Files { dir }
    }

    fn write(&self, name: &str, contents: &[u8]) -> PathBuf {
        let path = self.dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    fn identity(&self, authority: &Authority) -> TlsConfig {
        TlsConfig {
            cert_chain: self.write("chain.pem", &authority.server.to_pem().unwrap()),
            private_key: self.write(
                "key.pem",
                &authority.server_key.private_key_to_pem_pkcs8().unwrap(),
            ),
            trust: None,
            client_auth: ClientAuth::None,
        }
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct Server {
    state: ServerState,
    port: u16,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

async fn start(config: &TlsConfig, web: &Path) -> Server {
    let mut state = ServerState::new(ServerConfig {
        auth_handlers: AuthHandlers::new(vec![AuthHandler::psk(PSK).unwrap()]).unwrap(),
        token_expire_ms: DEFAULT_TOKEN_EXPIRE_MS,
        target_cycle_duration: None,
    })
    .unwrap();
    state.web_root = Some(Arc::new(WebRoot::open(web).unwrap()));
    let tls = Arc::new(TlsAcceptor::new(config).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown, stopped) = oneshot::channel::<()>();
    tokio::spawn({
        let state = state.clone();
        async move {
            serve_tls(state, listener, tls, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        }
    });
    Server {
        state,
        port,
        shutdown: Some(shutdown),
    }
}

/// How a test client connects.
struct Client<'a> {
    roots: &'a [u8],
    alpn: Option<&'a [u8]>,
    /// The name sent as SNI and checked against the certificate.
    name: &'a str,
    identity: Option<&'a (X509, PKey<Private>)>,
    max_version: Option<SslVersion>,
    ciphers: Option<&'a str>,
}

impl<'a> Client<'a> {
    fn new(roots: &'a [u8]) -> Self {
        Client {
            roots,
            alpn: None,
            name: "localhost",
            identity: None,
            max_version: None,
            ciphers: None,
        }
    }

    async fn connect(&self, port: u16) -> Result<SslStream<TcpStream>, String> {
        let mut builder = SslConnector::builder(SslMethod::tls_client()).unwrap();
        builder.set_verify(SslVerifyMode::PEER);
        let mut store = X509StoreBuilder::new().unwrap();
        for root in X509::stack_from_pem(self.roots).unwrap() {
            store.add_cert(root).unwrap();
        }
        builder.set_cert_store(store.build());
        if let Some(alpn) = self.alpn {
            builder.set_alpn_protos(alpn).unwrap();
        }
        if let Some((certificate, key)) = self.identity {
            builder.set_certificate(certificate).unwrap();
            builder.set_private_key(key).unwrap();
        }
        if let Some(version) = self.max_version {
            builder.set_max_proto_version(Some(version)).unwrap();
        }
        if let Some(ciphers) = self.ciphers {
            builder.set_cipher_list(ciphers).unwrap();
        }
        let ssl = builder
            .build()
            .configure()
            .unwrap()
            .into_ssl(self.name)
            .unwrap();
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut stream = SslStream::new(ssl, tcp).unwrap();
        Pin::new(&mut stream)
            .connect()
            .await
            .map_err(|e| e.to_string())?;
        Ok(stream)
    }
}

/// Sends `request` over HTTP/1.x and reads the answer until the server closes.
async fn http1(stream: &mut SslStream<TcpStream>, request: &str) -> String {
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    // the server closes the connection after the answer, or the client's TLS ends
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut answer)).await;
    String::from_utf8_lossy(&answer).into_owned()
}

fn get(host: &str) -> String {
    format!("GET /ide/ HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
}

/// Whether the server's application data reaches the client: an answer to a request, or
/// the end of the connection without one.
async fn answers(stream: &mut SslStream<TcpStream>) -> bool {
    let answer = http1(stream, &get("localhost")).await;
    answer.starts_with("HTTP/1.1 200")
}

/// `GET /ide/` naming `host`, on a new connection by the name `localhost`.
async fn request(roots: &[u8], port: u16, host: &str) -> String {
    let mut stream = Client::new(roots).connect(port).await.unwrap();
    http1(&mut stream, &get(host)).await
}

/// Whether a client presenting `identity` is served.
async fn served(roots: &[u8], port: u16, identity: Option<&(X509, PKey<Private>)>) -> bool {
    let mut client = Client::new(roots);
    client.identity = identity;
    match client.connect(port).await {
        // TLS 1.3: the server checks the client's certificate after the client has
        // finished its handshake, so a refusal arrives in place of the answer
        Ok(mut stream) => answers(&mut stream).await,
        Err(_) => false,
    }
}

/// Makes one HTTP/2 request on a new connection and returns the status, the headers and
/// the body.
async fn http2(
    stream: SslStream<TcpStream>,
    request: http::Request<()>,
    body: Option<Bytes>,
) -> (StatusCode, http::HeaderMap, Bytes) {
    let (client, connection) = h2::client::handshake(stream).await.unwrap();
    tokio::spawn(connection);
    let mut client = client.ready().await.unwrap();
    let (response, mut send) = client.send_request(request, body.is_none()).unwrap();
    if let Some(body) = body {
        send.send_data(body, true).unwrap();
    }
    let response = response.await.unwrap();
    let (parts, mut received) = response.into_parts();
    let mut bytes = Vec::new();
    while let Some(chunk) = received.data().await {
        let chunk = chunk.unwrap();
        let _ = received.flow_control().release_capacity(chunk.len());
        bytes.extend_from_slice(&chunk);
    }
    (parts.status, parts.headers, Bytes::from(bytes))
}

/// The Barrage client of this crate, as a `dh://` URI or `barrage_session(...,
/// use_tls=True)` makes it, snapshots and subscribes a table over TLS, by name and by
/// address.
#[tokio::test(flavor = "multi_thread")]
async fn the_barrage_client_reads_a_table_over_tls() {
    let files = Files::new();
    let authority = Authority::new(&["localhost", "127.0.0.1"]);
    let server = start(&files.identity(&authority), &files.dir.join("web")).await;
    let definition = oxhaven_contracts::schema::TableDefinition::new(vec![
        oxhaven_contracts::schema::ColumnDefinition::new(
            "X",
            oxhaven_contracts::schema::ColumnType::Long,
        ),
    ])
    .unwrap();
    let values = arrow::array::Int64Array::from(vec![1, 2, 3]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("X", arrow::datatypes::DataType::Int64, true),
        ])),
        vec![Arc::new(values)],
    )
    .unwrap();
    let table: Arc<dyn Table> =
        oxhaven_engine::table::in_memory_table(definition, &[batch]).unwrap();
    server
        .state
        .scope
        .put("t", ExportValue::Table(table.clone()))
        .unwrap();
    for host in ["localhost", "127.0.0.1"] {
        let mut config = ClientConfig::new(host, server.port).with_psk(PSK);
        config.use_tls = true;
        config.tls_root_certs = Some(authority.pem.clone());
        let session = BarrageSession::connect(config, SteppedUpdateGraph::new(), None)
            .await
            .unwrap();
        let ticket = crate::app::scope_ticket("t");
        let snapshot = session
            .snapshot(&ticket, SnapshotOptions::default())
            .await
            .unwrap();
        oxhaven_testkit::assert_tables_equal(table.as_ref(), snapshot.as_ref());
        session.close().await.unwrap();
    }
}

/// ALPN picks `h2` when the client offers it, in either order, and `http/1.1` otherwise; a
/// client without ALPN speaks HTTP/1.1; one that offers only protocols the server does not
/// speak completes its handshake and is then disconnected.
#[tokio::test]
async fn alpn_picks_the_protocol_and_the_ide_files_arrive_over_both() {
    let files = Files::new();
    let authority = Authority::new(&["localhost"]);
    let server = start(&files.identity(&authority), &files.dir.join("web")).await;
    for offered in [&b"\x02h2"[..], b"\x08http/1.1\x02h2", b"\x02h2\x08http/1.1"] {
        let mut client = Client::new(&authority.pem);
        client.alpn = Some(offered);
        let stream = client.connect(server.port).await.unwrap();
        assert_eq!(stream.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
        let request = http::Request::get("https://localhost/ide/")
            .body(())
            .unwrap();
        let (status, headers, body) = http2(stream, request, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"<html>the IDE</html>");
        assert!(headers.contains_key("x-content-type-options"));
    }
    for offered in [Some(&b"\x08http/1.1"[..]), None] {
        let mut client = Client::new(&authority.pem);
        client.alpn = offered;
        let mut stream = client.connect(server.port).await.unwrap();
        let expected = offered.map(|_| &b"http/1.1"[..]);
        assert_eq!(stream.ssl().selected_alpn_protocol(), expected);
        let answer = http1(&mut stream, &get("localhost")).await;
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
        assert!(answer.ends_with("<html>the IDE</html>"), "{answer}");
    }
    let mut client = Client::new(&authority.pem);
    client.alpn = Some(b"\x03foo");
    let mut stream = client.connect(server.port).await.unwrap();
    assert_eq!(stream.ssl().selected_alpn_protocol(), None);
    assert!(!answers(&mut stream).await);
}

/// A browser on the `https://` page makes its calls as gRPC-web over HTTP/2; the websocket
/// is not offered over TLS, and its handshake is a request for a file that is not there.
#[tokio::test]
async fn grpc_web_is_served_over_h2_and_websockets_are_not() {
    let files = Files::new();
    let authority = Authority::new(&["localhost"]);
    let server = start(&files.identity(&authority), &files.dir.join("web")).await;
    let mut client = Client::new(&authority.pem);
    client.alpn = Some(b"\x02h2");
    let request = http::Request::post(
        "https://localhost/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants",
    )
    .header("content-type", "application/grpc-web+proto")
    .header("x-grpc-web", "1")
    .body(())
    .unwrap();
    let stream = client.connect(server.port).await.unwrap();
    let (status, headers, body) =
        http2(stream, request, Some(Bytes::from_static(&[0, 0, 0, 0, 0]))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("content-type").unwrap(), "application/grpc-web");
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("grpc-status: 0\r\n"), "{text}");
    assert!(text.contains("PskAuthenticationHandler"), "{text}");

    let mut stream = Client::new(&authority.pem)
        .connect(server.port)
        .await
        .unwrap();
    let upgrade = "GET /io.deephaven.proto.backplane.grpc.SessionService/NewSession HTTP/1.1\r\n\
                   Host: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
                   Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                   Sec-WebSocket-Protocol: grpc-websockets-multiplex\r\n\r\n";
    let answer = http1(&mut stream, upgrade).await;
    assert!(answer.starts_with("HTTP/1.1 404"), "{answer}");
}

/// Every request must name a host the certificate names, whatever the client sent as SNI;
/// a request without a host is checked against the address it was sent to.
#[tokio::test]
async fn a_request_for_a_host_the_certificate_does_not_name_is_bad() {
    let files = Files::new();
    let authority = Authority::new(&["localhost", "*.example.test"]);
    let server = start(&files.identity(&authority), &files.dir.join("web")).await;
    for host in [
        "localhost",
        "LocalHost:443",
        "a.example.test",
        "example.test",
    ] {
        let answer = request(&authority.pem, server.port, host).await;
        assert!(answer.starts_with("HTTP/1.1 200"), "{host}: {answer}");
    }
    for host in ["other.test", "a.b.example.test", "127.0.0.1"] {
        let answer = request(&authority.pem, server.port, host).await;
        assert!(answer.starts_with("HTTP/1.1 400"), "{host}: {answer}");
        assert!(
            answer.contains("<title>Error 400 Invalid SNI</title>"),
            "{answer}"
        );
        assert!(answer.contains("connection: close"), "{answer}");
    }
    // HTTP/1.0 without a host: the address, which this certificate does not name
    let mut stream = Client::new(&authority.pem)
        .connect(server.port)
        .await
        .unwrap();
    let answer = http1(&mut stream, "GET /ide/ HTTP/1.0\r\n\r\n").await;
    assert!(answer.starts_with("HTTP/1.0 400"), "{answer}");

    // over HTTP/2 the authority is the host, for a gRPC call as for a file
    let mut client = Client::new(&authority.pem);
    client.alpn = Some(b"\x02h2");
    let request = http::Request::post(
        "https://other.test/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants",
    )
    .header("content-type", "application/grpc")
    .body(())
    .unwrap();
    let stream = client.connect(server.port).await.unwrap();
    let (status, _, body) = http2(stream, request, Some(Bytes::from_static(&[0; 5]))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("Invalid SNI"));

    // a certificate for the address: then the request without a host is served
    let files = Files::new();
    let authority = Authority::new(&["127.0.0.1"]);
    let server = start(&files.identity(&authority), &files.dir.join("web")).await;
    let mut client = Client::new(&authority.pem);
    client.name = "127.0.0.1";
    let mut stream = client.connect(server.port).await.unwrap();
    let answer = http1(&mut stream, "GET /ide/ HTTP/1.0\r\n\r\n").await;
    assert!(answer.starts_with("HTTP/1.0 200"), "{answer}");
}

/// TLS 1.3 and 1.2 only, and of 1.2 only the ECDHE ciphers with AEAD: no DHE, though an
/// RSA key could sign it, and no CBC. A server key on RSA is read as well as one on P-256.
#[tokio::test]
async fn old_protocols_and_ciphers_are_refused() {
    let files = Files::new();
    let authority = Authority::new_rsa(&["localhost"]);
    let mut identity = files.identity(&authority);
    // the traditional form of an RSA key, as `openssl genrsa -traditional` writes it
    identity.private_key = files.write(
        "rsa-key.pem",
        &authority
            .server_key
            .rsa()
            .unwrap()
            .private_key_to_pem()
            .unwrap(),
    );
    let server = start(&identity, &files.dir.join("web")).await;
    let mut client = Client::new(&authority.pem);
    client.max_version = Some(SslVersion::TLS1_2);
    let stream = client.connect(server.port).await.unwrap();
    assert_eq!(stream.ssl().version_str(), "TLSv1.2");
    for (ciphers, served) in [
        ("ECDHE-RSA-AES128-GCM-SHA256", true),
        ("ECDHE-RSA-CHACHA20-POLY1305", true),
        ("DHE-RSA-AES128-GCM-SHA256", false),
        ("DHE-RSA-AES256-GCM-SHA384", false),
        ("ECDHE-RSA-AES128-SHA256", false),
        ("ECDHE-RSA-AES256-SHA", false),
    ] {
        client.ciphers = Some(ciphers);
        assert_eq!(
            client.connect(server.port).await.is_ok(),
            served,
            "{ciphers}"
        );
    }
    let mut client = Client::new(&authority.pem);
    client.max_version = Some(SslVersion::TLS1_1);
    assert!(client.connect(server.port).await.is_err());
}

/// `NEEDED` refuses a client without a certificate the trusted roots sign; `WANTED` serves
/// a client without one and refuses one with an untrusted one; with neither, no
/// certificate is asked for.
#[tokio::test]
async fn client_certificates_are_asked_for_and_checked() {
    let files = Files::new();
    let authority = Authority::new(&["localhost"]);
    let clients = Authority::new(&[]);
    let strangers = Authority::new(&[]);
    let trusted = clients.client("trusted client");
    let untrusted = strangers.client("untrusted client");
    let mut config = files.identity(&authority);
    config.trust = Some(files.write("trust.pem", &clients.pem));
    for (auth, without, with_untrusted) in [
        (ClientAuth::Needed, false, false),
        (ClientAuth::Wanted, true, false),
        (ClientAuth::None, true, true),
    ] {
        config.client_auth = auth;
        let server = start(&config, &files.dir.join("web")).await;
        let port = server.port;
        let pem = &authority.pem;
        assert_eq!(
            served(pem, port, None).await,
            without,
            "{auth:?} without a certificate"
        );
        assert!(
            served(pem, port, Some(&trusted)).await,
            "{auth:?} with a trusted certificate"
        );
        assert_eq!(
            served(pem, port, Some(&untrusted)).await,
            with_untrusted,
            "{auth:?} with an untrusted certificate"
        );
    }
}

/// The server does not start with a key that is not the certificate's, files it cannot
/// read, or files without a certificate or key.
#[test]
fn a_certificate_and_key_that_do_not_belong_together_are_refused() {
    let files = Files::new();
    let authority = Authority::new(&["localhost"]);
    let other = Authority::new(&["localhost"]);
    let config = files.identity(&authority);
    assert!(TlsAcceptor::new(&config).is_ok());
    let wrong_key = TlsConfig {
        private_key: files.write(
            "other-key.pem",
            &other.server_key.private_key_to_pem_pkcs8().unwrap(),
        ),
        ..config.clone()
    };
    let refused = TlsAcceptor::new(&wrong_key).err().unwrap();
    assert!(
        refused.contains("is not the key of the first certificate"),
        "{refused}"
    );
    let missing = TlsConfig {
        cert_chain: files.dir.join("missing.pem"),
        ..config.clone()
    };
    let refused = TlsAcceptor::new(&missing).err().unwrap();
    assert!(
        refused.starts_with("cannot read the certificate chain"),
        "{refused}"
    );
    let no_certificate = TlsConfig {
        cert_chain: files.write("empty.pem", b"nothing here\n"),
        ..config.clone()
    };
    let refused = TlsAcceptor::new(&no_certificate).err().unwrap();
    assert!(
        refused.contains("holds no PEM encoded certificate"),
        "{refused}"
    );
    let key_as_certificate = TlsConfig {
        private_key: config.cert_chain.clone(),
        ..config.clone()
    };
    let refused = TlsAcceptor::new(&key_as_certificate).err().unwrap();
    assert!(
        refused.contains("cannot be read as an unencrypted PEM key"),
        "{refused}"
    );
    // a chain: the server's certificate and then another that the client does not need
    let mut chain = authority.server.to_pem().unwrap();
    chain.extend_from_slice(&other.server.to_pem().unwrap());
    let chained = TlsConfig {
        cert_chain: files.write("chained.pem", &chain),
        ..config.clone()
    };
    assert!(TlsAcceptor::new(&chained).is_ok());
}
