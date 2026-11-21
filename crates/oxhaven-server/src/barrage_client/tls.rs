//! TLS connections of the Barrage client (`dh://` targets), through the system's OpenSSL.
//!
//! Java's client connects with Netty's TLS as gRPC configures it: the JDK's root
//! certificates or only the given ones, the server's name checked against its certificate
//! (the `HTTPS` endpoint identification), and HTTP/2 agreed by ALPN. This does the same
//! with OpenSSL: the certificates are checked by OpenSSL's verifier with the system's roots
//! (its default paths, which `SSL_CERT_FILE` and `SSL_CERT_DIR` move) or only the given
//! ones, the name by `X509_check_host` or, for an address, `X509_check_ip`, and the
//! connection is refused unless the server picks `h2`.

use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::{X509VerifyResult, X509};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;
use tonic::transport::{Channel, Endpoint};

use super::error::ClientError;
use super::session::ClientConfig;

type BoxError = Box<dyn Error + Send + Sync>;

/// The ALPN protocol list of a gRPC client: HTTP/2 alone.
const ALPN_H2: &[u8] = b"\x02h2";

/// How a client of `config` checks a server: the system's roots, or only the PEM roots of
/// `config.tls_root_certs` when given; refused when those hold no certificate.
///
/// Behavior: `io.deephaven.client.impl.ChannelHelper#channelBuilder` (the secure branch:
/// `SSLConfig#orTrust(TrustJdk)`); `io.deephaven.ssl.config.impl.KickstartUtils#create`,
/// whose `TrustCustom` adds the given certificates as the only trust material, and whose
/// `TrustJdk` takes the JDK's default trust material; `deephaven.barrage._build_client_config`
/// (`TrustCustom#ofX509` of the bytes)
fn connector(config: &ClientConfig) -> Result<SslConnector, ClientError> {
    let tls = |problem: openssl::error::ErrorStack| {
        ClientError::Config(format!("cannot set up TLS: {problem}"))
    };
    // verifies the peer, with the default paths and OpenSSL's default ciphers and protocols
    let mut builder = SslConnector::builder(SslMethod::tls_client()).map_err(tls)?;
    builder.set_verify(SslVerifyMode::PEER);
    builder.set_alpn_protos(ALPN_H2).map_err(tls)?;
    if let Some(pem) = &config.tls_root_certs {
        let certificates = X509::stack_from_pem(pem).map_err(|problem| {
            ClientError::Config(format!("tls_root_certs cannot be read as PEM: {problem}"))
        })?;
        if certificates.is_empty() {
            return Err(ClientError::Config(
                "tls_root_certs holds no PEM encoded certificate".to_string(),
            ));
        }
        // a store of their own, without the default paths: the given roots replace the
        // system's
        let mut store = X509StoreBuilder::new().map_err(tls)?;
        for certificate in certificates {
            store.add_cert(certificate).map_err(tls)?;
        }
        builder.set_cert_store(store.build());
    }
    Ok(builder.build())
}

/// `host` as the certificate must name it: an IPv6 address without its brackets.
fn verified_name(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host)
}

/// Why a handshake failed: the certificate check's reason when the check failed, OpenSSL's
/// error otherwise (a server that does not speak TLS, a protocol it refuses).
fn handshake_failure(stream: &SslStream<TcpStream>, problem: openssl::ssl::Error) -> String {
    let verified = stream.ssl().verify_result();
    if verified != X509VerifyResult::OK {
        return format!(
            "TLS handshake failed: the server's certificate is not trusted: {}",
            verified.error_string()
        );
    }
    match problem.ssl_error() {
        Some(stack) => format!("TLS handshake failed: {stack}"),
        None => format!("TLS handshake failed: {problem}"),
    }
}

/// A TCP connection to `host:port` with a completed TLS handshake that agreed on `h2`.
///
/// Behavior: `io.grpc.netty.ProtocolNegotiators.ClientTlsHandler` (the `HTTPS` endpoint
/// identification; "Failed ALPN negotiation: Unable to find compatible protocol" when the
/// server does not pick `h2`)
async fn handshake(
    connector: &SslConnector,
    host: &str,
    port: u16,
) -> Result<TokioIo<SslStream<TcpStream>>, BoxError> {
    let name = verified_name(host);
    let tcp = TcpStream::connect((name, port)).await?;
    tcp.set_nodelay(true)?;
    // sends the name as SNI unless it is an address, and checks it against the certificate
    let ssl = connector.configure()?.into_ssl(name)?;
    let mut stream = SslStream::new(ssl, tcp)?;
    if let Err(problem) = Pin::new(&mut stream).connect().await {
        return Err(handshake_failure(&stream, problem).into());
    }
    if stream.ssl().selected_alpn_protocol() != Some(b"h2".as_slice()) {
        return Err("Failed ALPN negotiation: Unable to find compatible protocol".into());
    }
    Ok(TokioIo::new(stream))
}

/// A channel to the server of `config` over TLS, or the configuration error that keeps it
/// from being made before anything is sent. Each connection the channel makes, the first
/// and any after it lost one, goes through the same checks.
///
/// Behavior: `io.deephaven.client.impl.ChannelHelper#channelBuilder` (the secure branch)
pub(super) async fn connect(
    config: &ClientConfig,
    endpoint: Endpoint,
) -> Result<Result<Channel, tonic::transport::Error>, ClientError> {
    let connector = Arc::new(connector(config)?);
    let host = config.host.clone();
    let port = config.port;
    let connect = tower::service_fn(move |_: http::Uri| {
        let connector = connector.clone();
        let host = host.clone();
        async move { handshake(&connector, &host, port).await }
    });
    Ok(endpoint.connect_with_connector(connect).await)
}
