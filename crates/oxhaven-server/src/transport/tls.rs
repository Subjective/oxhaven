//! The port over TLS: the server's certificate and key, the clients it trusts, and what a
//! connection agrees to speak.
//!
//! Java's Jetty connector with `ssl.identity.*` set puts TLS in front of the same
//! protocols: ALPN picks `h2` or `http/1.1` (the server's order, `h2` first), a client that
//! offers neither has its connection closed after the handshake, and a client that offers
//! none speaks HTTP/1.1. TLS 1.3 and 1.2 only, with Mozilla's intermediate ciphers without
//! the DHE ones. Every request must name a host the certificate names ("Invalid SNI").
//! This does the same through the system's OpenSSL.
//!
//! Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer#createConnector` (the TLS
//! branch), `io.deephaven.server.runner.MainHelper#parseSSLConfig`

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

/// Whether the server asks a client for a certificate.
///
/// Behavior: `io.deephaven.ssl.config.SSLConfig.ClientAuth`
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClientAuth {
    /// No certificate is asked for.
    #[default]
    None,
    /// A certificate is asked for; a client without one is served, one with a certificate
    /// the trusted roots do not sign is refused.
    Wanted,
    /// A client without a certificate the trusted roots sign is refused.
    Needed,
}

impl ClientAuth {
    /// `NONE`, `WANTED` or `NEEDED`, as `ssl.clientAuthentication` takes them (Java's
    /// `ClientAuth#valueOf`: exactly these, in capitals).
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "NONE" => Ok(ClientAuth::None),
            "WANTED" => Ok(ClientAuth::Wanted),
            "NEEDED" => Ok(ClientAuth::Needed),
            other => Err(format!(
                "No enum constant io.deephaven.ssl.config.SSLConfig.ClientAuth.{other}"
            )),
        }
    }
}

/// What the server serves TLS with: Java's `ssl.identity.type=privatekey` with
/// `ssl.identity.certChainPath` and `ssl.identity.privateKeyPath`, and optionally
/// `ssl.trust.type=certs` with `ssl.trust.path` and `ssl.clientAuthentication`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsConfig {
    /// PEM certificates: the server's first, then the ones that chain it to a root.
    pub cert_chain: PathBuf,
    /// The PEM private key of the first certificate, without a password.
    pub private_key: PathBuf,
    /// PEM certificates that a client's certificate must chain to.
    pub trust: Option<PathBuf>,
    pub client_auth: ClientAuth,
}

impl TlsConfig {
    /// Refuses asking clients for a certificate without saying which ones to trust.
    ///
    /// Behavior: `io.deephaven.ssl.config.SSLConfig#checkMutualTLS`
    pub fn check(&self) -> Result<(), String> {
        if self.client_auth != ClientAuth::None && self.trust.is_none() {
            return Err("Trust material must be present when requesting mutual TLS".to_string());
        }
        Ok(())
    }
}

/// The name a request gives for the server, without its port, lower case: the authority of
/// an HTTP/2 request or the `Host` header, otherwise the address the connection was made to.
///
/// Behavior: `org.eclipse.jetty.server.Request#getServerName` (as Java 42.9's connector was
/// observed to answer: an HTTP/1.0 request without `Host` is checked against the address)
fn server_name<B>(request: &http::Request<B>, local: SocketAddr) -> String {
    let authority = request.uri().host().map(str::to_string).or_else(|| {
        let host = request.headers().get(http::header::HOST)?.to_str().ok()?;
        Some(strip_port(host).to_string())
    });
    match authority {
        Some(host) if !host.is_empty() => host.to_ascii_lowercase(),
        _ => local.ip().to_string(),
    }
}

/// `host[:port]` without the port; an IPv6 address keeps its brackets.
fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return match host.find(']') {
            Some(end) => &host[..=end],
            None => host,
        };
    }
    match host.rsplit_once(':') {
        Some((name, _)) if !name.contains(':') => name,
        _ => host,
    }
}

/// The names a certificate is for, as the check of a request's host reads them: its DNS
/// names and its common names, a `*.` name matching its domain and one label more; and its
/// addresses.
///
/// Behavior: Jetty's `org.eclipse.jetty.util.ssl.X509` as Java 42.9's connector was observed
/// to answer: the common name counts beside the DNS names, `*.example.test` matches
/// `example.test` and `a.example.test` but not `a.b.example.test`, an address matches only
/// in a form Java reads as one (`127.0.0.01` is `127.0.0.1`, `0127.0.0.1` and `127.1` are
/// no address; `[0:0:0:0:0:ffff:7f00:1]` is `127.0.0.1`, `[::ffff:127.0.0.1]` no address)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CertificateNames {
    hosts: Vec<String>,
    wildcards: Vec<String>,
    addresses: Vec<IpAddr>,
}

impl CertificateNames {
    #[cfg(any(feature = "tls", test))]
    fn add_name(&mut self, name: &str) {
        let name = name.to_ascii_lowercase();
        match name.strip_prefix("*.") {
            Some(domain) => self.wildcards.push(domain.to_string()),
            None => self.hosts.push(name),
        }
    }

    /// Whether `host` (lower case, an IPv6 address in brackets) is one of the names.
    ///
    /// Behavior: `org.eclipse.jetty.server.SecureRequestCustomizer#customize` with
    /// `https.sniHostCheck` (Java's default `JettyConfig#sniHostCheck`: on)
    fn matches(&self, host: &str) -> bool {
        if self.hosts.iter().any(|name| name == host)
            || self.wildcards.iter().any(|domain| domain == host)
        {
            return true;
        }
        if let Some((_, domain)) = host.split_once('.') {
            if self.wildcards.iter().any(|wildcard| wildcard == domain) {
                return true;
            }
        }
        address_literal(host).is_some_and(|address| self.addresses.contains(&address))
    }
}

/// `host` as an address, when it is written as one in the forms Java's check reads: four
/// decimal parts of one to three digits, or hexadecimal groups and colons, in brackets or
/// not. An IPv6 address that maps an IPv4 one is that IPv4 address.
fn address_literal(host: &str) -> Option<IpAddr> {
    let parts = host.split('.').collect::<Vec<_>>();
    let decimal =
        |part: &&str| (1..=3).contains(&part.len()) && part.bytes().all(|b| b.is_ascii_digit());
    if parts.len() == 4 && parts.iter().all(decimal) {
        let mut octets = [0u8; 4];
        for (octet, part) in octets.iter_mut().zip(parts) {
            *octet = part.parse().ok()?;
        }
        return Some(IpAddr::from(octets));
    }
    let bare = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);
    if bare.contains(':') && bare.bytes().all(|b| b == b':' || b.is_ascii_hexdigit()) {
        return bare
            .parse::<std::net::Ipv6Addr>()
            .ok()
            .map(|address| IpAddr::V6(address).to_canonical());
    }
    None
}

/// The answer to a request for a host the certificate does not name, as Jetty's error page
/// gives it.
///
/// Behavior: `org.eclipse.jetty.server.SecureRequestCustomizer#customize` (400, "Invalid
/// SNI"; Jetty's `ErrorHandler` page)
pub(crate) fn invalid_sni<B>(request: &http::Request<B>) -> http::Response<super::Body> {
    // the URL as the client named it, as Jetty's page shows it
    let path = request
        .uri()
        .path_and_query()
        .map_or("/", |path| path.as_str());
    let authority = request.uri().authority().map(|a| a.as_str().to_string());
    let host = authority.or_else(|| {
        let host = request.headers().get(http::header::HOST)?.to_str().ok()?;
        Some(host.to_string())
    });
    let uri = format!("https://{}{path}", host.unwrap_or_default());
    let page = format!(
        "<html>\n<head>\n<meta http-equiv=\"Content-Type\" content=\"text/html;charset=ISO-8859-1\"/>\n\
         <title>Error 400 Invalid SNI</title>\n</head>\n<body>\n<h2>HTTP ERROR 400 Invalid SNI</h2>\n\
         <table>\n<tr><th>URI:</th><td>{}</td></tr>\n<tr><th>STATUS:</th><td>400</td></tr>\n\
         <tr><th>MESSAGE:</th><td>Invalid SNI</td></tr>\n</table>\n\n</body>\n</html>\n",
        escape(&uri)
    );
    let mut response = http::Response::new(super::Body::new(http_body_util::Full::new(
        bytes::Bytes::from(page),
    )));
    *response.status_mut() = http::StatusCode::BAD_REQUEST;
    let headers = response.headers_mut();
    headers.insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("must-revalidate,no-cache,no-store"),
    );
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/html;charset=iso-8859-1"),
    );
    if request.version() < http::Version::HTTP_2 {
        headers.insert(
            http::header::CONNECTION,
            http::HeaderValue::from_static("close"),
        );
    }
    response
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(feature = "tls")]
pub use with_openssl::TlsAcceptor;
#[cfg(not(feature = "tls"))]
pub use without_openssl::TlsAcceptor;

impl TlsAcceptor {
    /// Whether `request` names a host the server's certificate names.
    pub(crate) fn host_matches<B>(&self, request: &http::Request<B>, local: SocketAddr) -> bool {
        self.names().matches(&server_name(request, local))
    }
}

#[cfg(not(feature = "tls"))]
mod without_openssl {
    use super::{CertificateNames, TlsConfig};

    /// TLS for the server's port; this build has none.
    pub struct TlsAcceptor {
        never: std::convert::Infallible,
    }

    impl TlsAcceptor {
        /// Refuses by name: the build has no TLS.
        pub fn new(config: &TlsConfig) -> Result<Self, String> {
            let _ = config;
            Err(
                "this server was built without TLS; build it with `--features tls` \
                 (docs/TLS.md)"
                    .to_string(),
            )
        }

        pub(crate) fn names(&self) -> &CertificateNames {
            match self.never {}
        }

        pub(crate) fn never(&self) -> ! {
            match self.never {}
        }
    }
}

#[cfg(feature = "tls")]
mod with_openssl {
    use std::pin::Pin;

    use openssl::nid::Nid;
    use openssl::pkey::PKey;
    use openssl::ssl::{
        select_next_proto, AlpnError, Ssl, SslAcceptor, SslMethod, SslVerifyMode, SslVersion,
    };
    use openssl::stack::Stack;
    use openssl::x509::store::X509StoreBuilder;
    use openssl::x509::{X509Name, X509Ref, X509};
    use tokio::net::TcpStream;
    use tokio_openssl::SslStream;

    use super::{CertificateNames, ClientAuth, TlsConfig};

    /// The protocols the server offers by ALPN, in its order of preference.
    const ALPN: &[u8] = b"\x02h2\x08http/1.1";

    /// What a connection speaks once its handshake is done.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Protocol {
        Http2,
        Http1,
    }

    /// TLS for the server's port, made once from a [`TlsConfig`].
    pub struct TlsAcceptor {
        acceptor: SslAcceptor,
        names: CertificateNames,
        /// Set on a connection whose client offered protocols, none of them ours.
        refused_alpn: openssl::ex_data::Index<Ssl, bool>,
    }

    fn read(path: &std::path::Path, what: &str) -> Result<Vec<u8>, String> {
        std::fs::read(path).map_err(|e| format!("cannot read {what} {}: {e}", path.display()))
    }

    fn certificates(path: &std::path::Path, what: &str) -> Result<Vec<X509>, String> {
        let certificates = X509::stack_from_pem(&read(path, what)?)
            .map_err(|e| format!("{what} {} cannot be read as PEM: {e}", path.display()))?;
        if certificates.is_empty() {
            return Err(format!(
                "{what} {} holds no PEM encoded certificate",
                path.display()
            ));
        }
        Ok(certificates)
    }

    fn names_of(certificate: &X509Ref) -> CertificateNames {
        let mut names = CertificateNames::default();
        for name in certificate.subject_alt_names().into_iter().flatten() {
            if let Some(dns) = name.dnsname() {
                names.add_name(dns);
            } else if let Some(bytes) = name.ipaddress() {
                let address = match bytes.len() {
                    4 => <[u8; 4]>::try_from(bytes).ok().map(std::net::IpAddr::from),
                    16 => <[u8; 16]>::try_from(bytes).ok().map(std::net::IpAddr::from),
                    _ => None,
                };
                names
                    .addresses
                    .extend(address.map(|address| address.to_canonical()));
            }
        }
        for entry in certificate.subject_name().entries_by_nid(Nid::COMMONNAME) {
            if let Ok(name) = entry.data().to_string() {
                names.add_name(&name);
            }
        }
        names
    }

    impl TlsAcceptor {
        /// Reads the certificates and keys of `config`, and refuses what Java refuses: a key
        /// that is not the first certificate's, files that hold no certificate or key,
        /// asking for client certificates without trusted roots.
        ///
        /// Behavior: `io.deephaven.ssl.config.impl.KickstartUtils#create` (identity, trust,
        /// client authentication); `JettyBackedGrpcServer#createConnector` (`TrustJdk`,
        /// `ProtocolsIntermediate`, `CiphersIntermediate` when none are configured)
        pub fn new(config: &TlsConfig) -> Result<Self, String> {
            config.check()?;
            let tls = |e: openssl::error::ErrorStack| format!("cannot set up TLS: {e}");
            let mut chain = certificates(&config.cert_chain, "the certificate chain")?;
            let key = PKey::private_key_from_pem(&read(&config.private_key, "the private key")?)
                .map_err(|e| {
                    format!(
                        "the private key {} cannot be read as an unencrypted PEM key: {e}",
                        config.private_key.display()
                    )
                })?;
            let leaf = chain.remove(0);
            let names = names_of(&leaf);
            let mut builder =
                SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).map_err(tls)?;
            // ProtocolsIntermediate and CiphersIntermediate: TLS 1.3 and 1.2, no DHE
            builder
                .set_min_proto_version(Some(SslVersion::TLS1_2))
                .map_err(tls)?;
            builder
                .set_ciphersuites(
                    "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256",
                )
                .map_err(tls)?;
            builder
                .set_cipher_list(
                    "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:\
                     ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-AES256-GCM-SHA384:\
                     ECDHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-GCM-SHA256",
                )
                .map_err(tls)?;
            builder.set_certificate(&leaf).map_err(tls)?;
            for certificate in chain {
                builder.add_extra_chain_cert(certificate).map_err(tls)?;
            }
            let mismatch = |e: openssl::error::ErrorStack| {
                format!(
                    "the private key {} is not the key of the first certificate of {}: {e}",
                    config.private_key.display(),
                    config.cert_chain.display()
                )
            };
            // OpenSSL compares the key with the certificate as it is set
            builder.set_private_key(&key).map_err(mismatch)?;
            builder.check_private_key().map_err(mismatch)?;
            // trusted roots without asking for a certificate do nothing (`check` refuses
            // asking without roots)
            let asking = config.client_auth != ClientAuth::None;
            if let Some(path) = config.trust.as_deref().filter(|_| asking) {
                let roots = certificates(path, "the trusted certificates")?;
                let mut store = X509StoreBuilder::new().map_err(tls)?;
                let mut issuers = Stack::<X509Name>::new().map_err(tls)?;
                for root in roots {
                    issuers
                        .push(root.subject_name().to_owned().map_err(tls)?)
                        .map_err(tls)?;
                    store.add_cert(root).map_err(tls)?;
                }
                builder.set_cert_store(store.build());
                builder.set_client_ca_list(issuers);
                builder.set_verify(match config.client_auth {
                    ClientAuth::Needed => SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT,
                    _ => SslVerifyMode::PEER,
                });
                // a session resumed from another context's would skip the check
                builder.set_session_id_context(b"oxhaven").map_err(tls)?;
            }
            let refused_alpn = Ssl::new_ex_index::<bool>().map_err(tls)?;
            builder.set_alpn_select_callback(move |ssl, offered| {
                match select_next_proto(ALPN, offered) {
                    Some(chosen) => Ok(chosen),
                    None => {
                        ssl.set_ex_data(refused_alpn, true);
                        Err(AlpnError::NOACK)
                    }
                }
            });
            Ok(TlsAcceptor {
                acceptor: builder.build(),
                names,
                refused_alpn,
            })
        }

        pub(crate) fn names(&self) -> &CertificateNames {
            &self.names
        }

        /// The handshake of a connection, and what it then speaks; `Err` when it failed or
        /// the client offered no protocol the server speaks, and the connection is to be
        /// closed.
        ///
        /// Behavior: `org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory` (the default
        /// protocol `http/1.1` without ALPN, as Java's connector sets it; the connection
        /// closed when none of the offered protocols is served)
        pub(crate) async fn accept(
            &self,
            tcp: TcpStream,
        ) -> Result<(SslStream<TcpStream>, Protocol), String> {
            let ssl = Ssl::new(self.acceptor.context()).map_err(|e| e.to_string())?;
            let mut stream = SslStream::new(ssl, tcp).map_err(|e| e.to_string())?;
            Pin::new(&mut stream)
                .accept()
                .await
                .map_err(|e| format!("TLS handshake failed: {e}"))?;
            if stream.ssl().ex_data(self.refused_alpn).is_some() {
                return Err("the client offered no protocol the server speaks".to_string());
            }
            let protocol = match stream.ssl().selected_alpn_protocol() {
                Some(b"h2") => Protocol::Http2,
                _ => Protocol::Http1,
            };
            Ok((stream, protocol))
        }
    }
}

#[cfg(feature = "tls")]
pub(crate) use with_openssl::Protocol;

#[cfg(test)]
mod tests {
    use super::*;

    fn names(hosts: &[&str], addresses: &[&str]) -> CertificateNames {
        let mut names = CertificateNames::default();
        for host in hosts {
            names.add_name(host);
        }
        names.addresses = addresses.iter().map(|a| a.parse().unwrap()).collect();
        names
    }

    #[test]
    fn a_host_matches_a_name_a_wildcard_of_one_label_or_an_address() {
        let names = names(&["localhost", "*.Example.test"], &["127.0.0.1", "::1"]);
        for host in [
            "localhost",
            "a.example.test",
            "example.test",
            "127.0.0.1",
            "127.0.0.01",
            "[0:0:0:0:0:ffff:7f00:1]",
            "[::1]",
        ] {
            assert!(names.matches(host), "{host}");
        }
        for host in [
            "other",
            "a.b.example.test",
            "127.0.0.2",
            "0127.0.0.1",
            "127.1",
            "127.0.0.1.",
            "0x7f.0.0.1",
            "2130706433",
            "[::ffff:127.0.0.1]",
            "[::2]",
            "localhost.",
        ] {
            assert!(!names.matches(host), "{host}");
        }
    }

    #[test]
    fn the_server_name_is_the_host_without_its_port_or_the_local_address() {
        let local: SocketAddr = "10.1.2.3:443".parse().unwrap();
        let named = |host: &str| {
            let request = http::Request::get("/ide/")
                .header(http::header::HOST, host)
                .body(())
                .unwrap();
            server_name(&request, local)
        };
        assert_eq!(named("LocalHost:10000"), "localhost");
        assert_eq!(named("localhost"), "localhost");
        assert_eq!(named("[::1]:443"), "[::1]");
        assert_eq!(named("[::1]"), "[::1]");
        assert_eq!(named(""), "10.1.2.3");
        let h2 = http::Request::get("https://Example.test:8443/ide/")
            .body(())
            .unwrap();
        assert_eq!(server_name(&h2, local), "example.test");
        let none = http::Request::get("/ide/").body(()).unwrap();
        assert_eq!(server_name(&none, local), "10.1.2.3");
    }

    #[test]
    fn asking_for_client_certificates_needs_trusted_roots() {
        let mut config = TlsConfig {
            cert_chain: "chain.pem".into(),
            private_key: "key.pem".into(),
            trust: None,
            client_auth: ClientAuth::None,
        };
        assert_eq!(config.check(), Ok(()));
        for auth in [ClientAuth::Wanted, ClientAuth::Needed] {
            config.client_auth = auth;
            assert_eq!(
                config.check().unwrap_err(),
                "Trust material must be present when requesting mutual TLS"
            );
        }
        config.trust = Some("roots.pem".into());
        assert_eq!(config.check(), Ok(()));
        assert_eq!(ClientAuth::parse("WANTED"), Ok(ClientAuth::Wanted));
        assert!(ClientAuth::parse("wanted").is_err());
    }
}
