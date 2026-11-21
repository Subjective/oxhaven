//! Certificates made for a test run, with keys that exist only in memory, and a TLS relay
//! that presents them. Shared by the TLS tests of the Barrage client (`src/barrage_client/
//! tls_tests.rs`) and `tests/tls_system_roots.rs`.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use openssl::asn1::Asn1Time;
use openssl::bn::{BigNum, MsbOption};
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::ssl::{select_next_proto, AlpnError, Ssl, SslAcceptor, SslMethod};
use openssl::x509::extension::{BasicConstraints, KeyUsage, SubjectAlternativeName};
use openssl::x509::{X509Builder, X509NameBuilder, X509};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio_openssl::SslStream;

/// A key pair on P-256.
fn key() -> PKey<Private> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
    PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap()
}

/// A certificate for `key` named `name`, valid for a day: a certificate authority signed by
/// itself when `issuer` is `None`, otherwise a server certificate for the DNS names and
/// addresses `names` signed by `issuer`.
fn certificate(
    name: &str,
    key: &PKey<Private>,
    issuer: Option<(&X509, &PKey<Private>)>,
    names: &[&str],
) -> X509 {
    let mut subject = X509NameBuilder::new().unwrap();
    subject.append_entry_by_nid(Nid::COMMONNAME, name).unwrap();
    let subject = subject.build();
    let mut builder = X509Builder::new().unwrap();
    builder.set_version(2).unwrap();
    let mut serial = BigNum::new().unwrap();
    serial.rand(64, MsbOption::MAYBE_ZERO, false).unwrap();
    builder
        .set_serial_number(&serial.to_asn1_integer().unwrap())
        .unwrap();
    builder.set_subject_name(&subject).unwrap();
    builder.set_pubkey(key).unwrap();
    builder
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    match issuer {
        None => {
            builder.set_issuer_name(&subject).unwrap();
            let constraints = BasicConstraints::new().critical().ca().build().unwrap();
            builder.append_extension(constraints).unwrap();
            let usage = KeyUsage::new()
                .critical()
                .key_cert_sign()
                .crl_sign()
                .build()
                .unwrap();
            builder.append_extension(usage).unwrap();
            builder.sign(key, MessageDigest::sha256()).unwrap();
        }
        Some((issuer, issuer_key)) => {
            builder.set_issuer_name(issuer.subject_name()).unwrap();
            let mut alternatives = SubjectAlternativeName::new();
            for name in names {
                if name.parse::<std::net::IpAddr>().is_ok() {
                    alternatives.ip(name);
                } else {
                    alternatives.dns(name);
                }
            }
            let alternatives = alternatives
                .build(&builder.x509v3_context(Some(issuer), None))
                .unwrap();
            builder.append_extension(alternatives).unwrap();
            builder.sign(issuer_key, MessageDigest::sha256()).unwrap();
        }
    }
    builder.build()
}

/// A certificate authority, as the PEM a client is given, and a server certificate and key
/// it signed for `names`.
pub struct Authority {
    pub pem: Vec<u8>,
    pub server: X509,
    pub server_key: PKey<Private>,
}

impl Authority {
    pub fn new(names: &[&str]) -> Self {
        let ca_key = key();
        // a name of its own: OpenSSL looks an issuer up by its name
        static COUNT: AtomicU32 = AtomicU32::new(0);
        let count = COUNT.fetch_add(1, Ordering::Relaxed);
        let ca = certificate(
            &format!("oxhaven test authority {count}"),
            &ca_key,
            None,
            &[],
        );
        let server_key = key();
        let server = certificate(
            "oxhaven test server",
            &server_key,
            Some((&ca, &ca_key)),
            names,
        );
        Authority {
            pem: ca.to_pem().unwrap(),
            server,
            server_key,
        }
    }
}

/// A TLS relay to `to` with the server certificate of `authority`, offering `h2` by ALPN
/// unless `alpn` is false.
pub async fn tls_relay(to: SocketAddr, authority: &Authority, alpn: bool) -> u16 {
    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).unwrap();
    acceptor.set_private_key(&authority.server_key).unwrap();
    acceptor.set_certificate(&authority.server).unwrap();
    if alpn {
        acceptor.set_alpn_select_callback(|_, offered| {
            select_next_proto(b"\x02h2", offered).ok_or(AlpnError::NOACK)
        });
    }
    let acceptor = Arc::new(acceptor.build());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((near, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let mut near = SslStream::new(Ssl::new(acceptor.context()).unwrap(), near).unwrap();
                if Pin::new(&mut near).accept().await.is_err() {
                    return;
                }
                let Ok(mut far) = TcpStream::connect(to).await else {
                    return;
                };
                let _ = copy_bidirectional(&mut near, &mut far).await;
            });
        }
    });
    port
}
