//! Without `tls_root_certs`, a TLS session trusts the system's roots, which OpenSSL reads
//! from `SSL_CERT_FILE` and `SSL_CERT_DIR` when they are set; with them, it trusts those
//! alone. A test binary of its own, because it sets those variables for the process.

#[path = "tls/support.rs"]
mod support;

use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_server::barrage_client::{BarrageSession, ClientConfig, ClientError};
use oxhaven_server::session::{AuthHandler, AuthHandlers};
use oxhaven_server::{serve, ServerConfig, ServerState};
use support::{tls_relay, Authority};

fn tls(port: u16, roots: Option<&[u8]>) -> ClientConfig {
    let mut config = ClientConfig::new("127.0.0.1", port);
    config.use_tls = true;
    config.tls_root_certs = roots.map(<[u8]>::to_vec);
    config
}

async fn connect(config: ClientConfig) -> Result<BarrageSession, ClientError> {
    BarrageSession::connect(config, SteppedUpdateGraph::new(), None).await
}

#[test]
fn the_system_roots_are_trusted_unless_roots_are_given() {
    let authority = Authority::new(&["127.0.0.1"]);
    let other = Authority::new(&["127.0.0.1"]);
    let directory = std::env::temp_dir().join(format!("oxhaven-tls-roots-{}", std::process::id()));
    let empty = directory.join("certs");
    std::fs::create_dir_all(&empty).unwrap();
    let file = directory.join("roots.pem");
    std::fs::write(&file, &authority.pem).unwrap();
    // before any thread runs that could read the environment
    std::env::set_var("SSL_CERT_FILE", &file);
    std::env::set_var("SSL_CERT_DIR", &empty);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = ServerState::new(ServerConfig {
            auth_handlers: AuthHandlers::new(vec![AuthHandler::Anonymous]).unwrap(),
            token_expire_ms: 300_000,
            target_cycle_duration: None,
        })
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve(state, listener, std::future::pending()));
        let port = tls_relay(address, &authority, true).await;

        // the system's roots, which now hold the authority
        connect(tls(port, None))
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        // the given roots replace them
        let refused = connect(tls(port, Some(&other.pem))).await.err().unwrap();
        assert!(
            refused
                .to_string()
                .contains("the server's certificate is not trusted"),
            "{refused}"
        );
        connect(tls(port, Some(&authority.pem)))
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    });
    let _ = std::fs::remove_dir_all(&directory);
}
