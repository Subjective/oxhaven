//! The client over TLS, against this crate's server behind a TLS relay whose certificates
//! are made for each run (`tests/tls/support.rs`). The system's roots are tried in
//! `tests/tls_system_roots.rs`, which sets the environment OpenSSL reads them from.

use std::sync::Arc;

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_testkit::{
    assert_tables_equal, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable,
};

use super::tests::{caught_up, empty_of, publish, random_types, PROMPT};
use super::*;
use crate::test_support::tls::{tls_relay, Authority};
use crate::test_support::{TestServer, PSK};

fn tls_config(host: &str, port: u16, roots: Option<&[u8]>) -> ClientConfig {
    let mut config = ClientConfig::new(host, port).with_psk(PSK);
    config.use_tls = true;
    config.tls_root_certs = roots.map(<[u8]>::to_vec);
    config
}

async fn refusal(config: ClientConfig) -> ClientError {
    BarrageSession::connect(config, SteppedUpdateGraph::new(), None)
        .await
        .err()
        .expect("the connection should have been refused")
}

/// With the given roots, a session over TLS snapshots and subscribes as over plaintext, by
/// address and by name, and follows a ticking table change for change.
#[tokio::test(flavor = "multi_thread")]
async fn snapshots_and_subscriptions_arrive_over_tls() {
    let server = TestServer::start().await;
    let authority = Authority::new(&["localhost", "127.0.0.1"]);
    let port = tls_relay(server.address, &authority, true).await;
    let mut random = RandomUpdates::new(3);
    let definition = random_types();
    let values = definition
        .columns()
        .iter()
        .map(|column| random.values(&column.column_type, 300))
        .collect::<Vec<_>>();
    let fixed: Arc<dyn Table> = oxhaven_engine::table::in_memory_table(
        definition.clone(),
        &[super::tests::batch_of(&definition, values)],
    )
    .unwrap();
    let fixed_ticket = publish(&server, "fixed", fixed.clone());
    for host in ["127.0.0.1", "localhost"] {
        let local = SteppedUpdateGraph::new();
        let session = BarrageSession::connect(
            tls_config(host, port, Some(&authority.pem)),
            local.clone(),
            None,
        )
        .await
        .unwrap();
        let snapshot = session
            .snapshot(&fixed_ticket, SnapshotOptions::default())
            .await
            .unwrap();
        assert_tables_equal(fixed.as_ref(), snapshot.as_ref());
        let subscribed = session.subscribe(&fixed_ticket, PROMPT).await.unwrap();
        assert_tables_equal(fixed.as_ref(), subscribed.as_ref());

        for seed in test_seeds(0..5) {
            let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
            let scripted = ScriptedTable::new(
                graph,
                definition.clone(),
                RowSet::empty(),
                empty_of(&definition),
            );
            let source = scripted.table();
            let mut random = RandomUpdates::new(seed);
            scripted.push(random.next_change(source.as_ref(), 40));
            server.state.graph.run_cycle();
            let ticket = publish(&server, "ticking", source.clone());
            let copy = session.subscribe(&ticket, PROMPT).await.unwrap();
            let checker = ConsistencyChecker::attach(copy.clone());
            caught_up(&local, source.as_ref(), copy.as_ref()).await;
            for _ in 0..8 {
                scripted.push(random.next_change(source.as_ref(), 25));
                server.state.graph.run_cycle();
                caught_up(&local, source.as_ref(), copy.as_ref()).await;
                checker.assert_consistent();
            }
            assert!(checker.updates_seen() > 0, "seed {seed}");
        }
        session.close().await.unwrap();
    }
}

/// A certificate is trusted only through the given roots, which replace the system's: a
/// server signed by another authority, or any test server when no roots are given (the
/// system's roots do not hold this run's authority), is refused at the handshake.
#[tokio::test(flavor = "multi_thread")]
async fn an_untrusted_certificate_is_refused() {
    let server = TestServer::start().await;
    let authority = Authority::new(&["127.0.0.1"]);
    let port = tls_relay(server.address, &authority, true).await;
    let other = Authority::new(&["127.0.0.1"]);
    for roots in [None, Some(other.pem.as_slice())] {
        let refused = refusal(tls_config("127.0.0.1", port, roots)).await;
        let ClientError::Connect(text) = &refused else {
            panic!("{refused}");
        };
        assert!(
            text.contains("the server's certificate is not trusted"),
            "{refused}"
        );
    }
    // two roots, one of them the right one
    let mut both = other.pem.clone();
    both.extend_from_slice(&authority.pem);
    BarrageSession::connect(
        tls_config("127.0.0.1", port, Some(&both)),
        SteppedUpdateGraph::new(),
        None,
    )
    .await
    .unwrap()
    .close()
    .await
    .unwrap();
}

/// The name the client connects to must be one the certificate names: an address against a
/// certificate for names only, a name it does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_host_name_is_refused() {
    let server = TestServer::start().await;
    let authority = Authority::new(&["other.example", "127.0.0.2"]);
    let port = tls_relay(server.address, &authority, true).await;
    for host in ["127.0.0.1", "localhost"] {
        let refused = refusal(tls_config(host, port, Some(&authority.pem))).await;
        let ClientError::Connect(text) = &refused else {
            panic!("{refused}");
        };
        assert!(text.contains("not trusted"), "{refused}");
        assert!(text.contains("mismatch"), "{refused}");
    }
}

/// A server that does not agree to `h2` by ALPN is refused as gRPC's Netty client refuses
/// it; a server that does not speak TLS fails the handshake; roots that are not PEM are
/// refused before anything is sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_without_h2_or_tls_and_unreadable_roots_are_refused() {
    let server = TestServer::start().await;
    let authority = Authority::new(&["127.0.0.1"]);
    let port = tls_relay(server.address, &authority, false).await;
    let refused = refusal(tls_config("127.0.0.1", port, Some(&authority.pem))).await;
    assert!(
        refused
            .to_string()
            .contains("Failed ALPN negotiation: Unable to find compatible protocol"),
        "{refused}"
    );

    let plaintext = server.address.port();
    let refused = refusal(tls_config("127.0.0.1", plaintext, Some(&authority.pem))).await;
    let ClientError::Connect(text) = &refused else {
        panic!("{refused}");
    };
    assert!(text.contains("TLS handshake failed"), "{refused}");

    for roots in [b"pem".as_slice(), b"".as_slice()] {
        let refused = refusal(tls_config("127.0.0.1", plaintext, Some(roots))).await;
        assert!(matches!(refused, ClientError::Config(_)), "{refused}");
    }
}
