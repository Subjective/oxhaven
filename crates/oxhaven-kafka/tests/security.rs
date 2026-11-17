//! Consuming and producing over TLS and SASL, against the listeners `tests/broker.sh` gives
//! its broker: SSL with client certificates, SASL_SSL and SASL_PLAINTEXT with PLAIN,
//! SCRAM-SHA-256 and SCRAM-SHA-512. Ignored unless asked for: broker.sh makes the
//! certificates with openssl, passes their directory in `OXHAVEN_KAFKA_CERTS` and the ports
//! in `OXHAVEN_KAFKA_SSL`, `OXHAVEN_KAFKA_SASL_SSL` and `OXHAVEN_KAFKA_SASL_PLAINTEXT`, and
//! runs these. The broker's certificate names `localhost` only, so a connection to
//! `127.0.0.1` fails hostname verification unless it is turned off.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, AsArray, StringArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{
    consume, ConsumeOptions, InitialOffset, KafkaError, Offsets, ProduceOptions, ProduceSpec, Spec,
    TableType,
};
use oxhaven_testkit::ReferenceTable;

const PLAIN: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";
const SCRAM: &str = "org.apache.kafka.common.security.scram.ScramLoginModule";

/// Prints librdkafka's logs and error events with the output of a test that fails, so that
/// a failure in CI says what the client saw.
fn trace() {
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG);
    let _ = subscriber.try_init();
}

fn port(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is unset; run tests/broker.sh"))
}

fn certificate(file: &str) -> String {
    format!("{}/{file}", port("OXHAVEN_KAFKA_CERTS"))
}

type Properties = Vec<(String, String)>;

fn properties(pairs: &[(&str, &str)]) -> Properties {
    let pair = |(name, value): &(&str, &str)| ((*name).to_owned(), (*value).to_owned());
    pairs.iter().map(pair).collect()
}

/// The SSL listener, through `host`, trusting the test's authority and showing the
/// client's certificate from its PKCS#12 key store.
fn ssl(host: &str) -> Properties {
    properties(&[
        (
            "bootstrap.servers",
            &format!("{host}:{}", port("OXHAVEN_KAFKA_SSL")),
        ),
        ("security.protocol", "SSL"),
        ("ssl.truststore.type", "PEM"),
        ("ssl.truststore.location", &certificate("ca.pem")),
        ("ssl.keystore.type", "PKCS12"),
        ("ssl.keystore.location", &certificate("client.p12")),
        ("ssl.keystore.password", "keystore-secret"),
    ])
}

/// A SASL listener with `mechanism`, the login module and alice's `password`.
fn sasl(tls: bool, mechanism: &str, module: &str, password: &str) -> Properties {
    let jaas = format!(r#"{module} required username="alice" password="{password}";"#);
    let (listener, protocol, host) = if tls {
        ("OXHAVEN_KAFKA_SASL_SSL", "SASL_SSL", "localhost")
    } else {
        (
            "OXHAVEN_KAFKA_SASL_PLAINTEXT",
            "SASL_PLAINTEXT",
            "127.0.0.1",
        )
    };
    let mut properties = properties(&[
        ("bootstrap.servers", &format!("{host}:{}", port(listener))),
        ("security.protocol", protocol),
        ("sasl.mechanism", mechanism),
        ("sasl.jaas.config", &jaas),
    ]);
    if tls {
        properties.extend(self::properties(&[
            ("ssl.truststore.type", "PEM"),
            ("ssl.truststore.location", &certificate("ca.pem")),
        ]));
    }
    properties
}

fn with(mut properties: Properties, more: &[(&str, &str)]) -> Properties {
    for (name, value) in more {
        properties.retain(|(known, _)| known != name);
        properties.push(((*name).to_owned(), (*value).to_owned()));
    }
    properties
}

fn without(mut properties: Properties, names: &[&str]) -> Properties {
    properties.retain(|(name, _)| !names.contains(&name.as_str()));
    properties
}

fn fresh(what: &str) -> String {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    format!("oxhaven_{what}_{}", now.unwrap().as_nanos())
}

fn texts(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn consume_options(config: &Properties, topic: &str) -> ConsumeOptions {
    ConsumeOptions {
        config: config.clone(),
        topic: topic.to_owned(),
        partitions: None,
        offsets: Offsets::Everywhere(InitialOffset::Beginning),
        key: Spec::Ignore,
        value: Spec::Simple {
            column: Some("V".to_owned()),
            data_type: Some(ColumnType::String),
        },
        table_type: TableType::Append,
    }
}

fn produce(config: &Properties, topic: &str, values: &[&str]) -> Result<(), KafkaError> {
    let definition = TableDefinition::new(vec![ColumnDefinition::new("V", ColumnType::String)]);
    let table = ReferenceTable::new_static(
        definition.unwrap(),
        RowSet::flat(values.len() as u64),
        vec![texts(values)],
    );
    let options = ProduceOptions {
        config: with(config.clone(), &[("max.block.ms", "20000")]),
        topic: topic.to_owned(),
        partition: Some(0),
        topic_column: None,
        partition_column: None,
        timestamp_column: None,
        key: ProduceSpec::Ignore,
        value: ProduceSpec::Simple("V".to_owned()),
        last_by: false,
        publish_initial: true,
    };
    oxhaven_kafka::produce(table, &options).map(|_| ())
}

/// The values of `topic`, read over `config` until there are `rows`.
fn values(config: &Properties, topic: &str, rows: u64) -> Vec<String> {
    let graph = SteppedUpdateGraph::new();
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let table = consume(shared, &consume_options(config, topic)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while table.row_set().size() != rows && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
    }
    assert!(!table.is_failed(), "the table failed");
    let index = table.definition().index_of("V").unwrap();
    let column = table.column(index).chunk(&table.row_set()).unwrap();
    let values = column.as_string::<i32>().iter();
    values.map(|value| value.unwrap().to_owned()).collect()
}

/// Records go out and come back over `config`, and the topic is listed.
fn round_trip(config: &Properties) {
    trace();
    let topic = fresh("secure");
    produce(config, &topic, &["one", "two"]).unwrap();
    assert_eq!(values(config, &topic, 2), ["one", "two"]);
    let listed = oxhaven_kafka::topics(config).unwrap();
    assert!(listed.contains(&topic), "{listed:?}");
}

/// What `consume`, `produce` and `topics` say over `config`, which the broker refuses:
/// each fails, and in a few seconds, not after `default.api.timeout.ms`.
fn refused(config: &Properties) -> [String; 3] {
    trace();
    let timed = |call: &dyn Fn() -> String| {
        let started = Instant::now();
        let error = call();
        (error, started.elapsed())
    };
    let consumed = timed(&|| {
        let graph = SteppedUpdateGraph::new();
        let shared: Arc<dyn UpdateGraph> = graph.clone();
        let consumed = consume(shared, &consume_options(config, "oxhaven_one"));
        consumed.err().expect("the consumer is refused").to_string()
    });
    let produced = timed(&|| {
        let produced = produce(config, &fresh("refused"), &["x"]);
        produced.expect_err("the producer is refused").to_string()
    });
    let listed = timed(&|| {
        let listed = oxhaven_kafka::topics(config);
        listed.expect_err("the listing is refused").to_string()
    });
    let calls = [
        ("consume", consumed),
        ("produce", produced),
        ("topics", listed),
    ];
    let total: Duration = calls.iter().map(|(_, (_, elapsed))| *elapsed).sum();
    let each: Vec<String> = calls
        .iter()
        .map(|(call, (error, elapsed))| format!("{call} after {elapsed:?}: {error}"))
        .collect();
    assert!(
        total < Duration::from_secs(20),
        "refused after {total:?}\n{}",
        each.join("\n")
    );
    calls.map(|(_, (error, _))| error)
}

fn assert_all_contain(errors: &[String; 3], words: &[&str]) {
    for error in errors {
        for word in words {
            assert!(error.contains(word), "{error:?} lacks {word:?}");
        }
    }
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn ssl_works_with_each_kind_of_key_store() {
    // A PKCS#12 key store and a PEM trust store file.
    round_trip(&ssl("localhost"));
    // The key and its chain in one PEM file.
    let pem = with(
        without(ssl("localhost"), &["ssl.keystore.password"]),
        &[
            ("ssl.keystore.type", "PEM"),
            ("ssl.keystore.location", &certificate("client-keystore.pem")),
        ],
    );
    round_trip(&pem);
    // The key, its chain and the trusted certificates as text.
    let read = |file: &str| std::fs::read_to_string(certificate(file)).unwrap();
    let text = with(
        without(
            ssl("localhost"),
            &[
                "ssl.keystore.location",
                "ssl.keystore.password",
                "ssl.truststore.location",
            ],
        ),
        &[
            ("ssl.keystore.type", "PEM"),
            ("ssl.keystore.key", &read("client.key")),
            ("ssl.keystore.certificate.chain", &read("client.pem")),
            ("ssl.truststore.certificates", &read("ca.pem")),
            ("security.protocol", "ssl"),
        ],
    );
    round_trip(&text);
    // Java's default key store type, JKS, reads a PKCS#12 file.
    round_trip(&without(ssl("localhost"), &["ssl.keystore.type"]));
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn hostname_verification_is_on_unless_turned_off() {
    let errors = refused(&ssl("127.0.0.1"));
    assert_all_contain(&errors, &["SslAuthenticationException"]);
    let unverified = with(
        ssl("127.0.0.1"),
        &[("ssl.endpoint.identification.algorithm", "")],
    );
    round_trip(&unverified);
    let upper = with(
        ssl("localhost"),
        &[("ssl.endpoint.identification.algorithm", "HTTPS")],
    );
    round_trip(&upper);
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn an_untrusted_broker_or_a_missing_client_certificate_fails_at_once() {
    let untrusted = with(
        ssl("localhost"),
        &[("ssl.truststore.location", &certificate("other-ca.pem"))],
    );
    let errors = refused(&untrusted);
    assert_all_contain(
        &errors,
        &["SslAuthenticationException", "certificate verify failed"],
    );
    // The SSL listener asks for a client certificate.
    let anonymous = without(
        ssl("localhost"),
        &[
            "ssl.keystore.type",
            "ssl.keystore.location",
            "ssl.keystore.password",
        ],
    );
    let errors = refused(&anonymous);
    assert_all_contain(&errors, &["SslAuthenticationException"]);
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn sasl_plain_and_scram_work_with_and_without_tls() {
    for tls in [false, true] {
        round_trip(&sasl(tls, "PLAIN", PLAIN, "alice-secret"));
        round_trip(&sasl(tls, "SCRAM-SHA-256", SCRAM, "alice-secret"));
        round_trip(&sasl(tls, "SCRAM-SHA-512", SCRAM, "alice-secret"));
    }
    // Java takes the credentials of either module for either mechanism.
    round_trip(&sasl(false, "SCRAM-SHA-512", PLAIN, "alice-secret"));
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_wrong_password_fails_at_once() {
    for (tls, mechanism, module) in [
        (false, "PLAIN", PLAIN),
        (true, "PLAIN", PLAIN),
        (false, "SCRAM-SHA-256", SCRAM),
        (true, "SCRAM-SHA-512", SCRAM),
    ] {
        let errors = refused(&sasl(tls, mechanism, module, "wrong"));
        assert_all_contain(&errors, &["SaslAuthenticationException"]);
    }
    // The broker's own words, as Java's exception carries them.
    let errors = refused(&sasl(false, "PLAIN", PLAIN, "wrong"));
    assert_all_contain(&errors, &["Invalid username or password"]);
}
