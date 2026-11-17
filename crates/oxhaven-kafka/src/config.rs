//! The properties of a Java consumer, as librdkafka takes them.
//!
//! Java hands the whole `Properties` to `KafkaConsumer`, which acts on the names in
//! `ConsumerConfig` and logs and ignores the rest. librdkafka refuses a name it does not
//! know, has other names for some settings, and other defaults for a few. So a property
//! reaches the client only if it is in [`PASSED`]; the Java defaults that differ from
//! librdkafka's are set first. `docs/KAFKA.md`, "Consumer properties", has the table.

use std::time::Duration;

use rdkafka::ClientConfig;

use crate::{not_ported, property, KafkaError};

/// Java's name of a property, librdkafka's name of it, and how its value is rewritten.
struct Passed {
    java: &'static str,
    client: &'static str,
    value: fn(&str) -> String,
}

fn same(value: &str) -> String {
    value.to_owned()
}

/// Java writes "the default of the operating system" as -1 and librdkafka as 0.
fn buffer_size(value: &str) -> String {
    if value.trim() == "-1" {
        "0".to_owned()
    } else {
        value.to_owned()
    }
}

const fn passed(java: &'static str) -> Passed {
    Passed {
        java,
        client: java,
        value: same,
    }
}

const fn renamed(java: &'static str, client: &'static str) -> Passed {
    Passed {
        java,
        client,
        value: same,
    }
}

/// The properties of `org.apache.kafka.clients.consumer.ConsumerConfig` (and of
/// `AdminClientConfig`, for the listing of topics) that librdkafka has a setting for.
/// `group.id`, `enable.auto.commit` and `auto.offset.reset` are not here: [`ClientSettings`]
/// reads them, and `crate::security` the properties of TLS and SASL.
const PASSED: &[Passed] = &[
    passed("bootstrap.servers"),
    passed("client.id"),
    passed("client.rack"),
    passed("group.instance.id"),
    passed("auto.commit.interval.ms"),
    passed("fetch.min.bytes"),
    passed("fetch.max.bytes"),
    passed("max.partition.fetch.bytes"),
    renamed("fetch.max.wait.ms", "fetch.wait.max.ms"),
    passed("check.crcs"),
    passed("isolation.level"),
    passed("allow.auto.create.topics"),
    passed("metadata.max.age.ms"),
    passed("reconnect.backoff.ms"),
    passed("reconnect.backoff.max.ms"),
    passed("retry.backoff.ms"),
    passed("retry.backoff.max.ms"),
    passed("connections.max.idle.ms"),
    passed("socket.connection.setup.timeout.ms"),
    renamed("request.timeout.ms", "socket.timeout.ms"),
    Passed {
        java: "receive.buffer.bytes",
        client: "socket.receive.buffer.bytes",
        value: buffer_size,
    },
    Passed {
        java: "send.buffer.bytes",
        client: "socket.send.buffer.bytes",
        value: buffer_size,
    },
];

/// Where a Java consumer's default is not librdkafka's.
///
/// Behavior: org.apache.kafka.clients.consumer.ConsumerConfig, the defaults of
/// `isolation.level`, `allow.auto.create.topics`, `check.crcs` and `request.timeout.ms`
const JAVA_DEFAULTS: &[(&str, &str)] = &[
    ("isolation.level", "read_uncommitted"),
    ("allow.auto.create.topics", "true"),
    ("check.crcs", "true"),
    ("socket.timeout.ms", "30000"),
];

/// The group librdkafka is given when the properties name none. librdkafka assigns
/// partitions only to a consumer that has a group; this one is never committed to and
/// never asked for offsets.
const NO_GROUP: &str = "oxhaven-no-group";

/// Behavior: org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Reset {
    Earliest,
    Latest,
    None,
}

/// What the properties say, beyond the settings that are handed to librdkafka as they are.
pub(crate) struct ClientSettings {
    pub client: ClientConfig,
    /// The consumer group of `group.id`, whose committed offsets `DONT_SEEK` starts from
    /// and to which positions are committed.
    pub group: Option<String>,
    pub reset: Reset,
    /// Behavior: `ConsumerConfig#DEFAULT_API_TIMEOUT_MS_CONFIG`: how long the calls that
    /// ask the broker for the partitions of a topic and for offsets may take.
    pub api_timeout: Duration,
    /// Behavior: `ConsumerConfig#MAX_POLL_RECORDS_CONFIG`
    pub max_poll_records: usize,
}

fn config_exception(name: &str, value: &str, why: &str) -> KafkaError {
    KafkaError::Invalid(format!(
        "org.apache.kafka.common.config.ConfigException: Invalid value {value} for \
         configuration {name}: {why}"
    ))
}

fn number(config: &[(String, String)], name: &str, default: i64) -> Result<i64, KafkaError> {
    let Some(text) = property(config, name) else {
        return Ok(default);
    };
    text.trim()
        .parse::<i32>()
        .map(i64::from)
        .map_err(|_| config_exception(name, text, &format!("Not a number of type {}", "INT")))
}

fn boolean(config: &[(String, String)], name: &str) -> Result<Option<bool>, KafkaError> {
    // Behavior: org.apache.kafka.common.config.ConfigDef#parseType for BOOLEAN
    match property(config, name).map(str::trim) {
        None => Ok(None),
        Some(text) if text.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(text) if text.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(text) => Err(config_exception(
            name,
            text,
            "Expected value to be either true or false",
        )),
    }
}

impl ClientSettings {
    /// Behavior: org.apache.kafka.clients.consumer.ConsumerConfig, as
    /// `KafkaConsumer#KafkaConsumer(Properties, Deserializer, Deserializer)` reads the
    /// properties `io.deephaven.kafka.ingest.KafkaIngester` passes it
    pub fn new(config: &[(String, String)]) -> Result<Self, KafkaError> {
        require_bootstrap_servers(config)?;
        let group = property(config, "group.id").map(str::to_owned);
        if group
            .as_deref()
            .is_some_and(|group| group.trim().is_empty())
        {
            // Behavior: org.apache.kafka.clients.consumer.internals.ConsumerUtils, where
            // the group of a consumer is read
            return Err(KafkaError::Invalid(
                "org.apache.kafka.common.errors.InvalidGroupIdException: The configured \
                 group.id should not be an empty string or whitespace."
                    .to_owned(),
            ));
        }
        if let Some(level) = property(config, "isolation.level") {
            if level != "read_committed" && level != "read_uncommitted" {
                let why = "String must be one of: read_committed, read_uncommitted";
                return Err(config_exception("isolation.level", level, why));
            }
        }
        let reset = match property(config, "auto.offset.reset") {
            None => Reset::Latest,
            // As Java, in lower case only.
            Some(text) => match text {
                "earliest" => Reset::Earliest,
                "latest" => Reset::Latest,
                "none" => Reset::None,
                // Java also takes `by_duration:<ISO 8601 duration>`.
                other if other.starts_with("by_duration:") => {
                    return Err(not_ported(
                        "org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy\
                         #timestamp",
                        &format!("auto.offset.reset={text}"),
                    ))
                }
                _ => {
                    return Err(config_exception(
                        "auto.offset.reset",
                        text,
                        &format!(
                            "Invalid value `{text}` for configuration auto.offset.reset. \
                             The value must be either 'earliest', 'latest', 'none' or of \
                             the format 'by_duration:<PnDTnHnMn.nS.>'."
                        ),
                    ))
                }
            },
        };
        // Behavior: ConsumerConfig#maybeOverrideEnableAutoCommit: without a group nothing
        // is committed, and asking for it is an error.
        let auto_commit = match (boolean(config, "enable.auto.commit")?, &group) {
            (Some(true), None) => {
                return Err(KafkaError::Invalid(
                    "org.apache.kafka.common.errors.InvalidConfigurationException: \
                     enable.auto.commit cannot be set to true when default group id (null) \
                     is used."
                        .to_owned(),
                ))
            }
            (choice, Some(_)) => choice.unwrap_or(true),
            (_, None) => false,
        };
        let api_timeout = number(config, "default.api.timeout.ms", 60_000)?;
        let max_poll_records = number(config, "max.poll.records", 500)?;
        if max_poll_records < 1 {
            let text = max_poll_records.to_string();
            let why = "Value must be at least 1";
            return Err(config_exception("max.poll.records", &text, why));
        }

        let mut client = ClientConfig::new();
        for (name, value) in JAVA_DEFAULTS {
            client.set(*name, *value);
        }
        // Later pairs replace earlier ones, as in the `Properties` Java is given.
        for (name, value) in config {
            if let Some(passed) = PASSED.iter().find(|passed| passed.java == name) {
                client.set(passed.client, (passed.value)(value));
            }
        }
        crate::security::translate(config, &mut client)?;
        client.set("group.id", group.as_deref().unwrap_or(NO_GROUP));
        client.set("enable.auto.commit", auto_commit.to_string());
        // The positions to commit are recorded by `client::ClientSource`, which also
        // records the ones no record has been read from yet.
        client.set("enable.auto.offset.store", "false");
        // The position is always a number by the time a partition is assigned (see
        // `client::Client::start`); this is for an offset that goes out of range later.
        client.set(
            "auto.offset.reset",
            match reset {
                Reset::Earliest => "earliest",
                Reset::Latest => "latest",
                Reset::None => "error",
            },
        );
        Ok(Self {
            client,
            group,
            reset,
            api_timeout: Duration::from_millis(api_timeout.max(0) as u64),
            max_poll_records: max_poll_records as usize,
        })
    }
}

fn require_bootstrap_servers(config: &[(String, String)]) -> Result<(), KafkaError> {
    if property(config, "bootstrap.servers").is_none() {
        return Err(KafkaError::Invalid(
            "org.apache.kafka.common.config.ConfigException: Missing required \
             configuration \"bootstrap.servers\" which has no default value."
                .to_owned(),
        ));
    }
    Ok(())
}

/// Java's batch size of 0 turns batching off; librdkafka's smallest is 1.
fn batch_size(value: &str) -> String {
    if value.trim() == "0" {
        "1".to_owned()
    } else {
        value.to_owned()
    }
}

/// The properties of `org.apache.kafka.clients.producer.ProducerConfig` that librdkafka
/// has a setting for. `acks`, `enable.idempotence`, `max.block.ms`, `compression.type`
/// and the ones refused are not here: [`ProducerSettings`] reads them.
const PRODUCER_PASSED: &[Passed] = &[
    passed("bootstrap.servers"),
    passed("client.id"),
    passed("linger.ms"),
    passed("retries"),
    passed("retry.backoff.ms"),
    passed("retry.backoff.max.ms"),
    passed("max.in.flight.requests.per.connection"),
    passed("metadata.max.age.ms"),
    passed("reconnect.backoff.ms"),
    passed("reconnect.backoff.max.ms"),
    passed("connections.max.idle.ms"),
    passed("socket.connection.setup.timeout.ms"),
    passed("request.timeout.ms"),
    renamed("delivery.timeout.ms", "message.timeout.ms"),
    renamed("max.request.size", "message.max.bytes"),
    Passed {
        java: "batch.size",
        client: "batch.size",
        value: batch_size,
    },
    Passed {
        java: "receive.buffer.bytes",
        client: "socket.receive.buffer.bytes",
        value: buffer_size,
    },
    Passed {
        java: "send.buffer.bytes",
        client: "socket.send.buffer.bytes",
        value: buffer_size,
    },
];

/// Where a Java producer's default is not librdkafka's. Keys are partitioned as Java's
/// default partitioner does (`murmur2` of the key's bytes, positive, modulo the number of
/// partitions; `murmur2_random` is librdkafka's copy of it), and records without a key
/// go to a partition chosen at random, sticky for a while in both.
///
/// Behavior: org.apache.kafka.clients.producer.ProducerConfig, the defaults of
/// `delivery.timeout.ms`, `max.request.size` and `batch.size`;
/// org.apache.kafka.clients.producer.internals.BuiltInPartitioner#partitionForKey
const PRODUCER_JAVA_DEFAULTS: &[(&str, &str)] = &[
    ("message.timeout.ms", "120000"),
    ("message.max.bytes", "1048576"),
    ("batch.size", "16384"),
    ("partitioner", "murmur2_random"),
];

/// Producer properties that load a Java class which changes what is sent.
const PRODUCER_REFUSED: &[(&str, &str)] = &[
    (
        "partitioner.class",
        "org.apache.kafka.clients.producer.Partitioner",
    ),
    (
        "interceptor.classes",
        "org.apache.kafka.clients.producer.ProducerInterceptor",
    ),
];

/// What the properties of a producer say.
pub(crate) struct ProducerSettings {
    pub client: ClientConfig,
    /// Behavior: `ProducerConfig#MAX_BLOCK_MS_CONFIG`: how long a send waits for the
    /// topic's metadata.
    pub max_block: Duration,
}

impl ProducerSettings {
    /// Behavior: org.apache.kafka.clients.producer.ProducerConfig, as
    /// `KafkaProducer#KafkaProducer(Properties, Serializer, Serializer)` reads the
    /// properties `io.deephaven.kafka.publish.PublishToKafka` passes it. The serializers
    /// are the specs', so `key.serializer` and `value.serializer` have no effect.
    pub fn new(config: &[(String, String)]) -> Result<Self, KafkaError> {
        require_bootstrap_servers(config)?;
        for (name, java) in PRODUCER_REFUSED {
            if let Some(value) = property(config, name) {
                return Err(not_ported(java, &format!("{name}={value}")));
            }
        }
        if let Some(id) = property(config, "transactional.id") {
            // The reference never calls `initTransactions`, so its every send fails.
            return Err(not_ported(
                "org.apache.kafka.clients.producer.KafkaProducer#initTransactions",
                &format!(
                    "transactional.id={id}: the reference never initializes transactions, \
                     so each record it sends fails"
                ),
            ));
        }
        let acks = match property(config, "acks").map(str::trim) {
            None => "all",
            Some(acks @ ("all" | "-1" | "0" | "1")) => acks,
            Some(acks) => {
                let why = "String must be one of: all, -1, 0, 1";
                return Err(config_exception("acks", acks, why));
            }
        };
        let compression = match property(config, "compression.type").map(str::trim) {
            None => "none",
            Some(codec @ ("none" | "gzip" | "snappy" | "lz4" | "zstd")) => codec,
            Some(codec) => {
                let why = "String must be one of: none, gzip, snappy, lz4, zstd";
                return Err(config_exception("compression.type", codec, why));
            }
        };
        let max_block = match property(config, "max.block.ms") {
            None => 60_000,
            Some(text) => text
                .trim()
                .parse::<i64>()
                .map_err(|_| config_exception("max.block.ms", text, "Not a number of type LONG"))?,
        };
        // Behavior: ProducerConfig#postProcessAndValidateIdempotenceConfigs: on unless
        // asked otherwise, and quietly off where `acks`, `retries` or the number of
        // requests in flight do not allow it, unless it was asked for.
        let all_acks = acks == "all" || acks == "-1";
        let idempotence = match boolean(config, "enable.idempotence")? {
            Some(true) if !all_acks => {
                return Err(KafkaError::Invalid(
                    "org.apache.kafka.common.config.ConfigException: Must set acks to all in \
                     order to use the idempotent producer. Otherwise we cannot guarantee \
                     idempotence."
                        .to_owned(),
                ))
            }
            Some(chosen) => chosen,
            None => {
                let retries = property(config, "retries").map(str::trim);
                let in_flight = property(config, "max.in.flight.requests.per.connection");
                let in_flight = in_flight.and_then(|text| text.trim().parse::<i64>().ok());
                all_acks && retries != Some("0") && in_flight.is_none_or(|count| count <= 5)
            }
        };

        let mut client = ClientConfig::new();
        for (name, value) in PRODUCER_JAVA_DEFAULTS {
            client.set(*name, *value);
        }
        for (name, value) in config {
            if let Some(passed) = PRODUCER_PASSED.iter().find(|passed| passed.java == name) {
                client.set(passed.client, (passed.value)(value));
            }
        }
        crate::security::translate(config, &mut client)?;
        client.set("acks", acks);
        client.set("compression.type", compression);
        client.set("enable.idempotence", idempotence.to_string());
        Ok(Self {
            client,
            max_block: Duration::from_millis(max_block.max(0) as u64),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let pair = |(name, value): &(&str, &str)| ((*name).to_owned(), (*value).to_owned());
        pairs.iter().map(pair).collect()
    }

    const BROKER: (&str, &str) = ("bootstrap.servers", "127.0.0.1:1");

    #[test]
    fn java_names_and_defaults_become_those_of_the_client() {
        let settings = ClientSettings::new(&pairs(&[
            BROKER,
            ("fetch.max.wait.ms", "100"),
            ("receive.buffer.bytes", "-1"),
            ("max.poll.records", "7"),
            ("default.api.timeout.ms", "1500"),
            (
                "key.deserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer",
            ),
            ("deephaven.offset.column.name", "Offset"),
            ("no.such.property", "x"),
        ]))
        .unwrap();
        let client = settings.client.config_map();
        let expected = [
            ("allow.auto.create.topics", "true"),
            ("auto.offset.reset", "latest"),
            ("bootstrap.servers", "127.0.0.1:1"),
            ("check.crcs", "true"),
            ("enable.auto.commit", "false"),
            ("enable.auto.offset.store", "false"),
            ("fetch.wait.max.ms", "100"),
            ("group.id", NO_GROUP),
            ("isolation.level", "read_uncommitted"),
            ("security.protocol", "plaintext"),
            ("socket.receive.buffer.bytes", "0"),
            ("socket.timeout.ms", "30000"),
        ];
        assert_eq!(client.into_iter().collect::<Vec<_>>(), expected);
        assert_eq!(settings.group, None);
        assert_eq!(settings.reset, Reset::Latest);
        assert_eq!(settings.api_timeout, Duration::from_millis(1500));
        assert_eq!(settings.max_poll_records, 7);
    }

    #[test]
    fn a_group_commits_unless_told_not_to() {
        let grouped = |more: &[(&str, &str)]| {
            let mut config = pairs(&[BROKER, ("group.id", "g")]);
            config.extend(pairs(more));
            ClientSettings::new(&config).unwrap()
        };
        let settings = grouped(&[("auto.offset.reset", "none")]);
        assert_eq!(settings.group.as_deref(), Some("g"));
        assert_eq!(settings.reset, Reset::None);
        assert_eq!(settings.client.get("group.id"), Some("g"));
        assert_eq!(settings.client.get("enable.auto.commit"), Some("true"));
        assert_eq!(settings.client.get("auto.offset.reset"), Some("error"));
        let settings = grouped(&[
            ("enable.auto.commit", "False"),
            ("isolation.level", "read_committed"),
        ]);
        assert_eq!(settings.client.get("enable.auto.commit"), Some("false"));
        assert_eq!(
            settings.client.get("isolation.level"),
            Some("read_committed")
        );
    }

    #[test]
    fn what_java_refuses_is_refused() {
        let refused = |config: &[(&str, &str)]| match ClientSettings::new(&pairs(config)) {
            Ok(_) => panic!("accepted"),
            Err(error) => error,
        };
        let text = |config: &[(&str, &str)]| refused(config).to_string();
        assert!(text(&[]).contains("Missing required configuration \"bootstrap.servers\""));
        assert!(text(&[BROKER, ("enable.auto.commit", "true")])
            .contains("enable.auto.commit cannot be set to true when default group id"));
        for reset in ["sometimes", "EARLIEST"] {
            let expected = format!("Invalid value {reset} for configuration auto.offset.reset");
            assert!(text(&[BROKER, ("auto.offset.reset", reset)]).contains(&expected));
        }
        assert!(text(&[BROKER, ("group.id", " ")]).contains("InvalidGroupIdException"));
        assert!(text(&[BROKER, ("isolation.level", "READ_COMMITTED")])
            .contains("String must be one of: read_committed, read_uncommitted"));
        assert!(text(&[BROKER, ("max.poll.records", "0")]).contains("Value must be at least 1"));
        assert!(text(&[BROKER, ("enable.auto.commit", "yes")])
            .contains("Expected value to be either true or false"));
        for (name, value) in [
            ("sasl.mechanism", "GSSAPI"),
            ("auto.offset.reset", "by_duration:PT1H"),
        ] {
            let error = refused(&[BROKER, ("security.protocol", "SASL_SSL"), (name, value)]);
            assert!(matches!(error, KafkaError::NotPorted { .. }), "{error}");
        }
    }

    fn producer(config: &[(&str, &str)]) -> Result<ProducerSettings, KafkaError> {
        ProducerSettings::new(&pairs(config))
    }

    #[test]
    fn a_producer_takes_java_s_names_and_defaults() {
        let settings = producer(&[
            BROKER,
            ("delivery.timeout.ms", "5000"),
            ("max.request.size", "2000"),
            ("batch.size", "0"),
            ("send.buffer.bytes", "-1"),
            ("linger.ms", "3"),
            ("compression.type", "lz4"),
            ("max.block.ms", "250"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer",
            ),
            ("no.such.property", "x"),
        ])
        .unwrap();
        let client = settings.client.config_map();
        let expected = [
            ("acks", "all"),
            ("batch.size", "1"),
            ("bootstrap.servers", "127.0.0.1:1"),
            ("compression.type", "lz4"),
            ("enable.idempotence", "true"),
            ("linger.ms", "3"),
            ("message.max.bytes", "2000"),
            ("message.timeout.ms", "5000"),
            ("partitioner", "murmur2_random"),
            ("security.protocol", "plaintext"),
            ("socket.send.buffer.bytes", "0"),
        ];
        assert_eq!(client.into_iter().collect::<Vec<_>>(), expected);
        assert_eq!(settings.max_block, Duration::from_millis(250));

        let defaults = producer(&[BROKER]).unwrap();
        assert_eq!(defaults.max_block, Duration::from_millis(60_000));
        let client = &defaults.client;
        assert_eq!(client.get("message.timeout.ms"), Some("120000"));
        assert_eq!(client.get("message.max.bytes"), Some("1048576"));
        assert_eq!(client.get("batch.size"), Some("16384"));
        assert_eq!(client.get("compression.type"), Some("none"));
        let gzip = producer(&[BROKER, ("compression.type", "gzip")]).unwrap();
        assert_eq!(gzip.client.get("compression.type"), Some("gzip"));
    }

    /// Behavior: ProducerConfig#postProcessAndValidateIdempotenceConfigs
    #[test]
    fn idempotence_is_on_unless_the_other_settings_rule_it_out() {
        let idempotent = |config: &[(&str, &str)]| {
            let mut all = vec![BROKER];
            all.extend_from_slice(config);
            let settings = producer(&all).unwrap();
            let idempotence = settings.client.get("enable.idempotence").unwrap();
            idempotence.to_owned()
        };
        assert_eq!(idempotent(&[]), "true");
        assert_eq!(idempotent(&[("acks", "-1")]), "true");
        assert_eq!(idempotent(&[("acks", "1")]), "false");
        assert_eq!(idempotent(&[("acks", "0")]), "false");
        assert_eq!(idempotent(&[("retries", "0")]), "false");
        assert_eq!(
            idempotent(&[("max.in.flight.requests.per.connection", "5")]),
            "true"
        );
        assert_eq!(
            idempotent(&[("max.in.flight.requests.per.connection", "6")]),
            "false"
        );
        assert_eq!(idempotent(&[("enable.idempotence", "false")]), "false");
        assert_eq!(idempotent(&[("enable.idempotence", "true")]), "true");
    }

    #[test]
    fn what_a_producer_cannot_honour_is_refused() {
        let text = |config: &[(&str, &str)]| match producer(config) {
            Ok(_) => panic!("accepted"),
            Err(error) => error.to_string(),
        };
        assert!(text(&[]).contains("Missing required configuration \"bootstrap.servers\""));
        assert!(text(&[BROKER, ("acks", "2")]).contains("String must be one of: all, -1, 0, 1"));
        assert!(text(&[BROKER, ("compression.type", "brotli")])
            .contains("String must be one of: none, gzip, snappy, lz4, zstd"));
        assert!(text(&[BROKER, ("max.block.ms", "soon")]).contains("Not a number of type LONG"));
        assert!(
            text(&[BROKER, ("acks", "0"), ("enable.idempotence", "true")])
                .contains("Must set acks to all in order to use the idempotent producer")
        );
        for (name, value, java) in [
            ("transactional.id", "tx", "KafkaProducer#initTransactions"),
            ("partitioner.class", "p.P", "producer.Partitioner"),
            ("interceptor.classes", "p.I", "ProducerInterceptor"),
            ("ssl.cipher.suites", "x", "ssl.cipher.suites"),
        ] {
            let config = [BROKER, ("security.protocol", "ssl"), (name, value)];
            let error = producer(&config).err().expect("refused");
            assert!(matches!(error, KafkaError::NotPorted { .. }), "{error}");
            assert!(error.to_string().contains(java), "{error}");
        }
    }
}
