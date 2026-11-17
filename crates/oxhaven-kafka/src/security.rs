//! `security.protocol` and the `ssl.*` and `sasl.*` properties of Java's client, as
//! librdkafka takes them.
//!
//! Java's client reads TLS material from key stores (JKS, PKCS#12 or PEM) and SASL
//! credentials from a JAAS line; librdkafka reads PEM files or a PKCS#12 key store, and a
//! user name and password. What has an exact counterpart is translated; what has none is
//! refused by name. `docs/KAFKA.md`, "TLS and SASL", has the table.

use std::io::Read;
use std::sync::Mutex;

use rdkafka::consumer::ConsumerContext;
use rdkafka::error::{KafkaError as ClientError, RDKafkaErrorCode};
use rdkafka::{ClientConfig, ClientContext};

use crate::{not_ported, property, KafkaError};

/// A refusal of TLS or SASL that librdkafka reports while it connects, named as Java names
/// it: a failed SASL handshake is a `SaslAuthenticationException`, a failed TLS handshake
/// an `SslAuthenticationException`. librdkafka retries both; Java gives up at once.
///
/// Behavior: org.apache.kafka.common.errors.AuthenticationException
pub(crate) fn connection_failure(error: &ClientError, reason: &str) -> Option<String> {
    let java = match error.rdkafka_error_code()? {
        RDKafkaErrorCode::Authentication => "SaslAuthenticationException",
        RDKafkaErrorCode::SSL => "SslAuthenticationException",
        // The broker ended the TLS handshake with an alert, which OpenSSL names: under TLS
        // 1.3 a client certificate the broker refuses (or none) shows only on the first
        // read, which librdkafka reports as a failed transport.
        RDKafkaErrorCode::BrokerTransportFailure if reason.contains("SSL alert number") => {
            "SslAuthenticationException"
        }
        _ => return None,
    };
    Some(format!(
        "org.apache.kafka.common.errors.{java}: the broker refused the connection: {reason}"
    ))
}

/// Keeps the first refusal of TLS or SASL among the error events librdkafka reports, and
/// logs every event, as `rdkafka`'s own context would.
pub(crate) fn record(first: &Mutex<Option<String>>, error: &ClientError, reason: &str) {
    tracing::warn!(target: "librdkafka", "{error}: {reason}");
    if let Some(failure) = connection_failure(error, reason) {
        let mut first = first.lock().unwrap_or_else(|p| p.into_inner());
        first.get_or_insert(failure);
    }
}

/// The most events served between two requests for metadata: all there are, in practice.
/// librdkafka puts its logs on the queue its errors are on, and a poll serves one event.
/// It keeps quiet about an error identical to the last one, but when a name resolves to
/// two addresses and one of them refuses connections (`localhost` as `::1` and
/// `127.0.0.1`, to a broker listening on the second), the errors alternate, none is
/// identical to the one before, and every attempt queues a log and an error, several a
/// second. One event a slice fell behind them, and the refusal waited for the timeout.
pub(crate) const EVENTS_PER_SLICE: usize = 1000;

/// The context of a consumer: it keeps the first refusal of TLS or SASL that librdkafka
/// reports, for the calls that wait on a broker to end with.
#[derive(Default)]
pub(crate) struct Watch {
    failure: Mutex<Option<String>>,
}

impl Watch {
    pub(crate) fn failure(&self) -> Option<KafkaError> {
        let failure = self.failure.lock().unwrap_or_else(|p| p.into_inner());
        failure.clone().map(KafkaError::Broker)
    }
}

impl ClientContext for Watch {
    fn error(&self, error: ClientError, reason: &str) {
        record(&self.failure, &error, reason);
    }
}

impl ConsumerContext for Watch {}

/// Behavior: org.apache.kafka.common.security.auth.SecurityProtocol
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Protocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl Protocol {
    fn tls(self) -> bool {
        matches!(self, Protocol::Ssl | Protocol::SaslSsl)
    }

    fn sasl(self) -> bool {
        matches!(self, Protocol::SaslPlaintext | Protocol::SaslSsl)
    }

    fn client_name(self) -> &'static str {
        match self {
            Protocol::Plaintext => "plaintext",
            Protocol::Ssl => "ssl",
            Protocol::SaslPlaintext => "sasl_plaintext",
            Protocol::SaslSsl => "sasl_ssl",
        }
    }
}

fn config_exception(name: &str, value: &str, why: &str) -> KafkaError {
    KafkaError::Invalid(format!(
        "org.apache.kafka.common.config.ConfigException: Invalid value {value} for \
         configuration {name}: {why}"
    ))
}

/// Behavior: CommonClientConfigs#SECURITY_PROTOCOL_CONFIG, checked by
/// `ConfigDef.CaseInsensitiveValidString`
fn protocol(config: &[(String, String)]) -> Result<Protocol, KafkaError> {
    let Some(text) = property(config, "security.protocol") else {
        return Ok(Protocol::Plaintext);
    };
    match text.to_ascii_uppercase().as_str() {
        "PLAINTEXT" => Ok(Protocol::Plaintext),
        "SSL" => Ok(Protocol::Ssl),
        "SASL_PLAINTEXT" => Ok(Protocol::SaslPlaintext),
        "SASL_SSL" => Ok(Protocol::SaslSsl),
        _ => Err(config_exception(
            "security.protocol",
            text,
            // The order of Java's set of names (probe protocol_tls).
            "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, \
             SASL_PLAINTEXT",
        )),
    }
}

/// Sets librdkafka's `security.protocol`, and its TLS and SASL settings from Java's, on
/// `client`. Properties of TLS or SASL under a protocol without it are ignored, as Java's
/// client ignores them.
pub(crate) fn translate(
    config: &[(String, String)],
    client: &mut ClientConfig,
) -> Result<(), KafkaError> {
    let protocol = protocol(config)?;
    if protocol.tls() {
        tls(config, client)?;
    }
    if protocol.sasl() {
        sasl(config, client)?;
    }
    client.set("security.protocol", protocol.client_name());
    Ok(())
}

/// Java's properties of TLS that librdkafka has no setting for, with their defaults: a
/// property left out or set to its default is accepted, any other value refused.
///
/// Behavior: org.apache.kafka.common.config.SslConfigs
const TLS_DEFAULTS: &[(&str, Option<&str>, &str)] = &[
    (
        "ssl.protocol",
        Some("TLSv1.3"),
        "javax.net.ssl.SSLContext#getInstance",
    ),
    (
        "ssl.enabled.protocols",
        Some("TLSv1.2,TLSv1.3"),
        "javax.net.ssl.SSLEngine#setEnabledProtocols",
    ),
    (
        "ssl.cipher.suites",
        None,
        "javax.net.ssl.SSLEngine#setEnabledCipherSuites",
    ),
    ("ssl.provider", None, "java.security.Provider"),
    (
        "ssl.keymanager.algorithm",
        Some("SunX509"),
        "javax.net.ssl.KeyManagerFactory#getInstance",
    ),
    (
        "ssl.trustmanager.algorithm",
        Some("PKIX"),
        "javax.net.ssl.TrustManagerFactory#getInstance",
    ),
    (
        "ssl.secure.random.implementation",
        None,
        "java.security.SecureRandom#getInstance",
    ),
    (
        "ssl.engine.factory.class",
        None,
        "org.apache.kafka.common.security.auth.SslEngineFactory",
    ),
];

/// A key store's type as Java reads `ssl.keystore.type` and `ssl.truststore.type`: JKS
/// unless given, in any case.
fn store_type(config: &[(String, String)], name: &str) -> String {
    property(config, name)
        .map(|text| text.trim().to_ascii_uppercase())
        .unwrap_or_else(|| "JKS".to_owned())
}

/// Whether the file at `path` is PKCS#12 (DER, so a `SEQUENCE` first), which a key store
/// of type JKS reads in Java too (the JDK's `keystore.type.compat`; probe
/// ssl_keystore_no_type).
fn is_pkcs12(path: &str) -> bool {
    let mut first = [0_u8; 1];
    std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut first))
        .is_ok_and(|()| first[0] == 0x30)
}

fn tls(config: &[(String, String)], client: &mut ClientConfig) -> Result<(), KafkaError> {
    for (name, default, java) in TLS_DEFAULTS {
        if let Some(value) = property(config, name) {
            if Some(value.trim()) != *default {
                return Err(not_ported(java, &format!("{name}={value}")));
            }
        }
    }

    // Behavior: sun.security.ssl.X509TrustManagerImpl#checkIdentity: `HTTPS` in any case
    // checks the broker's name against its certificate as librdkafka's `https` does, and
    // an empty value checks nothing. `LDAP` and `LDAPS` check it by other rules (no
    // wildcards but the leftmost name), which librdkafka does not have; any other name
    // fails every handshake (probes ssl_alg_*).
    let verify = match property(config, "ssl.endpoint.identification.algorithm") {
        None => "https",
        Some("") => "none",
        Some(text) if text.eq_ignore_ascii_case("https") => "https",
        Some(text) if text.eq_ignore_ascii_case("ldap") || text.eq_ignore_ascii_case("ldaps") => {
            return Err(not_ported(
                "sun.security.util.HostnameChecker#matchDNS",
                &format!("ssl.endpoint.identification.algorithm={text}"),
            ))
        }
        Some(text) => {
            return Err(KafkaError::Invalid(format!(
                "java.security.cert.CertificateException: Unknown identification algorithm: \
                 {text}"
            )))
        }
    };
    client.set("ssl.endpoint.identification.algorithm", verify);

    trust_store(config, client)?;
    key_store(config, client)
}

fn invalid_configuration(text: &str) -> KafkaError {
    KafkaError::Invalid(format!(
        "org.apache.kafka.common.errors.InvalidConfigurationException: {text}"
    ))
}

/// What a key store of type JKS (Java's default) at `location` is to librdkafka: a JKS or
/// PKCS#12 file Java reads and librdkafka cannot or may not, or neither, which Java does
/// not read either (probe ssl_truststore_no_type).
///
/// Behavior: java.security.KeyStore#load for `JKS`, which also reads PKCS#12 files (the
/// JDK's `keystore.type.compat`)
fn unreadable_store(name: &str, kind: &str, location: &str, why: &str) -> KafkaError {
    let mut magic = [0_u8; 4];
    let read = std::fs::File::open(location).and_then(|mut file| file.read_exact(&mut magic));
    let java_reads = magic == [0xFE, 0xED, 0xFE, 0xED] || magic[0] == 0x30;
    if kind == "JKS" && read.is_ok() && !java_reads {
        return KafkaError::Invalid("java.io.IOException: Invalid keystore format".to_owned());
    }
    not_ported(
        "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory$FileBasedStore#load",
        &format!("{name}={kind} ({location}): {why}"),
    )
}

/// Behavior: org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createTruststore
fn trust_store(config: &[(String, String)], client: &mut ClientConfig) -> Result<(), KafkaError> {
    let kind = store_type(config, "ssl.truststore.type");
    let location = property(config, "ssl.truststore.location");
    let certificates = property(config, "ssl.truststore.certificates");
    if let Some(certificates) = certificates {
        if kind != "PEM" {
            return Err(invalid_configuration(&format!(
                "SSL trust store certs can be specified only for PEM, but trust store type is \
                 {kind}."
            )));
        }
        if location.is_some() {
            // Not probed: Java refuses the two together too.
            return Err(invalid_configuration(
                "Both SSL trust store location and separate trust certificates are \
                 specified.",
            ));
        }
        client.set("ssl.ca.pem", certificates);
    }
    if kind == "PEM" && property(config, "ssl.truststore.password").is_some() {
        return Err(invalid_configuration(
            "SSL trust store password cannot be specified for PEM format.",
        ));
    }
    match location {
        // Without one, Java trusts what the JVM trusts and librdkafka what OpenSSL does,
        // the system's certificates.
        None => Ok(()),
        Some(location) if kind == "PEM" => {
            client.set("ssl.ca.location", location);
            Ok(())
        }
        Some(location) => Err(unreadable_store(
            "ssl.truststore.type",
            &kind,
            location,
            "librdkafka reads trusted certificates from PEM only; give them in a PEM file \
             with ssl.truststore.type=PEM",
        )),
    }
}

/// Behavior: org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createKeystore
fn key_store(config: &[(String, String)], client: &mut ClientConfig) -> Result<(), KafkaError> {
    let kind = store_type(config, "ssl.keystore.type");
    let location = property(config, "ssl.keystore.location");
    let store_password = property(config, "ssl.keystore.password");
    let key_password = property(config, "ssl.key.password");
    let key = property(config, "ssl.keystore.key");
    let chain = property(config, "ssl.keystore.certificate.chain");
    if kind == "PEM" {
        match (location, key, chain) {
            (None, None, None) => {}
            (Some(location), None, None) => {
                // One file holds the key and its chain; librdkafka finds each in it.
                client.set("ssl.key.location", location);
                client.set("ssl.certificate.location", location);
            }
            (None, Some(key), Some(chain)) => {
                client.set("ssl.key.pem", key);
                client.set("ssl.certificate.pem", chain);
            }
            // Not probed: Java refuses these too, in other words.
            _ => {
                return Err(KafkaError::Invalid(
                    "ssl.keystore.key and ssl.keystore.certificate.chain go together, and \
                     not with ssl.keystore.location"
                        .to_owned(),
                ))
            }
        }
        if let Some(password) = key_password {
            client.set("ssl.key.password", password);
        }
        return Ok(());
    }
    let Some(location) = location else {
        return Ok(());
    };
    let pkcs12 = kind == "PKCS12" || (kind == "JKS" && is_pkcs12(location));
    if !pkcs12 {
        return Err(unreadable_store(
            "ssl.keystore.type",
            &kind,
            location,
            "librdkafka reads a key store of PKCS#12 or PEM only",
        ));
    }
    if key_password.is_some() && key_password != store_password {
        return Err(not_ported(
            "java.security.KeyStore#getKey",
            "ssl.key.password other than ssl.keystore.password: librdkafka opens a PKCS#12 \
             key store with one password",
        ));
    }
    client.set("ssl.keystore.location", location);
    if let Some(password) = store_password {
        client.set("ssl.keystore.password", password);
    }
    Ok(())
}

/// The login modules whose user name and password librdkafka takes.
const PLAIN_LOGIN: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";
const SCRAM_LOGIN: &str = "org.apache.kafka.common.security.scram.ScramLoginModule";

/// SASL properties that load Java classes.
const SASL_CLASSES: &[&str] = &[
    "sasl.login.class",
    "sasl.client.callback.handler.class",
    "sasl.login.callback.handler.class",
];

fn sasl(config: &[(String, String)], client: &mut ClientConfig) -> Result<(), KafkaError> {
    for name in SASL_CLASSES {
        if let Some(value) = property(config, name) {
            return Err(not_ported(
                "org.apache.kafka.common.security.authenticator.LoginManager",
                &format!("{name}={value}"),
            ));
        }
    }
    // Behavior: SaslConfigs#DEFAULT_SASL_MECHANISM, GSSAPI; the name is matched as written.
    let mechanism = property(config, "sasl.mechanism").unwrap_or("GSSAPI");
    match mechanism {
        "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" => {}
        "GSSAPI" => {
            return Err(not_ported(
                "org.apache.kafka.common.security.kerberos.KerberosLogin",
                "sasl.mechanism=GSSAPI (the default under SASL): Kerberos needs Cyrus SASL, \
                 which is not built in; set sasl.mechanism to PLAIN, SCRAM-SHA-256 or \
                 SCRAM-SHA-512",
            ))
        }
        "OAUTHBEARER" => {
            return Err(not_ported(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                "sasl.mechanism=OAUTHBEARER: tokens from an OIDC provider need libcurl, \
                 which is not built in",
            ))
        }
        // Java connects and waits until `default.api.timeout.ms` is up (probe mech_foo).
        other => {
            return Err(KafkaError::Invalid(format!(
                "sasl.mechanism={other}: the client has PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512"
            )))
        }
    }
    let Some(jaas) = property(config, "sasl.jaas.config") else {
        return Err(KafkaError::Invalid(
            "java.lang.IllegalArgumentException: Could not find a 'KafkaClient' entry in \
             the JAAS configuration. System property 'java.security.auth.login.config' is \
             not set"
                .to_owned(),
        ));
    };
    let entries = jaas_entries(jaas)?;
    let [entry] = entries.as_slice() else {
        return Err(KafkaError::Invalid(format!(
            "java.lang.IllegalArgumentException: JAAS config property contains {} login \
             modules, should be 1 module",
            entries.len()
        )));
    };
    match entry.module.as_deref() {
        Some(PLAIN_LOGIN) => {}
        Some(SCRAM_LOGIN) => {
            if option(entry, "tokenauth").is_some_and(|value| value.eq_ignore_ascii_case("true")) {
                return Err(not_ported(
                    "org.apache.kafka.common.security.scram.internals.ScramSaslClient",
                    "tokenauth=true in sasl.jaas.config: delegation tokens",
                ));
            }
        }
        other => {
            return Err(not_ported(
                other.unwrap_or("javax.security.auth.spi.LoginModule"),
                "a login module of sasl.jaas.config other than PlainLoginModule and \
                 ScramLoginModule",
            ))
        }
    }
    client.set("sasl.mechanism", mechanism);
    if let Some(username) = option(entry, "username") {
        client.set("sasl.username", username);
    }
    if let Some(password) = option(entry, "password") {
        client.set("sasl.password", password);
    }
    Ok(())
}

/// One entry of a JAAS line: the login module and its options, the last of a repeated
/// option winning (a `HashMap`).
#[derive(Debug, PartialEq)]
struct JaasEntry {
    module: Option<String>,
    options: Vec<(String, String)>,
}

fn option<'a>(entry: &'a JaasEntry, name: &str) -> Option<&'a str> {
    let found = entry.options.iter().rev().find(|(key, _)| key == name);
    found.map(|(_, value)| value.as_str())
}

/// A token of `java.io.StreamTokenizer`, as Kafka's JAAS parser sets it up.
#[derive(Clone, Debug, PartialEq)]
enum Token {
    End,
    /// A word or a quoted string: `sval`.
    Text(String),
    /// A number: `sval` is null.
    Number,
    /// Any other character, `ttype` itself.
    Char(char),
}

impl Token {
    fn text(&self) -> Option<&str> {
        match self {
            Token::Text(text) => Some(text),
            _ => None,
        }
    }
}

/// `java.io.StreamTokenizer` with its defaults (words of letters and the characters from
/// U+00A0, `/` a comment character, `"` and `'` quotes, numbers parsed) and Kafka's
/// changes: `//` and `/* */` comments, and `-`, `_` and `$` in words.
///
/// Behavior: java.io.StreamTokenizer#nextToken
struct Tokenizer {
    chars: Vec<char>,
    at: usize,
}

impl Tokenizer {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.at).copied()
    }

    fn read(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.at += 1;
        }
        c
    }

    fn is_digit(c: char) -> bool {
        c.is_ascii_digit() || c == '.' || c == '-'
    }

    fn is_alpha(c: char) -> bool {
        c.is_ascii_alphabetic() || c as u32 >= 0xA0 || matches!(c, '-' | '_' | '$')
    }

    fn skip_line(&mut self) {
        while let Some(c) = self.read() {
            if c == '\n' || c == '\r' {
                break;
            }
        }
    }

    fn next(&mut self) -> Token {
        loop {
            let Some(c) = self.read() else {
                return Token::End;
            };
            if c as u32 <= 0x20 {
                continue;
            }
            if Self::is_digit(c) {
                return self.number(c);
            }
            if Self::is_alpha(c) {
                let mut word = String::from(c);
                while let Some(next) = self.peek() {
                    if !(Self::is_alpha(next) || Self::is_digit(next)) {
                        break;
                    }
                    word.push(next);
                    self.at += 1;
                }
                return Token::Text(word);
            }
            if c == '"' || c == '\'' {
                return Token::Text(self.quoted(c));
            }
            if c == '/' {
                match self.peek() {
                    Some('*') => {
                        self.at += 1;
                        let mut last = '\0';
                        while let Some(next) = self.read() {
                            if last == '*' && next == '/' {
                                break;
                            }
                            last = next;
                        }
                    }
                    // `//`, or `/` alone, which is still a comment character.
                    _ => self.skip_line(),
                }
                continue;
            }
            return Token::Char(c);
        }
    }

    /// Behavior: StreamTokenizer#nextToken for a character of `parseNumbers`: a `-`
    /// that no digit or `.` follows is the character `-` itself.
    fn number(&mut self, first: char) -> Token {
        let digit_or_dot = |c: Option<char>| c.is_some_and(|c| c == '.' || c.is_ascii_digit());
        if first == '-' && !digit_or_dot(self.peek()) {
            return Token::Char('-');
        }
        let mut seen_dot = first == '.';
        while let Some(c) = self.peek() {
            if c == '.' && !seen_dot {
                seen_dot = true;
            } else if !c.is_ascii_digit() {
                break;
            }
            self.at += 1;
        }
        Token::Number
    }

    /// Behavior: StreamTokenizer#nextToken for a quote: up to the same quote, a line end
    /// or the end, with C's escapes and octal ones.
    fn quoted(&mut self, quote: char) -> String {
        let mut text = String::new();
        while let Some(c) = self.peek() {
            if c == quote || c == '\n' || c == '\r' {
                if c == quote {
                    self.at += 1;
                }
                break;
            }
            self.at += 1;
            if c != '\\' {
                text.push(c);
                continue;
            }
            let Some(escaped) = self.read() else {
                break;
            };
            let value = match escaped {
                'a' => '\u{7}',
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'v' => '\u{b}',
                '0'..='7' => {
                    let mut value = escaped as u32 - '0' as u32;
                    let most = if escaped <= '3' { 2 } else { 1 };
                    for _ in 0..most {
                        match self.peek() {
                            Some(digit @ '0'..='7') => {
                                value = value * 8 + (digit as u32 - '0' as u32);
                                self.at += 1;
                            }
                            _ => break,
                        }
                    }
                    char::from_u32(value).unwrap_or('\u{fffd}')
                }
                other => other,
            };
            text.push(value);
        }
        text
    }
}

fn jaas_error(text: &str) -> KafkaError {
    KafkaError::Invalid(format!("java.lang.IllegalArgumentException: {text}"))
}

/// The entries of a JAAS line: `<module> <flag> (<key>=<value>)* ;`, repeated.
///
/// Behavior: org.apache.kafka.common.security.JaasConfig#JaasConfig and
/// #parseAppConfigurationEntry
fn jaas_entries(text: &str) -> Result<Vec<JaasEntry>, KafkaError> {
    let mut tokens = Tokenizer {
        chars: text.chars().collect(),
        at: 0,
    };
    let mut entries = Vec::new();
    loop {
        let module = match tokens.next() {
            Token::End => break,
            token => token.text().map(str::to_owned),
        };
        let flag = tokens.next();
        if flag == Token::End {
            return Err(jaas_error(
                "Login module control flag not specified in JAAS config",
            ));
        }
        match flag.text() {
            Some(text)
                if ["REQUIRED", "REQUISITE", "SUFFICIENT", "OPTIONAL"]
                    .contains(&text.to_ascii_uppercase().as_str()) => {}
            Some(text) => {
                return Err(jaas_error(&format!(
                    "Invalid login module control flag '{text}' in JAAS config"
                )));
            }
            None => {
                return Err(jaas_error(
                    "Login module control flag is not available in the JAAS config",
                ))
            }
        }
        let mut options = Vec::new();
        let mut last = tokens.next();
        while last != Token::End && last != Token::Char(';') {
            let key = last.text().map_or_else(|| "null".to_owned(), str::to_owned);
            let equals = tokens.next();
            let value = if equals == Token::Char('=') {
                tokens.next()
            } else {
                Token::End
            };
            let Some(value) = value.text() else {
                return Err(jaas_error(&format!(
                    "Value not specified for key '{key}' in JAAS config"
                )));
            };
            options.push((key, value.to_owned()));
            last = tokens.next();
        }
        if last != Token::Char(';') {
            return Err(jaas_error("JAAS config entry not terminated"));
        }
        entries.push(JaasEntry { module, options });
    }
    if entries.is_empty() {
        return Err(jaas_error("Login module not specified in JAAS config"));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAIN: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";

    fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let pair = |(name, value): &(&str, &str)| ((*name).to_owned(), (*value).to_owned());
        pairs.iter().map(pair).collect()
    }

    fn client(config: &[(&str, &str)]) -> Result<Vec<(String, String)>, KafkaError> {
        let mut client = ClientConfig::new();
        translate(&pairs(config), &mut client)?;
        // `config_map` hides the passwords; `get` does not.
        let names = client.config_map().into_keys();
        let value = |name: &str| client.get(name).unwrap_or_default().to_owned();
        Ok(names.map(|name| (name.to_owned(), value(name))).collect())
    }

    fn set(config: &[(&str, &str)]) -> Vec<(String, String)> {
        client(config).unwrap_or_else(|error| panic!("{config:?}: {error}"))
    }

    fn refusal(config: &[(&str, &str)]) -> KafkaError {
        client(config).expect_err("refused")
    }

    /// A file in the temporary directory with `bytes`, removed when dropped.
    struct File(std::path::PathBuf);

    impl File {
        fn new(name: &str, bytes: &[u8]) -> Self {
            let path = std::env::temp_dir().join(format!("oxhaven-{}-{name}", std::process::id()));
            std::fs::write(&path, bytes).unwrap();
            Self(path)
        }

        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for File {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn the_protocol_is_read_in_any_case_and_tls_and_sasl_only_under_theirs() {
        assert_eq!(
            set(&[
                ("ssl.truststore.location", "x"),
                ("sasl.mechanism", "GSSAPI")
            ]),
            [("security.protocol".to_owned(), "plaintext".to_owned())]
        );
        let ssl = set(&[("security.protocol", "ssl")]);
        assert!(ssl.contains(&("security.protocol".to_owned(), "ssl".to_owned())));
        assert!(ssl.contains(&(
            "ssl.endpoint.identification.algorithm".to_owned(),
            "https".to_owned()
        )));
        let error = refusal(&[("security.protocol", "TLS")]).to_string();
        assert_eq!(
            error,
            "org.apache.kafka.common.config.ConfigException: Invalid value TLS for \
             configuration security.protocol: String must be one of (case insensitive): \
             SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"
        );
    }

    #[test]
    fn pem_files_and_texts_and_pkcs12_key_stores_are_translated() {
        const SSL: (&str, &str) = ("security.protocol", "SSL");
        let has = |config: &[(&str, &str)], expected: &[(&str, &str)]| {
            let got = set(config);
            for (name, value) in expected {
                let pair = ((*name).to_owned(), (*value).to_owned());
                assert!(
                    got.contains(&pair),
                    "{config:?} lacks {name}={value}: {got:?}"
                );
            }
        };
        has(
            &[
                SSL,
                ("ssl.truststore.type", "pem"),
                ("ssl.truststore.location", "/c/ca.pem"),
                ("ssl.keystore.type", "PKCS12"),
                ("ssl.keystore.location", "/c/client.p12"),
                ("ssl.keystore.password", "secret"),
                ("ssl.key.password", "secret"),
                ("ssl.endpoint.identification.algorithm", ""),
            ],
            &[
                ("ssl.ca.location", "/c/ca.pem"),
                ("ssl.keystore.location", "/c/client.p12"),
                ("ssl.keystore.password", "secret"),
                ("ssl.endpoint.identification.algorithm", "none"),
            ],
        );
        has(
            &[
                SSL,
                ("ssl.truststore.type", "PEM"),
                ("ssl.truststore.certificates", "-----BEGIN CERTIFICATE-----"),
                ("ssl.keystore.type", "PEM"),
                ("ssl.keystore.location", "/c/both.pem"),
                ("ssl.key.password", "k"),
                ("ssl.endpoint.identification.algorithm", "HTTPS"),
                ("ssl.protocol", "TLSv1.3"),
            ],
            &[
                ("ssl.ca.pem", "-----BEGIN CERTIFICATE-----"),
                ("ssl.key.location", "/c/both.pem"),
                ("ssl.certificate.location", "/c/both.pem"),
                ("ssl.key.password", "k"),
                ("ssl.endpoint.identification.algorithm", "https"),
            ],
        );
        has(
            &[
                ("security.protocol", "SASL_SSL"),
                ("ssl.keystore.type", "PEM"),
                ("ssl.keystore.key", "KEY"),
                ("ssl.keystore.certificate.chain", "CHAIN"),
                ("sasl.mechanism", "PLAIN"),
                (
                    "sasl.jaas.config",
                    &format!("{PLAIN} required username=u password=p;"),
                ),
            ],
            &[("ssl.key.pem", "KEY"), ("ssl.certificate.pem", "CHAIN")],
        );
        // A key store of Java's default type JKS that is a PKCS#12 file, which Java reads.
        let pkcs12 = File::new("store.p12", &[0x30, 0x82, 0x01]);
        has(
            &[SSL, ("ssl.keystore.location", pkcs12.path())],
            &[("ssl.keystore.location", pkcs12.path())],
        );
    }

    #[test]
    fn stores_librdkafka_cannot_read_are_refused_by_name_and_java_s_errors_kept() {
        const SSL: (&str, &str) = ("security.protocol", "SSL");
        let not_ported = |config: &[(&str, &str)], java: &str| {
            let error = refusal(config);
            assert!(
                matches!(&error, KafkaError::NotPorted { java: named, .. } if named.contains(java)),
                "{config:?}: {error}"
            );
        };
        let jks = File::new("store.jks", &[0xFE, 0xED, 0xFE, 0xED, 0, 0, 0, 2]);
        let pem = File::new("store.pem", b"-----BEGIN CERTIFICATE-----\n");
        let store = "DefaultSslEngineFactory$FileBasedStore#load";
        not_ported(&[SSL, ("ssl.truststore.location", jks.path())], store);
        not_ported(
            &[
                SSL,
                ("ssl.truststore.type", "PKCS12"),
                ("ssl.truststore.location", "/c/t.p12"),
            ],
            store,
        );
        not_ported(&[SSL, ("ssl.keystore.location", jks.path())], store);
        not_ported(
            &[SSL, ("ssl.cipher.suites", "TLS_AES_128_GCM_SHA256")],
            "SSLEngine",
        );
        not_ported(&[SSL, ("ssl.protocol", "TLSv1.2")], "SSLContext");
        not_ported(
            &[SSL, ("ssl.endpoint.identification.algorithm", "ldaps")],
            "HostnameChecker",
        );
        not_ported(
            &[
                SSL,
                ("ssl.keystore.type", "PKCS12"),
                ("ssl.keystore.location", "/c/k.p12"),
                ("ssl.keystore.password", "a"),
                ("ssl.key.password", "b"),
            ],
            "KeyStore#getKey",
        );

        let text = |config: &[(&str, &str)]| refusal(config).to_string();
        // Probes ssl_truststore_no_type, ssl_truststore_pem_password,
        // ssl_truststore_certs_no_type and ssl_alg_foo.
        assert_eq!(
            text(&[SSL, ("ssl.truststore.location", pem.path())]),
            "java.io.IOException: Invalid keystore format"
        );
        assert_eq!(
            text(&[
                SSL,
                ("ssl.truststore.type", "PEM"),
                ("ssl.truststore.location", pem.path()),
                ("ssl.truststore.password", "x"),
            ]),
            "org.apache.kafka.common.errors.InvalidConfigurationException: SSL trust store \
             password cannot be specified for PEM format."
        );
        assert_eq!(
            text(&[SSL, ("ssl.truststore.certificates", "C")]),
            "org.apache.kafka.common.errors.InvalidConfigurationException: SSL trust store \
             certs can be specified only for PEM, but trust store type is JKS."
        );
        assert_eq!(
            text(&[SSL, ("ssl.endpoint.identification.algorithm", "foo")]),
            "java.security.cert.CertificateException: Unknown identification algorithm: foo"
        );
    }

    #[test]
    fn sasl_takes_the_user_and_password_of_a_plain_or_scram_login_module() {
        let sasl = |mechanism: &str, jaas: &str| {
            set(&[
                ("security.protocol", "SASL_PLAINTEXT"),
                ("sasl.mechanism", mechanism),
                ("sasl.jaas.config", jaas),
            ])
        };
        let expected = |mechanism: &str, user: &str, password: &str| {
            let mut expected = vec![
                ("sasl.mechanism".to_owned(), mechanism.to_owned()),
                ("sasl.password".to_owned(), password.to_owned()),
                ("sasl.username".to_owned(), user.to_owned()),
                ("security.protocol".to_owned(), "sasl_plaintext".to_owned()),
            ];
            expected.sort();
            expected
        };
        assert_eq!(
            sasl(
                "PLAIN",
                &format!(r#"{PLAIN} required username="alice" password="a b";"#)
            ),
            expected("PLAIN", "alice", "a b")
        );
        // Probes jaas_unquoted, jaas_comment and jaas_escape: words, comments, escapes.
        assert_eq!(
            sasl(
                "SCRAM-SHA-512",
                "/* c */ org.apache.kafka.common.security.scram.ScramLoginModule REQUIRED // x\n \
                 username=al-i_c$e password='p\\x\\101\\n';"
            ),
            expected("SCRAM-SHA-512", "al-i_c$e", "pxA\n")
        );
        // A SCRAM mechanism with the PLAIN module's credentials works in Java (probe
        // scram_with_plain_module).
        assert_eq!(
            sasl(
                "SCRAM-SHA-256",
                &format!("{PLAIN} optional username=u password=p;")
            ),
            expected("SCRAM-SHA-256", "u", "p")
        );
    }

    #[test]
    fn jaas_lines_java_refuses_are_refused_with_its_words() {
        let text = |jaas: &str| {
            refusal(&[
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanism", "PLAIN"),
                ("sasl.jaas.config", jaas),
            ])
            .to_string()
        };
        let illegal = |words: &str| format!("java.lang.IllegalArgumentException: {words}");
        // Probes jaas_*.
        let entry = format!(r#"{PLAIN} required username="a" password="b";"#);
        assert_eq!(
            text(&entry[..entry.len() - 1]),
            illegal("JAAS config entry not terminated")
        );
        assert_eq!(
            text(&format!(r#"{PLAIN} username="a";"#)),
            illegal("Invalid login module control flag 'username' in JAAS config")
        );
        assert_eq!(
            text(&format!(r#"{PLAIN} must username="a";"#)),
            illegal("Invalid login module control flag 'must' in JAAS config")
        );
        assert_eq!(
            text(&format!("{entry} {entry}")),
            illegal("JAAS config property contains 2 login modules, should be 1 module")
        );
        assert_eq!(
            text(""),
            illegal("Login module not specified in JAAS config")
        );
        assert_eq!(
            text(" // nothing"),
            illegal("Login module not specified in JAAS config")
        );
        for value in ["123", ""] {
            assert_eq!(
                text(&format!("{PLAIN} required username={value} ;")),
                illegal("Value not specified for key 'username' in JAAS config")
            );
        }
        assert_eq!(
            text(PLAIN),
            illegal("Login module control flag not specified in JAAS config")
        );
        let other = refusal(&[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.jaas.config", "com.example.Login required;"),
        ]);
        assert!(
            matches!(&other, KafkaError::NotPorted { java, .. } if java == "com.example.Login"),
            "{other}"
        );
    }

    #[test]
    fn mechanisms_other_than_plain_and_scram_are_refused() {
        let jaas = format!("{PLAIN} required username=u password=p;");
        let with = |more: &[(&str, &str)]| {
            let mut config = vec![
                ("security.protocol", "SASL_PLAINTEXT"),
                ("sasl.jaas.config", jaas.as_str()),
            ];
            config.extend_from_slice(more);
            refusal(&config)
        };
        for (more, java) in [
            (&[][..], "KerberosLogin"),
            (&[("sasl.mechanism", "GSSAPI")][..], "KerberosLogin"),
            (
                &[("sasl.mechanism", "OAUTHBEARER")][..],
                "OAuthBearerLoginModule",
            ),
            (
                &[("sasl.mechanism", "PLAIN"), ("sasl.login.class", "a.B")][..],
                "LoginManager",
            ),
        ] {
            let error = with(more);
            assert!(
                matches!(&error, KafkaError::NotPorted { java: named, .. } if named.contains(java)),
                "{more:?}: {error}"
            );
        }
        assert!(matches!(
            with(&[("sasl.mechanism", "plain")]),
            KafkaError::Invalid(_)
        ));
        let missing = refusal(&[
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "PLAIN"),
        ]);
        assert_eq!(
            missing.to_string(),
            "java.lang.IllegalArgumentException: Could not find a 'KafkaClient' entry in the \
             JAAS configuration. System property 'java.security.auth.login.config' is not set"
        );
    }
}
