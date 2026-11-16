//! A client of a Confluent schema registry, over plain HTTP: the calls that Confluent's
//! `CachedSchemaRegistryClient` makes for an Avro spec and for `KafkaAvroDeserializer`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::avro::{self, Schema};
use crate::json::{self, Node};
use crate::{not_ported, property, KafkaError};

const URL_PROPERTY: &str = "schema.registry.url";
const REST: &str = "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException";
const UNCHECKED: &str = "io.deephaven.UncheckedDeephavenException";
/// Confluent's default for `http.connect.timeout.ms` and `http.read.timeout.ms`.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Where a schema registry is, and the writers' schemas read from it so far.
#[derive(Debug)]
pub(crate) struct Registry {
    /// `host:port` and the path prefix of each URL, in the order given, or the text of
    /// a URL without a scheme, which Java's `java.net.URL` refuses when it is asked.
    urls: Vec<Result<(String, String), String>>,
    by_id: Mutex<HashMap<i32, Arc<Schema>>>,
}

/// Why a call to the registry failed: what the registry answered, or why it could not be
/// asked. The text is that of Java's exception.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct CallFailure(pub(crate) String);

impl Registry {
    /// The registry of `schema.registry.url`, which may list several URLs separated by
    /// commas. Java requires the property as soon as a spec has a schema provider, so an
    /// Avro spec with its schema given as text needs it too.
    ///
    /// Behavior: io.deephaven.kafka.KafkaTools#newSchemaRegistryClient,
    /// io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig#getSchemaRegistryUrls
    pub(crate) fn from_config(config: &[(String, String)]) -> Result<Self, KafkaError> {
        let refused = [
            ("basic.auth.credentials.source", "io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider"),
            ("basic.auth.user.info", "io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider"),
            ("bearer.auth.credentials.source", "io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider"),
            ("key.subject.name.strategy", "io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy"),
            ("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy"),
            ("avro.use.logical.type.converters", "io.confluent.kafka.serializers.KafkaAvroDeserializerConfig#AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG"),
            ("specific.avro.reader", "io.confluent.kafka.serializers.KafkaAvroDeserializerConfig#SPECIFIC_AVRO_READER_CONFIG"),
        ];
        for (name, java) in refused {
            if let Some(value) = property(config, name) {
                if !(matches!(
                    name,
                    "avro.use.logical.type.converters" | "specific.avro.reader"
                ) && value == "false")
                {
                    return Err(not_ported(java, &format!("{name}={value}")));
                }
            }
        }
        let Some(list) = property(config, URL_PROPERTY) else {
            return Err(KafkaError::Invalid(format!(
                "org.apache.kafka.common.config.ConfigException: Missing required configuration \
                 \"{URL_PROPERTY}\" which has no default value."
            )));
        };
        let mut urls = Vec::new();
        for url in list.split(',').map(str::trim).filter(|url| !url.is_empty()) {
            let Some(rest) = url.strip_prefix("http://") else {
                if url.starts_with("https://") {
                    return Err(not_ported(
                        "io.confluent.kafka.schemaregistry.client.rest.RestService#setSslSocketFactory",
                        &format!("a schema registry over TLS ({url})"),
                    ));
                }
                if url.contains("://") {
                    // Java opens the scheme's own connection and fails to use it as HTTP
                    // (probe url_ftp: a ClassCastException), or knows no such scheme.
                    return Err(not_ported(
                        "java.net.URL#openConnection",
                        &format!("a schema registry URL other than http:// ({url})"),
                    ));
                }
                // Behavior: java.net.URL#URL(String), when the request is made (probe
                // url_noscheme).
                urls.push(Err(url.to_owned()));
                continue;
            };
            let (authority, path) = match rest.find('/') {
                Some(slash) => (&rest[..slash], rest[slash..].trim_end_matches('/')),
                None => (rest, ""),
            };
            let authority = if authority.contains(':') {
                authority.to_owned()
            } else {
                format!("{authority}:80")
            };
            urls.push(Ok((authority, path.to_owned())));
        }
        // Behavior: io.confluent.kafka.schemaregistry.client.rest.RestService#RestService,
        // via UrlList (probe url_empty)
        if urls.is_empty() {
            return Err(KafkaError::Invalid(
                "java.lang.IllegalArgumentException: Expected at least one URL to be passed in \
                 constructor"
                    .to_owned(),
            ));
        }
        Ok(Self {
            urls,
            by_id: Mutex::new(HashMap::new()),
        })
    }

    /// GETs `path` from the first URL that answers, as Confluent's `RestService` fails
    /// over to the next URL when one cannot be reached.
    ///
    /// Behavior: io.confluent.kafka.schemaregistry.client.rest.RestService#httpRequest
    fn get(&self, path: &str) -> Result<Node, CallFailure> {
        let mut last = CallFailure("java.net.ConnectException: no URL".to_owned());
        for url in &self.urls {
            let (authority, prefix) = match url {
                Ok(http) => http,
                Err(text) => {
                    last = CallFailure(format!(
                        "java.net.MalformedURLException: no protocol: {text}{path}"
                    ));
                    continue;
                }
            };
            match http_get(authority, &format!("{prefix}{path}")) {
                Ok((status, body)) => return answer(status, &body),
                Err(failure) => last = failure,
            }
        }
        Err(last)
    }

    /// The schema of `subject` at `version` (`latest` or a number, as Java's
    /// `Integer#parseInt` reads it), fetched by its id as Java fetches it.
    ///
    /// Behavior: io.deephaven.kafka.AvroImpl#getAvroSchema;
    /// CachedSchemaRegistryClient#getLatestSchemaMetadata, #getSchemaMetadata and
    /// #getSchemaById
    pub(crate) fn subject_schema(
        &self,
        subject: &str,
        version: Option<&str>,
    ) -> Result<Schema, KafkaError> {
        let unchecked = |CallFailure(text)| KafkaError::Invalid(format!("{UNCHECKED}: {text}"));
        let path = if version == Some("latest") {
            format!("/subjects/{}/versions/latest", encode(subject))
        } else {
            let number = version.and_then(parse_java_int).ok_or_else(|| {
                KafkaError::Invalid(match version {
                    Some(version) => {
                        format!("java.lang.NumberFormatException: For input string: \"{version}\"")
                    }
                    None => {
                        "java.lang.NumberFormatException: Cannot parse null string: null".to_owned()
                    }
                })
            })?;
            format!(
                "/subjects/{}/versions/{number}?deleted=false",
                encode(subject)
            )
        };
        let metadata = self.get(&path).map_err(unchecked)?;
        let id = metadata.get("id").and_then(Node::exact_long).unwrap_or(0);
        let text = self
            .get(&format!("/schemas/ids/{id}?fetchMaxId=false&subject="))
            .and_then(|body| schema_text(&body))
            .map_err(unchecked)?;
        avro::parse(&text).map_err(|failure| KafkaError::Invalid(failure))
    }

    /// The schema a record was written with, by the id in the record, cached by id.
    ///
    /// Behavior: io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer#deserialize,
    /// CachedSchemaRegistryClient#getSchemaBySubjectAndId
    pub(crate) fn writer_schema(&self, id: i32, subject: &str) -> Result<Arc<Schema>, CallFailure> {
        if let Some(schema) = self.by_id.lock().expect("not poisoned").get(&id) {
            return Ok(schema.clone());
        }
        let body = self.get(&format!(
            "/schemas/ids/{id}?fetchMaxId=false&subject={}",
            encode(subject)
        ))?;
        let schema = Arc::new(avro::parse(&schema_text(&body)?).map_err(CallFailure)?);
        let mut cache = self.by_id.lock().expect("not poisoned");
        Ok(cache.entry(id).or_insert(schema).clone())
    }
}

/// The schema of a `/schemas/ids/<id>` answer. References to other schemas and other
/// schema types are not followed.
///
/// Behavior: io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString,
/// io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider#parseSchemaOrElseThrow
fn schema_text(body: &Node) -> Result<String, CallFailure> {
    if let Some(Node::Array(references)) = body.get("references") {
        if !references.is_empty() {
            return Err(CallFailure(
                "not ported yet: io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider#\
                 parseSchemaOrElseThrow: a schema with references"
                    .to_owned(),
            ));
        }
    }
    if let Some(kind) = body.get("schemaType").and_then(Node::as_str) {
        if kind != "AVRO" {
            return Err(CallFailure(format!(
                "java.lang.IllegalArgumentException: Invalid schema type {kind}"
            )));
        }
    }
    body.get("schema")
        .and_then(Node::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            CallFailure(format!(
                "{REST}: no schema in the answer; error code: 50005"
            ))
        })
}

/// Behavior: java.lang.Integer#parseInt(String) for ASCII digits: an optional sign, then
/// digits, within an `int`
fn parse_java_int(text: &str) -> Option<i32> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Percent-encodes a path segment as `URLEncoder#encode` does for a subject, with `%20`
/// for a space as Confluent's client writes it.
fn encode(segment: &str) -> String {
    let mut out = String::new();
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(b as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// The JSON of a successful answer, or Java's `RestClientException` for an error: the
/// registry's message and error code.
///
/// Behavior: io.confluent.kafka.schemaregistry.client.rest.RestService#sendHttpRequest
fn answer(status: u16, body: &[u8]) -> Result<Node, CallFailure> {
    let text = String::from_utf8_lossy(body);
    let node = json::parse(&text).ok();
    if (200..300).contains(&status) {
        return node.ok_or_else(|| {
            CallFailure(format!(
                "{REST}: the registry's answer is not JSON; error code: 50005"
            ))
        });
    }
    let message = node
        .as_ref()
        .and_then(|n| n.get("message"))
        .and_then(Node::as_str);
    let code = node
        .as_ref()
        .and_then(|n| n.get("error_code"))
        .and_then(Node::exact_long);
    Err(CallFailure(match (message, code) {
        (Some(message), Some(code)) => format!("{REST}: {message}; error code: {code}"),
        _ => format!(
            "{REST}: Unexpected response from the registry: {text}; error code: {}",
            u32::from(status) * 100 + 5
        ),
    }))
}

/// One HTTP/1.1 GET, with `Connection: close`: the status and the body.
fn http_get(authority: &str, path: &str) -> Result<(u16, Vec<u8>), CallFailure> {
    let io = |error: std::io::Error| {
        let class = match error.kind() {
            std::io::ErrorKind::ConnectionRefused => "java.net.ConnectException",
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
                "java.net.SocketTimeoutException"
            }
            _ => "java.io.IOException",
        };
        let text = match error.kind() {
            std::io::ErrorKind::ConnectionRefused => "Connection refused".to_owned(),
            _ => error.to_string(),
        };
        CallFailure(format!("{class}: {text}"))
    };
    let address = authority
        .to_socket_addrs()
        .map_err(|_| CallFailure(format!("java.net.UnknownHostException: {authority}")))?
        .next()
        .ok_or_else(|| CallFailure(format!("java.net.UnknownHostException: {authority}")))?;
    let mut stream = TcpStream::connect_timeout(&address, TIMEOUT).map_err(io)?;
    stream.set_read_timeout(Some(TIMEOUT)).map_err(io)?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: */*\r\n\
         Content-Type: application/vnd.schemaregistry.v1+json\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).map_err(io)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).map_err(io)?;
    parse_response(&response).ok_or_else(|| {
        CallFailure("java.io.IOException: the schema registry's answer is not HTTP".to_owned())
    })
}

/// The status and body of an HTTP/1.1 response read to its end.
fn parse_response(response: &[u8]) -> Option<(u16, Vec<u8>)> {
    let end = response.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&response[..end]).ok()?;
    let body = &response[end + 4..];
    let mut lines = head.split("\r\n");
    let status: u16 = lines.next()?.split(' ').nth(1)?.parse().ok()?;
    let mut chunked = false;
    let mut length = None;
    for line in lines {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked") {
            chunked = true;
        } else if name.eq_ignore_ascii_case("content-length") {
            length = value.parse::<usize>().ok();
        }
    }
    if !chunked {
        let body = match length {
            Some(length) => body.get(..length)?,
            None => body,
        };
        return Some((status, body.to_vec()));
    }
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let line_end = rest.windows(2).position(|w| w == b"\r\n")?;
        let size_text = std::str::from_utf8(&rest[..line_end]).ok()?;
        let size = usize::from_str_radix(size_text.split(';').next()?.trim(), 16).ok()?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Some((status, out));
        }
        out.extend_from_slice(rest.get(..size)?);
        rest = rest.get(size + 2..)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_are_read_with_a_length_or_in_chunks() {
        let plain = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(parse_response(plain), Some((200, b"{}".to_vec())));
        let chunked = b"HTTP/1.1 404 Not Found\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n{\"a\r\n4\r\n\":1}\r\n0\r\n\r\n";
        assert_eq!(parse_response(chunked), Some((404, b"{\"a\":1}".to_vec())));
    }

    /// The texts of the probes reg_v3 and reg_nosubj.
    #[test]
    fn errors_are_java_rest_client_exceptions() {
        let body = br#"{"error_code": 40401, "message": "Subject 'nosuch-value' not found."}"#;
        assert_eq!(
            answer(404, body).unwrap_err().0,
            "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: \
             Subject 'nosuch-value' not found.; error code: 40401"
        );
    }

    #[test]
    fn versions_are_read_as_integer_parse_int_reads_them() {
        assert_eq!(parse_java_int("2"), Some(2));
        assert_eq!(parse_java_int("-1"), Some(-1));
        assert_eq!(parse_java_int("+007"), Some(7));
        assert_eq!(parse_java_int(" 1"), None);
        assert_eq!(parse_java_int("abc"), None);
        assert_eq!(parse_java_int("2147483648"), None);
        assert_eq!(parse_java_int("-"), None);
    }

    #[test]
    fn urls_are_split_and_tls_is_refused_by_name() {
        let config = |url: &str| vec![(URL_PROPERTY.to_owned(), url.to_owned())];
        let registry = Registry::from_config(&config("http://h:1/, http://g/x/")).unwrap();
        assert_eq!(
            registry.urls,
            [
                Ok(("h:1".into(), "".into())),
                Ok(("g:80".into(), "/x".into()))
            ]
        );
        assert_eq!(
            Registry::from_config(&config(" ")).unwrap_err().to_string(),
            "java.lang.IllegalArgumentException: Expected at least one URL to be passed in \
             constructor"
        );
        assert!(matches!(
            Registry::from_config(&config("ftp://h:1")),
            Err(KafkaError::NotPorted { .. })
        ));
        let bare = Registry::from_config(&config("127.0.0.1:1")).unwrap();
        assert_eq!(
            bare.get("/subjects/s-value/versions/latest").unwrap_err().0,
            "java.net.MalformedURLException: no protocol: \
             127.0.0.1:1/subjects/s-value/versions/latest"
        );
        assert!(matches!(
            Registry::from_config(&config("https://h:1")),
            Err(KafkaError::NotPorted { .. })
        ));
        assert_eq!(
            Registry::from_config(&[]).unwrap_err().to_string(),
            "org.apache.kafka.common.config.ConfigException: Missing required configuration \
             \"schema.registry.url\" which has no default value."
        );
    }
}
