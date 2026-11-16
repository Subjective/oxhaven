//! Avro keys and values, read through a schema registry that the test serves.
//!
//! The texts and requests asserted here are the reference's, from the probes recorded in
//! `tasks/7.4.7-kafka-avro.md`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{Int32Type, Int64Type, TimestampNanosecondType};
use bytes::Bytes;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_kafka::{AvroMapping, AvroSchema, KafkaError, Plan, Record, Spec};

/// A schema registry that answers each path with what the test gave it, and 404 with
/// Confluent's error body otherwise, and keeps the paths it was asked for.
struct Registry {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Registry {
    fn serve(answers: &[(&str, u16, &str)]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let answers: HashMap<String, (u16, String)> = answers
            .iter()
            .map(|(path, status, body)| (path.to_string(), (*status, body.to_string())))
            .collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buffer[..n]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split(' ').nth(1).unwrap_or("").to_owned();
                seen.lock().unwrap().push(path.clone());
                let (status, body) = answers.get(&path).cloned().unwrap_or((
                    404,
                    r#"{"error_code":40403,"message":"Schema not found"}"#.to_owned(),
                ));
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self { url, requests }
    }

    fn config(&self) -> Vec<(String, String)> {
        vec![("schema.registry.url".to_owned(), self.url.clone())]
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

/// The answer to `/schemas/ids/<id>`: the schema as a JSON string.
fn schema_answer(schema: &str) -> String {
    let quoted: String = schema
        .chars()
        .flat_map(|c| match c {
            '"' => vec!['\\', '"'],
            '\\' => vec!['\\', '\\'],
            '\n' => vec!['\\', 'n'],
            c => vec![c],
        })
        .collect();
    format!(r#"{{"schema":"{quoted}"}}"#)
}

fn zigzag(value: i64, out: &mut Vec<u8>) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn string(text: &[u8], out: &mut Vec<u8>) {
    zigzag(text.len() as i64, out);
    out.extend_from_slice(text);
}

/// The Confluent wire format: magic byte 0, the schema id, then the Avro body.
fn framed(id: i32, body: &[u8]) -> Vec<u8> {
    let mut out = vec![0];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn record(offset: i64, value: Option<Vec<u8>>) -> Record {
    Record {
        partition: 0,
        offset,
        timestamp: 0,
        key: None,
        value: value.map(Bytes::from),
    }
}

fn value_spec(schema: AvroSchema, mapping: AvroMapping) -> Spec {
    Spec::Avro { schema, mapping }
}

fn text(schema: &str) -> AvroSchema {
    AvroSchema::Text(schema.to_owned())
}

/// The value columns of a plan, after Kafka's partition, offset and timestamp.
fn columns(plan: &Plan) -> Vec<(String, ColumnType)> {
    let columns = plan.definition().columns().iter().skip(3);
    columns
        .map(|c| (c.name.clone(), c.column_type.clone()))
        .collect()
}

fn decoded(plan: &Plan, records: &[Record]) -> Result<Vec<ArrayRef>, KafkaError> {
    plan.decode(records, 0)
        .map(|columns| columns.into_iter().skip(3).collect())
}

fn ints(column: &ArrayRef) -> Vec<Option<i32>> {
    column.as_primitive::<Int32Type>().iter().collect()
}

fn longs(column: &ArrayRef) -> Vec<Option<i64>> {
    column.as_primitive::<Int64Type>().iter().collect()
}

fn strings(column: &ArrayRef) -> Vec<Option<&str>> {
    column.as_string::<i32>().iter().collect()
}

const READER: &str = r#"{"type": "record", "name": "Trade", "fields": [
    {"name": "id", "type": "long"},
    {"name": "sym", "type": "string"},
    {"name": "qty", "type": ["null", "int"]},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}},
    {"name": "leg", "type": {"type": "record", "name": "Leg", "fields": [
        {"name": "n", "type": "int"}]}},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "venue", "type": "string", "default": "X"}]}"#;

/// The reader's schema, with `id` an int, no `venue`, and a `junk` field at the end.
const WRITER: &str = r#"{"type": "record", "name": "Trade", "fields": [
    {"name": "id", "type": "int"},
    {"name": "sym", "type": "string"},
    {"name": "qty", "type": ["null", "int"]},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}},
    {"name": "leg", "type": {"type": "record", "name": "Leg", "fields": [
        {"name": "n", "type": "int"}]}},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "junk", "type": "string"}]}"#;

/// A `Trade` in the writer's schema.
fn trade(
    id: i64,
    sym: &str,
    qty: Option<i64>,
    ts: i64,
    side: i64,
    n: i64,
    tags: &[&str],
) -> Vec<u8> {
    let mut out = Vec::new();
    zigzag(id, &mut out);
    string(sym.as_bytes(), &mut out);
    match qty {
        None => zigzag(0, &mut out),
        Some(qty) => {
            zigzag(1, &mut out);
            zigzag(qty, &mut out);
        }
    }
    zigzag(ts, &mut out);
    zigzag(side, &mut out);
    zigzag(n, &mut out);
    if !tags.is_empty() {
        zigzag(tags.len() as i64, &mut out);
        for tag in tags {
            string(tag.as_bytes(), &mut out);
        }
    }
    zigzag(0, &mut out);
    string(b"skipped", &mut out);
    out
}

#[test]
fn fields_become_columns_and_records_are_read_in_the_readers_schema() {
    let registry = Registry::serve(&[(
        "/schemas/ids/7?fetchMaxId=false&subject=t-value",
        200,
        &schema_answer(WRITER),
    )]);
    let value = value_spec(text(READER), AvroMapping::Direct);
    let plan = Plan::new("t", &registry.config(), &Spec::Ignore, &value).unwrap();
    assert_eq!(
        columns(&plan),
        [
            ("id".to_owned(), ColumnType::Long),
            ("sym".to_owned(), ColumnType::String),
            ("qty".to_owned(), ColumnType::Int),
            ("ts".to_owned(), ColumnType::Instant),
            ("side".to_owned(), ColumnType::String),
            ("leg__n".to_owned(), ColumnType::Int),
            ("tags".to_owned(), ColumnType::array(ColumnType::String)),
            ("venue".to_owned(), ColumnType::String),
        ]
    );
    // A schema given as text asks the registry for nothing until a record arrives.
    assert_eq!(registry.requests(), Vec::<String>::new());

    let records = [
        record(
            0,
            Some(framed(7, &trade(1, "A", Some(5), 1_000, 1, 3, &["x", "y"]))),
        ),
        // Java's null long is a null cell, but Java's null int widened to a long is not;
        // the trailing bytes are ignored.
        record(
            1,
            Some(framed(
                7,
                &[
                    trade(i32::MIN as i64, "é", None, i64::MIN, 0, 4, &[]),
                    vec![9],
                ]
                .concat(),
            )),
        ),
        // A tombstone is a row of null cells.
        record(2, None),
    ];
    let cells = decoded(&plan, &records).unwrap();
    assert_eq!(longs(&cells[0]), [Some(1), Some(i32::MIN as i64), None]);
    assert_eq!(strings(&cells[1]), [Some("A"), Some("é"), None]);
    assert_eq!(ints(&cells[2]), [Some(5), None, None]);
    let ts = cells[3].as_primitive::<TimestampNanosecondType>();
    assert_eq!(
        ts.iter().collect::<Vec<_>>(),
        [Some(1_000_000_000), None, None]
    );
    assert_eq!(strings(&cells[4]), [Some("SELL"), Some("BUY"), None]);
    assert_eq!(ints(&cells[5]), [Some(3), Some(4), None]);
    let tags = cells[6].as_list::<i32>();
    assert_eq!(strings(&tags.value(0)), [Some("x"), Some("y")]);
    assert_eq!(tags.value(1).len(), 0);
    assert!(tags.is_null(2));
    assert_eq!(strings(&cells[7]), [Some("X"), Some("X"), None]);
    // The writer's schema is fetched once, by the subject of the topic's values.
    assert_eq!(
        registry.requests(),
        ["/schemas/ids/7?fetchMaxId=false&subject=t-value"]
    );
}

#[test]
fn a_record_that_cannot_be_read_fails_with_the_text_java_gives() {
    let narrowing =
        r#"{"type": "record", "name": "Trade", "fields": [{"name": "id", "type": "long"}]}"#;
    let registry = Registry::serve(&[
        (
            "/schemas/ids/7?fetchMaxId=false&subject=t-value",
            200,
            &schema_answer(WRITER),
        ),
        (
            "/schemas/ids/8?fetchMaxId=false&subject=t-value",
            200,
            &schema_answer(narrowing),
        ),
    ]);
    let reader =
        r#"{"type": "record", "name": "Trade", "fields": [{"name": "id", "type": "int"}]}"#;
    let value = value_spec(text(reader), AvroMapping::Direct);
    let plan = Plan::new("t", &registry.config(), &Spec::Ignore, &value).unwrap();
    let failure = |offset: i64, bytes: Vec<u8>| {
        decoded(&plan, &[record(offset, Some(bytes))])
            .unwrap_err()
            .to_string()
    };
    let outer = |offset: i64| {
        format!(
            "org.apache.kafka.common.errors.RecordDeserializationException: Error deserializing \
             VALUE for partition t-0 at offset {offset}. If needed, please seek past the record \
             to continue consumption. (caused by "
        )
    };
    let schema_id = |id: i32| {
        format!("SchemaId{{schemaType='AVRO', id={id}, guid='null', messageIndexes=null}}")
    };
    for (offset, bytes) in [(0, vec![]), (1, vec![0, 0, 0]), (2, vec![2, 0, 0, 0, 7, 2])] {
        assert_eq!(
            failure(offset, bytes),
            format!(
                "{}org.apache.kafka.common.errors.SerializationException: Error deserializing \
                 schema ID)",
                outer(offset)
            )
        );
    }
    assert_eq!(
        failure(3, framed(999, &[2])),
        format!(
            "{}org.apache.kafka.common.errors.SerializationException: Error retrieving Avro value \
             schema for id {}: io.confluent.kafka.schemaregistry.client.rest.exceptions.\
             RestClientException: Schema not found; error code: 40403)",
            outer(3),
            schema_id(999)
        )
    );
    // The writer's long cannot be narrowed to the reader's int.
    assert_eq!(
        failure(4, framed(8, &[2])),
        format!(
            "{}org.apache.kafka.common.errors.SerializationException: Error deserializing Avro \
             message for id {}: org.apache.avro.AvroTypeException: Field \"id\" content \
             mismatch: Found long, expecting int)",
            outer(4),
            schema_id(8)
        )
    );
    // A record cut short.
    assert_eq!(
        failure(5, framed(7, &[2])),
        format!(
            "{}org.apache.kafka.common.errors.SerializationException: Error deserializing Avro \
             message for id {}: java.io.EOFException)",
            outer(5),
            schema_id(7)
        )
    );
}

#[test]
fn a_subject_is_fetched_by_version_with_the_requests_java_makes() {
    let registry = Registry::serve(&[
        (
            "/subjects/my%20topic-value/versions/latest",
            200,
            r#"{"subject":"my topic-value","version":2,"id":5,"schema":"ignored"}"#,
        ),
        (
            "/subjects/my%20topic-value/versions/1?deleted=false",
            200,
            r#"{"subject":"my topic-value","version":1,"id":4,"schema":"ignored"}"#,
        ),
        (
            "/schemas/ids/5?fetchMaxId=false&subject=",
            200,
            &schema_answer(READER),
        ),
        (
            "/schemas/ids/4?fetchMaxId=false&subject=",
            200,
            &schema_answer(WRITER),
        ),
        (
            "/subjects/my%20topic-value/versions/3?deleted=false",
            404,
            r#"{"error_code":40402,"message":"Version 3 not found."}"#,
        ),
    ]);
    let subject = |version: Option<&str>| AvroSchema::Registry {
        subject: "my topic-value".to_owned(),
        version: version.map(str::to_owned),
    };
    let plan = |version: Option<&str>| {
        let value = value_spec(subject(version), AvroMapping::Direct);
        Plan::new("t", &registry.config(), &Spec::Ignore, &value)
    };
    let latest = plan(Some("latest")).unwrap();
    assert_eq!(columns(&latest).last().unwrap().0, "venue");
    let first = plan(Some("1")).unwrap();
    assert_eq!(columns(&first).last().unwrap().0, "junk");
    assert_eq!(
        registry.requests(),
        [
            "/subjects/my%20topic-value/versions/latest",
            "/schemas/ids/5?fetchMaxId=false&subject=",
            "/subjects/my%20topic-value/versions/1?deleted=false",
            "/schemas/ids/4?fetchMaxId=false&subject=",
        ]
    );
    let refused = |version| plan(version).err().unwrap().to_string();
    assert_eq!(
        refused(Some("3")),
        "io.deephaven.UncheckedDeephavenException: io.confluent.kafka.schemaregistry.client.\
         rest.exceptions.RestClientException: Version 3 not found.; error code: 40402"
    );
    assert_eq!(
        refused(Some("abc")),
        "java.lang.NumberFormatException: For input string: \"abc\""
    );
    assert_eq!(
        refused(Some(" 1")),
        "java.lang.NumberFormatException: For input string: \" 1\""
    );
}

#[test]
fn a_mapping_names_and_selects_the_columns() {
    let registry = Registry::serve(&[]);
    let pairs = |pairs: &[(&str, Option<&str>)]| {
        let owned = |(f, c): &(&str, Option<&str>)| (f.to_string(), c.map(str::to_owned));
        pairs.iter().map(owned).collect::<Vec<_>>()
    };
    let names = |mapping: AvroMapping| {
        let value = value_spec(text(READER), mapping);
        let plan = Plan::new("t", &registry.config(), &Spec::Ignore, &value).unwrap();
        columns(&plan)
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    };
    // Fields not mapped keep their path, with its dot; a field mapped to None is left out.
    assert_eq!(
        names(AvroMapping::Identity(pairs(&[
            ("sym", Some("bad name")),
            ("tags", None)
        ]))),
        ["id", "bad name", "qty", "ts", "side", "leg.n", "venue"]
    );
    // Only the mapped fields; a nested field needs its parent mapped too.
    assert_eq!(
        names(AvroMapping::Only(pairs(&[
            ("venue", Some("V")),
            ("leg.n", Some("N")),
            ("id", Some("I"))
        ]))),
        ["I", "V"]
    );
    assert_eq!(
        names(AvroMapping::Only(pairs(&[
            ("leg", Some("L")),
            ("leg.n", Some("N"))
        ]))),
        ["N"]
    );
    let value = value_spec(
        text(READER),
        AvroMapping::Identity(pairs(&[("sym", Some("id"))])),
    );
    let duplicate = Plan::new("t", &registry.config(), &Spec::Ignore, &value);
    assert_eq!(
        duplicate.err().unwrap().to_string(),
        "java.lang.IllegalArgumentException: Supplied ColumnDefinitions include duplicate names [id]"
    );
}

#[test]
fn a_schema_registry_url_is_required_and_tls_is_refused_by_name() {
    let value = value_spec(text(READER), AvroMapping::Direct);
    let missing = Plan::new("t", &[], &Spec::Ignore, &value);
    assert_eq!(
        missing.err().unwrap().to_string(),
        "org.apache.kafka.common.config.ConfigException: Missing required configuration \
         \"schema.registry.url\" which has no default value."
    );
    let tls = vec![(
        "schema.registry.url".to_owned(),
        "https://127.0.0.1:1".to_owned(),
    )];
    let refused = Plan::new("t", &tls, &Spec::Ignore, &value).err().unwrap();
    assert!(matches!(refused, KafkaError::NotPorted { .. }), "{refused}");
}

/// The reference's arrays of a union with null, and of enums (probes ai, al, ae, atm).
#[test]
fn arrays_of_unions_and_enums_are_read_as_java_reads_them() {
    let array = |items: &str| {
        format!(
            r#"{{"type": "record", "name": "A", "fields": [
                {{"name": "a", "type": {{"type": "array", "items": {items}}}}}]}}"#
        )
    };
    let nullable_ints = array(r#"["null", "int"]"#);
    let enums = array(r#"{"type": "enum", "name": "E", "symbols": ["R", "G"]}"#);
    let registry = Registry::serve(&[
        (
            "/schemas/ids/1?fetchMaxId=false&subject=t-value",
            200,
            &schema_answer(&nullable_ints),
        ),
        (
            "/schemas/ids/2?fetchMaxId=false&subject=t-value",
            200,
            &schema_answer(&enums),
        ),
    ]);
    let plan = |schema: &str| {
        let value = value_spec(text(schema), AvroMapping::Direct);
        Plan::new("t", &registry.config(), &Spec::Ignore, &value)
    };

    // A null element of a primitive array is Java's null of the element type.
    let int_plan = plan(&nullable_ints).unwrap();
    assert_eq!(
        columns(&int_plan),
        [("a".to_owned(), ColumnType::array(ColumnType::Int))]
    );
    let mut body = Vec::new();
    zigzag(3, &mut body);
    for branch_and_value in [&[1, 2][..], &[0], &[1, 4]] {
        for v in branch_and_value {
            zigzag(*v, &mut body);
        }
    }
    zigzag(0, &mut body);
    let cells = decoded(&int_plan, &[record(0, Some(framed(1, &body)))]).unwrap();
    let list = cells[0].as_list::<i32>();
    assert_eq!(ints(&list.value(0)), [Some(2), None, Some(4)]);

    // Java declares String[] for an array of enums, fills an empty one, and fails on any
    // other.
    let enum_plan = plan(&enums).unwrap();
    assert_eq!(
        columns(&enum_plan),
        [("a".to_owned(), ColumnType::array(ColumnType::String))]
    );
    let empty = decoded(&enum_plan, &[record(0, Some(framed(2, &[0])))]).unwrap();
    assert_eq!(empty[0].as_list::<i32>().value(0).len(), 0);
    let failure = decoded(&enum_plan, &[record(1, Some(framed(2, &[2, 2, 0])))]).unwrap_err();
    assert!(
        failure.to_string().starts_with(
            "java.lang.ClassCastException: class org.apache.avro.generic.GenericData$EnumSymbol \
             cannot be cast to class java.lang.CharSequence"
        ),
        "{failure}"
    );

    // The element's logical type is read from the union, which has none.
    let stamps = array(r#"["null", {"type": "long", "logicalType": "timestamp-micros"}]"#);
    assert_eq!(
        plan(&stamps).err().unwrap().to_string(),
        "java.lang.IllegalArgumentException: Can not map field without a logical type to \
         Instant[]: field=a"
    );
}
