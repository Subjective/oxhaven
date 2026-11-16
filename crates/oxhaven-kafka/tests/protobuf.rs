//! Protobuf keys and values, their schemas read from a registry that the test serves.
//!
//! The texts, cells and requests asserted here are the reference's, from the probes
//! recorded in `tasks/7.4.8-kafka-protobuf.md`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, TimestampNanosecondType};
use arrow::util::display::array_value_to_string;
use bytes::Bytes;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_kafka::{KafkaError, Plan, ProtobufProtocol, Record, Spec};

/// A schema registry that answers each path with what the test gave it, and 404 with
/// Confluent's error body otherwise, and keeps the paths it was asked for.
struct Registry {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Registry {
    fn serve(answers: Vec<(String, u16, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let answers: HashMap<String, (u16, String)> = answers
            .into_iter()
            .map(|(path, status, body)| (path, (status, body)))
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
                    r#"{"error_code":40401,"message":"Subject 'nope' not found."}"#.to_owned(),
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

fn quoted(text: &str) -> String {
    let mut out = String::from("\"");
    for c in text.chars() {
        match c {
            '"' => out += "\\\"",
            '\\' => out += "\\\\",
            '\n' => out += "\\n",
            c => out.push(c),
        }
    }
    out + "\""
}

const DEP: &str = r#"syntax = "proto3";
package dep;
message Inner { int32 x = 1; string s = 2; }
"#;

const MAIN: &str = r#"syntax = "proto3";
package t;
import "dep.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";
message M {
  int32 i = 1; int64 l = 2; uint32 u = 3; sint32 z = 4; fixed64 f = 5;
  float fl = 6; double d = 7; bool b = 8; string s = 9; bytes by = 10;
  optional int32 o = 11;
  dep.Inner in = 12;
  repeated int32 ri = 13; repeated string rs = 14; repeated dep.Inner rin = 15;
  google.protobuf.Timestamp ts = 16;
  google.protobuf.Int32Value w = 17;
  oneof choice { int32 c1 = 18; string c2 = 19; }
  repeated bytes rby = 20;
  map<string, dep.Inner> mi = 21;
}
message Holder { dep.Inner in = 1; }
message RH { repeated Holder rh = 1; }
message Cyc { int32 v = 1; Cyc next = 2; }
message E { enum Color { RED = 0; } Color c = 1; int32 x = 2; }
message Mp { map<string, int32> m = 1; }
"#;

/// A registry with the subject `t-value` (id 3, referencing `dep` version 2), and
/// `a-value`, an Avro schema.
fn registry() -> Registry {
    let answer = |path: &str, body: String| (path.to_owned(), 200, body);
    Registry::serve(vec![
        answer(
            "/subjects/t-value/versions/latest",
            r#"{"subject":"t-value","version":1,"id":3,"schemaType":"PROTOBUF","schema":"x"}"#
                .to_owned(),
        ),
        answer(
            "/subjects/t-value/versions/1?deleted=false",
            r#"{"subject":"t-value","version":1,"id":3,"schemaType":"PROTOBUF","schema":"x"}"#
                .to_owned(),
        ),
        answer(
            "/schemas/ids/3?fetchMaxId=false&subject=t-value",
            format!(
                r#"{{"schemaType":"PROTOBUF","schema":{},"references":[{{"name":"dep.proto","subject":"dep","version":2}}]}}"#,
                quoted(MAIN)
            ),
        ),
        answer(
            "/subjects/dep/versions/2?deleted=true",
            format!(
                r#"{{"subject":"dep","version":2,"id":2,"schemaType":"PROTOBUF","schema":{}}}"#,
                quoted(DEP)
            ),
        ),
        answer(
            "/subjects/a-value/versions/latest",
            r#"{"subject":"a-value","version":1,"id":9,"schema":"\"int\""}"#.to_owned(),
        ),
        answer(
            "/subjects/bad-value/versions/latest",
            r#"{"subject":"bad-value","version":1,"id":8,"schemaType":"PROTOBUF","schema":"x"}"#
                .to_owned(),
        ),
        answer(
            "/schemas/ids/8?fetchMaxId=false&subject=bad-value",
            format!(
                r#"{{"schemaType":"PROTOBUF","schema":{}}}"#,
                quoted("syntax = \"proto3\";\nmessage { int32 a = 1; }\n")
            ),
        ),
    ])
}

fn spec(message: Option<&str>, include: Option<&[&str]>, protocol: ProtobufProtocol) -> Spec {
    Spec::Protobuf {
        subject: "t-value".to_owned(),
        version: None,
        message: message.map(str::to_owned),
        include: include.map(|paths| paths.iter().map(|p| p.to_string()).collect()),
        protocol,
    }
}

fn plan(registry: &Registry, value: &Spec) -> Result<Plan, KafkaError> {
    Plan::new("t", &registry.config(), &Spec::Ignore, value)
}

/// The value columns of a plan, after Kafka's partition, offset and timestamp.
fn columns(plan: &Plan) -> Vec<(String, ColumnType)> {
    let columns = plan.definition().columns().iter().skip(3);
    columns
        .map(|c| (c.name.clone(), c.column_type.clone()))
        .collect()
}

fn names(plan: &Plan) -> Vec<String> {
    columns(plan).into_iter().map(|(name, _)| name).collect()
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

fn decoded(plan: &Plan, records: &[Record]) -> Result<Vec<ArrayRef>, KafkaError> {
    plan.decode(records, 0)
        .map(|columns| columns.into_iter().skip(3).collect())
}

/// Each row of the columns as text, a null cell as `null`.
fn rows(plan: &Plan, columns: &[ArrayRef]) -> Vec<Vec<String>> {
    let names = names(plan);
    (0..columns[0].len())
        .map(|row| {
            columns
                .iter()
                .zip(&names)
                .map(|(column, name)| {
                    let cell = if column.is_null(row) {
                        "null".to_owned()
                    } else if let DataType::Timestamp(..) = column.data_type() {
                        let nanos = column.as_primitive::<TimestampNanosecondType>();
                        nanos.value(row).to_string()
                    } else {
                        array_value_to_string(column, row).unwrap()
                    };
                    format!("{name}={cell}")
                })
                .collect()
        })
        .collect()
}

fn varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// A varint field.
fn v(field: u32, value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    varint(u64::from(field << 3), &mut out);
    varint(value as u64, &mut out);
    out
}

/// A length-delimited field.
fn ld(field: u32, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    varint(u64::from((field << 3) | 2), &mut out);
    varint(bytes.len() as u64, &mut out);
    out.extend_from_slice(bytes);
    out
}

fn fixed(field: u32, wire: u32, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    varint(u64::from((field << 3) | wire), &mut out);
    out.extend_from_slice(bytes);
    out
}

/// Confluent's framing: magic byte 0, the schema id, no message indexes.
fn serdes(body: &[u8]) -> Vec<u8> {
    let mut out = vec![0, 0, 0, 0, 3, 0];
    out.extend_from_slice(body);
    out
}

/// Behavior: io.deephaven.protobuf.ProtobufDescriptorParserImpl and
/// ProtobufImpl#withMostAppropriateType (probes scalars, opt, nest, rep, repbytes, one,
/// wkt, mapmsg, ref)
#[test]
fn fields_become_columns_of_javas_types_and_values() {
    let registry = registry();
    let plan = plan(&registry, &spec(Some("M"), None, ProtobufProtocol::Serdes)).unwrap();
    let bytes = || ColumnType::array(ColumnType::Byte);
    let array = ColumnType::array;
    assert_eq!(
        columns(&plan),
        [
            ("i", ColumnType::Int),
            ("l", ColumnType::Long),
            ("u", ColumnType::Int),
            ("z", ColumnType::Int),
            ("f", ColumnType::Long),
            ("fl", ColumnType::Float),
            ("d", ColumnType::Double),
            ("b", ColumnType::Boolean),
            ("s", ColumnType::String),
            ("by", bytes()),
            ("o", ColumnType::Int),
            ("in_x", ColumnType::Int),
            ("in_s", ColumnType::String),
            ("ri", array(ColumnType::Int)),
            ("rs", array(ColumnType::String)),
            ("rin_x", array(ColumnType::Int)),
            ("rin_s", array(ColumnType::String)),
            ("ts", ColumnType::Instant),
            ("w", ColumnType::Int),
            ("c1", ColumnType::Int),
            ("c2", ColumnType::String),
            ("rby", array(bytes())),
            ("mi_key", array(ColumnType::String)),
            ("mi_value_x", array(ColumnType::Int)),
            ("mi_value_s", array(ColumnType::String)),
        ]
        .map(|(name, column_type)| (name.to_owned(), column_type))
    );
    let full = [
        v(1, -5),
        v(2, 1 << 62),
        v(3, 4_294_967_295),
        v(4, 5),
        fixed(5, 1, &7u64.to_le_bytes()),
        fixed(6, 5, &1.5f32.to_le_bytes()),
        fixed(7, 1, &(-2.25f64).to_le_bytes()),
        v(8, 1),
        ld(9, "hé".as_bytes()),
        ld(10, &[0, 255]),
        v(11, 0),
        ld(12, &v(1, 4)),
        ld(13, &[1, 2]),
        ld(14, b"a"),
        ld(14, b""),
        ld(15, &[v(1, 1), ld(2, b"p")].concat()),
        ld(15, b""),
        ld(16, &[v(1, 1), v(2, 2)].concat()),
        ld(17, &v(1, 9)),
        ld(19, b"c"),
        ld(20, &[1]),
        ld(20, b""),
        ld(21, &[ld(1, b"k"), ld(2, &v(1, 3))].concat()),
        ld(21, &ld(2, &v(1, 4))),
    ]
    .concat();
    let records = [
        record(0, Some(serdes(&full))),
        record(1, Some(serdes(b""))),
        record(
            2,
            Some(serdes(&[v(1, i64::from(i32::MIN)), v(18, 7)].concat())),
        ),
    ];
    let rows = rows(&plan, &decoded(&plan, &records).unwrap());
    assert_eq!(
        rows[0].join(" "),
        "i=-5 l=4611686018427387904 u=-1 z=-3 f=7 fl=1.5 d=-2.25 b=true s=hé by=[0, -1] o=0 \
         in_x=4 in_s= ri=[1, 2] rs=[a, ] rin_x=[1, 0] rin_s=[p, ] \
         ts=1000000002 w=9 c1=null c2=c rby=[[1], []] mi_key=[k, ] \
         mi_value_x=[3, 4] mi_value_s=[, ]"
    );
    // Presence fields and missing messages are null, the others their defaults; Java's
    // null of a type is a null.
    assert_eq!(
        rows[1].join(" "),
        "i=0 l=0 u=0 z=0 f=0 fl=0.0 d=0.0 b=false s= by=[] o=null in_x=null in_s=null ri=[] \
         rs=[] rin_x=[] rin_s=[] ts=null w=null c1=null c2=null rby=[] mi_key=[] \
         mi_value_x=[] mi_value_s=[]"
    );
    assert_eq!(rows[2][0], "i=null");
    assert_eq!(rows[2][19], "c1=7");
    assert_eq!(
        registry.requests(),
        [
            "/subjects/t-value/versions/latest",
            "/schemas/ids/3?fetchMaxId=false&subject=t-value",
            "/subjects/dep/versions/2?deleted=true",
        ]
    );
}

/// Behavior: io.deephaven.protobuf.FieldPath#anyMatches (probes inc1 to inc9)
#[test]
fn include_paths_select_the_fields() {
    let registry = registry();
    let names_of = |include: &[&str]| {
        let spec = spec(Some("M"), Some(include), ProtobufProtocol::Raw);
        names(&plan(&registry, &spec).unwrap())
    };
    assert_eq!(names_of(&["/in/x", "/s"]), ["s", "in_x"]);
    assert_eq!(names_of(&["/in/*"]), ["in_x", "in_s"]);
    assert_eq!(names_of(&["in"]), Vec::<String>::new());
    assert_eq!(names_of(&[]), Vec::<String>::new());
    assert_eq!(names_of(&["*"]).len(), 25);
    // A path through a map leaves its value one column, which Java makes a Map (probe
    // inc_map); the cycle is left out.
    let map = spec(Some("M"), Some(&["/mi/value/x"]), ProtobufProtocol::Raw);
    assert!(matches!(
        plan(&registry, &map),
        Err(KafkaError::NotPorted { .. })
    ));
    let cyclic = spec(Some("Cyc"), Some(&["/v"]), ProtobufProtocol::Raw);
    assert_eq!(names(&plan(&registry, &cyclic).unwrap()), ["v"]);
}

/// Behavior: ProtobufConsumeImpl#descriptor and ProtobufDescriptorParserImpl (probes
/// avro_subject, nosubject, v5, badproto, outer_in2, cyc, ts_root, en, map1, any)
#[test]
fn schemas_and_messages_that_cannot_be_read_fail_as_in_java() {
    let registry = registry();
    let refused = |spec: Spec| plan(&registry, &spec).err().unwrap().to_string();
    let subject = |subject: &str, version: Option<i32>| Spec::Protobuf {
        subject: subject.to_owned(),
        version,
        message: None,
        include: None,
        protocol: ProtobufProtocol::Serdes,
    };
    assert_eq!(
        refused(subject("a-value", None)),
        "java.lang.IllegalStateException: Expected schema type PROTOBUF but was AVRO"
    );
    assert_eq!(
        refused(subject("nope-value", Some(5))),
        "io.deephaven.UncheckedDeephavenException: io.confluent.kafka.schemaregistry.client.\
         rest.exceptions.RestClientException: Subject 'nope' not found.; error code: 40401 \
         (caused by io.confluent.kafka.schemaregistry.client.rest.exceptions.\
         RestClientException: Subject 'nope' not found.; error code: 40401)"
    );
    assert!(refused(subject("bad-value", None)).starts_with(
        "com.google.common.util.concurrent.UncheckedExecutionException: \
         java.lang.IllegalArgumentException: Invalid schema of type PROTOBUF, details: Could \
         not parse Protobuf - "
    ));
    // Version 1 is asked for as Java asks for it.
    assert!(plan(&registry, &subject("t-value", Some(1))).is_ok());
    assert!(registry
        .requests()
        .contains(&"/subjects/t-value/versions/1?deleted=false".to_owned()));
    let message = |name: &str| spec(Some(name), None, ProtobufProtocol::Serdes);
    assert_eq!(refused(message("Nope")), "java.lang.NullPointerException");
    assert_eq!(
        refused(message("Cyc")),
        "java.lang.IllegalArgumentException: Cyclical protobuf message descriptor detected at \
         [`t.Cyc` \"next\" -> `t.Cyc`]. Cyclic protobuf message descriptors are not \
         supported; use `io.deephaven.protobuf.ProtobufDescriptorParserOptions` fieldOptions \
         to exclude one of the fields to work around this."
    );
    assert_eq!(
        refused(message("google.protobuf.Timestamp")),
        "io.deephaven.api.util.NameValidator$InvalidNameException: Invalid column name: name \
         can not be null or empty"
    );
    assert_eq!(
        refused(message("E")),
        "not ported yet: io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl#getIngestData: \
         the column c of the Protobuf field t.E.c, which Java makes of the class \
         com.google.protobuf.Descriptors.EnumValueDescriptor"
    );
    assert_eq!(
        refused(message("Mp")),
        "not ported yet: io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl#getIngestData: \
         the column m of the Protobuf field t.Mp.m, which Java makes of the class java.util.Map"
    );
    // Without a name, the schema's first message.
    let first = plan(&registry, &spec(None, None, ProtobufProtocol::Serdes)).unwrap();
    assert_eq!(names(&first)[0], "i");
}

/// Behavior: CompletedFetch#parseRecord around ProtobufDeserializers, then
/// KafkaStreamPublisher#consumeRecords and the field copiers (probes w_short, w_tag0,
/// tomb, rh_absent, npe_i)
#[test]
fn records_that_cannot_be_read_fail_the_table_with_javas_texts() {
    let registry = registry();
    let failure = |spec: Spec, value: Option<Vec<u8>>| {
        let plan = plan(&registry, &spec).unwrap();
        decoded(&plan, &[record(4, value)])
            .err()
            .unwrap()
            .to_string()
    };
    let wrapped = "org.apache.kafka.common.errors.RecordDeserializationException: Error \
                   deserializing VALUE for partition t-0 at offset 4. If needed, please seek \
                   past the record to continue consumption.";
    let m = || spec(Some("M"), None, ProtobufProtocol::Serdes);
    assert_eq!(
        failure(m(), Some(vec![0, 0, 0])),
        format!(
            "{wrapped} (caused by org.apache.kafka.common.errors.SerializationException: Not \
             serdes protocol, expected at least 6 bytes)"
        )
    );
    assert_eq!(
        failure(spec(Some("M"), None, ProtobufProtocol::Raw), Some(vec![0])),
        format!(
            "{wrapped} (caused by org.apache.kafka.common.errors.SerializationException: \
             com.google.protobuf.InvalidProtocolBufferException: Protocol message contained an \
             invalid tag (zero).)"
        )
    );
    let null = "io.deephaven.kafka.ingest.KafkaIngesterException: Null records are not permitted";
    assert_eq!(failure(m(), None), null);
    assert_eq!(
        failure(spec(Some("M"), Some(&[]), ProtobufProtocol::Raw), None),
        null
    );
    // Right below an element of a repeated field Java does not skip a missing message.
    let rh = spec(Some("RH"), None, ProtobufProtocol::Raw);
    assert_eq!(
        failure(rh, Some(ld(1, b""))),
        "java.lang.NullPointerException: Cannot invoke \"java.lang.Integer.intValue()\" because \
         \"value\" is null"
    );
}
