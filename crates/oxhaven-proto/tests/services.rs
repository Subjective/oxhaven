//! Checks that the generated stubs cover exactly the services and RPCs of the vendored `.proto`
//! files.
//!
//! No Java counterpart. The expected RPC lists are not written down here: they are read from
//! the text of the `.proto` files by a small scanner in this file, and compared with two
//! independent views of what was generated, the compiled descriptors and the method paths in
//! the generated Rust source.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use oxhaven_proto::{browser_flight, grpc, script};
use prost::Message;
use prost_types::FileDescriptorSet;

/// Fully qualified service name to the names of its RPCs.
type Services = BTreeMap<String, BTreeSet<String>>;

/// Every service the vendored definitions declare. `FlightService` is declared by the imported
/// `Flight.proto` and implemented by the `arrow-flight` crate.
const EXPECTED_SERVICES: [&str; 12] = [
    "arrow.flight.protocol.FlightService",
    "io.deephaven.flightjs.protocol.BrowserFlightService",
    "io.deephaven.proto.backplane.grpc.ApplicationService",
    "io.deephaven.proto.backplane.grpc.ConfigService",
    "io.deephaven.proto.backplane.grpc.HierarchicalTableService",
    "io.deephaven.proto.backplane.grpc.InputTableService",
    "io.deephaven.proto.backplane.grpc.ObjectService",
    "io.deephaven.proto.backplane.grpc.PartitionedTableService",
    "io.deephaven.proto.backplane.grpc.SessionService",
    "io.deephaven.proto.backplane.grpc.StorageService",
    "io.deephaven.proto.backplane.grpc.TableService",
    "io.deephaven.proto.backplane.script.grpc.ConsoleService",
];

const FLIGHT_SERVICE: &str = "arrow.flight.protocol.FlightService";

fn proto_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("proto")
}

fn proto_files() -> Vec<PathBuf> {
    let root = proto_root();
    let mut files = vec![
        root.join("arrow/Flight.proto"),
        root.join("barrage/BrowserFlight.proto"),
    ];
    let deephaven = root.join("deephaven-core/deephaven_core/proto");
    for entry in fs::read_dir(&deephaven).expect("read the vendored proto directory") {
        let path = entry.expect("directory entry").path();
        if path.extension().is_some_and(|ext| ext == "proto") {
            files.push(path);
        }
    }
    files.sort();
    files
}

/// Splits protobuf source into identifiers (dots included) and single punctuation characters,
/// dropping comments and string literals.
fn tokens(source: &str) -> Vec<String> {
    let chars: Vec<char> = source.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
        } else if c == '"' || c == '\'' {
            i += 1;
            while i < chars.len() && chars[i] != c {
                if chars[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
        } else if c.is_alphanumeric() || c == '_' || c == '.' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
            {
                i += 1;
            }
            out.push(chars[start..i].iter().collect());
        } else {
            if !c.is_whitespace() {
                out.push(c.to_string());
            }
            i += 1;
        }
    }
    out
}

/// Collects the services of one `.proto` file from its text: a `package` statement, then for
/// each top-level `service Name { ... }` block the `rpc Name` entries directly inside it.
fn scan_services(source: &str, into: &mut Services) {
    let tokens = tokens(source);
    let mut package = String::new();
    let mut depth = 0usize;
    let mut current: Option<String> = None;
    let mut i = 0;
    while i < tokens.len() {
        match tokens[i].as_str() {
            "{" => depth += 1,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    current = None;
                }
            }
            "package" if depth == 0 => {
                package = tokens[i + 1].clone();
                i += 1;
            }
            "service" if depth == 0 => {
                let name = format!("{package}.{}", tokens[i + 1]);
                let previous = into.insert(name.clone(), BTreeSet::new());
                assert!(previous.is_none(), "service {name} declared twice");
                current = Some(name);
                i += 1;
            }
            "rpc" if depth == 1 && current.is_some() => {
                let service = current.as_ref().expect("rpc outside a service block");
                let method = tokens[i + 1].clone();
                let rpcs = into.get_mut(service).expect("service was inserted");
                assert!(rpcs.insert(method.clone()), "{service}/{method} twice");
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    assert_eq!(depth, 0, "unbalanced braces");
}

/// The services declared by the text of the vendored `.proto` files.
fn services_in_proto_text() -> Services {
    let mut services = Services::new();
    for file in proto_files() {
        let source = fs::read_to_string(&file).expect("read a vendored proto file");
        scan_services(&source, &mut services);
    }
    services
}

/// The services in the descriptors that the build script compiled the stubs from.
fn services_in_descriptors() -> Services {
    let set = FileDescriptorSet::decode(oxhaven_proto::FILE_DESCRIPTOR_SET)
        .expect("decode FILE_DESCRIPTOR_SET");
    let mut services = Services::new();
    for file in &set.file {
        for service in &file.service {
            let name = format!("{}.{}", file.package(), service.name());
            let rpcs = service
                .method
                .iter()
                .map(|method| method.name().to_owned())
                .collect();
            assert!(services.insert(name, rpcs).is_none());
        }
    }
    services
}

/// How often each `/package.Service/Method` path literal appears in the generated Rust source
/// of the three packages compiled by the build script, keyed by (service, method).
fn method_paths_in_generated_source() -> BTreeMap<(String, String), usize> {
    let out_dir = Path::new(env!("OUT_DIR"));
    let mut counts = BTreeMap::new();
    for package in [
        "io.deephaven.proto.backplane.grpc",
        "io.deephaven.proto.backplane.script.grpc",
        "io.deephaven.flightjs.protocol",
    ] {
        let source = fs::read_to_string(out_dir.join(format!("{package}.rs")))
            .expect("read generated source");
        for piece in source.split("\"/").skip(1) {
            let Some(end) = piece.find('"') else { continue };
            let path = &piece[..end];
            let Some((service, method)) = path.split_once('/') else {
                continue;
            };
            if service.starts_with(package) {
                *counts
                    .entry((service.to_owned(), method.to_owned()))
                    .or_insert(0) += 1;
            }
        }
    }
    counts
}

/// The name under which the `arrow-flight` crate's server stub registers.
fn flight_service_name() -> &'static str {
    use oxhaven_proto::flight::flight_service_server::FlightServiceServer;
    <FlightServiceServer<()> as tonic::server::NamedService>::NAME
}

#[test]
fn scanner_reads_services_and_ignores_comments_and_nested_braces() {
    let source = r#"
        syntax = "proto3";
        package a.b; // rpc NotThis(X) returns (Y) {}
        /* service Hidden { rpc Nope(X) returns (Y) {} } */
        message M { message rpc { } string service = 1; }
        service S {
          option (x) = "rpc Fake(";
          rpc One(M) returns (M) {}
          rpc Two(stream M) returns (stream M) { option deprecated = true; }
          rpc Three(M) returns (M);
        }
        service Empty {}
    "#;
    let mut services = Services::new();
    scan_services(source, &mut services);
    let names: Vec<&str> = services.keys().map(String::as_str).collect();
    assert_eq!(names, ["a.b.Empty", "a.b.S"]);
    let rpcs: Vec<&str> = services["a.b.S"].iter().map(String::as_str).collect();
    assert_eq!(rpcs, ["One", "Three", "Two"]);
    assert!(services["a.b.Empty"].is_empty());
}

#[test]
fn proto_files_declare_exactly_the_expected_services() {
    let services = services_in_proto_text();
    let names: Vec<&str> = services.keys().map(String::as_str).collect();
    assert_eq!(names, EXPECTED_SERVICES);
    for (name, rpcs) in &services {
        assert!(!rpcs.is_empty(), "{name} has no RPCs");
    }
}

#[test]
fn descriptors_match_the_proto_text() {
    assert_eq!(services_in_descriptors(), services_in_proto_text());
}

#[test]
fn generated_stubs_have_every_rpc_in_client_and_server() {
    let mut expected = services_in_proto_text();
    // Not generated here; see `flight_service_is_the_arrow_flight_one`.
    expected
        .remove(FLIGHT_SERVICE)
        .expect("Flight.proto parsed");

    let paths = method_paths_in_generated_source();
    let mut generated = Services::new();
    for ((service, method), count) in &paths {
        // One occurrence in the client method, one in the server's dispatch.
        assert_eq!(*count, 2, "/{service}/{method}");
        generated
            .entry(service.clone())
            .or_default()
            .insert(method.clone());
    }
    assert_eq!(generated, expected);

    // Print the derived counts; visible with `cargo test -- --nocapture`.
    for (service, rpcs) in &expected {
        println!("{service}: {} rpcs", rpcs.len());
    }
}

#[test]
fn generated_service_names_match_the_proto_text() {
    let generated = [
        grpc::application_service_server::SERVICE_NAME,
        grpc::config_service_server::SERVICE_NAME,
        grpc::hierarchical_table_service_server::SERVICE_NAME,
        grpc::input_table_service_server::SERVICE_NAME,
        grpc::object_service_server::SERVICE_NAME,
        grpc::partitioned_table_service_server::SERVICE_NAME,
        grpc::session_service_server::SERVICE_NAME,
        grpc::storage_service_server::SERVICE_NAME,
        grpc::table_service_server::SERVICE_NAME,
        script::console_service_server::SERVICE_NAME,
        browser_flight::browser_flight_service_server::SERVICE_NAME,
        flight_service_name(),
    ];
    let generated: BTreeSet<&str> = generated.into_iter().collect();
    let services = services_in_proto_text();
    let declared: BTreeSet<&str> = services.keys().map(String::as_str).collect();
    assert_eq!(generated, declared);
}

#[test]
fn flight_service_is_the_arrow_flight_one() {
    assert_eq!(flight_service_name(), FLIGHT_SERVICE);
}
