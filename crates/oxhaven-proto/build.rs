//! Compiles the vendored `.proto` files into prost message types and tonic client and server
//! stubs.
//!
//! No Java counterpart: the reference runs `protoc` in a Docker image
//! (`proto/proto-backplane-grpc/Dockerfile`). This script uses `protox`, a `protoc` replacement
//! written in Rust, so that a build needs nothing installed besides the Rust toolchain.

use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use prost::Message;

/// Include roots, in the order the reference passes them to `protoc` (`-I/dependencies
/// -I/includes`). `BrowserFlight.proto` imports `Flight.proto` by its bare file name.
const INCLUDE_ROOTS: [&str; 3] = ["proto/barrage", "proto/arrow", "proto/deephaven-core"];

/// Where the Deephaven service definitions live, relative to the crate root.
const DEEPHAVEN_PROTO_DIR: &str = "proto/deephaven-core/deephaven_core/proto";

fn main() -> Result<(), Box<dyn Error>> {
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(DEEPHAVEN_PROTO_DIR)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "proto") {
            files.push(path);
        }
    }
    // Directory order is not specified; the generated code must not depend on it.
    files.sort();
    files.push(PathBuf::from("proto/barrage/BrowserFlight.proto"));

    for root in INCLUDE_ROOTS {
        println!("cargo:rerun-if-changed={root}");
    }

    let descriptors = protox::compile(&files, INCLUDE_ROOTS)?;

    // The descriptors that the stubs are generated from, kept for `FILE_DESCRIPTOR_SET`.
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    fs::write(
        out_dir.join("oxhaven_descriptor.bin"),
        descriptors.encode_to_vec(),
    )?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // The Arrow Flight messages and the Flight service itself come from the `arrow-flight`
        // crate. `Flight.proto` is vendored only so that this import resolves.
        .extern_path(".arrow.flight.protocol", "::arrow_flight")
        .compile_fds(descriptors)?;
    Ok(())
}
