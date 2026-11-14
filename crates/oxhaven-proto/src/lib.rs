//! Protocol types for Oxhaven: the messages and gRPC service stubs that make it wire-compatible
//! with Deephaven clients.
//!
//! No Java counterpart: everything here is generated from the protocol definitions of the pinned
//! reference release, which are vendored unmodified under `proto/` (see `proto/README.md` for
//! where each file comes from). Nothing in this crate is written by hand except the module
//! layout and [`barrage::MAGIC`].
//!
//! # Layout
//!
//! The generated modules mirror the protobuf and flatbuffers packages under [`io`]. Shorter
//! aliases are exported from the crate root:
//!
//! | Alias | Package | Holds |
//! | --- | --- | --- |
//! | [`grpc`] | `io.deephaven.proto.backplane.grpc` | Application, Config, HierarchicalTable, InputTable, Object, PartitionedTable, Session, Storage and Table services; tickets |
//! | [`script`] | `io.deephaven.proto.backplane.script.grpc` | Console service |
//! | [`browser_flight`] | `io.deephaven.flightjs.protocol` | BrowserFlight service, which emulates Flight's client streams with unary calls |
//! | [`flight`] | `arrow.flight.protocol` | Arrow Flight messages and service, re-exported from the `arrow-flight` crate |
//! | [`barrage`] | `io.deephaven.barrage.flatbuf` | Barrage flatbuffers messages carried in Flight `app_metadata` |
//!
//! Each protobuf service `FooService` has a tonic client in `foo_service_client` and a server
//! trait and wrapper in `foo_service_server`, next to its messages.
//!
//! The reference server also serves `grpc.health.v1.Health`
//! (`io.deephaven.server.healthcheck.HealthCheckModule`). It is a standard gRPC service, not a
//! Deephaven definition, and is not generated here.
//!
//! # Example
//!
//! ```
//! use oxhaven_proto::grpc::{EmptyTableRequest, Ticket};
//! use prost::Message;
//!
//! let request = EmptyTableRequest {
//!     result_id: Some(Ticket {
//!         ticket: vec![b'e', 1, 0, 0, 0],
//!     }),
//!     size: 10,
//! };
//! let bytes = request.encode_to_vec();
//! assert_eq!(EmptyTableRequest::decode(bytes.as_slice()), Ok(request));
//! ```

/// The generated code, in modules named after the packages of the definitions.
pub mod io {
    /// `io.deephaven`
    pub mod deephaven {
        /// `io.deephaven.proto`
        pub mod proto {
            /// `io.deephaven.proto.backplane`
            pub mod backplane {
                /// `io.deephaven.proto.backplane.grpc`: every Deephaven service except the
                /// console, and the ticket types they share.
                #[allow(clippy::all, clippy::pedantic, deprecated, rustdoc::all)]
                pub mod grpc {
                    tonic::include_proto!("io.deephaven.proto.backplane.grpc");
                }

                /// `io.deephaven.proto.backplane.script`
                pub mod script {
                    /// `io.deephaven.proto.backplane.script.grpc`: the console service.
                    #[allow(clippy::all, clippy::pedantic, deprecated, rustdoc::all)]
                    pub mod grpc {
                        tonic::include_proto!("io.deephaven.proto.backplane.script.grpc");
                    }
                }
            }
        }

        /// `io.deephaven.flightjs`
        pub mod flightjs {
            /// `io.deephaven.flightjs.protocol`: the BrowserFlight service. Its Flight message
            /// types are those of the `arrow-flight` crate.
            #[allow(clippy::all, clippy::pedantic, deprecated, rustdoc::all)]
            pub mod protocol {
                tonic::include_proto!("io.deephaven.flightjs.protocol");
            }
        }

        /// `io.deephaven.barrage`
        pub mod barrage {
            /// `io.deephaven.barrage.flatbuf`: the Barrage flatbuffers messages.
            pub mod flatbuf {
                pub use crate::barrage_generated::io::deephaven::barrage::flatbuf::*;
            }
        }
    }
}

/// Output of `flatc --rust` for `proto/barrage/Barrage.fbs`, checked in byte for byte; see
/// `proto/README.md` for how to regenerate it. It is reached through [`barrage`].
#[rustfmt::skip]
#[allow(
    clippy::all,
    clippy::pedantic,
    rustdoc::all,
    unsafe_op_in_unsafe_fn,
    unused_imports,
    dead_code
)]
#[path = "barrage/Barrage_generated.rs"]
mod barrage_generated;

pub mod barrage;

pub use arrow_flight as flight;
pub use io::deephaven::flightjs::protocol as browser_flight;
pub use io::deephaven::proto::backplane::grpc;
pub use io::deephaven::proto::backplane::script::grpc as script;

/// The encoded `google.protobuf.FileDescriptorSet` that the service stubs were generated from,
/// including the imported `Flight.proto` and `google/protobuf/any.proto`.
///
/// No Java counterpart: Java reaches the same information through each generated class's
/// `getDescriptor()`.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/oxhaven_descriptor.bin"));
