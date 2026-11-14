//! Arrow Flight: schemas, snapshots of tables, and uploads.
//!
//! | Call | What it does |
//! | --- | --- |
//! | `Handshake` | Authenticates; see [`crate::session::handshake`] |
//! | `ListFlights` | Lists nothing: exports are not listed |
//! | `GetSchema`, `GetFlightInfo` | Describe the table a path descriptor `export/<id>` names |
//! | `DoGet` | Sends the schema of a table and a consistent snapshot of its rows |
//! | `DoPut` | Makes a static table from the record batches a client sends |
//! | `DoExchange` | Barrage: subscriptions and snapshots; see [`crate::barrage`] |
//! | `DoAction`, `ListActions` | Not ported |
//!
//! For the Barrage subscription code: [`wire_schema`] is the schema a client is told,
//! [`snapshot`] reads a table consistently, and [`record_batch_messages`] writes rows the
//! way the Java server does, with room for the `app_metadata` of the first message.
//!
//! `fixtures/` holds what the Java reference server answered to the same calls; the tests
//! compare with it.

mod encode;
mod service;
mod upload;
mod wire;

#[cfg(test)]
pub(crate) mod tests;

pub use encode::{
    encode_batch, record_batch_messages, schema_message, BatchOptions, EncodeError, EncodedBatch,
};
pub use service::{do_get_messages, flight_info, FlightGrpc};
pub use upload::{convert_schema, not_ported, schema_from_ipc_bytes, TableUpload, UploadError};
pub use wire::{snapshot, wire_schema, Snapshot};
