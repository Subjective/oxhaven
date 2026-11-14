//! Barrage: subscriptions to tables and snapshots of them, over Flight's `DoExchange`.
//!
//! | Module | Holds |
//! | --- | --- |
//! | [`codec`] | Row sets, shift data and column sets as Barrage sends them |
//! | [`writer`] | A table update as the Flight messages one subscriber receives |
//! | [`producer`] | The updates of one table, recorded and sent to its subscribers |
//! | [`snapshot`] | Snapshots outside a subscription: `DoGet` and snapshot requests |
//! | [`exchange`] | The `DoExchange` call: requests, and the stream of answers |
//!
//! `fixtures/` holds what the Java reference server sent for the same requests; the tests
//! compare with it.

pub mod codec;
pub mod exchange;
pub mod producer;
pub mod snapshot;
pub mod writer;

#[cfg(test)]
mod tests;

pub use exchange::Barrage;
pub use snapshot::snapshot_request_messages;
pub use writer::WireOptions;
