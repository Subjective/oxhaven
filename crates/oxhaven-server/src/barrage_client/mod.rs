//! The client side of Barrage: tables of another Deephaven-compatible server, fetched,
//! snapshotted, and kept up to date here as refreshing tables.
//!
//! A [`BarrageSession`] is a session with the other server. [`BarrageSession::snapshot`]
//! gives a static table with the rows the server's table has now.
//! [`BarrageSession::subscribe`] gives a refreshing table of the local update graph that
//! changes as the server's table does: the server's update stream is read by a task of
//! the session, and each cycle of the local graph applies what has arrived.
//!
//! This is a module of the server crate and not a crate of its own because it is the
//! other end of what the crate already has: the row set, shift and column set codecs of
//! `barrage::codec`, the schema metadata of `schema`, the ticket encodings of `session`
//! and `app`. A crate of its own would need those moved out from under the server first.
//!
//! Port of `io.deephaven.client.impl.BarrageSession`, `BarrageSubscriptionImpl`,
//! `BarrageSnapshotImpl` and `SessionImpl`, and of
//! `io.deephaven.extensions.barrage.table.BarrageTable` with its two subclasses.

mod decode;
mod error;
mod message;
mod replica;
mod schema;
mod session;

#[cfg(test)]
mod tests;

pub use error::ClientError;
pub use message::{ModifiedColumn, RemoteMessage};
pub use replica::Wake;
pub use schema::{column_type_of, RemoteAttribute, RemoteSchema};
pub use session::{
    BarrageSession, ClientConfig, RemoteTable, SnapshotOptions, SubscriptionOptions,
    DEFAULT_MAX_INBOUND_MESSAGE_SIZE, PSK_AUTH_TYPE,
};

pub use crate::app::{application_field_ticket, scope_ticket};
pub use crate::session::shared_id_to_bytes;
