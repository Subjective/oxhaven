//! The update graph and the building blocks that connect tables to it.
//!
//! - [`SteppedUpdateGraph`] is the graph: clock, sources, notification queue and lock.
//! - [`Subscribers`] is the listener side of a table. Every refreshing table owns one.
//! - [`ParentListener`] connects a result to one parent table.
//! - [`MergedListener`] connects a result to several parent tables.
//!
//! An operator implements [`UpdateHandler`] or [`MergedHandler`] on its result and gets the
//! `satisfied` bookkeeping, failure propagation and ownership rule from these types.

mod listener;
mod lock;
mod merged;
mod stepped;
mod subscribers;

#[cfg(test)]
mod tests;

pub use listener::{ParentListener, UpdateHandler};
pub use merged::{MergedHandler, MergedListener};
pub use stepped::SteppedUpdateGraph;
pub use subscribers::Subscribers;
