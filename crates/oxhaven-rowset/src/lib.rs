//! Row sets: sets of 64-bit row keys, shift data, and previous-value tracking.
//!
//! The row set and shift data types themselves are contracts and live in `oxhaven-contracts`;
//! they are re-exported here. This crate adds what is built around them: builders, the tracking
//! row set, and the shift helpers that the contract type leaves out.

pub mod builder;
pub mod shift;
pub mod tracking;

pub use builder::{concat_ordered, union_all, RowSetBuilderRandom, RowSetBuilderSequential};
pub use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, RowSetError, NULL_ROW_KEY};
pub use oxhaven_contracts::shift::{RowSetShiftData, ShiftError, ShiftRange};
pub use shift::{CoalescingShiftBuilder, ShiftBuilder};
pub use tracking::TrackingRowSet;
