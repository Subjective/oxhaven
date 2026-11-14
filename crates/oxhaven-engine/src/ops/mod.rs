//! Table operations, one module per operation family.

pub mod agg;
pub mod asof;
pub mod crossjoin;
pub mod distinct;
pub mod filter;
pub mod join;
#[cfg(test)]
mod list_columns;
pub mod merge;
#[cfg(test)]
pub(crate) mod oracle;
pub mod range_join;
pub mod select;
pub mod snapshot;
pub mod sort;
pub mod ungroup;
pub mod update_by;
