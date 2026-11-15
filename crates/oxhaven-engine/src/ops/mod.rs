//! Table operations, one module per operation family.

pub mod agg;
pub mod asof;
#[cfg(test)]
mod copied_tests;
pub mod crossjoin;
pub mod distinct;
pub mod filter;
#[cfg(test)]
mod flat_select_tests;
pub mod head_by;
pub mod hierarchical;
pub mod join;
#[cfg(test)]
mod list_columns;
pub mod merge;
pub mod multi_join;
#[cfg(test)]
pub(crate) mod oracle;
pub mod partitioned;
pub mod range_join;
pub mod select;
pub mod snapshot;
pub mod sort;
pub mod ungroup;
pub mod update_by;
