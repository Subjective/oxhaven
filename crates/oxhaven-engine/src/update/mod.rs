//! Helpers on [`TableUpdate`](oxhaven_contracts::update::TableUpdate) that every operator
//! needs: mapping modified columns from a parent to a child, composing consecutive updates,
//! and reading an update in the previous key space.

mod columns;
mod compose;

pub use columns::ModifiedColumnTransformer;
pub use compose::{compose, modified_pre_shift, previous_row_set};
