//! Arrow-backed column sources with Deephaven null semantics.
//!
//! - [`FlatSource`]: an immutable Arrow array addressed by position.
//! - [`WritableSource`]: a sparse source addressed by row key that operators and in-memory
//!   tables write into, and that answers previous-value reads for the current cycle.
//! - [`RedirectedSource`]: reads another source through a [`RowRedirection`].
//! - [`sentinel`]: Java's sentinel null values and conversion to and from validity bits.

mod flat;
mod redirect;
pub mod sentinel;
mod tracked;
mod writable;

pub use flat::FlatSource;
pub use redirect::{RedirectedSource, RowRedirection, WritableRowRedirection};
pub use tracked::Clock;
pub use writable::WritableSource;

use arrow::error::ArrowError;
use oxhaven_contracts::table::TableFailure;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn arrow_failure(e: ArrowError) -> TableFailure {
    failure(e.to_string())
}
