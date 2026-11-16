//! What the web IDE asks of a table besides the relational operations. Task 4.10.
//!
//! | Module | Holds |
//! | --- | --- |
//! | [`downsample`] | `run_chart_downsample`: the rows of a table a line chart needs |
//! | [`seek`] | `seek_row`: the position of the next row that holds a value |
//!
//! Column statistics and the preview of columns live with their gRPC handlers in the
//! server; they read a table and need nothing of the engine's internals.

pub mod downsample;
pub mod seek;

use std::cmp::Ordering;

/// Java's `String#compareTo`: by UTF-16 code unit, so a character outside the basic plane
/// sorts below `U+E000`.
///
/// Behavior: java.lang.String#compareTo
pub(crate) fn compare_java_strings(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}
