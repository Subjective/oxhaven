//! Key values as bytes, and the row of a key in a table that has one row per key.

use super::failure;
use crate::ops::agg::keys::canonical_floats;
use arrow::array::ArrayRef;
use arrow::row::{RowConverter, SortField};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{Table, TableFailure};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// One key value as bytes: equal values of one column type have equal bytes, by the
/// grouping rule of PARITY.md (null is a value, every NaN is one key, `-0.0` is `0.0`).
pub type Part = Box<[u8]>;

/// The values of `column`, each as a [`Part`].
pub(crate) fn parts(column: &ArrayRef) -> Result<Vec<Part>, TableFailure> {
    let converter = RowConverter::new(vec![SortField::new(column.data_type().clone())])
        .map_err(|error| failure(error.to_string()))?;
    let rows = converter
        .convert_columns(&[canonical_floats(column)])
        .map_err(|error| failure(error.to_string()))?;
    Ok(rows.iter().map(|row| row.as_ref().into()).collect())
}

/// Several parts as one map key: each with its length in front, so that no two lists of
/// parts give the same bytes.
fn joined(parts: &[Part]) -> Box<[u8]> {
    let mut out = Vec::with_capacity(parts.iter().map(|part| part.len() + 4).sum());
    for part in parts {
        out.extend_from_slice(&(part.len() as u32).to_le_bytes());
        out.extend_from_slice(part);
    }
    out.into()
}

#[derive(Default)]
struct Indexed {
    rows: RowSet,
    by_key: HashMap<Box<[u8]>, RowKey>,
}

/// The row key of each key of `table`, a table with one row per distinct value of its
/// key columns in which a key keeps its row key for good: the result of an aggregation.
///
/// Rows are indexed when a key is looked up, the rows not seen before in one bulk read;
/// an update cycle does no work for it. Because a key keeps its row key, an entry is
/// never wrong, only possibly of a row that is not there now, which [`KeyIndex::find`]
/// checks.
///
/// Behavior: the role of io.deephaven.engine.table.impl.by.AggregationRowLookup in
/// io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#nodeKeyToNodeId and
/// #findRowKeyInParentUnsorted
pub(crate) struct KeyIndex {
    table: Arc<dyn Table>,
    key_columns: Vec<usize>,
    indexed: Mutex<Indexed>,
}

impl KeyIndex {
    pub fn new(table: Arc<dyn Table>, key_columns: Vec<usize>) -> KeyIndex {
        KeyIndex {
            table,
            key_columns,
            indexed: Mutex::default(),
        }
    }

    /// The row key of the row of `key` (one part per key column), if the table has it
    /// now.
    pub fn find(&self, key: &[Part]) -> Result<Option<RowKey>, TableFailure> {
        let rows = self.table.row_set();
        let mut indexed = self.indexed.lock().unwrap_or_else(PoisonError::into_inner);
        let unseen = rows.minus(&indexed.rows);
        if !unseen.is_empty() {
            let columns = self
                .key_columns
                .iter()
                .map(|&column| parts(&self.table.column(column).chunk(&unseen)?))
                .collect::<Result<Vec<_>, _>>()?;
            for (position, row) in unseen.iter().enumerate() {
                let key: Vec<Part> = columns
                    .iter()
                    .map(|column| column[position].clone())
                    .collect();
                indexed.by_key.insert(joined(&key), row);
            }
            indexed.rows.insert(&unseen);
        }
        Ok(indexed
            .by_key
            .get(&joined(key))
            .copied()
            .filter(|&row| rows.contains(row)))
    }
}
