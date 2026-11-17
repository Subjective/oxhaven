//! An immutable column: one Arrow array, with row key `k` at position `k`.

use crate::tables::kept_by;
use crate::{arrow_failure, failure, parallel};
use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::compute::{concat, take, TakeOptions};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use std::sync::Arc;

/// An immutable source backed by one Arrow array. Row keys are positions in the array, so it
/// serves tables whose row set is a subset of `0..len`. Previous values equal current values.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource#getMemoryColumnSource
/// and the immutable array sources it creates
/// (io.deephaven.engine.table.impl.ImmutableColumnSource#getPrev).
/// Scattered rows are gathered in pieces of this many rows, on the threads
/// [`parallel`] allows, when there are at least two pieces of them.
const GATHER_PIECE_ROWS: u64 = 1 << 16;

pub struct FlatSource {
    column_type: ColumnType,
    values: ArrayRef,
    /// The tables its cells name, for a column of tables.
    _tables: Vec<Arc<dyn Table>>,
}

impl FlatSource {
    /// Wraps `values`, which must have the Arrow type of `column_type`.
    pub fn new(column_type: ColumnType, values: ArrayRef) -> Result<Self, TableFailure> {
        let expected = column_type.arrow_type();
        if values.data_type() != &expected {
            return Err(failure(format!(
                "column of type {column_type:?} needs Arrow type {expected}, got {}",
                values.data_type()
            )));
        }
        Ok(Self {
            _tables: kept_by(&column_type, &values),
            column_type,
            values,
        })
    }

    /// Number of rows held; valid row keys are `0..len`.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn take(&self, positions: &UInt64Array) -> Result<ArrayRef, TableFailure> {
        let checked = TakeOptions { check_bounds: true };
        take(self.values.as_ref(), positions, Some(checked)).map_err(arrow_failure)
    }
}

impl ColumnSource for FlatSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        if let Some(range) = rows.as_single_range() {
            let size = range.size() as usize;
            if range.last() as u64 >= self.values.len() as u64 {
                return Err(failure(format!(
                    "row key {} is outside a column of {} rows",
                    range.last(),
                    self.values.len()
                )));
            }
            return Ok(self.values.slice(range.first() as usize, size));
        }
        let gather = |rows: &RowSet| {
            let mut positions = Vec::with_capacity(rows.size() as usize);
            for range in rows.ranges() {
                positions.extend(range.first() as u64..=range.last() as u64);
            }
            self.take(&UInt64Array::from(positions))
        };
        let size = rows.size();
        if size < 2 * GATHER_PIECE_ROWS || parallel::threads() == 1 {
            return gather(rows);
        }
        // The array never changes, so a piece reads it without a lock. The pieces come
        // back in row order, and the first that fails holds the first key that fails.
        let pieces = size.div_ceil(GATHER_PIECE_ROWS) as usize;
        let gathered = parallel::map_pieces(pieces, |piece| {
            let start = piece as u64 * GATHER_PIECE_ROWS;
            gather(&rows.sub_set_by_position_range(start, start + GATHER_PIECE_ROWS))
        });
        let gathered = gathered.into_iter().collect::<Result<Vec<_>, _>>()?;
        let pieces: Vec<&dyn Array> = gathered.iter().map(|piece| piece.as_ref()).collect();
        concat(&pieces).map_err(arrow_failure)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], _use_prev: bool) -> Result<ArrayRef, TableFailure> {
        if !keys.contains(&NULL_ROW_KEY) {
            // Every key is a position: a gather without a validity per key.
            let positions: Vec<u64> = keys.iter().map(|&k| k as u64).collect();
            return self.take(&UInt64Array::from(positions));
        }
        let positions: UInt64Array = keys
            .iter()
            .map(|&k| (k != NULL_ROW_KEY).then_some(k as u64))
            .collect();
        self.take(&positions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int32Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Int32Type};
    use std::sync::Arc;

    fn ints(source: &FlatSource, rows: &RowSet) -> Vec<Option<i32>> {
        let chunk = source.chunk(rows).unwrap();
        let chunk = chunk.as_any().downcast_ref::<Int32Array>().unwrap();
        chunk.iter().collect()
    }

    fn source() -> FlatSource {
        let values = Int32Array::from(vec![Some(10), None, Some(12), Some(13), Some(14)]);
        FlatSource::new(ColumnType::Int, Arc::new(values)).unwrap()
    }

    /// More scattered rows than two pieces, read on one thread and on several: the same
    /// cells in row order, strings among them, and the failure of a key past the end.
    #[test]
    fn many_scattered_rows_are_read_the_same_on_any_number_of_threads() {
        let len = 5 * GATHER_PIECE_ROWS as i64 + 77;
        let cell = |k: i64| (k % 11 != 0).then_some((k * 7919 % 100_003) as i32);
        let ints = Int32Array::from_iter((0..len).map(cell));
        let ints = FlatSource::new(ColumnType::Int, Arc::new(ints)).unwrap();
        let text = StringArray::from_iter((0..len).map(|k| cell(k).map(|v| format!("s{v}"))));
        let text = FlatSource::new(ColumnType::String, Arc::new(text)).unwrap();
        // Runs of one to three keys with gaps, and one long run across a piece's edge.
        let mut keys: Vec<RowKey> = (0..len).filter(|k| k % 5 < 1 + k % 3).collect();
        keys.extend(200_000..200_000 + GATHER_PIECE_ROWS as i64);
        let rows = RowSet::from_keys(keys);
        assert!(rows.size() > 2 * GATHER_PIECE_ROWS);
        let expected: Vec<Option<i32>> = rows.iter().map(cell).collect();
        for threads in [1, 2, 4] {
            parallel::with_threads(threads, || {
                let chunk = ints.chunk(&rows).unwrap();
                let read: Vec<Option<i32>> = chunk.as_primitive::<Int32Type>().iter().collect();
                assert!(read == expected, "ints on {threads} threads");
                let chunk = text.chunk(&rows).unwrap();
                let read = chunk.as_string::<i32>().iter();
                let expected = expected.iter().map(|v| v.map(|v| format!("s{v}")));
                assert!(
                    read.map(|v| v.map(str::to_string)).eq(expected),
                    "strings on {threads} threads"
                );
                let mut beyond = rows.clone();
                beyond.insert(&RowSet::from_range(len + 3, len + 3));
                let failure = ints.chunk(&beyond).expect_err("a key past the end");
                let alone = ints.chunk(&RowSet::from_keys([0, len + 3])).err();
                assert_eq!(Some(failure.message), alone.map(|f| f.message));
            });
        }
    }

    #[test]
    fn reads_positions_as_row_keys() {
        let source = source();
        assert_eq!(
            ints(&source, &RowSet::from_range(1, 3)),
            vec![None, Some(12), Some(13)]
        );
        assert_eq!(
            ints(&source, &RowSet::from_keys([0, 2, 4])),
            vec![Some(10), Some(12), Some(14)]
        );
        assert_eq!(ints(&source, &RowSet::empty()), vec![]);
    }

    #[test]
    fn previous_values_equal_current_values() {
        let source = source();
        let rows = RowSet::from_keys([0, 1, 4]);
        let prev = source.prev_chunk(&rows).unwrap();
        assert_eq!(&prev, &source.chunk(&rows).unwrap());
        assert_eq!(prev.len(), 3);
    }

    #[test]
    fn reads_by_keys_in_given_order_with_nulls() {
        let source = source();
        let chunk = source
            .chunk_by_keys(&[4, NULL_ROW_KEY, 0, 4, 1], false)
            .unwrap();
        let chunk = chunk.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            chunk.iter().collect::<Vec<_>>(),
            vec![Some(14), None, Some(10), Some(14), None]
        );
    }

    #[test]
    fn keys_past_the_end_are_errors() {
        let source = source();
        assert!(source.chunk(&RowSet::from_range(3, 5)).is_err());
        assert!(source.chunk(&RowSet::from_keys([0, 5])).is_err());
        assert!(source.chunk_by_keys(&[5], false).is_err());
    }

    #[test]
    fn list_cells_are_read_whole_and_a_list_with_another_child_field_is_refused() {
        let array = ColumnType::array(ColumnType::Int);
        let DataType::List(field) = array.arrow_type() else {
            unreachable!("an array is a list")
        };
        // [1, null], null, [], [4]
        let cells = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![Some(4)]),
        ]);
        // Arrow names the child of a list it builds "item"; Java names it "".
        let named_item: ArrayRef = Arc::new(cells.clone());
        assert!(FlatSource::new(array.clone(), named_item).is_err());
        let (_, offsets, values, nulls) = cells.into_parts();
        let cells: ArrayRef = Arc::new(ListArray::new(field, offsets, values, nulls));
        let source = FlatSource::new(array, cells.clone()).unwrap();

        assert_eq!(&source.chunk(&RowSet::flat(4)).unwrap(), &cells);
        let read = |chunk: ArrayRef| -> Vec<Option<Vec<Option<i32>>>> {
            let chunk = chunk.as_any().downcast_ref::<ListArray>().unwrap();
            let ints = |cell: ArrayRef| cell.as_primitive::<Int32Type>().iter().collect();
            chunk.iter().map(|cell| cell.map(ints)).collect()
        };
        assert_eq!(
            read(source.chunk(&RowSet::from_keys([0, 2, 3])).unwrap()),
            vec![Some(vec![Some(1), None]), Some(vec![]), Some(vec![Some(4)])]
        );
        assert_eq!(
            read(
                source
                    .chunk_by_keys(&[3, NULL_ROW_KEY, 1, 0], false)
                    .unwrap()
            ),
            vec![Some(vec![Some(4)]), None, None, Some(vec![Some(1), None])]
        );
    }

    #[test]
    fn rejects_an_array_of_the_wrong_type() {
        let strings = Arc::new(StringArray::from(vec!["a"]));
        assert!(FlatSource::new(ColumnType::Int, strings.clone()).is_err());
        assert!(FlatSource::new(ColumnType::String, strings).is_ok());
    }
}
