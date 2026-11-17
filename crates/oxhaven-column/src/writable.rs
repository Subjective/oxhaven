//! A writable column addressed by row key, with previous values for the current cycle.

use crate::tables::named_by;
use crate::tracked::{Clock, Tracked};
use crate::{arrow_failure, failure, parallel};
use arrow::array::{new_null_array, Array, ArrayRef, UInt32Array};
use arrow::compute::{concat, interleave, take};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

/// A value's place in storage: the batch it was written in and its index there.
type Slot = (u32, u32);

/// Where every unset row key reads from: the single null of batch 0. No written value is
/// in batch 0, so this slot also stands for "no value" in the map of slots.
const NULL_SLOT: Slot = (0, 0);

/// Scattered rows are gathered in pieces of this many rows, on the threads [`parallel`]
/// allows, when there are at least two pieces of them.
const GATHER_PIECE_ROWS: usize = 1 << 16;

/// A sparse, writable column source. A row key that was never written, or was removed, reads
/// as null.
///
/// This is the result column of operators and the storage of in-memory tables. Writers hold it
/// as `Arc<WritableSource>` and hand the same `Arc` out as `Arc<dyn ColumnSource>`; all
/// methods take `&self`.
///
/// # Previous values
///
/// After [`start_tracking_prev`](Self::start_tracking_prev), a write made while the clock's
/// phase is updating keeps the value the row key held when the cycle began, and `prev_chunk`
/// answers with it until the clock moves on. Without a clock, or while idle, previous values
/// equal current values. The owner must write a cycle's changes inside that cycle, as Java's
/// operators do.
///
/// # Storage
///
/// Values are kept as the Arrow arrays they were written from; a write stores its array and
/// points the written row keys at it. Nothing here is specific to a column type. Arrays
/// whose values were mostly overwritten or removed are compacted away on a later write.
///
/// The place of each row key's value is kept in a flat array indexed by row key while the
/// keys are dense, as those of an in-memory table, an input table or a stream's table
/// are, and in an ordered map otherwise. A read of rows that were written in row order is
/// a slice of the written array, or a concatenation of a few; any other read gathers the
/// values, in pieces on several threads when there are many. A read takes the source's
/// lock once, whatever the number of rows.
///
/// Behavior: io.deephaven.engine.table.impl.sources.SparseArrayColumnSource and
/// io.deephaven.engine.table.impl.sources.IntegerSparseArraySource.
pub struct WritableSource {
    column_type: ColumnType,
    state: RwLock<State>,
}

struct State {
    slots: Tracked<Slot>,
    /// Written arrays. Batch 0 is a single null.
    batches: Vec<ArrayRef>,
    /// Total length of `batches`, excluding batch 0.
    stored: u64,
    /// For a column of tables, the tables that the handles in `batches` name, kept alive
    /// while they are stored; `None` when the owner of the column keeps them itself.
    tables: Option<Vec<Arc<dyn Table>>>,
}

impl WritableSource {
    /// An empty source that does not track previous values.
    ///
    /// For a column of tables (`ColumnType::Table`) the source keeps the tables that the
    /// handles written to it name, as long as it stores the handles: a copy of a cell
    /// keeps its table, as a Java object source does.
    pub fn new(column_type: ColumnType) -> Self {
        let tables = matches!(column_type, ColumnType::Table(_)).then(Vec::new);
        Self::with_tables(column_type, tables)
    }

    /// An empty source for the handles of a column of tables whose owner keeps the
    /// tables and decides when to give them up (a partition, a transform).
    pub fn of_owned_handles(column_type: ColumnType) -> Self {
        Self::with_tables(column_type, None)
    }

    fn with_tables(column_type: ColumnType, tables: Option<Vec<Arc<dyn Table>>>) -> Self {
        let null = new_null_array(&column_type.arrow_type(), 1);
        Self {
            column_type,
            state: RwLock::new(State {
                slots: Tracked::new(NULL_SLOT),
                batches: vec![null],
                stored: 0,
                tables,
            }),
        }
    }

    /// Starts keeping previous values, with `clock` telling cycles apart. Call it when the
    /// owning table becomes refreshing.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#startTrackingPrevValues
    ///
    /// # Panics
    /// If called twice, as Java throws.
    pub fn start_tracking_prev(&self, clock: Arc<dyn Clock>) {
        self.write().slots.start_tracking_prev(clock);
    }

    /// Writes `values` at `rows`: the i-th value goes to the i-th row key in increasing order.
    /// A null value makes the row read as null. Writing one row is a fill of one value.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#fillFromChunk
    pub fn fill(&self, rows: &RowSet, values: &ArrayRef) -> Result<(), TableFailure> {
        self.check(values, rows.size())?;
        self.write().store(rows.iter(), values)
    }

    /// Writes `values` at `keys` in the order given. When a key repeats, its last value wins.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#fillFromChunkUnordered
    pub fn fill_by_keys(&self, keys: &[RowKey], values: &ArrayRef) -> Result<(), TableFailure> {
        self.check(values, keys.len() as u64)?;
        if let Some(bad) = keys.iter().find(|&&k| k < 0) {
            return Err(failure(format!("cannot write at row key {bad}")));
        }
        self.write().store(keys.iter().copied(), values)
    }

    /// Makes `rows` read as null and releases their values. Use it for removed rows.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.SparseArrayColumnSource#remove and
    /// io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#setNull
    pub fn remove(&self, rows: &RowSet) {
        let mut state = self.write();
        state.compact_if_wasteful();
        let mut writer = state.slots.write();
        for key in rows.iter() {
            writer.put(key, None);
        }
    }

    /// Moves the value at each key of `rows` to that key plus `delta`. The old keys read as
    /// null afterwards unless another moved value landed on them.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#shift
    pub fn shift(&self, rows: &RowSet, delta: i64) -> Result<(), TableFailure> {
        self.write().move_keys(rows.iter().map(|k| (k, delta)))
    }

    /// Applies every range of `shifts` to the keys of `rows` that it covers, all at once.
    /// `rows` is in the pre-shift key space: pass the table's previous row set less the
    /// removed rows.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetShiftData#apply(RowKeyRangeShiftCallback) driving
    /// io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#shift
    pub fn apply_shifts(
        &self,
        shifts: &RowSetShiftData,
        rows: &RowSet,
    ) -> Result<(), TableFailure> {
        let moves = shifts.ranges().iter().flat_map(|s| {
            let covered = rows.sub_set_by_key_range(s.first, s.last);
            let keys: Vec<RowKey> = covered.iter().collect();
            keys.into_iter().map(move |k| (k, s.delta))
        });
        self.write().move_keys(moves)
    }

    fn check(&self, values: &ArrayRef, keys: u64) -> Result<(), TableFailure> {
        let expected = self.column_type.arrow_type();
        if values.data_type() != &expected {
            return Err(failure(format!(
                "column of type {:?} needs Arrow type {expected}, got {}",
                self.column_type,
                values.data_type()
            )));
        }
        if values.len() as u64 != keys {
            return Err(failure(format!(
                "{} values for {keys} row keys",
                values.len()
            )));
        }
        Ok(())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn chunk_at(&self, rows: &RowSet, use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let state = self.read();
        let reader = state.slots.read(use_prev);
        let within = |array: &&[Slot]| {
            let last = rows
                .last_row_key()
                .and_then(|last| usize::try_from(last).ok());
            last.is_none_or(|last| last < array.len())
        };
        if let Some(array) = reader.flat_array().filter(within) {
            // Rows written in row order are found without a copy of their slots.
            let slices = rows
                .ranges()
                .map(|range| &array[range.first() as usize..=range.last() as usize]);
            if let Some(runs) = runs_of(slices, rows.size() as usize) {
                return concat_runs(&state.batches, &runs);
            }
        }
        let slots = reader.values_of(rows);
        let batches = &state.batches;
        in_pieces(slots.len(), |offset, length| {
            gather(batches, &slots[offset..offset + length])
        })
    }
}

/// `piece(offset, length)` for all `rows` rows at once, or for pieces of
/// [`GATHER_PIECE_ROWS`] rows on several threads, concatenated in row order. The first
/// piece that fails gives the failure.
fn in_pieces(
    rows: usize,
    piece: impl Fn(usize, usize) -> Result<ArrayRef, TableFailure> + Sync,
) -> Result<ArrayRef, TableFailure> {
    if rows < 2 * GATHER_PIECE_ROWS || parallel::threads() == 1 {
        return piece(0, rows);
    }
    let pieces = parallel::pieces_of(rows, GATHER_PIECE_ROWS);
    let gathered = parallel::map_pieces(pieces.len(), |index| {
        let (offset, length) = pieces[index];
        piece(offset, length)
    });
    let gathered = gathered.into_iter().collect::<Result<Vec<_>, _>>()?;
    let pieces: Vec<&dyn Array> = gathered.iter().map(|piece| piece.as_ref()).collect();
    concat(&pieces).map_err(arrow_failure)
}

/// A run of slots that follow each other in one batch: the batch, the index of the first
/// and how many.
type Run = (u32, u32, usize);

/// The runs that the `rows` slots of `slices`, taken one slice after the other, are made
/// of, if they are few enough (one for every 32 rows) for a concatenation of slices of the
/// batches to cost less than a gather; `None` otherwise, and for no rows.
fn runs_of<'a>(slices: impl Iterator<Item = &'a [Slot]>, rows: usize) -> Option<Vec<Run>> {
    let most = (rows / 32).max(1);
    let mut runs: Vec<Run> = Vec::new();
    for slice in slices {
        let mut rest = slice;
        while let Some(&(batch, index)) = rest.first() {
            // The slots at the start of `rest` that follow each other from this one.
            let next = (u64::from(index)..).zip(rest);
            let length = next
                .take_while(|&(place, &(b, i))| b == batch && u64::from(i) == place)
                .count();
            let continued = |&(b, first, length): &Run| {
                b == batch && u64::from(first) + length as u64 == u64::from(index)
            };
            if let Some(run) = runs.last_mut().filter(|run| continued(run)) {
                run.2 += length;
            } else if runs.len() == most {
                return None;
            } else {
                runs.push((batch, index, length));
            }
            rest = &rest[length..];
        }
    }
    (!runs.is_empty()).then_some(runs)
}

/// The values of `runs`, one run after the other.
fn concat_runs(batches: &[ArrayRef], runs: &[Run]) -> Result<ArrayRef, TableFailure> {
    let slice =
        |&(batch, first, length): &Run| batches[batch as usize].slice(first as usize, length);
    if let [run] = runs {
        return Ok(slice(run));
    }
    let slices: Vec<ArrayRef> = runs.iter().map(slice).collect();
    let slices: Vec<&dyn Array> = slices.iter().map(|slice| slice.as_ref()).collect();
    concat(&slices).map_err(arrow_failure)
}

impl State {
    fn store(
        &mut self,
        keys: impl Iterator<Item = RowKey>,
        values: &ArrayRef,
    ) -> Result<(), TableFailure> {
        if values.is_empty() {
            return Ok(());
        }
        self.compact_if_wasteful();
        let (Ok(batch), Ok(_)) = (
            u32::try_from(self.batches.len()),
            u32::try_from(values.len()),
        ) else {
            return Err(failure("writable column source is full"));
        };
        self.batches.push(values.clone());
        self.stored += values.len() as u64;
        if let Some(tables) = &mut self.tables {
            tables.extend(named_by(values));
        }
        let mut writer = self.slots.write();
        for (index, key) in keys.enumerate() {
            writer.put(key, Some((batch, index as u32)));
        }
        Ok(())
    }

    /// Moves values from `key` to `key + delta` for every `(key, delta)`, as if all at once:
    /// one move's destination may be another's source.
    fn move_keys(
        &mut self,
        moves: impl Iterator<Item = (RowKey, i64)>,
    ) -> Result<(), TableFailure> {
        let moves: Vec<(RowKey, i64)> = moves.collect();
        if let Some((key, delta)) = moves
            .iter()
            .find(|(k, d)| k.checked_add(*d).is_none_or(|to| to < 0))
        {
            return Err(failure(format!(
                "shift of row key {key} by {delta} leaves the key space"
            )));
        }
        self.compact_if_wasteful();
        let mut writer = self.slots.write();
        let lifted: Vec<(RowKey, Option<Slot>)> = moves
            .iter()
            .map(|&(key, delta)| (key + delta, writer.put(key, None)))
            .collect();
        for (to, slot) in lifted {
            writer.put(to, slot);
        }
        Ok(())
    }

    /// Rewrites storage as one array of the live values when most stored values are dead or
    /// the values are spread over many small arrays. Skipped while previous values are
    /// recorded, because those still point into the old arrays; the first write of the next
    /// cycle gets the chance instead.
    fn compact_if_wasteful(&mut self) {
        let mut writer = self.slots.write();
        if writer.map().has_prev_record() {
            return;
        }
        let live = writer.map().len() as u64;
        let batches = self.batches.len() as u64 - 1;
        let mostly_dead = self.stored > 2 * live + 1024;
        let fragmented = batches > 64 && batches * 64 > live;
        if !(mostly_dead || fragmented) || live > u64::from(u32::MAX) {
            return;
        }
        let slots = writer.map().values();
        let Ok(compacted) = gather(&self.batches, &slots) else {
            return;
        };
        self.batches.truncate(1);
        self.stored = 0;
        if let Some(tables) = &mut self.tables {
            *tables = named_by(&compacted);
        }
        if live > 0 {
            self.batches.push(compacted);
            self.stored = live;
            writer.replace_all((0..live as u32).map(|index| (1, index)));
        }
    }
}

/// The values at `slots`, in order.
fn gather(batches: &[ArrayRef], slots: &[Slot]) -> Result<ArrayRef, TableFailure> {
    if let Some(runs) = runs_of(std::iter::once(slots), slots.len()) {
        return concat_runs(batches, &runs);
    }
    // Hand `interleave` only the batches this read touches, so a read costs what it returns
    // and not the number of batches stored. Slots that follow each other are mostly of one
    // batch, so the last batch is remembered before the map is asked.
    let mut dense: HashMap<u32, usize> = HashMap::new();
    let mut used: Vec<&dyn Array> = Vec::new();
    let mut last = None;
    let indices: Vec<(usize, usize)> = slots
        .iter()
        .map(|&(batch, index)| {
            let position = match last {
                Some((known, position)) if known == batch => position,
                _ => *dense.entry(batch).or_insert_with(|| {
                    used.push(batches[batch as usize].as_ref());
                    used.len() - 1
                }),
            };
            last = Some((batch, position));
            (position, index as usize)
        })
        .collect();
    match used[..] {
        [] => interleave(&[batches[0].as_ref()], &indices).map_err(arrow_failure),
        [only] => {
            // One batch: a gather by position, which carries four bytes a row.
            let positions = UInt32Array::from_iter_values(slots.iter().map(|slot| slot.1));
            take(only, &positions, None).map_err(arrow_failure)
        }
        _ => interleave(&used, &indices).map_err(arrow_failure),
    }
}

impl ColumnSource for WritableSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    /// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#getInt
    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk_at(rows, false)
    }

    /// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#getPrevInt
    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk_at(rows, true)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let state = self.read();
        let reader = state.slots.read(use_prev);
        let batches = &state.batches;
        in_pieces(keys.len(), |offset, length| {
            let keys = &keys[offset..offset + length];
            // `NULL_ROW_KEY` is negative, and no negative key has a value.
            let slots: Vec<Slot> = keys.iter().map(|&key| reader.get(key)).collect();
            gather(batches, &slots)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracked::testing::ManualClock;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        ListArray, StringArray, TimestampNanosecondArray, UInt16Array,
    };
    use arrow::datatypes::{DataType, Int32Type, Int64Type};
    use oxhaven_contracts::rowset::NULL_ROW_KEY;
    use oxhaven_contracts::shift::ShiftRange;
    use std::collections::BTreeMap;

    fn int_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        Arc::new(values.into_iter().collect::<Int32Array>())
    }

    fn ints(chunk: Result<ArrayRef, TableFailure>) -> Vec<Option<i32>> {
        let chunk = chunk.unwrap();
        let chunk = chunk.as_any().downcast_ref::<Int32Array>().unwrap();
        chunk.iter().collect()
    }

    /// An int source holding `10 * k` at each key of `keys`.
    fn filled(keys: &[RowKey]) -> WritableSource {
        let source = WritableSource::new(ColumnType::Int);
        let rows = RowSet::from_keys(keys.iter().copied());
        let values = int_array(rows.iter().map(|k| Some((10 * k) as i32)));
        source.fill(&rows, &values).unwrap();
        source
    }

    fn tracked(keys: &[RowKey]) -> (WritableSource, Arc<ManualClock>) {
        let source = filled(keys);
        let clock = ManualClock::idle_at(7);
        source.start_tracking_prev(clock.clone());
        (source, clock)
    }

    #[test]
    fn unset_keys_read_as_null() {
        let source = filled(&[1, 2, 1_000_000_000_000]);
        let rows = RowSet::from_keys([0, 1, 2, 3, 1_000_000_000_000]);
        assert_eq!(
            ints(source.chunk(&rows)),
            vec![
                None,
                Some(10),
                Some(20),
                None,
                Some(10_000_000_000_000_i64 as i32)
            ]
        );
        assert_eq!(ints(source.chunk(&RowSet::empty())), vec![]);
        assert_eq!(
            ints(WritableSource::new(ColumnType::Int).chunk(&RowSet::flat(2))),
            vec![None, None]
        );
    }

    #[test]
    fn fill_overwrites_and_stores_nulls() {
        let source = filled(&[1, 2, 3]);
        source
            .fill(
                &RowSet::from_keys([2, 3, 9]),
                &int_array([None, Some(-3), Some(-9)]),
            )
            .unwrap();
        assert_eq!(
            ints(source.chunk(&RowSet::from_keys([1, 2, 3, 9]))),
            vec![Some(10), None, Some(-3), Some(-9)]
        );
    }

    #[test]
    fn fill_by_keys_follows_the_given_order_and_last_repeat_wins() {
        let source = filled(&[1]);
        source
            .fill_by_keys(&[5, 1, 5], &int_array([Some(1), Some(2), Some(3)]))
            .unwrap();
        assert_eq!(
            ints(source.chunk(&RowSet::from_keys([1, 5]))),
            vec![Some(2), Some(3)]
        );
    }

    #[test]
    fn fill_rejects_wrong_type_wrong_length_and_bad_keys() {
        let source = filled(&[1]);
        let longs: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        assert!(source.fill(&RowSet::from_keys([1]), &longs).is_err());
        assert!(source
            .fill(&RowSet::from_keys([1, 2]), &int_array([Some(1)]))
            .is_err());
        assert!(source
            .fill_by_keys(&[NULL_ROW_KEY], &int_array([Some(1)]))
            .is_err());
        assert_eq!(ints(source.chunk(&RowSet::from_keys([1]))), vec![Some(10)]);
    }

    #[test]
    fn remove_makes_rows_null() {
        let source = filled(&[1, 2, 3]);
        source.remove(&RowSet::from_keys([2, 8]));
        assert_eq!(
            ints(source.chunk(&RowSet::from_range(1, 3))),
            vec![Some(10), None, Some(30)]
        );
    }

    #[test]
    fn chunk_by_keys_reads_directly_with_repeats_and_nulls() {
        let source = filled(&[1, 2, 3]);
        assert_eq!(
            ints(source.chunk_by_keys(&[3, NULL_ROW_KEY, 1, 3, 4], false)),
            vec![Some(30), None, Some(10), Some(30), None]
        );
    }

    #[test]
    fn shift_moves_values_in_both_directions_over_overlapping_keys() {
        let source = filled(&[1, 2, 3]);
        source.shift(&RowSet::from_range(1, 3), 1).unwrap();
        assert_eq!(
            ints(source.chunk(&RowSet::from_range(1, 4))),
            vec![None, Some(10), Some(20), Some(30)]
        );
        source.shift(&RowSet::from_range(2, 4), -2).unwrap();
        assert_eq!(
            ints(source.chunk(&RowSet::from_range(0, 4))),
            vec![Some(10), Some(20), Some(30), None, None]
        );
        assert!(source.shift(&RowSet::from_range(0, 2), -1).is_err());
        assert_eq!(
            ints(source.chunk(&RowSet::from_range(0, 2))),
            vec![Some(10), Some(20), Some(30)]
        );
    }

    #[test]
    fn shift_carries_an_unset_source_over_the_destination() {
        let source = filled(&[1, 5]);
        // Key 4 holds nothing; Java sets the destination to the null it reads there.
        source.shift(&RowSet::from_keys([4]), 1).unwrap();
        assert_eq!(
            ints(source.chunk(&RowSet::from_keys([1, 4, 5]))),
            vec![Some(10), None, None]
        );
    }

    #[test]
    fn apply_shifts_moves_only_the_given_rows_and_all_ranges_at_once() {
        let source = filled(&[0, 1, 5, 6, 10]);
        let shifts = RowSetShiftData::new(vec![
            ShiftRange {
                first: 0,
                last: 1,
                delta: 2,
            },
            ShiftRange {
                first: 5,
                last: 6,
                delta: -1,
            },
        ])
        .unwrap();
        // Key 6 holds a value but is not a row of the table, so it stays put.
        source
            .apply_shifts(&shifts, &RowSet::from_keys([0, 1, 5, 10]))
            .unwrap();
        assert_eq!(
            ints(source.chunk(&RowSet::from_range(0, 10))),
            vec![
                None,
                None,
                Some(0),
                Some(10),
                Some(50),
                None,
                Some(60),
                None,
                None,
                None,
                Some(100)
            ]
        );
    }

    #[test]
    fn without_a_clock_previous_values_are_current_values() {
        let source = filled(&[1, 2]);
        source
            .fill(&RowSet::from_keys([1]), &int_array([Some(-1)]))
            .unwrap();
        assert_eq!(
            ints(source.prev_chunk(&RowSet::from_keys([1, 2]))),
            vec![Some(-1), Some(20)]
        );
    }

    #[test]
    fn previous_values_last_for_the_whole_cycle_and_only_the_cycle() {
        let (source, clock) = tracked(&[1, 2, 3]);
        let rows = RowSet::from_range(1, 4);

        clock.start_cycle();
        source
            .fill(&RowSet::from_keys([1, 4]), &int_array([Some(-1), Some(-4)]))
            .unwrap();
        // A second write to the same key in one cycle must not replace the recorded value.
        source
            .fill(&RowSet::from_keys([1]), &int_array([Some(-11)]))
            .unwrap();
        source.remove(&RowSet::from_keys([2]));
        assert_eq!(
            ints(source.chunk(&rows)),
            vec![Some(-11), None, Some(30), Some(-4)]
        );
        assert_eq!(
            ints(source.prev_chunk(&rows)),
            vec![Some(10), Some(20), Some(30), None]
        );
        assert_eq!(
            ints(source.chunk_by_keys(&[4, 2, NULL_ROW_KEY, 1], true)),
            vec![None, Some(20), None, Some(10)]
        );

        clock.finish_cycle();
        assert_eq!(ints(source.prev_chunk(&rows)), ints(source.chunk(&rows)));

        // The next cycle starts from the committed values even if nothing wrote in between.
        clock.start_cycle();
        assert_eq!(ints(source.prev_chunk(&rows)), ints(source.chunk(&rows)));
        source
            .fill(&RowSet::from_keys([1]), &int_array([Some(5)]))
            .unwrap();
        assert_eq!(
            ints(source.prev_chunk(&rows)),
            vec![Some(-11), None, Some(30), Some(-4)]
        );
    }

    #[test]
    fn idle_writes_record_no_previous_values() {
        let (source, clock) = tracked(&[1]);
        source
            .fill(&RowSet::from_keys([1]), &int_array([Some(-1)]))
            .unwrap();
        assert_eq!(
            ints(source.prev_chunk(&RowSet::from_keys([1]))),
            vec![Some(-1)]
        );
        // Nothing may be kept either: a record would hold up compaction until the next write.
        assert!(!source.read().slots.has_prev_record());
        clock.start_cycle();
        assert_eq!(
            ints(source.prev_chunk(&RowSet::from_keys([1]))),
            vec![Some(-1)]
        );
    }

    #[test]
    fn shift_keeps_previous_values_at_the_old_keys() {
        let (source, clock) = tracked(&[1, 2, 3]);
        clock.start_cycle();
        source.shift(&RowSet::from_range(2, 3), 1).unwrap();
        let rows = RowSet::from_range(1, 4);
        assert_eq!(
            ints(source.chunk(&rows)),
            vec![Some(10), None, Some(20), Some(30)]
        );
        assert_eq!(
            ints(source.prev_chunk(&rows)),
            vec![Some(10), Some(20), Some(30), None]
        );
    }

    #[test]
    #[should_panic(expected = "start_tracking_prev called twice")]
    fn tracking_cannot_start_twice() {
        let (source, clock) = tracked(&[1]);
        source.start_tracking_prev(clock);
    }

    #[test]
    fn storage_is_compacted_without_changing_values() {
        let (source, clock) = tracked(&[]);
        let rows = RowSet::flat(100);
        for round in 0..200 {
            clock.start_cycle();
            let before = ints(source.chunk(&rows));
            let values = int_array((0..100).map(|i| Some(round * 1000 + i)));
            source.fill(&rows, &values).unwrap();
            source.remove(&RowSet::from_keys([i64::from(round % 100)]));
            assert_eq!(ints(source.prev_chunk(&rows)), before);
            clock.finish_cycle();
        }
        let mut expected: Vec<Option<i32>> = (0..100).map(|i| Some(199_000 + i)).collect();
        expected[99] = None;
        assert_eq!(ints(source.chunk(&rows)), expected);
        let state = source.read();
        assert!(
            state.stored <= 2 * 100 + 1024 + 100,
            "stored {}",
            state.stored
        );
        assert!(state.batches.len() < 20, "batches {}", state.batches.len());
    }

    #[test]
    fn many_single_row_writes_are_merged() {
        let source = WritableSource::new(ColumnType::Int);
        for key in 0..500 {
            source
                .fill(&RowSet::from_keys([key]), &int_array([Some(key as i32)]))
                .unwrap();
        }
        assert_eq!(
            ints(source.chunk(&RowSet::flat(500))),
            (0..500).map(Some).collect::<Vec<_>>()
        );
        assert!(source.read().batches.len() <= 66);
    }

    /// Cells of a vector of `int`; `None` is a null cell.
    fn int_lists(cells: &[Option<&[Option<i32>]>]) -> ArrayRef {
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>(
            cells.iter().map(|cell| cell.map(|cell| cell.to_vec())),
        );
        let (_, offsets, values, nulls) = lists.into_parts();
        let DataType::List(field) = ColumnType::vector(ColumnType::Int).arrow_type() else {
            unreachable!("a vector is a list")
        };
        Arc::new(ListArray::new(field, offsets, values, nulls))
    }

    fn lists(chunk: Result<ArrayRef, TableFailure>) -> Vec<Option<Vec<Option<i32>>>> {
        let chunk = chunk.unwrap();
        assert_eq!(
            chunk.data_type(),
            &ColumnType::vector(ColumnType::Int).arrow_type()
        );
        let chunk = chunk.as_any().downcast_ref::<ListArray>().unwrap();
        chunk
            .iter()
            .map(|cell| cell.map(|cell| ints(Ok(cell))))
            .collect()
    }

    #[test]
    fn list_cells_are_written_read_removed_and_shifted_whole() {
        let source = WritableSource::new(ColumnType::vector(ColumnType::Int));
        assert_eq!(lists(source.chunk(&RowSet::flat(2))), vec![None, None]);
        source
            .fill(
                &RowSet::from_keys([1, 2, 5, 9]),
                &int_lists(&[Some(&[Some(1), None]), None, Some(&[]), Some(&[Some(9)])]),
            )
            .unwrap();
        let (one, empty, nine) = (vec![Some(1), None], vec![], vec![Some(9)]);
        // A written null cell and a key never written both read as a null cell; an empty
        // cell stays an empty cell.
        assert_eq!(
            lists(source.chunk(&RowSet::from_keys([0, 1, 2, 5, 9]))),
            vec![
                None,
                Some(one.clone()),
                None,
                Some(empty.clone()),
                Some(nine.clone())
            ]
        );
        assert_eq!(
            lists(source.chunk_by_keys(&[9, NULL_ROW_KEY, 1, 9, 5], false)),
            vec![
                Some(nine.clone()),
                None,
                Some(one.clone()),
                Some(nine.clone()),
                Some(empty.clone())
            ]
        );

        // An overwrite replaces the whole cell, whatever its length was.
        source
            .fill(
                &RowSet::from_keys([1, 5]),
                &int_lists(&[Some(&[]), Some(&[Some(5), Some(6), Some(7)])]),
            )
            .unwrap();
        source.remove(&RowSet::from_keys([9]));
        source.shift(&RowSet::from_keys([5]), 3).unwrap();
        assert_eq!(
            lists(source.chunk(&RowSet::from_keys([1, 5, 8, 9]))),
            vec![
                Some(empty),
                None,
                Some(vec![Some(5), Some(6), Some(7)]),
                None
            ]
        );

        // Cells of another element type, or that are not lists, are refused.
        let longs = ListArray::from_iter_primitive::<Int64Type, _, _>([Some([Some(1)])]);
        assert!(source
            .fill(&RowSet::from_keys([1]), &(Arc::new(longs) as ArrayRef))
            .is_err());
        assert!(source
            .fill(&RowSet::from_keys([1]), &int_array([Some(1)]))
            .is_err());
    }

    #[test]
    fn list_cells_keep_their_previous_value_for_the_cycle() {
        let source = WritableSource::new(ColumnType::vector(ColumnType::Int));
        let rows = RowSet::from_keys([1, 2, 3]);
        source
            .fill(
                &rows,
                &int_lists(&[Some(&[Some(1)]), None, Some(&[Some(3), Some(3)])]),
            )
            .unwrap();
        let clock = ManualClock::idle_at(7);
        source.start_tracking_prev(clock.clone());
        let before = vec![Some(vec![Some(1)]), None, Some(vec![Some(3), Some(3)])];

        clock.start_cycle();
        source
            .fill(
                &RowSet::from_keys([1, 2]),
                &int_lists(&[None, Some(&[Some(2), None])]),
            )
            .unwrap();
        source.remove(&RowSet::from_keys([3]));
        let after = vec![None, Some(vec![Some(2), None]), None];
        assert_eq!(lists(source.chunk(&rows)), after);
        assert_eq!(lists(source.prev_chunk(&rows)), before);
        assert_eq!(
            lists(source.chunk_by_keys(&[3, NULL_ROW_KEY, 1], true)),
            vec![before[2].clone(), None, before[0].clone()]
        );
        clock.finish_cycle();
        assert_eq!(lists(source.prev_chunk(&rows)), after);
    }

    #[test]
    fn list_storage_is_compacted_without_changing_cells() {
        let source = WritableSource::new(ColumnType::vector(ColumnType::Int));
        let rows = RowSet::flat(50);
        for round in 0..100 {
            let cells: Vec<Vec<Option<i32>>> = (0..50)
                .map(|row| vec![Some(round * 100 + row); (row % 3) as usize])
                .collect();
            let cells: Vec<Option<&[Option<i32>]>> =
                cells.iter().map(|cell| Some(cell.as_slice())).collect();
            source.fill(&rows, &int_lists(&cells)).unwrap();
        }
        let expected: Vec<Option<Vec<Option<i32>>>> = (0..50)
            .map(|row| Some(vec![Some(9900 + row); (row % 3) as usize]))
            .collect();
        assert_eq!(lists(source.chunk(&rows)), expected);
        let state = source.read();
        assert!(state.stored < 5000, "stored {}", state.stored);
    }

    #[test]
    fn every_column_type_round_trips_with_nulls() {
        let instants = TimestampNanosecondArray::from(vec![Some(1), None, Some(i64::MAX)])
            .with_timezone("UTC");
        let cases: Vec<(ColumnType, ArrayRef)> = vec![
            (
                ColumnType::Boolean,
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            ),
            (
                ColumnType::Byte,
                Arc::new(Int8Array::from(vec![Some(1), None, Some(i8::MIN)])),
            ),
            (
                ColumnType::Char,
                Arc::new(UInt16Array::from(vec![Some(65), None, Some(u16::MAX)])),
            ),
            (
                ColumnType::Short,
                Arc::new(Int16Array::from(vec![Some(1), None, Some(i16::MIN)])),
            ),
            (
                ColumnType::Int,
                Arc::new(Int32Array::from(vec![Some(1), None, Some(i32::MIN)])),
            ),
            (
                ColumnType::Long,
                Arc::new(Int64Array::from(vec![Some(1), None, Some(i64::MIN)])),
            ),
            (
                ColumnType::Float,
                Arc::new(Float32Array::from(vec![Some(1.5), None, Some(f32::NAN)])),
            ),
            (
                ColumnType::Double,
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-0.0)])),
            ),
            (
                ColumnType::String,
                Arc::new(StringArray::from(vec![Some("a"), None, Some("")])),
            ),
            (ColumnType::Instant, Arc::new(instants)),
            (
                ColumnType::vector(ColumnType::Int),
                int_lists(&[Some(&[Some(1), None]), None, Some(&[])]),
            ),
        ];
        for (column_type, values) in cases {
            let source = WritableSource::new(column_type.clone());
            let rows = RowSet::from_keys([3, 40, 500]);
            source.fill(&rows, &values).unwrap();
            let chunk = source.chunk(&rows).unwrap();
            assert_eq!(chunk.to_data(), values.to_data(), "{column_type:?}");
            let sparse = source.chunk(&RowSet::from_keys([2, 500])).unwrap();
            assert_eq!(sparse.data_type(), &column_type.arrow_type());
            assert!(sparse.is_null(0) && sparse.is_valid(1), "{column_type:?}");
        }
    }

    /// A generator of the numbers the tests below draw.
    fn random_from(mut state: u64) -> impl FnMut(u64) -> u64 {
        move |below| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % below
        }
    }

    /// What a model of a source reads at `keys`: `None` for a key without a value, as for
    /// one that holds a null.
    fn model_at(model: &BTreeMap<RowKey, Option<i32>>, keys: &[RowKey]) -> Vec<Option<i32>> {
        keys.iter()
            .map(|key| model.get(key).copied().flatten())
            .collect()
    }

    /// Random fills, removes and shifts of keys near and far, over many cycles, against a
    /// plain map with a copy of itself from the start of the cycle: reads of row sets and
    /// of keys, now and before, while the slots are in an array and while they are in a map.
    #[test]
    fn a_source_agrees_with_a_map_through_random_changes() {
        let clock = ManualClock::idle_at(1);
        let source = WritableSource::new(ColumnType::Int);
        source.start_tracking_prev(clock.clone());
        let mut model: BTreeMap<RowKey, Option<i32>> = BTreeMap::new();
        let mut random = random_from(0x9e37_79b9_7f4a_7c15);
        let mut forms = [0, 0];
        for cycle in 0..300 {
            let before = model.clone();
            clock.start_cycle();
            // Phases of dense keys, of keys spread wide, and of removals back to few keys.
            let (spread, removes) = match (cycle / 50) % 3 {
                0 => (3000, 1),
                1 => (1 << 30, 2),
                _ => (3000, 8),
            };
            for _ in 0..6 {
                let first = random(spread) as RowKey;
                let mut keys: Vec<RowKey> = (first..first + random(40) as RowKey).collect();
                keys.extend((0..random(8)).map(|_| random(spread) as RowKey));
                let rows = RowSet::from_keys(keys.iter().copied());
                match random(10) {
                    kind if kind < removes => {
                        source.remove(&rows);
                        for key in rows.iter() {
                            model.remove(&key);
                        }
                    }
                    8 => {
                        // Every move reads its key before any move writes one.
                        let delta = random(7) as i64 - 3;
                        let moved = rows.sub_set_by_key_range(3, RowKey::MAX);
                        source.shift(&moved, delta).unwrap();
                        let lifted: Vec<_> = moved.iter().map(|k| (k, model.remove(&k))).collect();
                        for (key, value) in lifted {
                            match value {
                                Some(value) => model.insert(key + delta, value),
                                None => model.remove(&(key + delta)),
                            };
                        }
                    }
                    9 => {
                        // In the order drawn, with repeats: the last value of a key wins.
                        let values: Vec<Option<i32>> =
                            keys.iter().map(|_| Some(random(1000) as i32)).collect();
                        source
                            .fill_by_keys(&keys, &int_array(values.clone()))
                            .unwrap();
                        model.extend(keys.iter().copied().zip(values));
                    }
                    _ => {
                        let value = |_| (random(9) > 0).then(|| random(1000) as i32);
                        let values: Vec<Option<i32>> = rows.iter().map(value).collect();
                        source.fill(&rows, &int_array(values.clone())).unwrap();
                        model.extend(rows.iter().zip(values));
                    }
                }
            }
            if cycle % 50 == 49 && spread > 3000 {
                // Drop the keys far away, so that the rest can be dense again.
                let far = RowSet::from_keys(model.range(3010..).map(|(key, _)| *key));
                source.remove(&far);
                model.retain(|key, _| *key < 3010);
            }
            let known = model.keys().chain(before.keys()).copied();
            let rows = RowSet::from_keys(known.chain([0, 1, 2999, 3050, (1 << 30) + 50]));
            let mut keys: Vec<RowKey> = rows.iter().collect();
            keys.reverse();
            keys.extend([NULL_ROW_KEY, 7, 7, (1 << 31) + 1]);
            let ordered: Vec<RowKey> = rows.iter().collect();
            let now = [model_at(&model, &ordered), model_at(&model, &keys)];
            let then = [model_at(&before, &ordered), model_at(&before, &keys)];
            assert_eq!(ints(source.chunk(&rows)), now[0], "{cycle}");
            assert_eq!(ints(source.chunk_by_keys(&keys, false)), now[1], "{cycle}");
            assert_eq!(ints(source.prev_chunk(&rows)), then[0], "{cycle}");
            assert_eq!(ints(source.chunk_by_keys(&keys, true)), then[1], "{cycle}");
            // A part of the rows, so that changed keys lie before, in and after it.
            let part = rows.sub_set_by_position_range(rows.size() / 3, rows.size() / 2);
            let ordered: Vec<RowKey> = part.iter().collect();
            assert_eq!(ints(source.chunk(&part)), model_at(&model, &ordered));
            assert_eq!(ints(source.prev_chunk(&part)), model_at(&before, &ordered));
            clock.finish_cycle();
            assert_eq!(ints(source.prev_chunk(&rows)), now[0], "{cycle}");
            assert_eq!(ints(source.chunk_by_keys(&keys, true)), now[1], "{cycle}");
            assert_eq!(source.read().slots.len(), model.len(), "{cycle}");
            forms[usize::from(source.read().slots.is_flat_array())] += 1;
        }
        // Both forms were in use.
        assert!(forms[0] > 20 && forms[1] > 20, "{forms:?}");
    }

    /// More rows than two pieces, written in three arrays and then changed in a cycle, read
    /// on one thread and on several: the same cells, now and before, in row order, at
    /// scattered rows and at keys in any order, strings among them.
    #[test]
    fn many_rows_are_read_the_same_on_any_number_of_threads() {
        let len = 5 * GATHER_PIECE_ROWS as i64 + 77;
        let cell = |k: i64| (k % 11 != 0).then_some((k * 7919 % 100_003) as i32);
        let text_of = |values: &[Option<i32>]| -> ArrayRef {
            let text = values.iter().map(|v| v.map(|v| format!("s{v}")));
            Arc::new(text.collect::<StringArray>())
        };
        let clock = ManualClock::idle_at(3);
        let ints_source = WritableSource::new(ColumnType::Int);
        let text_source = WritableSource::new(ColumnType::String);
        ints_source.start_tracking_prev(clock.clone());
        text_source.start_tracking_prev(clock.clone());
        type Model = BTreeMap<RowKey, Option<i32>>;
        let mut model = Model::new();
        let write = |model: &mut Model, rows: RowSet, cell: &dyn Fn(i64) -> Option<i32>| {
            let values: Vec<Option<i32>> = rows.iter().map(cell).collect();
            ints_source.fill(&rows, &int_array(values.clone())).unwrap();
            text_source.fill(&rows, &text_of(&values)).unwrap();
            model.extend(rows.iter().zip(values));
        };
        // Two arrays one after the other, and a third over rows of both.
        write(&mut model, RowSet::from_range(0, len / 2), &cell);
        write(&mut model, RowSet::from_range(len / 2 + 1, len - 1), &cell);
        let some = RowSet::from_keys((0..len).filter(|k| k % 1000 < 3));
        write(&mut model, some, &|k| Some(-(k as i32)));
        let before = model.clone();
        clock.start_cycle();
        let some = RowSet::from_keys((0..len).filter(|k| k % 777 == 5));
        write(&mut model, some, &|k| (k % 2 == 0).then_some(k as i32));
        let gone = RowSet::from_range(100_000, 100_020);
        ints_source.remove(&gone);
        text_source.remove(&gone);
        model.retain(|key, _| !gone.contains(*key));

        let all = RowSet::from_range(0, len + 9);
        // Runs of one to three keys with gaps, and one long run across a piece's edge.
        let mut some: Vec<RowKey> = (0..len).filter(|k| k % 5 < 1 + k % 3).collect();
        some.extend(200_000..200_000 + GATHER_PIECE_ROWS as i64);
        let some = RowSet::from_keys(some);
        assert!(some.size() as usize > 2 * GATHER_PIECE_ROWS);
        // Every key once in an order that is not theirs, then a null key and a repeat.
        let mut keys: Vec<RowKey> = (0..len).map(|i| (i * 7919) % len).collect();
        keys.extend([NULL_ROW_KEY, 5, 5, len + 1]);
        for threads in [1, 2, 4] {
            parallel::with_threads(threads, || {
                for (model, use_prev) in [(&model, false), (&before, true)] {
                    for rows in [&all, &some] {
                        let ordered: Vec<RowKey> = rows.iter().collect();
                        let expected = model_at(model, &ordered);
                        let read = |source: &WritableSource| match use_prev {
                            true => source.prev_chunk(rows).unwrap(),
                            false => source.chunk(rows).unwrap(),
                        };
                        let what = format!("{threads} threads, previous {use_prev}");
                        assert!(ints(Ok(read(&ints_source))) == expected, "{what}");
                        assert!(read(&text_source) == text_of(&expected), "{what}");
                    }
                    let expected = model_at(model, &keys);
                    let what = format!("keys, {threads} threads, previous {use_prev}");
                    let read = ints_source.chunk_by_keys(&keys, use_prev);
                    assert!(ints(read) == expected, "{what}");
                    let read = text_source.chunk_by_keys(&keys, use_prev).unwrap();
                    assert!(read == text_of(&expected), "{what}");
                }
            });
        }
    }

    /// Rows read in the order they were written in are slices of the written arrays: no
    /// value is copied for a read of one array, and a read across arrays concatenates.
    #[test]
    fn rows_written_in_row_order_are_read_without_a_gather() {
        let source = WritableSource::new(ColumnType::Int);
        let first = int_array((0..5000).map(Some));
        source.fill(&RowSet::from_range(0, 4999), &first).unwrap();
        let second = int_array((5000..9000).map(Some));
        source
            .fill(&RowSet::from_range(5000, 8999), &second)
            .unwrap();
        let start = |array: &ArrayRef| array.to_data().buffers()[0].as_ptr();

        let whole = source.chunk(&RowSet::from_range(0, 4999)).unwrap();
        assert_eq!(start(&whole), start(&first));
        let part = source.chunk(&RowSet::from_range(5100, 5199)).unwrap();
        // The hundredth int of the second array, in the same memory.
        assert_eq!(start(&part), start(&second).wrapping_add(400));
        assert_eq!(ints(Ok(part)), (5100..5200).map(Some).collect::<Vec<_>>());

        // Three runs: two of the first array, around a gap, and one of the second.
        let mut rows = RowSet::from_range(4000, 5999);
        rows.remove(&RowSet::from_range(4500, 4599));
        let expected: Vec<Option<i32>> = rows.iter().map(|k| Some(k as i32)).collect();
        assert_eq!(ints(source.chunk(&rows)), expected);
        // The same rows by key are runs too, and scattered ones are not.
        let keys: Vec<RowKey> = rows.iter().collect();
        assert_eq!(ints(source.chunk_by_keys(&keys, false)), expected);
        let slots = [(1, 7), (1, 8), (2, 0), (2, 1), (1, 9)];
        let runs = runs_of([&slots[..2], &slots[2..]].into_iter(), 96);
        assert_eq!(runs, Some(vec![(1, 7, 2), (2, 0, 2), (1, 9, 1)]));
        assert_eq!(runs_of([&slots[..]].into_iter(), 95), None);
        assert_eq!(runs_of(std::iter::empty(), 0), None);
    }
}
