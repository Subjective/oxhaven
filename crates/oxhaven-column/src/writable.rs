//! A writable column addressed by row key, with previous values for the current cycle.

use crate::tracked::{Clock, Reader, Tracked};
use crate::{arrow_failure, failure};
use arrow::array::{new_null_array, Array, ArrayRef};
use arrow::compute::interleave;
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

/// A value's place in storage: the batch it was written in and its index there.
type Slot = (u32, u32);

/// Where every unset row key reads from: the single null of batch 0.
const NULL_SLOT: Slot = (0, 0);

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
/// points the written row keys at it, and a read gathers values with Arrow's `interleave`.
/// Nothing here is specific to a column type. Arrays whose values were mostly overwritten or
/// removed are compacted away on a later write.
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
}

impl WritableSource {
    /// An empty source that does not track previous values.
    pub fn new(column_type: ColumnType) -> Self {
        let null = new_null_array(&column_type.arrow_type(), 1);
        Self {
            column_type,
            state: RwLock::new(State {
                slots: Tracked::new(),
                batches: vec![null],
                stored: 0,
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
        let mut slots = vec![NULL_SLOT; rows.size() as usize];
        let mut offset = 0;
        for range in rows.ranges() {
            reader.for_each_in(range.first(), range.last(), |key, slot| {
                slots[offset + (key - range.first()) as usize] = slot;
            });
            offset += range.size() as usize;
        }
        gather(&state.batches, &slots)
    }
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
        let live = writer.map().current().len() as u64;
        let batches = self.batches.len() as u64 - 1;
        let mostly_dead = self.stored > 2 * live + 1024;
        let fragmented = batches > 64 && batches * 64 > live;
        if !(mostly_dead || fragmented) || live > u64::from(u32::MAX) {
            return;
        }
        let slots: Vec<Slot> = writer.map().current().values().copied().collect();
        let Ok(compacted) = gather(&self.batches, &slots) else {
            return;
        };
        self.batches.truncate(1);
        self.stored = 0;
        if live > 0 {
            self.batches.push(compacted);
            self.stored = live;
            writer.replace_all((0..live as u32).map(|index| (1, index)));
        }
    }
}

/// The values at `slots`, in order.
fn gather(batches: &[ArrayRef], slots: &[Slot]) -> Result<ArrayRef, TableFailure> {
    // Hand `interleave` only the batches this read touches, so a read costs what it returns
    // and not the number of batches stored.
    let mut dense: HashMap<u32, usize> = HashMap::new();
    let mut used: Vec<&dyn Array> = Vec::new();
    let indices: Vec<(usize, usize)> = slots
        .iter()
        .map(|&(batch, index)| {
            let position = *dense.entry(batch).or_insert_with(|| {
                used.push(batches[batch as usize].as_ref());
                used.len() - 1
            });
            (position, index as usize)
        })
        .collect();
    if used.is_empty() {
        used.push(batches[0].as_ref());
    }
    interleave(&used, &indices).map_err(arrow_failure)
}

fn slot_of(reader: &Reader<'_, Slot>, key: RowKey) -> Slot {
    if key == NULL_ROW_KEY {
        return NULL_SLOT;
    }
    reader.get(key).unwrap_or(NULL_SLOT)
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
        let slots: Vec<Slot> = keys.iter().map(|&k| slot_of(&reader, k)).collect();
        gather(&state.batches, &slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracked::testing::ManualClock;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, TimestampNanosecondArray, UInt16Array,
    };
    use oxhaven_contracts::shift::ShiftRange;

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
}
