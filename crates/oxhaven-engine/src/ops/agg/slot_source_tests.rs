//! Tests of [`SlotSource`]: values by row key, previous values within a cycle, and
//! compaction.

use super::*;
use arrow::array::{Int32Array, StringArray};
use oxhaven_contracts::rowset::NULL_ROW_KEY;
use std::sync::Mutex;

/// A clock the test moves by hand.
struct ManualClock(Mutex<ClockValue>);

impl ManualClock {
    fn idle() -> Arc<ManualClock> {
        Arc::new(ManualClock(Mutex::new(ClockValue {
            step: 1,
            phase: ClockPhase::Idle,
        })))
    }

    fn start_cycle(&self) {
        let mut value = self.0.lock().unwrap();
        value.step += 1;
        value.phase = ClockPhase::Updating;
    }

    fn finish_cycle(&self) {
        self.0.lock().unwrap().phase = ClockPhase::Idle;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> ClockValue {
        *self.0.lock().unwrap()
    }
}

fn ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn read(source: &SlotSource, keys: &[RowKey], prev: bool) -> Vec<Option<i32>> {
    let chunk = source.chunk_by_keys(keys, prev).expect("a read");
    let chunk = chunk.as_any().downcast_ref::<Int32Array>().expect("ints");
    chunk.iter().collect()
}

#[test]
fn unwritten_keys_read_as_null_and_the_last_write_wins() {
    let source = SlotSource::new(ColumnType::Int, None);
    assert_eq!(read(&source, &[0, 3], false), vec![None, None]);
    source
        .fill_by_keys(&[2, 0, 2], &ints(&[Some(1), Some(2), Some(3)]))
        .unwrap();
    assert_eq!(
        read(&source, &[0, 1, 2, 7, NULL_ROW_KEY], false),
        vec![Some(2), None, Some(3), None, None]
    );
    source.fill_by_keys(&[1], &ints(&[None])).unwrap();
    source.fill_by_keys(&[0], &ints(&[Some(9)])).unwrap();
    let rows = RowSet::from_keys([0, 1, 2]);
    let chunk = source.chunk(&rows).unwrap();
    assert_eq!(chunk.as_ref(), ints(&[Some(9), None, Some(3)]).as_ref());
    // Without a clock, previous values are current values.
    assert_eq!(source.prev_chunk(&rows).unwrap().as_ref(), chunk.as_ref());
}

#[test]
fn a_write_in_a_cycle_keeps_the_value_from_before_it_until_the_cycle_ends() {
    let clock = ManualClock::idle();
    let source = SlotSource::new(ColumnType::Int, Some(clock.clone()));
    source
        .fill_by_keys(&[0, 1], &ints(&[Some(10), Some(11)]))
        .unwrap();
    assert_eq!(read(&source, &[0, 1], true), vec![Some(10), Some(11)]);

    clock.start_cycle();
    source
        .fill_by_keys(&[1, 2], &ints(&[Some(21), Some(22)]))
        .unwrap();
    // A second write in the cycle keeps the first previous value.
    source.fill_by_keys(&[1], &ints(&[Some(31)])).unwrap();
    assert_eq!(
        read(&source, &[0, 1, 2], false),
        vec![Some(10), Some(31), Some(22)]
    );
    assert_eq!(
        read(&source, &[0, 1, 2], true),
        vec![Some(10), Some(11), None]
    );
    clock.finish_cycle();
    assert_eq!(
        read(&source, &[0, 1, 2], true),
        vec![Some(10), Some(31), Some(22)]
    );

    // The record of an earlier cycle is not read in a later one.
    clock.start_cycle();
    assert_eq!(read(&source, &[1, 2], true), vec![Some(31), Some(22)]);
    source.fill_by_keys(&[2], &ints(&[None])).unwrap();
    assert_eq!(read(&source, &[1, 2], true), vec![Some(31), Some(22)]);
    assert_eq!(read(&source, &[1, 2], false), vec![Some(31), None]);
    clock.finish_cycle();
}

#[test]
fn many_small_writes_are_compacted_without_changing_a_value() {
    let clock = ManualClock::idle();
    let source = SlotSource::new(ColumnType::String, Some(clock.clone()));
    let mut expected: Vec<Option<String>> = vec![None; 300];
    for round in 0..40 {
        clock.start_cycle();
        for key in (round % 7..300).step_by(7) {
            let value = (key % 5 != 0).then(|| format!("{key}-{round}"));
            let array: ArrayRef = Arc::new(StringArray::from(vec![value.clone()]));
            source.fill_by_keys(&[key as RowKey], &array).unwrap();
            expected[key] = value;
        }
        clock.finish_cycle();
        let keys: Vec<RowKey> = (0..300).collect();
        let chunk = source.chunk_by_keys(&keys, false).unwrap();
        let chunk = chunk.as_any().downcast_ref::<StringArray>().unwrap();
        let read: Vec<Option<String>> = chunk.iter().map(|v| v.map(str::to_string)).collect();
        assert_eq!(read, expected, "round {round}");
    }
    let state = source.state.read().unwrap();
    assert!(
        state.batches.len() < 200,
        "{} batches were kept",
        state.batches.len()
    );
}

#[test]
fn a_write_of_the_wrong_type_or_length_is_refused() {
    let source = SlotSource::new(ColumnType::Long, None);
    assert!(source.fill_by_keys(&[0], &ints(&[Some(1)])).is_err());
    let longs: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2]));
    assert!(source.fill_by_keys(&[0], &longs).is_err());
    assert!(source.fill_by_keys(&[-1, 0], &longs).is_err());
}
