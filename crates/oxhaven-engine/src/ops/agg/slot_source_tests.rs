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

/// Consecutive values of one written array are read as a slice of it, without a copy;
/// other reads copy, and both read the same values.
#[test]
fn consecutive_values_are_read_without_a_copy() {
    let source = SlotSource::new(ColumnType::Int, None);
    let written = ints(&[Some(1), None, Some(3), Some(4), Some(5)]);
    source.fill_by_keys(&[0, 1, 2, 3, 4], &written).unwrap();
    let pointer = |array: &ArrayRef| array.to_data().buffers()[0].data_ptr();

    let middle = source.chunk(&RowSet::from_range(1, 3)).unwrap();
    assert_eq!(&middle, &ints(&[None, Some(3), Some(4)]));
    assert_eq!(pointer(&middle), pointer(&written));

    let scattered = source.chunk_by_keys(&[3, 1, 4], false).unwrap();
    assert_eq!(&scattered, &ints(&[Some(4), None, Some(5)]));
    assert_ne!(pointer(&scattered), pointer(&written));
    // A key past the end reads as null, from the null batch.
    let past = source.chunk(&RowSet::from_range(3, 5)).unwrap();
    assert_eq!(&past, &ints(&[Some(4), Some(5), None]));
}

/// A column written once from key 0 on keeps no place per key, reads as one written key
/// by key, and becomes one when it is written again.
#[test]
fn a_column_written_once_from_the_first_key_reads_as_one_written_by_keys() {
    let values = ints(&[Some(10), None, Some(12), Some(13)]);
    let clock = ManualClock::idle();
    let dense = SlotSource::new(ColumnType::Int, Some(clock.clone()));
    let keyed = SlotSource::new(ColumnType::Int, Some(clock.clone()));
    dense.fill_from(0, &values).unwrap();
    keyed.fill_by_keys(&[0, 1, 2, 3], &values).unwrap();
    {
        let state = dense.state.read().unwrap();
        assert_eq!((state.dense, state.places.len()), (4, 0));
    }
    let same = |context: &str| {
        for keys in [
            vec![0, 1, 2, 3],
            vec![3, 0],
            vec![2, 9, 1],
            vec![NULL_ROW_KEY],
        ] {
            for use_prev in [false, true] {
                assert_eq!(
                    dense.chunk_by_keys(&keys, use_prev).unwrap().as_ref(),
                    keyed.chunk_by_keys(&keys, use_prev).unwrap().as_ref(),
                    "{context}: {keys:?}, previous {use_prev}"
                );
            }
        }
    };
    same("written once");
    assert_eq!(
        dense.chunk(&RowSet::flat(4)).unwrap().as_ref(),
        values.as_ref()
    );
    // A write in a cycle: both keep what the keys held before it.
    clock.start_cycle();
    for source in [&dense, &keyed] {
        source
            .fill_by_keys(&[1, 5], &ints(&[Some(21), Some(25)]))
            .unwrap();
    }
    assert_eq!(dense.state.read().unwrap().dense, 0);
    same("written again in a cycle");
    clock.finish_cycle();
    same("after the cycle");
    // Written from another key on, or a second time, a column has a place per key.
    let later = SlotSource::new(ColumnType::Int, None);
    later.fill_from(2, &values).unwrap();
    assert_eq!(later.state.read().unwrap().dense, 0);
    assert_eq!(
        later.chunk_by_keys(&[0, 2, 5, 6], false).unwrap().as_ref(),
        ints(&[None, Some(10), Some(13), None]).as_ref()
    );
    dense.fill_from(0, &ints(&[Some(1)])).unwrap();
    assert_eq!(
        dense.chunk_by_keys(&[0, 1, 2], false).unwrap().as_ref(),
        ints(&[Some(1), Some(21), Some(12)]).as_ref()
    );
}
