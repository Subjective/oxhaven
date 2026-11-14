//! Port of `io.deephaven.engine.rowset.impl.TrackingWritableRowSetImplPrevTest`.
//!
//! The Java tests drive a `LogicalClock`. Here the clock is a step counter: `startUpdateCycle`
//! advances it and `completeUpdateCycle` leaves it alone, as `LogicalClockImpl` does.

use oxhaven_rowset::{RowSet, TrackingRowSet};

fn keys(keys: &[i64]) -> RowSet {
    RowSet::from_keys(keys.iter().copied())
}

fn insert(rows: &mut TrackingRowSet, step: u64, key: i64) {
    rows.write(step).insert(&keys(&[key]));
}

/// Port of `testPrevWithEmptyConstruction`.
#[test]
fn prev_of_an_empty_tracking_set_is_empty() {
    let rows = TrackingRowSet::new(RowSet::empty());
    assert!(rows.is_empty());
    assert!(rows.prev(0).is_empty());
}

/// Port of `testPrevWithSingleRangeIxOnly`.
#[test]
fn prev_changes_only_when_the_step_advances() {
    let mut step = 0;
    let mut rows = TrackingRowSet::new(keys(&[1]));
    assert_eq!(rows.size(), 1);
    assert_eq!(rows.first_row_key(), Some(1));
    assert_eq!(rows.prev(step).size(), 1);
    assert_eq!(rows.prev(step).first_row_key(), Some(1));

    insert(&mut rows, step, 2);
    assert_eq!(rows.prev(step).size(), 1);
    assert_eq!(rows.prev(step).first_row_key(), Some(1));

    step += 1; // startUpdateCycle
    assert_eq!(rows.prev(step).size(), 2);
    assert_eq!(rows.prev(step).last_row_key(), Some(2));

    insert(&mut rows, step, 3);
    assert_eq!(rows.size(), 3);
    assert_eq!(rows.prev(step).size(), 2);
    assert_eq!(rows.prev(step).last_row_key(), Some(2));
    insert(&mut rows, step, 4);
    assert_eq!(rows.size(), 4);
    assert_eq!(rows.prev(step).size(), 2);
    assert_eq!(rows.prev(step).last_row_key(), Some(2));

    // completeUpdateCycle: the step does not change, so neither does prev, even for a write
    // made while idle.
    assert_eq!(rows.size(), 4);
    assert_eq!(rows.prev(step).size(), 2);
    assert_eq!(rows.prev(step).last_row_key(), Some(2));
    insert(&mut rows, step, 5);
    assert_eq!(rows.size(), 5);
    assert_eq!(rows.prev(step).size(), 2);
    assert_eq!(rows.prev(step).last_row_key(), Some(2));

    step += 1; // startUpdateCycle
    assert_eq!(rows.size(), 5);
    assert_eq!(rows.prev(step).size(), 5);
    assert_eq!(rows.prev(step).last_row_key(), Some(5));
}

/// Port of `testPrevWithRspOnly`.
#[test]
fn prev_tracks_a_set_of_several_ranges() {
    let mut step = 0;
    let mut rows = TrackingRowSet::new(keys(&[1, 3]));
    assert_eq!(rows.size(), 2);
    assert_eq!(rows.first_row_key(), Some(1));
    assert_eq!(rows.last_row_key(), Some(3));
    assert_eq!(rows.prev(step), &keys(&[1, 3]));

    insert(&mut rows, step, 5);
    assert_eq!(rows.current(), &keys(&[1, 3, 5]));
    assert_eq!(rows.prev(step), &keys(&[1, 3]));

    step += 1; // startUpdateCycle
    assert_eq!(rows.prev(step), &keys(&[1, 3, 5]));

    insert(&mut rows, step, 7);
    assert_eq!(rows.size(), 4);
    assert_eq!(rows.prev(step), &keys(&[1, 3, 5]));
    insert(&mut rows, step, 9);
    assert_eq!(rows.size(), 5);
    assert_eq!(rows.prev(step), &keys(&[1, 3, 5]));

    // completeUpdateCycle
    assert_eq!(rows.size(), 5);
    assert_eq!(rows.prev(step), &keys(&[1, 3, 5]));
    insert(&mut rows, step, 11);
    assert_eq!(rows.size(), 6);
    assert_eq!(rows.prev(step), &keys(&[1, 3, 5]));

    step += 1; // startUpdateCycle
    assert_eq!(rows.size(), 6);
    assert_eq!(rows.prev(step).size(), 6);
    assert_eq!(rows.prev(step).last_row_key(), Some(11));
}

/// Port of `testPrevWithSingleThenRspThenEmptyThenSingle`.
#[test]
fn prev_follows_the_set_through_empty_and_back() {
    let mut step = 0;
    let mut rows = TrackingRowSet::new(keys(&[1]));
    assert_eq!(rows.size(), 1);
    assert_eq!(rows.first_row_key(), Some(1));
    assert_eq!(rows.prev(step), &keys(&[1]));

    step += 1;
    insert(&mut rows, step, 3);
    assert_eq!(rows.size(), 2);
    assert_eq!(rows.last_row_key(), Some(3));
    assert_eq!(rows.prev(step), &keys(&[1]));
    // completeUpdateCycle
    assert_eq!(rows.prev(step), &keys(&[1]));

    step += 1;
    assert_eq!(rows.prev(step), &keys(&[1, 3]));
    // completeUpdateCycle
    assert_eq!(rows.prev(step), &keys(&[1, 3]));

    step += 1;
    rows.write(step).remove(&RowSet::from_range(0, 4));
    assert_eq!(rows.prev(step), &keys(&[1, 3]));
    // completeUpdateCycle
    assert_eq!(rows.prev(step), &keys(&[1, 3]));

    step += 1;
    assert!(rows.prev(step).is_empty());
    insert(&mut rows, step, 1);
    assert!(rows.prev(step).is_empty());
    // completeUpdateCycle
    assert!(rows.prev(step).is_empty());

    step += 1;
    assert_eq!(rows.prev(step).size(), 1);
    assert_eq!(rows.prev(step).last_row_key(), Some(1));
}

/// `initializePreviousValue` has no Java test. It clears the saved value and the change step,
/// so the next read takes a fresh snapshot of the current value.
#[test]
fn initialize_previous_value_makes_prev_equal_current() {
    let mut rows = TrackingRowSet::new(keys(&[1]));
    insert(&mut rows, 7, 2);
    assert_eq!(rows.prev(7), &keys(&[1]));
    rows.initialize_previous_value();
    assert_eq!(rows.prev(7), &keys(&[1, 2]));
    insert(&mut rows, 7, 3);
    assert_eq!(rows.prev(7), &keys(&[1, 2]));
    assert_eq!(rows.into_current(), keys(&[1, 2, 3]));
}
