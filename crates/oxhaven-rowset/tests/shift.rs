//! Port of `io.deephaven.engine.table.impl.util.RowSetShiftDataTest`, against the contract type
//! `RowSetShiftData` and the helpers in `oxhaven_rowset::shift`.

use oxhaven_rowset::shift::{for_each_shift, for_each_unshift, intersect};
use oxhaven_rowset::{
    CoalescingShiftBuilder, RowSet, RowSetBuilderSequential, RowSetShiftData, ShiftBuilder,
    ShiftError, ShiftRange,
};

type Shift = (i64, i64, i64);

fn build(shifts: &[Shift]) -> Result<RowSetShiftData, ShiftError> {
    let mut builder = ShiftBuilder::new();
    for &(first, last, delta) in shifts {
        builder.shift_range(first, last, delta);
    }
    builder.build()
}

fn coalesce(pre_shift_keys: &RowSet, shifts: &[Shift]) -> RowSetShiftData {
    let mut builder = CoalescingShiftBuilder::new(pre_shift_keys.clone());
    for &(first, last, delta) in shifts {
        builder.shift_range(first, last, delta);
    }
    builder.build().unwrap()
}

fn gen_array(size: i64) -> Vec<i64> {
    (0..size).collect()
}

/// Port of `createMemMovCallback`: copies the values of one shifted range to their new slots,
/// in the direction that does not overwrite values of the same range before they are read.
fn mem_move(values: &mut [i64], s: ShiftRange) {
    let slots: Vec<i64> = if s.delta > 0 {
        (s.first..=s.last).rev().collect()
    } else {
        (s.first..=s.last).collect()
    };
    for slot in slots {
        values[(slot + s.delta) as usize] = values[slot as usize];
    }
}

fn apply_to_array(data: &RowSetShiftData, values: &mut [i64]) {
    for_each_shift(data, |s| mem_move(values, s));
}

fn unapply_to_array(data: &RowSetShiftData, values: &mut [i64]) {
    for_each_unshift(data, |s| mem_move(values, s));
}

/// Ports of `testPreOverlapOneLeft`, `testPreOverlapInsidePrevious`, `testPreOverlapInsideNext`,
/// `testPreOverlapOneRight` and `testPreCrossNoOverlap`.
#[test]
fn ranges_that_overlap_or_cross_before_shifting_are_rejected() {
    for shifts in [
        [(0, 9, 1), (9, 18, 2)],
        [(0, 9, 1), (1, 8, 2)],
        [(1, 8, 2), (0, 9, 1)],
        [(9, 18, 2), (0, 9, 1)],
        [(10, 19, 2), (0, 9, 1)],
    ] {
        assert!(
            matches!(build(&shifts), Err(ShiftError::OutOfOrder(..))),
            "{shifts:?}"
        );
    }
}

/// Ports of `testPostOverlapPositiveShifts`, `testPostCrossPositiveShifts`,
/// `testPostOverlapNegativeShifts`, `testPostCrossNegativeShifts`, `testPostOverlapPolarShifts`
/// and `testPostCrossPolarShifts`.
#[test]
fn ranges_that_overlap_or_cross_after_shifting_are_rejected() {
    for shifts in [
        [(0, 9, 20), (10, 19, 19)],
        [(0, 9, 100), (10, 19, 10)],
        [(0, 9, -20), (10, 19, -21)],
        [(0, 9, -10), (10, 19, -100)],
        [(0, 9, 10), (20, 29, -1)],
        [(0, 9, 20), (20, 29, -20)],
    ] {
        assert!(
            matches!(build(&shifts), Err(ShiftError::OutOfOrder(..))),
            "{shifts:?}"
        );
    }
}

/// Port of `testPositiveShiftSingleRun`.
#[test]
fn positive_shifts_in_one_run_keep_every_value() {
    let mut a = gen_array(40);
    let data = build(&[(0, 9, 10), (10, 19, 10), (20, 29, 10)]).unwrap();
    apply_to_array(&data, &mut a);
    for idx in 10..40 {
        assert_eq!(a[idx as usize], idx - 10);
    }
}

/// Port of `testNegativeShiftSingleRun`.
#[test]
fn negative_shifts_in_one_run_keep_every_value() {
    let mut a = gen_array(40);
    let data = build(&[(10, 19, -10), (20, 29, -10), (30, 39, -10)]).unwrap();
    apply_to_array(&data, &mut a);
    for idx in 0..30 {
        assert_eq!(a[idx as usize], idx + 10);
    }
}

/// Port of `testIgnoreZeroShifts`.
#[test]
fn zero_shifts_are_ignored() {
    let data = build(&[(10, 19, 0), (20, 29, 0), (30, 39, 0)]).unwrap();
    assert_eq!(data.ranges().len(), 0);
    assert!(data.is_empty());
    let mut any_shift = false;
    for_each_shift(&data, |_| any_shift = true);
    assert!(!any_shift);
}

/// Port of `testMemMovSafeLRL`.
#[test]
fn up_down_up_runs_apply_and_unapply_without_losing_values() {
    let data = build(&[
        (0, 0, 1),
        (1, 1, 1),
        (4, 4, -1),
        (5, 5, -1),
        (6, 6, 1),
        (7, 7, 1),
    ])
    .unwrap();
    let mut a = gen_array(9);
    apply_to_array(&data, &mut a);
    assert_eq!(a, [0, 0, 1, 4, 5, 5, 6, 6, 7]);

    let mut b = gen_array(9);
    unapply_to_array(&data, &mut b);
    assert_eq!(b, [1, 2, 2, 3, 3, 4, 7, 8, 8]);
}

/// Port of `testMemMovSafeRLR`.
#[test]
fn down_up_down_runs_apply_and_unapply_without_losing_values() {
    let data = build(&[
        (1, 1, -1),
        (2, 2, -1),
        (3, 3, 1),
        (4, 4, 1),
        (7, 7, -1),
        (8, 8, -1),
    ])
    .unwrap();
    let mut a = gen_array(9);
    apply_to_array(&data, &mut a);
    assert_eq!(a, [1, 2, 2, 3, 3, 4, 7, 8, 8]);

    let mut b = gen_array(9);
    unapply_to_array(&data, &mut b);
    assert_eq!(b, [0, 0, 1, 4, 5, 5, 6, 6, 7]);
}

/// Port of `testMoveSingleItemRange`.
#[test]
fn a_single_key_moves_and_moves_back() {
    let data = build(&[(0, 0, 9)]).unwrap();
    let mut a = gen_array(10);
    apply_to_array(&data, &mut a);
    assert_eq!(a, [0, 1, 2, 3, 4, 5, 6, 7, 8, 0]);

    let mut b = gen_array(10);
    unapply_to_array(&data, &mut b);
    assert_eq!(b, [9, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

/// Port of `getScbTestIndex`.
fn scb_test_rows() -> RowSet {
    let mut b = RowSetBuilderSequential::new();
    b.append_keys([100, 200, 300, 400, 500, 600]);
    b.append_range(1000, 1100);
    b.append_range(2000, 2100);
    b.append_range(3000, 3100);
    b.append_keys([4000, 4100, 4200]);
    b.build()
}

/// Checks that the coalesced shifts move `rows` exactly as the plain ones do, and that they
/// really move something. `presented` is the order the Java test gives the smart builder.
fn assert_coalesced_matches(
    rows: &RowSet,
    sorted: &[Shift],
    presented: &[Shift],
) -> RowSetShiftData {
    let plain = build(sorted).unwrap();
    let smart = coalesce(rows, presented);
    let expected = plain.apply(rows).unwrap();
    assert_eq!(smart.apply(rows).unwrap(), expected);
    assert!(smart.ranges().len() <= plain.ranges().len());
    smart
}

/// Port of `testSmartCoalescingBuilder1`.
#[test]
fn coalescing_builder_1() {
    let rows = scb_test_rows();
    let smart = assert_coalesced_matches(
        &rows,
        &[
            (50, 150, -10),
            (250, 350, -10),
            (3950, 4050, 10),
            (4150, 4250, 10),
        ],
        &[
            (50, 150, -10),
            (250, 350, -10),
            (4150, 4250, 10),
            (3950, 4050, 10),
        ],
    );
    // Keys 200 and 4100 lie between the ranges of each pair, so nothing may merge.
    assert_eq!(smart.ranges().len(), 4);
    assert_ne!(smart.apply(&rows).unwrap(), rows);
}

/// Port of `testSmartCoalescingBuilder2`.
#[test]
fn coalescing_builder_2() {
    let rows = scb_test_rows();
    let smart = assert_coalesced_matches(
        &rows,
        &[
            (50, 150, -10),
            (190, 210, -10),
            (250, 350, -10),
            (3950, 4050, 10),
            (4090, 4110, 10),
            (4150, 4250, 10),
        ],
        &[
            (50, 150, -10),
            (190, 210, -10),
            (250, 350, -10),
            (4150, 4250, 10),
            (4090, 4110, 10),
            (3950, 4050, 10),
        ],
    );
    assert_eq!(smart.ranges().len(), 2);
}

/// Port of `testSmartCoalescingBuilder3`.
#[test]
fn coalescing_builder_3() {
    let rows = scb_test_rows();
    assert_coalesced_matches(&rows, &[(1000, 4000, 10)], &[(1000, 4000, 10)]);
}

/// Port of `testSmartCoalescingBuilder4`.
#[test]
fn coalescing_builder_4() {
    let rows = scb_test_rows();
    assert_coalesced_matches(
        &rows,
        &[
            (20, 30, 10),
            (45, 45, -1),
            (50, 150, 10),
            (250, 350, 10),
            (600, 600, -1),
            (3950, 4050, 10),
            (4150, 4250, 10),
        ],
        &[
            (20, 30, 10),
            (45, 45, -1),
            (250, 350, 10),
            (50, 150, 10),
            (600, 600, -1),
            (4150, 4250, 10),
            (3950, 4050, 10),
        ],
    );
}

/// Ports of `testSmartCoalescingBuilder5` and `testSmartCoalescingBuilder6`.
#[test]
fn coalescing_builder_5_and_6() {
    for rows in [scb_test_rows(), RowSet::empty()] {
        let smart = assert_coalesced_matches(
            &rows,
            &[
                (30, 40, 10),
                (50, 150, 10),
                (250, 350, 10),
                (4300, 4301, -1),
            ],
            &[
                (250, 350, 10),
                (50, 150, 10),
                (30, 40, 10),
                (4300, 4301, -1),
            ],
        );
        if rows.is_empty() {
            assert!(smart.is_empty());
        }
    }
}

/// Port of `testSmartCoalescingBuilder7`.
#[test]
fn coalescing_builder_7() {
    let rows = RowSet::from_keys([1, 10, 13]);
    assert_coalesced_matches(
        &rows,
        &[(1, 2, 2), (10, 12, 1), (13, 13, 3)],
        &[(1, 2, 2), (13, 13, 3), (10, 12, 1)],
    );
}

/// Port of `testSmartCoalescingBuilder8`.
#[test]
fn coalescing_builder_8() {
    let rows = RowSet::from_keys([0, 2, 16, 17, 23, 30, 35, 40]);
    assert_coalesced_matches(
        &rows,
        &[
            (5, 6, 1),
            (7, 7, 2),
            (9, 10, 1),
            (11, 14, 2),
            (16, 18, 1),
            (24, 29, -1),
            (31, 36, -2),
            (38, 40, -3),
        ],
        &[
            (16, 18, 1),
            (11, 14, 2),
            (9, 10, 1),
            (7, 7, 2),
            (5, 6, 1),
            (24, 29, -1),
            (31, 36, -2),
            (38, 40, -3),
        ],
    );
}

/// Port of `testSmartCoalescingBuilder9`.
#[test]
fn coalescing_builder_9() {
    let rows = RowSet::from_keys([0, 1, 6, 13, 20]);
    let shifts = [(3, 7, 1), (13, 14, 1), (20, 27, -1)];
    let smart = assert_coalesced_matches(&rows, &shifts, &shifts);
    // No key lies in 8..=12, so the two upward shifts merge.
    assert_eq!(smart.ranges().len(), 2);
    assert_eq!(
        smart.apply(&rows).unwrap(),
        RowSet::from_keys([0, 1, 7, 14, 19])
    );
}

/// The Java suite has no direct test of `RowSetShiftData#intersect`. Expected values follow
/// from its source: a shift is kept when the row set has a key in its pre-shift range.
#[test]
fn intersect_keeps_shifts_that_touch_the_row_set() {
    let data = build(&[(10, 19, -5), (30, 39, 3), (50, 59, 3), (70, 79, 100)]).unwrap();
    let rows = RowSet::from_keys([9, 19, 40, 49, 50, 80]);
    assert_eq!(
        intersect(&data, &rows),
        build(&[(10, 19, -5), (50, 59, 3)]).unwrap()
    );
    assert!(intersect(&data, &RowSet::empty()).is_empty());
    assert_eq!(intersect(&data, &RowSet::from_range(0, 100)), data);
}

/// Port of the behavior of `RowSetShiftData#unapply(WritableRowSet)`, which has no direct Java
/// test: keys in post-shift ranges move back, other keys stay.
#[test]
fn inverse_moves_a_row_set_back_to_pre_shift_keys() {
    let rows = scb_test_rows();
    let data = build(&[
        (50, 150, -10),
        (250, 350, -10),
        (3950, 4050, 10),
        (4150, 4250, 10),
    ])
    .unwrap();
    let after = data.apply(&rows).unwrap();
    assert_ne!(after, rows);
    assert_eq!(data.inverse().apply(&after).unwrap(), rows);
}

/// Port of `Builder#shiftRange` coalescing and `Builder#lastShiftEnd`.
#[test]
fn builder_merges_adjacent_equal_deltas_and_reports_its_end() {
    let mut builder = ShiftBuilder::new();
    assert!(builder.is_empty());
    assert_eq!(builder.last_shift_end(), None);
    builder.shift_range(5, 4, 3);
    builder.shift_range(0, 4, 0);
    assert!(builder.is_empty());
    builder.shift_range(0, 4, 5);
    builder.shift_range(5, 9, 5);
    assert_eq!(builder.last_shift_end(), Some(9));
    builder.shift_range(11, 12, 5);
    assert_eq!(builder.last_shift_end(), Some(12));
    assert!(!builder.is_empty());
    let data = builder.build().unwrap();
    assert_eq!(
        data.ranges(),
        &[
            ShiftRange {
                first: 0,
                last: 9,
                delta: 5
            },
            ShiftRange {
                first: 11,
                last: 12,
                delta: 5
            },
        ]
    );
}

/// Every value lands where a simultaneous move would put it.
fn moved_at_once(data: &RowSetShiftData, values: &[i64]) -> Vec<i64> {
    let mut out = values.to_vec();
    for s in data.ranges() {
        for slot in s.first..=s.last {
            out[(slot + s.delta) as usize] = values[slot as usize];
        }
    }
    out
}

/// The Java memmove tests cannot tell a safe order from an unsafe one: their neighbouring
/// shifts have equal deltas, so the builder merges each run into one range. Here each run holds
/// two shifts with different deltas, and the destination of one covers the source of the other,
/// so only the order `RowSetShiftData#apply(RowKeyRangeShiftCallback)` prescribes (a run that
/// moves up from its highest range, a run that moves down from its lowest) keeps every value.
#[test]
fn shifts_within_a_run_are_visited_in_the_order_that_loses_nothing() {
    let data = build(&[
        (0, 1, 2),
        (3, 4, 3),
        (10, 11, -2),
        (13, 14, -3),
        (20, 21, 2),
        (23, 24, 3),
    ])
    .unwrap();
    assert_eq!(data.ranges().len(), 6);

    let mut visited = Vec::new();
    for_each_shift(&data, |s| visited.push((s.first, s.last, s.delta)));
    assert_eq!(
        visited,
        [
            (3, 4, 3),
            (0, 1, 2),
            (10, 11, -2),
            (13, 14, -3),
            (23, 24, 3),
            (20, 21, 2)
        ]
    );
    let mut a = gen_array(30);
    apply_to_array(&data, &mut a);
    assert_eq!(a, moved_at_once(&data, &gen_array(30)));

    let mut visited = Vec::new();
    for_each_unshift(&data, |s| visited.push((s.first, s.last, s.delta)));
    assert_eq!(
        visited,
        [
            (2, 3, -2),
            (6, 7, -3),
            (10, 11, 3),
            (8, 9, 2),
            (22, 23, -2),
            (26, 27, -3)
        ]
    );
    let mut b = gen_array(30);
    unapply_to_array(&data, &mut b);
    assert_eq!(b, moved_at_once(&data.inverse(), &gen_array(30)));
}

/// Ranges that touch have no key between them, whatever keys they hold, so they merge.
#[test]
fn coalescing_builder_merges_adjacent_ranges() {
    let rows = RowSet::from_keys([5, 6]);
    let smart = coalesce(&rows, &[(6, 9, 1), (0, 5, 1)]);
    assert_eq!(
        smart.ranges(),
        &[ShiftRange {
            first: 0,
            last: 9,
            delta: 1
        }]
    );
}
