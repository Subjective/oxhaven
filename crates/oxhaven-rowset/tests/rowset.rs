//! Ports of the Java row-set unit tests that exercise behavior exposed by the contract type
//! `RowSet`: `WritableRowSetImplTest`, `SingleRangeTest`, `RowSetFactoryUnionInsertTest` and
//! `MixedBuilderTest` from `io.deephaven.engine.rowset`.
//!
//! What changed in the port:
//! - Java asserts on the internal container (`SingleRange`, `SortedRanges`, `RspBitmap`), on
//!   reference counts and on `validate()`. Those have no counterpart; the observable result is
//!   compared with a sorted list of keys instead, and every result is checked to be canonical.
//! - Java's `find` returns `-insertion - 1` for a missing key; that is `Err(insertion)` here.
//! - The contract row set costs O(ranges) per operation, so the sizes of the randomized tests
//!   are smaller than in Java. Each test says what it runs.
//! - Random inputs come from a small fixed-seed generator, not `java.util.Random`; the expected
//!   values are computed from a model, so only reproducibility matters.
//!
//! `tasks/1.2-rowset-tests.md` lists the Java tests that are not ported and why.

use oxhaven_rowset::{
    union_all, RowKey, RowRange, RowSet, RowSetBuilderRandom, RowSetBuilderSequential,
};
use std::collections::BTreeSet;

/// `RspArray.BLOCK_SIZE`: the Java tests place keys around multiples of it.
const BLOCK_SIZE: i64 = 65536;
const BLOCK_LAST: i64 = BLOCK_SIZE - 1;

/// The `KEYS` table of `WritableRowSetImplTest`.
const KEYS: &[&[RowKey]] = &[
    &[],
    &[0],
    &[1],
    &[0, 1, 2],
    &[0, 1, 2, 4],
    &[0, 2, 4],
    &[0, 2, 4, 5],
    &[0, 2, 3, 4, 5],
    &[0, 1, 2, 3, 4, 5],
    &[0, 2, 3, 4, 5, 10],
    &[0, 2, 3, 4, 5, 8, 9, 10],
    &[0, 2, 3, 4, 5, 8, 9, 10, 20],
    &[2, 3, 4, 5, 8, 9, 10, 20],
];

/// Deterministic generator, so failures reproduce.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    /// A value in `0..bound`; 0 when `bound` is 0.
    fn below(&mut self, bound: i64) -> i64 {
        if bound <= 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as i64
        }
    }

    fn flip(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    /// A value in `0.0..1.0`.
    fn unit(&mut self) -> f64 {
        self.next_u64() as f64 / (1u64 << 31) as f64
    }
}

/// Port of `getRowSet`: the Java tests build their inputs with the random builder.
fn row_set(keys: &[RowKey]) -> RowSet {
    let mut builder = RowSetBuilderRandom::new();
    builder.add_keys(keys.iter().copied());
    builder.build()
}

// `WritableRowSet` single-key and range forms, expressed through the contract API.

fn insert_key(rows: &mut RowSet, key: RowKey) {
    rows.insert(&RowSet::from_range(key, key));
}

fn insert_range(rows: &mut RowSet, first: RowKey, last: RowKey) {
    rows.insert(&RowSet::from_range(first, last));
}

fn remove_key(rows: &mut RowSet, key: RowKey) {
    rows.remove(&RowSet::from_range(key, key));
}

fn remove_range(rows: &mut RowSet, first: RowKey, last: RowKey) {
    rows.remove(&RowSet::from_range(first, last));
}

fn contains_range(rows: &RowSet, first: RowKey, last: RowKey) -> bool {
    RowSet::from_range(first, last).is_subset_of(rows)
}

fn overlaps_range(rows: &RowSet, first: RowKey, last: RowKey) -> bool {
    rows.overlaps(&RowSet::from_range(first, last))
}

fn insert_with_shift(rows: &mut RowSet, shift: i64, other: &RowSet) {
    rows.insert(&other.shift(shift).unwrap());
}

fn assert_canonical(rows: &RowSet) {
    for w in rows.ranges().windows(2) {
        assert!(w[0].last() + 1 < w[1].first(), "not canonical: {rows:?}");
    }
}

/// Port of `compareIndexAndKeyValues`: forward iteration, size, positional access, ends.
/// `get` is checked at every position for small sets and at a spread of positions for large
/// ones, because each call walks the ranges.
fn assert_keys(context: &str, rows: &RowSet, keys: &[RowKey]) {
    assert_canonical(rows);
    assert_eq!(rows.size(), keys.len() as u64, "{context}: size");
    assert!(
        rows.iter().eq(keys.iter().copied()),
        "{context}: keys of {rows:?}"
    );
    assert_eq!(rows.is_empty(), keys.is_empty(), "{context}");
    assert_eq!(rows.first_row_key(), keys.first().copied(), "{context}");
    assert_eq!(rows.last_row_key(), keys.last().copied(), "{context}");
    let stride = (keys.len() / 64).max(1);
    for i in (0..keys.len())
        .step_by(stride)
        .chain(keys.len().checked_sub(1))
    {
        assert_eq!(rows.get(i as u64), Some(keys[i]), "{context}: get({i})");
    }
    assert_eq!(rows.get(keys.len() as u64), None, "{context}");
}

fn bools_to_keys(set: &[bool]) -> Vec<RowKey> {
    (0..set.len())
        .filter(|&i| set[i])
        .map(|i| i as RowKey)
        .collect()
}

/// Port of `doMinusSimple`.
fn minus_model(all: &[RowKey], sub: &[RowKey]) -> Vec<RowKey> {
    let sub: BTreeSet<RowKey> = sub.iter().copied().collect();
    all.iter().copied().filter(|k| !sub.contains(k)).collect()
}

// ---------------------------------------------------------------------------------------------
// WritableRowSetImplTest
// ---------------------------------------------------------------------------------------------

/// Ports of `testSimple` and `testSimpleRangeIterator`.
#[test]
fn single_key_and_single_range() {
    let one = RowSet::from_range(2, 2);
    assert_eq!(one.size(), 1);
    assert_eq!(one.iter().collect::<Vec<_>>(), [2]);

    let range = RowSet::from_range(3, 100);
    assert_eq!(range.ranges(), &[RowRange::new(3, 100)]);
}

/// Port of `testRange`, without its reference-count assertions.
#[test]
fn position_range_of_empty_and_short_sets() {
    let mut rows = RowSet::empty();
    assert_eq!(rows.sub_set_by_position_range(0, 10).size(), 0);
    insert_key(&mut rows, 1);
    insert_key(&mut rows, 10);
    let range = rows.sub_set_by_position_range(0, 3);
    assert_eq!(range.size(), 2);
    assert_eq!(range, rows);
}

/// Port of `testsubindexByKey`.
#[test]
fn sub_set_by_key_range_at_the_edges() {
    let rows = row_set(&[1, 2, 3]);
    assert_eq!(rows.sub_set_by_key_range(1, 2).size(), 2);
    assert_eq!(rows.sub_set_by_key_range(2, 3).size(), 2);
    for key in 1..=3 {
        assert_eq!(rows.sub_set_by_key_range(key, key), row_set(&[key]));
    }
    assert_eq!(rows.sub_set_by_key_range(0, 0).size(), 0);
    assert_eq!(rows.sub_set_by_key_range(4, 4).size(), 0);
    assert_eq!(row_set(&[2]).sub_set_by_key_range(0, 1).size(), 0);
}

/// Port of `testSubSetByKeyRegression0`.
#[test]
fn sub_set_by_key_range_that_ends_past_the_last_key() {
    let rows = row_set(&[1073741813, 1073741814]);
    let sub = rows.sub_set_by_key_range(1073741814, 1073741825);
    assert_eq!(sub.size(), 1);
    assert_eq!(sub.get(0), Some(1073741814));
}

/// Port of `testFind`.
#[test]
fn find_returns_the_position_or_the_insertion_point() {
    for keys in KEYS {
        let rows = row_set(keys);
        for (i, &key) in keys.iter().enumerate() {
            assert_eq!(rows.find_or_insertion(key), Ok(i as u64), "{rows:?}");
            assert_eq!(rows.find(key), Some(i as u64), "{rows:?}");
            if let Some(&next) = keys.get(i + 1) {
                for missing in key + 1..next {
                    assert_eq!(
                        rows.find_or_insertion(missing),
                        Err(i as u64 + 1),
                        "{rows:?}"
                    );
                    assert_eq!(rows.find(missing), None, "{rows:?}");
                }
            }
        }
        match (keys.first(), keys.last()) {
            (Some(&first), Some(&last)) => {
                if first > 0 {
                    assert_eq!(rows.find_or_insertion(first - 1), Err(0));
                }
                assert_eq!(rows.find_or_insertion(last + 1), Err(keys.len() as u64));
            }
            _ => assert_eq!(rows.find_or_insertion(10), Err(0)),
        }
    }
}

/// Port of `testInvert`: the fixed case, then 400 random row sets of up to 20000 keys with a
/// random subset each.
#[test]
fn invert_gives_the_positions_of_a_subset() {
    let rows = row_set(&[1, 4, 7, 9, 10]);
    let inverted = rows.invert(&row_set(&[4, 7, 10])).unwrap();
    assert_keys("fixed", &inverted, &[1, 2, 4]);

    for iteration in 0..400 {
        let mut rng = Rng(42 + iteration);
        let full_keys = generate_full_keys(20000, &mut rng);
        let full = row_set(&full_keys);
        let (subset, expected) = generate_subset(&full_keys, &full, &mut rng);
        assert_eq!(subset.size(), expected.len() as u64);
        let inverted = full.invert(&subset).unwrap();
        assert_keys(&format!("iteration {iteration}"), &inverted, &expected);
    }
}

/// Port of `generateFullKeys`: either scattered single keys in a small space, or runs of up to
/// 1000 keys separated by gaps of up to 1000, until `max_size` keys exist.
fn generate_full_keys(max_size: usize, rng: &mut Rng) -> Vec<RowKey> {
    if rng.flip() {
        let mut set = vec![false; 1000];
        for _ in 0..rng.below(1000) {
            let at = rng.below(1000) as usize;
            set[at] = rng.flip();
        }
        // An empty full set leaves nothing to invert; keep one key.
        set[rng.below(1000) as usize] = true;
        bools_to_keys(&set)
    } else {
        let mut keys = Vec::new();
        let mut last = 0;
        while keys.len() < max_size {
            let skip = rng.below(1000);
            if skip == 0 {
                continue;
            }
            last += skip;
            let count = rng.below(1000);
            keys.extend(last..last + count);
            last += count;
        }
        keys
    }
}

/// Port of `generateSubset`: either each key with a random probability, or a random slice of
/// some of the ranges. Returns the subset and the positions of its keys in the full set.
fn generate_subset(full_keys: &[RowKey], full: &RowSet, rng: &mut Rng) -> (RowSet, Vec<RowKey>) {
    let mut positions: Vec<RowKey> = Vec::new();
    if rng.flip() {
        let density = rng.unit();
        for position in 0..full_keys.len() {
            if rng.unit() < density {
                positions.push(position as RowKey);
            }
        }
    } else {
        let threshold = rng.unit();
        let mut running = 0;
        for range in full.ranges() {
            let size = range.size() as i64;
            if rng.unit() < threshold {
                let start = rng.below(size);
                let len = rng.below(size - start);
                positions.extend(running + start..running + start + len);
            }
            running += size;
        }
    }
    let subset = row_set(
        &positions
            .iter()
            .map(|&p| full_keys[p as usize])
            .collect::<Vec<_>>(),
    );
    (subset, positions)
}

/// Port of `testIteration`.
#[test]
fn iteration_visits_keys_in_order() {
    for keys in KEYS {
        assert_keys("iteration", &row_set(keys), keys);
    }
}

/// Port of `testInsertion` (`testInsertionAlreadyThere` and `testInsertionNotThere`).
#[test]
fn inserting_single_keys() {
    for keys in KEYS {
        // Already there: nothing changes.
        let mut rows = row_set(keys);
        for &key in *keys {
            let before = rows.size();
            insert_key(&mut rows, key);
            assert_eq!(rows.size(), before);
        }
        assert_keys("already there", &rows, keys);

        // Not there: every gap below the last key, and two keys above it.
        let mut not_there: Vec<RowKey> = match keys.last() {
            Some(&last) => (0..last).filter(|k| !keys.contains(k)).collect(),
            None => vec![0, 1, 10],
        };
        if let Some(&last) = keys.last() {
            not_there.extend([last + 1, last + 2]);
        }
        for &key in &not_there {
            let mut rows = row_set(keys);
            insert_key(&mut rows, key);
            let mut expected = keys.to_vec();
            expected.push(key);
            expected.sort_unstable();
            assert_keys("one insert", &rows, &expected);
        }
        // Runs of `run` consecutive inserts into the same set.
        for run in 1..=not_there.len() {
            let mut rows = row_set(keys);
            let mut expected = keys.to_vec();
            for (step, &key) in not_there.iter().enumerate() {
                if step % run == 0 {
                    rows = row_set(keys);
                    expected = keys.to_vec();
                }
                insert_key(&mut rows, key);
                expected.push(key);
                expected.sort_unstable();
                assert_keys("run of inserts", &rows, &expected);
            }
        }
    }
}

/// Port of `testRangeByPos`.
#[test]
fn sub_set_by_position_range_for_every_start_and_end() {
    for keys in KEYS {
        let rows = row_set(keys);
        for i in 0..keys.len() + 2 {
            for j in i..keys.len() + 3 {
                let start = i.min(keys.len());
                let end = start.max(j.min(keys.len()));
                let sub = rows.sub_set_by_position_range(i as u64, j as u64);
                assert_keys(
                    &format!("{rows:?} positions {i}..{j}"),
                    &sub,
                    &keys[start..end],
                );
            }
        }
    }
}

/// Port of `testRangeByKey`, for bounds of zero and above.
///
/// The Java test also asks for ranges that start, or start and end, below zero (it begins two
/// below the first key). Those cases are left out: `RowSet::sub_set_by_key_range` panics when
/// `last` is negative, where Java returns the empty set. See `tasks/1.2-rowset-tests.md`.
#[test]
fn sub_set_by_key_range_for_every_first_and_last() {
    for keys in KEYS {
        let rows = row_set(keys);
        let (low, high) = match (keys.first(), keys.last()) {
            (Some(&first), Some(&last)) => (first - 2, last + 3),
            _ => (1, 3),
        };
        for first in low..high {
            for last in first.max(0)..high {
                let expected: Vec<RowKey> = keys
                    .iter()
                    .copied()
                    .filter(|k| (first..=last).contains(k))
                    .collect();
                let sub = rows.sub_set_by_key_range(first, last);
                assert_keys(&format!("{rows:?} keys {first}..={last}"), &sub, &expected);
            }
        }
    }
}

/// Port of `testMinusSimple`. The Java test ends with a subtraction of a long literal list of
/// ranges from `0..105339`; here that operand is generated: runs and gaps of 1 to 300 keys.
#[test]
fn minus_simple() {
    let keys = [1, 2, 3];
    let rows = row_set(&keys);
    assert_keys("minus empty", &rows.minus(&RowSet::empty()), &keys);
    assert_keys("minus self", &rows.minus(&rows), &[]);
    let sub_keys = [2, 5];
    let sub = row_set(&sub_keys);
    assert_keys("minus 2,5", &rows.minus(&sub), &[1, 3]);

    let all_keys: Vec<RowKey> = (0..105339).collect();
    let rows = row_set(&all_keys);
    assert_keys(
        "flat minus 2,5",
        &rows.minus(&sub),
        &minus_model(&all_keys, &sub_keys),
    );

    let mut rng = Rng(7);
    let mut sub_keys = Vec::new();
    let mut next = 0;
    while next < 110000 {
        let run = 1 + rng.below(300);
        sub_keys.extend(next..next + run);
        next += run + 1 + rng.below(300);
    }
    let sub = row_set(&sub_keys);
    assert_keys(
        "flat minus runs",
        &rows.minus(&sub),
        &minus_model(&all_keys, &sub_keys),
    );
}

/// Port of `testMinusRandom`: 16384 slots and 20 runs here, 131072 and 50 in Java.
#[test]
fn minus_random_keys() {
    let max_size = 16 * 1024;
    let mut rng = Rng(42);
    let mut full_set: Vec<bool> = (0..max_size).map(|_| rng.flip()).collect();
    let mut sub_set: Vec<bool> = (0..max_size).map(|_| rng.flip()).collect();
    for run in 0..20 {
        for slot in full_set.iter_mut().take(rng.below(max_size) as usize) {
            *slot ^= rng.flip();
        }
        for slot in sub_set.iter_mut().take(rng.below(max_size) as usize) {
            *slot ^= rng.flip();
        }
        let (full_keys, sub_keys) = (bools_to_keys(&full_set), bools_to_keys(&sub_set));
        let (rows, sub) = (row_set(&full_keys), row_set(&sub_keys));
        let context = format!("run {run}");
        assert_keys(&context, &rows, &full_keys);
        assert_keys(&context, &sub, &sub_keys);
        assert_keys(
            &context,
            &rows.minus(&sub),
            &minus_model(&full_keys, &sub_keys),
        );
    }
}

/// Port of `testMinusRandomRanges`: 16384 slots and 20 iterations here, 131072 and 50 in Java.
/// Java means to set whole ranges but writes one slot per range (it indexes with the range
/// counter); this port sets the ranges, which is the behavior the test is named for.
#[test]
fn minus_random_ranges() {
    let max_size = 16 * 1024;
    let mut rng = Rng(42);
    let mut full_set: Vec<bool> = (0..max_size).map(|_| rng.flip()).collect();
    let mut sub_set: Vec<bool> = (0..max_size).map(|_| rng.flip()).collect();
    for iteration in 0..20 {
        for set in [&mut full_set, &mut sub_set] {
            for _ in 0..rng.below(100) {
                let value = rng.flip();
                let start = rng.below(max_size);
                let end = start + rng.below(max_size - start);
                set[start as usize..end as usize].fill(value);
            }
        }
        let (full_keys, sub_keys) = (bools_to_keys(&full_set), bools_to_keys(&sub_set));
        let result = row_set(&full_keys).minus(&row_set(&sub_keys));
        assert_keys(
            &format!("iteration {iteration}"),
            &result,
            &minus_model(&full_keys, &sub_keys),
        );
    }
}

/// Port of `testMinusIndexOps`: range inserts and key-by-key removes on both operands before
/// each subtraction. 1024 slots and 40 iterations here, 65536 and 100 in Java.
#[test]
fn minus_after_incremental_inserts_and_removes() {
    let max_size = 1024;
    let mut rng = Rng(42);
    let mut full_set: Vec<bool> = (0..max_size).map(|_| rng.flip()).collect();
    let mut sub_set: Vec<bool> = (0..max_size).map(|_| rng.flip()).collect();
    let mut full_rows = row_set(&bools_to_keys(&full_set));
    let mut sub_rows = row_set(&bools_to_keys(&sub_set));
    for iteration in 0..40 {
        for (rows, set) in [
            (&mut full_rows, &mut full_set),
            (&mut sub_rows, &mut sub_set),
        ] {
            for _ in 0..rng.below(10) {
                let value = rng.flip();
                let start = rng.below(max_size);
                let end = start + rng.below(max_size - start);
                if value {
                    insert_range(rows, start, end);
                    assert!(contains_range(rows, start, end), "iteration {iteration}");
                } else {
                    for key in start..=end {
                        remove_key(rows, key);
                        assert_eq!(rows.find(key), None, "iteration {iteration} key {key}");
                    }
                }
                set[start as usize..=end as usize].fill(value);
            }
        }
        assert_keys(
            &format!("iteration {iteration}"),
            &full_rows.minus(&sub_rows),
            &minus_model(&bools_to_keys(&full_set), &bools_to_keys(&sub_set)),
        );
    }
}

/// Port of `testRandomInsertMinus`: 100 rounds here, 500 in Java.
#[test]
fn random_insert_remove_then_split_and_rejoin() {
    let mut rng = Rng(1);
    let mut check = RowSet::empty();
    let mut model: BTreeSet<RowKey> = BTreeSet::new();
    for round in 0..100 {
        let mut builder = RowSetBuilderRandom::new();
        let mut operand_keys = BTreeSet::new();
        for _ in 0..128 {
            let start = rng.below(1 << 24);
            let end = start + rng.below(20);
            builder.add_range(start, end);
            operand_keys.extend(start..=end);
        }
        let operand = builder.build();
        assert_canonical(&operand);
        if rng.flip() {
            check.insert(&operand);
            model.extend(operand_keys);
        } else {
            check.remove(&operand);
            model.retain(|k| !operand_keys.contains(k));
        }
        assert_canonical(&check);
        assert!(check.iter().eq(model.iter().copied()), "round {round}");

        let mut half = RowSetBuilderSequential::new();
        for key in check.iter() {
            if rng.flip() {
                half.append_key(key);
            }
        }
        let subset_a = half.build();
        let mut check_a = check.intersect(&subset_a);
        assert_eq!(check_a, subset_a, "round {round}");
        let check_b = check.minus(&subset_a);
        assert!(!check_a.overlaps(&check_b), "round {round}");
        check_a.insert(&check_b);
        assert_eq!(check_a, check, "round {round}");
    }
}

/// Port of `testRandomBuilder`, comparing the random builder with sorted distinct values. The
/// Java test also compares with an internal priority-queue builder and exercises a search
/// iterator; neither exists here.
#[test]
fn random_builder_matches_sorted_distinct_values() {
    let mut rng = Rng(0);
    for step in 0..1000 {
        let mut builder = RowSetBuilderRandom::new();
        let mut values = BTreeSet::new();
        for _ in 0..rng.below(10) {
            let value = rng.below(100000);
            let size = 1 + rng.below(1000);
            values.extend(value..value + size);
            builder.add_range(value, value + size - 1);
        }
        let values: Vec<RowKey> = values.into_iter().collect();
        assert_keys(&format!("step {step}"), &builder.build(), &values);
    }
}

/// Port of the result checked by `testOverlappingRanges`. The Java test also counts the ranges
/// held by an internal builder after each add.
#[test]
fn random_builder_merges_overlapping_and_adjacent_ranges() {
    let mut builder = RowSetBuilderRandom::new();
    builder.add_range(10, 100);
    builder.add_range(99, 110);
    builder.add_range(120, 130);
    builder.add_range(125, 135);
    builder.add_range(140, 150);
    builder.add_range(135, 139);
    builder.add_range(160, 165);
    builder.add_range(155, 160);
    builder.add_keys([171, 172, 173, 174, 170]);
    builder.add_range(180, 185);
    builder.add_range(178, 179);
    builder.add_range(225, 250);
    builder.add_range(220, 260);
    assert_eq!(
        builder.build().ranges(),
        &[
            RowRange::new(10, 110),
            RowRange::new(120, 150),
            RowRange::new(155, 165),
            RowRange::new(170, 174),
            RowRange::new(178, 185),
            RowRange::new(220, 260),
        ]
    );
}

/// Port of `testRandomBuilderEmptyAdds`.
#[test]
fn random_builder_collapses_interleaved_keys() {
    let mut builder = RowSetBuilderRandom::new();
    builder.add_keys((1..=32).map(|i| 2 * i));
    builder.add_keys((0..32).map(|i| 2 * i + 1));
    builder.add_keys((0..10 * 32).map(|i| 1000 + 2 * i));
    let rows = builder.build();
    assert_eq!(rows.size(), 12 * 32);
    assert_eq!(rows.ranges()[0], RowRange::new(1, 64));
}

/// The values shared by the builder tests and `testSubSetForPositions`.
const BUILDER_KEYS: [RowKey; 19] = [
    3,
    4,
    5,
    8,
    10,
    12,
    29,
    31,
    44,
    45,
    46,
    59,
    60,
    61,
    72,
    65537,
    65539,
    65536 * 3,
    65536 * 3 + 5,
];

/// The range bounds shared by `testBuilderAddRanges` and `testBuilderAppendRanges`.
const BUILDER_RANGES: [(RowKey, RowKey); 10] = [
    (3, 4),
    (5, 8),
    (10, 12),
    (29, 31),
    (44, 45),
    (46, 59),
    (60, 61),
    (72, 91),
    (65537, 65539),
    (65536 * 3, 65536 * 3 + 5),
];

/// Port of `testBuilderAddKeys`.
#[test]
fn random_builder_add_keys() {
    let mut builder = RowSetBuilderRandom::new();
    builder.add_key(27);
    builder.add_keys(BUILDER_KEYS);
    let rows = builder.build();
    assert_eq!(rows.size(), BUILDER_KEYS.len() as u64 + 1);
    for key in BUILDER_KEYS {
        assert!(rows.contains(key));
    }
    assert!(rows.contains(27));
}

/// Port of `testBuilderAppendKeys`.
#[test]
fn sequential_builder_append_keys() {
    let mut builder = RowSetBuilderSequential::new();
    builder.append_key(1);
    builder.append_keys(BUILDER_KEYS);
    let rows = builder.build();
    assert_eq!(rows.size(), BUILDER_KEYS.len() as u64 + 1);
    for key in BUILDER_KEYS {
        assert!(rows.contains(key));
    }
    assert!(rows.contains(1));
}

/// Ports of `testBuilderAddRanges` and `testBuilderAppendRanges`. The Java range iterator stops
/// one pair early, so the bulk form sees every range but the last; the port keeps that and
/// checks the keys exactly.
#[test]
fn builders_take_ranges_in_bulk() {
    let all: Vec<RowRange> = BUILDER_RANGES
        .iter()
        .map(|&(first, last)| RowRange::new(first, last))
        .collect();
    let bulk = &all[..all.len() - 1];
    let expected: Vec<RowKey> = bulk.iter().flat_map(|r| r.first()..=r.last()).collect();

    let mut random = RowSetBuilderRandom::new();
    random.add_ranges(bulk.iter().copied());
    assert_keys("random", &random.build(), &expected);

    let mut sequential = RowSetBuilderSequential::new();
    sequential.append_ranges(bulk.iter().copied());
    let appended = sequential.build();
    assert_keys("sequential", &appended, &expected);

    let mut one_by_one = RowSetBuilderRandom::new();
    for r in &all {
        one_by_one.add_range(r.first(), r.last());
    }
    let every = one_by_one.build();
    assert!(appended.is_subset_of(&every));
    assert_eq!(every.size(), appended.size() + all[all.len() - 1].size());
}

/// Port of `testSequentialSplitRange`, with the built keys checked instead of `validate()`.
#[test]
fn sequential_builder_mixes_keys_and_ranges() {
    let mut builder = RowSetBuilderSequential::new();
    builder.append_keys((0..64).map(|i| i * 2));
    builder.append_key(127);
    let mut expected: Vec<RowKey> = (0..64).map(|i| i * 2).collect();
    expected.push(127);
    assert_keys("keys then key", &builder.build(), &expected);

    let mut builder = RowSetBuilderSequential::new();
    builder.append_keys((0..63).map(|i| i * 4));
    builder.append_range(252, 253);
    builder.append_key(256);
    let mut expected: Vec<RowKey> = (0..63).map(|i| i * 4).collect();
    expected.extend([252, 253, 256]);
    assert_keys("keys, range, key", &builder.build(), &expected);
}

/// The Java sequential builder throws on an append that is not above the previous one
/// (`RspBitmapBuilderSequential#appendKey` with the default `sequentialBuilderCheck`).
#[test]
#[should_panic(expected = "out of order append")]
fn sequential_builder_rejects_a_repeated_key() {
    let mut builder = RowSetBuilderSequential::new();
    builder.append_range(5, 9);
    builder.append_key(9);
}

#[test]
#[should_panic(expected = "out of order append")]
fn sequential_builder_rejects_an_earlier_range() {
    let mut builder = RowSetBuilderSequential::new();
    builder.append_key(20);
    builder.append_range(3, 4);
}

/// Port of `testSequentialBuilderMergeSpans`. Java sizes the input from an internal capacity
/// constant; 2048 ranges per input is used here.
#[test]
fn sequential_builder_appends_whole_row_sets() {
    let n = 1024;
    let mut builder1 = RowSetBuilderSequential::new();
    for i in 0..=n * 2 {
        builder1.append_range(i * BLOCK_SIZE, i * BLOCK_SIZE + 1);
    }
    let rows1 = builder1.build();
    let mut builder2 = RowSetBuilderSequential::new();
    builder2.append_range(n * 2 * BLOCK_SIZE + 10, n * 2 * BLOCK_SIZE + 12);
    for i in n * 2 + 1..n * 4 {
        builder2.append_range(i * BLOCK_SIZE, i * BLOCK_SIZE + 1);
    }
    let rows2 = builder2.build();

    let mut builder3 = RowSetBuilderSequential::new();
    builder3.append_row_set(&rows1);
    builder3.append_row_set(&rows2);
    let rows3 = builder3.build();
    let rows4 = rows1.union(&rows2);
    assert_eq!(rows3, rows4);
    assert_eq!(rows4.size(), rows1.size() + rows2.size());

    let mut shifted = RowSetBuilderSequential::new();
    shifted.append_row_set_with_offset(&rows1, 7);
    assert_eq!(shifted.build(), rows1.shift(7).unwrap());
}

/// Port of `testArrayOfSmallIndices`.
#[test]
fn inserting_many_single_key_sets() {
    let mut rows = RowSet::empty();
    for key in 0..10 {
        let mut one = RowSet::empty();
        insert_key(&mut one, key);
        rows.insert(&one);
    }
    assert_eq!(rows.size(), 10);
    assert_eq!(rows.first_row_key(), Some(0));
    assert_eq!(rows.last_row_key(), Some(9));
}

/// Port of `testSubindexByPosRegreesion0`.
#[test]
fn position_ranges_split_a_large_set_without_overlap() {
    let mut rows = RowSet::empty();
    insert_range(&mut rows, 0, 95 * BLOCK_SIZE);
    let s1 = 0x1000000 * BLOCK_SIZE;
    insert_range(&mut rows, s1, s1 + 95 * 65536);
    let s2 = s1 + 0x5F * 65535;
    insert_range(&mut rows, s2, s2 + 1);
    let s3 = 0x2000000 * BLOCK_SIZE;
    insert_range(&mut rows, s3, s3 + 95 * BLOCK_SIZE);
    let s4 = s3 + 0x5F * BLOCK_SIZE;
    insert_range(&mut rows, s4, s4 + 1);

    let head = rows.sub_set_by_position_range(0, 9_999_999);
    let tail = rows.sub_set_by_position_range(9_999_999, rows.size());
    assert_canonical(&head);
    assert_canonical(&tail);
    assert_eq!(head.size(), 9_999_999);
    assert!(!head.overlaps(&tail));
    let mut joined = head.clone();
    joined.insert(&tail);
    assert_eq!(joined.size(), rows.size());
    assert!(rows.is_subset_of(&joined));
}

/// Port of `testContainsRange`.
#[test]
fn contains_range_around_every_range_boundary() {
    let mut rows = RowSet::empty();
    insert_range(&mut rows, 1, 3);
    insert_range(&mut rows, BLOCK_LAST, BLOCK_SIZE);
    insert_range(&mut rows, 2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 1);
    insert_range(&mut rows, 3 * BLOCK_SIZE - 2, 3 * BLOCK_SIZE - 1);
    insert_range(&mut rows, 6 * BLOCK_SIZE, 6 * BLOCK_SIZE + BLOCK_LAST);
    insert_range(&mut rows, 10 * BLOCK_SIZE, 20 * BLOCK_SIZE - 1);
    let last_key = rows.last_row_key().unwrap();
    let ranges = rows.ranges().to_vec();
    assert_eq!(ranges.len(), 6);
    for (i, range) in ranges.iter().enumerate() {
        let (start, end) = (range.first(), range.last());
        for ds in -1..=1 {
            for de in -1..=1 {
                let (s, e) = (start + ds, end + de);
                if s < 0 || e < s {
                    continue;
                }
                let expected = start <= s && e <= end;
                assert_eq!(contains_range(&rows, s, e), expected, "{s}..={e}");
            }
        }
        if i + 1 < ranges.len() {
            assert!(!contains_range(&rows, start, last_key));
        }
    }
}

/// Port of `testOverlapsRange`.
#[test]
fn overlaps_range_around_every_gap() {
    let mut rows = RowSet::empty();
    insert_range(&mut rows, 3, 5);
    insert_range(&mut rows, BLOCK_LAST, BLOCK_SIZE);
    insert_range(&mut rows, 2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 1);
    insert_range(&mut rows, 3 * BLOCK_SIZE - 2, 3 * BLOCK_SIZE - 1);
    insert_range(&mut rows, 6 * BLOCK_SIZE, 6 * BLOCK_SIZE + BLOCK_LAST);
    insert_range(&mut rows, 10 * BLOCK_SIZE, 20 * BLOCK_SIZE - 1);
    let last_key = rows.last_row_key().unwrap();
    let mut prev_end: Option<RowKey> = None;
    for range in rows.ranges().to_vec() {
        let (start, end) = (range.first(), range.last());
        let Some(before) = prev_end else {
            assert!(!overlaps_range(&rows, 0, start - 1));
            assert!(overlaps_range(&rows, 0, start));
            prev_end = Some(end);
            continue;
        };
        for ds in -1..=1 {
            for de in -1..=1 {
                let (rs, re) = (before + ds, start + de);
                if rs < 0 || re < rs {
                    continue;
                }
                let expected = rs <= before || re >= start;
                assert_eq!(overlaps_range(&rows, rs, re), expected, "{rs}..={re}");
            }
        }
        if end < last_key {
            assert!(overlaps_range(&rows, end + 1, last_key));
        }
        assert!(overlaps_range(&rows, 0, start - 1));
        prev_end = Some(end);
    }
    assert!(!overlaps_range(
        &rows,
        last_key + 1,
        last_key + 10000 * BLOCK_SIZE
    ));
}

/// Port of `testOneRangeIndexMinus`. The Java test only checks that neither form throws; the
/// port also checks that they agree.
#[test]
fn removing_the_inside_of_a_single_range() {
    let rows = RowSet::from_range(1000, 5000);
    let minus = rows.minus(&RowSet::from_range(1001, 4999));
    let mut removed = rows.clone();
    remove_range(&mut removed, 1001, 4999);
    assert_eq!(minus, removed);
    assert_keys("ends", &minus, &[1000, 5000]);
}

/// Port of `testSubsetOf`.
#[test]
fn a_prefix_is_a_subset() {
    let mut rows = RowSet::empty();
    insert_range(&mut rows, 0, 1);
    assert!(rows.is_subset_of(&RowSet::from_range(0, 2)));
}

/// Port of `testMinusRegression0`.
#[test]
fn minus_the_first_key_of_a_range() {
    let rows = RowSet::from_range(1073741843, 1073741860);
    let result = rows.minus(&row_set(&[1073741843]));
    assert_eq!(result.size(), (1073741860 - 1073741843) as u64);
    assert!(contains_range(&result, 1073741844, 1073741860));
}

/// Port of `rvs2ix`: a negative value closes a range opened by the value before it.
fn row_set_from_signed(values: &[i64]) -> RowSet {
    let mut rows = RowSet::empty();
    let mut pending: Option<RowKey> = None;
    for &v in values {
        if v < 0 {
            insert_range(&mut rows, pending.take().unwrap(), -v);
        } else {
            if let Some(key) = pending.replace(v) {
                insert_key(&mut rows, key);
            }
        }
    }
    if let Some(key) = pending {
        insert_key(&mut rows, key);
    }
    rows
}

/// Port of `testMinusRegression1`.
#[test]
fn minus_agrees_with_removing_the_intersection() {
    let rows0 = row_set_from_signed(&[
        10, -12, 14, 16, 18, 20, 22, 24, 26, 28, -29, 31, 33, -34, 36, 38, -39, 41, 43, 45, -47,
        49, 51, 53, 55, 57, 59, 61, 63, -64, 66, -67, 69, 71, -76, 78, -80, 82, -83, 85, -88, 90,
        -91, 93, -94, 96, 98, 100, 102, -110, 112, -113,
    ]);
    let rows1 = row_set_from_signed(&[24, 28, 31, 38, 51, 57, 59, 61, 74, 90, 93, 107, -108, 110]);
    let minus = rows0.minus(&rows1);
    let mut removed = rows0.clone();
    removed.remove(&rows0.intersect(&rows1));
    assert_eq!(removed, minus);
    assert_eq!(minus.size(), rows0.size() - 14);
    assert!(!minus.overlaps(&rows1));
}

/// Port of `testRemoveRegression1`.
#[test]
fn removing_two_keys_inside_a_long_range() {
    let mut rows = RowSet::from_ranges([RowRange::new(0, 65536), RowRange::new(131071, 393215)]);
    rows.remove(&row_set(&[195608, 196607]));
    let expected = RowSet::from_ranges([
        RowRange::new(0, 65536),
        RowRange::new(131071, 195607),
        RowRange::new(195609, 196606),
        RowRange::new(196608, 393215),
    ]);
    assert_eq!(rows, expected);
}

/// Port of `testRemoveRegression2`.
#[test]
fn removing_the_first_and_last_key() {
    let mut rows = RowSet::empty();
    insert_key(&mut rows, 0);
    insert_key(&mut rows, 65535);
    let mut evens = RowSetBuilderSequential::new();
    evens.append_keys((1..=32767).map(|i| 2 * i));
    rows.insert(&evens.build());
    rows.remove(&row_set(&[0, 65535]));
    let expected = rows.sub_set_by_key_range(1, 65534);
    assert_eq!(rows.size(), 32767);
    assert_eq!(expected, rows);
}

/// Port of `testSubsetOfMixed`.
#[test]
fn one_extra_key_breaks_subset_of() {
    let off0 = BLOCK_SIZE;
    let off1 = off0 + 2 * BLOCK_SIZE;
    let off2 = off1 + 4 * BLOCK_SIZE;
    let off3 = off2 + 3 * BLOCK_SIZE;
    let mut rows0 = RowSet::empty();
    insert_range(&mut rows0, off0 + 7, off0 + 9);
    insert_range(&mut rows0, off1 + 13, off1 + 15);
    insert_key(&mut rows0, off2 + 20);
    insert_range(&mut rows0, off2 + 33, off3 + 45);
    let rows1 = rows0.clone();
    assert!(rows1.is_subset_of(&rows0));

    let mut pending_start = 0;
    for range in rows0.ranges() {
        for v in [pending_start, range.first() - BLOCK_SIZE, range.first() - 1] {
            let mut rows2 = rows1.clone();
            insert_key(&mut rows2, v);
            assert!(!rows2.is_subset_of(&rows0), "v == {v}");
        }
        pending_start = range.last() + 1;
    }
    for v in [pending_start, pending_start + 3] {
        let mut rows2 = rows1.clone();
        insert_key(&mut rows2, v);
        assert!(!rows2.is_subset_of(&rows0), "v == {v}");
    }
}

/// Ports of `testRetainMixed` and `testRetainSrPrefix`.
#[test]
fn retain_with_a_disjoint_set_leaves_nothing() {
    let mut rows = RowSet::from_range(3, 4);
    rows.retain(&row_set(&[20]));
    assert!(rows.is_empty());

    let mut rows = row_set(&[20]);
    insert_key(&mut rows, 21);
    rows.retain(&RowSet::from_ranges([
        RowRange::new(5, 10),
        RowRange::new(25, 30),
    ]));
    assert!(rows.is_empty());
}

/// Port of `testInsertWithShift`. Java repeats it for each pair of internal representations.
#[test]
fn insert_with_shift_adds_the_moved_keys() {
    let (start0, end0) = (
        2 * BLOCK_SIZE + BLOCK_SIZE / 2,
        3 * BLOCK_SIZE + BLOCK_SIZE / 2,
    );
    let (start1, end1) = (
        4 * BLOCK_SIZE + BLOCK_SIZE / 2,
        5 * BLOCK_SIZE + BLOCK_SIZE / 2,
    );
    let rows0 = RowSet::from_range(start0, end0);
    let rows1 = RowSet::from_range(start1, end1);
    for shift in [2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 1] {
        let mut rows2 = rows0.clone();
        insert_with_shift(&mut rows2, shift, &rows1);
        assert_eq!(rows2.size(), (end1 - start1 + 1 + end0 - start0 + 1) as u64);
        assert!(contains_range(&rows2, start0, end0));
        assert!(contains_range(&rows2, start1 + shift, end1 + shift));
    }
}

/// Port of `testSubSetForPositions`.
#[test]
fn sub_set_for_positions_forward_and_reversed() {
    let vs = BUILDER_KEYS;
    let n = vs.len() as i64;
    let mut builder = RowSetBuilderSequential::new();
    builder.append_keys(vs);
    let rows = builder.build();

    assert_eq!(
        rows.sub_set_for_positions(&RowSet::empty(), false).size(),
        0
    );
    assert_eq!(rows.sub_set_for_positions(&RowSet::empty(), true).size(), 0);

    const EXTRA_RANGE: i64 = 10;
    // One range of positions, some of it past the end.
    for start in 0..n {
        for end in start..=n + EXTRA_RANGE {
            let coerced_end = end.min(n - 1);
            let positions = RowSet::from_range(start, end);
            let context = format!("single range {start}..={end}");

            let forward = rows.sub_set_for_positions(&positions, false);
            assert_keys(
                &context,
                &forward,
                &vs[start as usize..=coerced_end as usize],
            );

            let reversed = rows.sub_set_for_positions(&positions, true);
            let (low, high) = (n - 1 - coerced_end, n - 1 - start);
            assert_keys(&context, &reversed, &vs[low as usize..=high as usize]);
        }
    }

    // Several ranges of `range_size` positions, each followed by one skipped position.
    for range_size in 1..n + EXTRA_RANGE {
        let mut positions = RowSetBuilderSequential::new();
        let mut picked = Vec::new();
        let mut at = 0;
        while at < n + EXTRA_RANGE {
            positions.append_range(at, at + range_size - 1);
            picked.extend((at..at + range_size).filter(|&p| p < n));
            at += range_size + 1;
        }
        let positions = positions.build();
        let context = format!("range size {range_size}");

        let expected: Vec<RowKey> = picked.iter().map(|&p| vs[p as usize]).collect();
        let forward = rows.sub_set_for_positions(&positions, false);
        assert_keys(&context, &forward, &expected);

        let expected: Vec<RowKey> = picked
            .iter()
            .rev()
            .map(|&p| vs[(n - 1 - p) as usize])
            .collect();
        let reversed = rows.sub_set_for_positions(&positions, true);
        assert_keys(&context, &reversed, &expected);
    }
}

// ---------------------------------------------------------------------------------------------
// SingleRangeTest
// ---------------------------------------------------------------------------------------------

/// Port of `SingleRangeTest#testRemoveIndex`.
#[test]
fn single_range_remove_row_set() {
    let (start, end) = (10, 21);
    let single = || RowSet::from_range(start, end);

    let mut rows = single();
    rows.remove(&RowSet::from_range(start - 1, end + 1));
    assert!(rows.is_empty());

    let mut rows = single();
    rows.remove(&RowSet::from_range(start - 1, end - 1));
    assert_keys("keeps the end", &rows, &[end]);

    let mut rows = single();
    rows.remove(&RowSet::from_range(start + 1, end + 1));
    assert_keys("keeps the start", &rows, &[start]);

    let mut rows = single();
    rows.remove(&RowSet::from_range(start + 1, end - 1));
    assert_keys("keeps both ends", &rows, &[start, end]);

    let mut rows = single();
    let mut ends = RowSet::empty();
    insert_range(&mut ends, 10, 10);
    insert_range(&mut ends, 21, 21);
    rows.remove(&ends);
    assert_eq!(rows.size(), 10);
    assert!(contains_range(&rows, 11, 20));
}

/// Port of `SingleRangeTest#testRemoveKey`.
#[test]
fn single_range_remove_key() {
    let mut rows = RowSet::from_range(10, 21);
    remove_key(&mut rows, 9);
    assert_eq!(rows, RowSet::from_range(10, 21));
    remove_key(&mut rows, 22);
    assert_eq!(rows, RowSet::from_range(10, 21));
    remove_key(&mut rows, 10);
    assert_eq!(rows, RowSet::from_range(11, 21));
    remove_key(&mut rows, 21);
    assert_eq!(rows, RowSet::from_range(11, 20));
    remove_key(&mut rows, 12);
    assert_eq!(rows.size(), 9);
    assert!(contains_range(&rows, 11, 11));
    assert!(contains_range(&rows, 13, 20));
    assert!(!rows.contains(12));

    let mut one = RowSet::from_range(9, 9);
    remove_key(&mut one, 9);
    assert!(one.is_empty());
}

/// Port of `SingleRangeTest#testInsertIndexAndInsertRange`.
#[test]
fn single_range_insert_row_set_and_range() {
    for (start2, end2, expected) in [(21, 22, (10, 22)), (8, 9, (8, 20))] {
        let mut rows = RowSet::from_range(10, 20);
        rows.insert(&RowSet::from_range(start2, end2));
        assert_eq!(rows.ranges(), &[RowRange::new(expected.0, expected.1)]);
        let mut rows = RowSet::empty();
        insert_range(&mut rows, 10, 20);
        insert_range(&mut rows, start2, end2);
        assert_eq!(rows.ranges(), &[RowRange::new(expected.0, expected.1)]);
    }

    let (start, end) = (10, 20);
    let mut rows = RowSet::from_range(start, end);
    rows.insert(&RowSet::from_range(start - 3, start - 2));
    assert_eq!(rows.size(), (end - start + 1 + 2) as u64);
    assert_eq!(rows.first_row_key(), Some(start - 3));
    assert_eq!(rows.find_or_insertion(start - 1), Err(2)); // Java: -3
    assert_eq!(rows.last_row_key(), Some(end));

    let mut rows = RowSet::from_range(start, end);
    rows.insert(&RowSet::from_range(end + 2, end + 3));
    assert_eq!(rows.size(), (end - start + 1 + 2) as u64);
    assert_eq!(rows.first_row_key(), Some(start));
    assert_eq!(rows.find_or_insertion(end + 1), Err(11)); // Java: -12
    assert_eq!(rows.last_row_key(), Some(end + 3));
}

/// Port of `SingleRangeTest#testRemoveIndexAndRemoveRangeAndMinus`.
#[test]
fn single_range_remove_row_set_range_and_minus_agree() {
    for (start2, end2, expected) in [
        (7, 9, vec![(10, 20)]),
        (21, 22, vec![(10, 20)]),
        (8, 10, vec![(11, 20)]),
        (20, 22, vec![(10, 19)]),
        (10, 20, vec![]),
        (8, 11, vec![(12, 20)]),
        (15, 25, vec![(10, 14)]),
        (11, 19, vec![(10, 10), (20, 20)]),
    ] {
        let expected: Vec<RowRange> = expected
            .into_iter()
            .map(|(first, last)| RowRange::new(first, last))
            .collect();
        let other = RowSet::from_range(start2, end2);

        let mut rows = RowSet::from_range(10, 20);
        rows.remove(&other);
        assert_eq!(rows.ranges(), expected, "remove {start2}..={end2}");

        let mut rows = RowSet::empty();
        insert_range(&mut rows, 10, 20);
        remove_range(&mut rows, start2, end2);
        assert_eq!(rows.ranges(), expected, "remove range {start2}..={end2}");

        let minus = RowSet::from_range(10, 20).minus(&other);
        assert_eq!(minus.ranges(), expected, "minus {start2}..={end2}");
    }
}

/// Port of `SingleRangeTest#testSubindexByKey`.
#[test]
fn single_range_sub_set_by_key_range() {
    let rows = RowSet::from_range(10, 30);
    assert_eq!(rows.sub_set_by_key_range(5, 15), RowSet::from_range(10, 15));
    assert_eq!(
        rows.sub_set_by_key_range(15, 20),
        RowSet::from_range(15, 20)
    );
    assert_eq!(
        rows.sub_set_by_key_range(25, 35),
        RowSet::from_range(25, 30)
    );
    assert_eq!(
        rows.sub_set_by_key_range(11, 29),
        RowSet::from_range(11, 29)
    );
}

/// Port of `SingleRangeTest#testSubindexByPos`.
#[test]
fn single_range_sub_set_by_position_range() {
    let rows = RowSet::from_range(10, 30);
    assert_eq!(
        rows.sub_set_by_position_range(0, 6),
        RowSet::from_range(10, 15)
    );
    assert_eq!(
        rows.sub_set_by_position_range(5, 11),
        RowSet::from_range(15, 20)
    );
    assert_eq!(
        rows.sub_set_by_position_range(15, 36),
        RowSet::from_range(25, 30)
    );
    assert_eq!(
        rows.sub_set_by_position_range(1, 20),
        RowSet::from_range(11, 29)
    );
}

/// Port of `SingleRangeTest#testGetKeysForPositions`, through `sub_set_for_positions`.
#[test]
fn single_range_keys_for_positions() {
    let rows = RowSet::from_range(10, 30);
    let card = rows.size() as i64;
    let positions = RowSet::from_keys([0, 1, card - 2, card - 1]);
    assert_keys(
        "positions",
        &rows.sub_set_for_positions(&positions, false),
        &[10, 11, 29, 30],
    );
}

/// Port of `SingleRangeTest#testSaturation`.
#[test]
fn position_range_with_a_huge_end_does_not_overflow() {
    let first = 1073741821;
    let last = first + 6;
    let rows = RowSet::from_range(first, last);
    let result = rows.sub_set_by_position_range(5, i64::MAX as u64);
    assert_eq!(result, RowSet::from_range(last - 1, last));
    assert_eq!(rows.sub_set_by_position_range(5, u64::MAX), result);
}

// ---------------------------------------------------------------------------------------------
// RowSetFactoryUnionInsertTest
// ---------------------------------------------------------------------------------------------

/// Port of `RowSetFactoryUnionInsertTest#empty`.
#[test]
fn union_all_of_nothing_is_empty() {
    assert!(union_all([]).is_empty());
    assert!(union_all([&RowSet::empty()]).is_empty());
}

/// Ports of `RowSetFactoryUnionInsertTest#unionInsert`, `#overlappingUnionInsert` and
/// `#outOfOrder`.
#[test]
fn union_all_matches_pairwise_union() {
    let r1 = RowSet::from_range(0, 9);
    let r2 = RowSet::from_range(20, 29);
    let r3 = RowSet::from_keys([42]);
    assert_eq!(union_all([&r1]), r1);
    assert_eq!(union_all([&r1, &r2]), r1.union(&r2));
    let expected = r1.union(&r2).union(&r3);
    assert_eq!(union_all([&r1, &r2, &r3]), expected);
    assert_eq!(union_all([&r3, &r2, &r1]), expected);
    assert_eq!(expected.size(), 21);

    let overlapping = RowSet::from_range(5, 19);
    assert_eq!(union_all([&r1, &overlapping]), RowSet::from_range(0, 19));
}

// ---------------------------------------------------------------------------------------------
// MixedBuilderTest
// ---------------------------------------------------------------------------------------------

/// Port of `MixedBuilderTest#testMixedBuilder`: random keys below 2^31 through the random
/// builder, compared with a sorted set. 100000 keys here, 1048576 in Java, which compares two
/// internal builders with each other.
#[test]
fn random_builder_with_many_scattered_keys() {
    let mut rng = Rng(5);
    let mut builder = RowSetBuilderRandom::new();
    let mut model = BTreeSet::new();
    for _ in 0..100_000 {
        let key = rng.below(1 << 31);
        builder.add_key(key);
        model.insert(key);
    }
    let rows = builder.build();
    assert_canonical(&rows);
    assert_eq!(rows.size(), model.len() as u64);
    assert!(rows.iter().eq(model.iter().copied()));
}
