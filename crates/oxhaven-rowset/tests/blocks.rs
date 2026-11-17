//! Every operation of the row set is compared with the vector-of-ranges reference
//! (`oxhaven_testkit::RangeRowSet`) on the same keys, and every result is checked to be in the
//! one layout its keys allow.

use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, RowSetError, SpanCounts};
use oxhaven_testkit::{at_least_for, test_seeds, RangeRowSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const B: RowKey = 1 << 16;
/// The last block of the key space.
const TOP: RowKey = RowKey::MAX - (B - 1);

/// splitmix64, so failures reproduce from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn key_below(&mut self, bound: RowKey) -> RowKey {
        self.below(bound as u64) as RowKey
    }
}

/// How often each layout turned up, so the test can say it reached all of them.
type Seen = SpanCounts;

fn add(seen: &mut Seen, counts: SpanCounts) {
    seen.full += counts.full;
    seen.one += counts.one;
    seen.runs += counts.runs;
    seen.bits += counts.bits;
}

/// `actual` holds exactly the keys of `expected`, in the one layout for them.
fn assert_same(context: &str, actual: &RowSet, expected: &RangeRowSet, seen: &mut Seen) {
    add(seen, actual.check_layout());
    let ranges: Vec<RowRange> = actual.ranges().collect();
    assert_eq!(ranges, expected.ranges(), "{context}");
    assert_eq!(actual.size(), expected.size(), "{context}: size");
    assert_eq!(actual.range_count(), ranges.len(), "{context}: range count");
    let single = match ranges[..] {
        [range] => Some(range),
        _ => None,
    };
    assert_eq!(actual.as_single_range(), single, "{context}: single range");
    assert_eq!(
        actual,
        &RowSet::from(expected),
        "{context}: same keys, different layout"
    );
}

/// Blocks at which the generated sets put their keys, so that two sets often meet.
const BASES: &[RowKey] = &[
    0,
    B,
    2 * B,
    3 * B,
    7 * B,
    1 << 40,
    (1 << 40) + B,
    TOP - 3 * B,
];

/// A set of keys with every layout in it: single keys, runs across block boundaries, blocks
/// dense enough to be bitmaps, whole blocks, and blocks with a few holes.
fn ranges(rng: &mut Rng) -> Vec<RowRange> {
    let mut out = Vec::new();
    for _ in 0..rng.below(6) {
        let base = BASES[rng.below(BASES.len() as u64) as usize];
        match rng.below(8) {
            0 => {
                for _ in 0..rng.below(40) {
                    let key = base + rng.key_below(2 * B);
                    out.push(RowRange::new(key, key));
                }
            }
            1 => {
                let mut next = base + rng.key_below(B);
                for _ in 0..rng.below(60) {
                    let first = next + 1 + rng.key_below(500);
                    let last = first + rng.key_below(300);
                    out.push(RowRange::new(first, last));
                    next = last + 1;
                }
            }
            2 => {
                // Dense and random: thousands of runs in a block.
                let one_in = 2 + rng.below(3);
                let first = base + rng.key_below(B / 2);
                for key in first..first + B / 2 + rng.key_below(B) {
                    if rng.below(one_in) == 0 {
                        out.push(RowRange::new(key, key));
                    }
                }
            }
            3 => {
                let first = (base + rng.key_below(3) - 1).max(0);
                let last = base + (1 + rng.key_below(3)) * B - 2 + rng.key_below(3);
                out.push(RowRange::new(first, last));
            }
            4 => {
                // A whole block but for a few keys.
                let mut first = base;
                for _ in 0..rng.below(4) {
                    let hole = first + rng.key_below(base + B - first);
                    if hole > first {
                        out.push(RowRange::new(first, hole - 1));
                    }
                    first = hole + 1;
                }
                if first < base + B {
                    out.push(RowRange::new(first, base + B - 1));
                }
            }
            5 => {
                // Around a block boundary.
                for key in [base + B - 2, base + B - 1, base + B, base + B + 1] {
                    if rng.below(2) == 0 {
                        out.push(RowRange::new(key, key));
                    }
                }
            }
            6 => {
                // Whole blocks exactly, so that two sets have whole blocks side by side.
                out.push(RowRange::new(base, base + (1 + rng.key_below(2)) * B - 1));
            }
            _ => {
                let first = base + rng.key_below(B);
                out.push(RowRange::new(first, first + rng.key_below(B)));
            }
        }
    }
    out
}

/// Keys worth probing in `rows`: the ends of some ranges and their neighbours, block
/// boundaries, and random keys.
fn probes(rows: &RangeRowSet, rng: &mut Rng) -> Vec<RowKey> {
    let mut keys = vec![-1, 0, B - 1, B, RowKey::MAX];
    let ranges = rows.ranges();
    for _ in 0..12.min(ranges.len()) {
        let r = ranges[rng.below(ranges.len() as u64) as usize];
        keys.extend([
            r.first() - 1,
            r.first(),
            r.last(),
            r.last().saturating_add(1),
        ]);
        keys.push(r.first() + rng.key_below(r.size() as RowKey));
    }
    for _ in 0..8 {
        keys.push(BASES[rng.below(BASES.len() as u64) as usize] + rng.key_below(3 * B));
    }
    keys
}

fn hash_of(rows: &RowSet) -> u64 {
    let mut hasher = DefaultHasher::new();
    rows.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn every_operation_matches_the_reference_row_set() {
    let seeds = 0..300;
    let mut seen = Seen::default();
    for seed in test_seeds(seeds.clone()) {
        let mut rng = Rng(seed);
        let (in_a, in_b) = (ranges(&mut rng), ranges(&mut rng));
        let (ra, rb) = (
            RangeRowSet::from_ranges(in_a.iter().copied()),
            RangeRowSet::from_ranges(in_b.iter().copied()),
        );
        let (a, b) = (
            RowSet::from_ranges(in_a.iter().copied()),
            RowSet::from_ranges(in_b.iter().copied()),
        );
        let ctx = |what: &str| format!("seed {seed}: {what}");
        assert_same(&ctx("from_ranges a"), &a, &ra, &mut seen);
        assert_same(&ctx("from_ranges b"), &b, &rb, &mut seen);
        assert_eq!(
            RangeRowSet::from(&a),
            ra,
            "seed {seed}: back to the reference type"
        );
        assert_eq!(format!("{a:?}"), format!("{ra:?}"), "seed {seed}: Debug");
        assert_eq!(a.is_empty(), ra.is_empty());
        assert_eq!(a.first_row_key(), ra.first_row_key(), "seed {seed}");
        assert_eq!(a.last_row_key(), ra.last_row_key(), "seed {seed}");
        assert!(
            a.iter().take(3000).eq(ra.iter().take(3000)),
            "seed {seed}: iter"
        );

        let (union, inter, minus) = (ra.union(&rb), ra.intersect(&rb), ra.minus(&rb));
        assert_same(&ctx("union"), &a.union(&b), &union, &mut seen);
        assert_same(&ctx("intersect"), &a.intersect(&b), &inter, &mut seen);
        assert_same(&ctx("minus"), &a.minus(&b), &minus, &mut seen);
        assert_same(
            &ctx("minus, swapped"),
            &b.minus(&a),
            &rb.minus(&ra),
            &mut seen,
        );
        assert_eq!(a.overlaps(&b), ra.overlaps(&rb), "seed {seed}: overlaps");
        assert_eq!(a.is_subset_of(&b), ra.is_subset_of(&rb), "seed {seed}");
        assert!(a.intersect(&b).is_subset_of(&a), "seed {seed}");
        assert_eq!(hash_of(&a.union(&b)), hash_of(&b.union(&a)), "seed {seed}");

        // The in-place forms, on a copy: the original must not change.
        let mut x = a.clone();
        x.insert(&b);
        assert_same(&ctx("insert"), &x, &union, &mut seen);
        let mut x = a.clone();
        x.remove(&b);
        assert_same(&ctx("remove"), &x, &minus, &mut seen);
        let mut x = a.clone();
        x.retain(&b);
        assert_same(&ctx("retain"), &x, &inter, &mut seen);
        assert_same(&ctx("a after changing copies"), &a, &ra, &mut seen);

        // Small changes to a large set, as an update cycle makes them.
        let (mut x, mut rx) = (a.clone(), ra.clone());
        for step in 0..6 {
            let key = BASES[rng.below(BASES.len() as u64) as usize] + rng.key_below(2 * B);
            let last = key.saturating_add(rng.key_below(4) * rng.key_below(B));
            let (piece, rpiece) = (
                RowSet::from_range(key, last),
                RangeRowSet::from_range(key, last),
            );
            if step % 2 == 0 {
                x.insert(&piece);
                rx.insert(&rpiece);
            } else {
                x.remove(&piece);
                rx.remove(&rpiece);
            }
            assert_same(&ctx("small change"), &x, &rx, &mut seen);
        }

        for key in probes(&ra, &mut rng) {
            assert_eq!(a.contains(key), ra.contains(key), "seed {seed}: {key}");
            assert_eq!(a.find(key), ra.find(key), "seed {seed}: find {key}");
            assert_eq!(
                a.find_or_insertion(key),
                ra.find_or_insertion(key),
                "seed {seed}: find_or_insertion {key}"
            );
        }
        let size = ra.size();
        let mut positions = vec![0, 1, size.saturating_sub(1), size, size + 1];
        positions.extend((0..20).map(|_| rng.below(size + 2)));
        for &pos in &positions {
            assert_eq!(a.get(pos), ra.get(pos), "seed {seed}: get {pos}");
        }

        for _ in 0..6 {
            let (start, end) = (rng.below(size + 3), rng.below(size + 3));
            assert_same(
                &ctx(&format!("positions {start}..{end}")),
                &a.sub_set_by_position_range(start, end),
                &ra.sub_set_by_position_range(start, end),
                &mut seen,
            );
            let keys = probes(&ra, &mut rng);
            let (first, last) = (
                keys[5 + rng.below(4) as usize],
                keys[9 + rng.below(4) as usize],
            );
            assert_eq!(
                a.overlaps_range(first, last),
                !ra.sub_set_by_key_range(first, last).is_empty(),
                "seed {seed}: overlaps_range {first}..={last}"
            );
            assert_same(
                &ctx(&format!("keys {first}..={last}")),
                &a.sub_set_by_key_range(first, last),
                &ra.sub_set_by_key_range(first, last),
                &mut seen,
            );
        }
        assert!(a.sub_set_by_key_range(-5, -1).is_empty());

        let picks: Vec<RowRange> = (0..rng.below(8))
            .map(|_| {
                let first = rng.below(size + 2) as RowKey;
                RowRange::new(first, first + rng.key_below(B + 5))
            })
            .collect();
        let (pb, pr) = (
            RowSet::from_ranges(picks.iter().copied()),
            RangeRowSet::from_ranges(picks.iter().copied()),
        );
        for reversed in [false, true] {
            assert_same(
                &ctx(&format!("for positions {pr:?}, reversed {reversed}")),
                &a.sub_set_for_positions(&pb, reversed),
                &ra.sub_set_for_positions(&pr, reversed),
                &mut seen,
            );
        }

        let subset = a.intersect(&b);
        match (a.invert(&subset), ra.invert(&inter)) {
            (Ok(actual), Ok(expected)) => {
                assert_same(&ctx("invert"), &actual, &expected, &mut seen);
            }
            other => panic!("seed {seed}: invert of a subset gave {other:?}"),
        }
        assert_eq!(
            a.invert(&b).is_err(),
            ra.invert(&rb).is_err(),
            "seed {seed}: invert of a set that may not be a subset"
        );

        let first = ra.first_row_key().unwrap_or(0);
        let room = RowKey::MAX - ra.last_row_key().unwrap_or(0);
        for delta in [1, B, 3 * B, 12345, rng.key_below(1 << 30)] {
            for delta in [delta.min(room), -delta.min(first)] {
                match (a.shift(delta), ra.shift(delta)) {
                    (Ok(actual), Ok(expected)) => {
                        assert_same(
                            &ctx(&format!("shift {delta}")),
                            &actual,
                            &expected,
                            &mut seen,
                        );
                    }
                    other => panic!("seed {seed}: shift by {delta} gave {other:?}"),
                }
            }
        }
        if !ra.is_empty() {
            assert_eq!(a.shift(-first - 1), Err(RowSetError::NegativeKey));
        }

        // Keys in any order, with repeats.
        let mut keys: Vec<RowKey> = ra.iter().take(500).chain(rb.iter().take(500)).collect();
        for i in (1..keys.len()).rev() {
            keys.swap(i, rng.below(i as u64 + 1) as usize);
        }
        assert_same(
            &ctx("from_keys"),
            &keys.iter().copied().collect::<RowSet>(),
            &RangeRowSet::from_keys(keys.iter().copied()),
            &mut seen,
        );
    }
    // The comparison is only worth something if the inputs reached every layout.
    for (layout, count) in [
        ("whole blocks", seen.full),
        ("one run", seen.one),
        ("run lists", seen.runs),
        ("bitmaps", seen.bits),
    ] {
        assert!(
            count >= at_least_for(2500, seeds.clone()),
            "only {count} spans of {layout}"
        );
    }
}
