//! Every operation of `BlockRowSet` is compared with the contract `RowSet` on the same keys,
//! and every result is checked to be in the one layout its keys allow.

use super::container::{count_runs, Body, Run, BLOCK_KEYS, MAX_RUNS};
use super::{BlockRowSet, BLOCK_BITS};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, RowSetError};
use oxhaven_testkit::{at_least_for, test_seeds};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const B: RowKey = BLOCK_KEYS as RowKey;
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

/// How often each layout turned up, so the tests can say they reached all of them.
#[derive(Default)]
struct Seen {
    full: u64,
    one: u64,
    runs: u64,
    bits: u64,
}

/// Panics unless `rows` is in the layout the module documents.
fn validate(rows: &BlockRowSet, seen: &mut Seen) {
    assert_eq!(rows.spans.len(), rows.ends.len());
    let mut total = 0;
    for (i, span) in rows.spans.iter().enumerate() {
        let keys = match &span.body {
            Body::Full(blocks) => {
                seen.full += 1;
                assert!(*blocks >= 1);
                blocks * BLOCK_KEYS
            }
            Body::One(run) => {
                seen.one += 1;
                assert!(run.first <= run.last);
                assert_ne!(*run, Run::WHOLE, "a whole block stored as a run");
                run.len()
            }
            Body::Runs(runs) => {
                seen.runs += 1;
                assert!((2..=MAX_RUNS).contains(&runs.len()), "{} runs", runs.len());
                for w in runs.windows(2) {
                    assert!(u32::from(w[0].last) + 1 < u32::from(w[1].first));
                }
                runs.iter().map(|r| r.len()).sum()
            }
            Body::Bits(words) => {
                seen.bits += 1;
                assert!(count_runs(words) > MAX_RUNS, "a bitmap with few runs");
                words.iter().map(|w| u64::from(w.count_ones())).sum()
            }
        };
        assert!(span.end() <= 1 << (63 - BLOCK_BITS), "beyond the key space");
        total += keys;
        assert_eq!(rows.ends[i], total, "key count of span {i}");
        if let Some(next) = rows.spans.get(i + 1) {
            assert!(span.end() <= next.block, "spans {i} and the next overlap");
            let both_full =
                matches!(span.body, Body::Full(_)) && matches!(next.body, Body::Full(_));
            assert!(!(both_full && span.end() == next.block), "unjoined blocks");
        }
    }
}

/// `actual` holds exactly the keys of `expected`, in the one layout for them.
fn assert_same(context: &str, actual: &BlockRowSet, expected: &RowSet, seen: &mut Seen) {
    validate(actual, seen);
    let ranges: Vec<RowRange> = actual.ranges().collect();
    assert_eq!(ranges, expected.ranges(), "{context}");
    assert_eq!(actual.size(), expected.size(), "{context}: size");
    assert_eq!(
        actual,
        &BlockRowSet::from(expected),
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
        match rng.below(7) {
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
fn probes(rows: &RowSet, rng: &mut Rng) -> Vec<RowKey> {
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

fn hash_of(rows: &BlockRowSet) -> u64 {
    let mut hasher = DefaultHasher::new();
    rows.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn every_operation_matches_the_contract_row_set() {
    let seeds = 0..300;
    let mut seen = Seen::default();
    for seed in test_seeds(seeds.clone()) {
        let mut rng = Rng(seed);
        let (in_a, in_b) = (ranges(&mut rng), ranges(&mut rng));
        let (ra, rb) = (
            RowSet::from_ranges(in_a.iter().copied()),
            RowSet::from_ranges(in_b.iter().copied()),
        );
        let (a, b) = (
            BlockRowSet::from_ranges(in_a.iter().copied()),
            BlockRowSet::from_ranges(in_b.iter().copied()),
        );
        let ctx = |what: &str| format!("seed {seed}: {what}");
        assert_same(&ctx("from_ranges a"), &a, &ra, &mut seen);
        assert_same(&ctx("from_ranges b"), &b, &rb, &mut seen);
        assert_eq!(
            RowSet::from(&a),
            ra,
            "seed {seed}: back to the contract type"
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
                BlockRowSet::from_range(key, last),
                RowSet::from_range(key, last),
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
            BlockRowSet::from_ranges(picks.iter().copied()),
            RowSet::from_ranges(picks.iter().copied()),
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
            &keys.iter().copied().collect::<BlockRowSet>(),
            &RowSet::from_keys(keys.iter().copied()),
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
            count >= at_least_for(3000, seeds.clone()),
            "only {count} spans of {layout}"
        );
    }
}

#[test]
fn whole_blocks_are_one_span_however_many() {
    let rows = BlockRowSet::flat(1_000_000_000_000);
    assert_eq!(rows.size(), 1_000_000_000_000);
    // 10^12 keys are 15258789 whole blocks and 4096 keys of the next one.
    assert_eq!(rows.spans.len(), 2);
    assert_eq!(rows.spans[0].body, Body::Full(15_258_789));
    assert_eq!(rows.get(999_999_999_999), Some(999_999_999_999));
    assert_eq!(rows.find(123_456_789_012), Some(123_456_789_012));
    assert_eq!(
        rows.heap_bytes(),
        rows.spans.capacity() * 32 + rows.ends.capacity() * 8
    );

    // Removing one key from the middle splits the span and rebuilds one block only.
    let mut holed = rows.clone();
    holed.remove(&BlockRowSet::from_range(500_000_000_000, 500_000_000_000));
    assert_eq!(holed.size(), 999_999_999_999);
    assert_eq!(holed.spans.len(), 4);
    assert!(!holed.contains(500_000_000_000));
    assert_eq!(holed.get(500_000_000_000), Some(500_000_000_001));
    // Putting it back gives the original layout.
    holed.insert(&BlockRowSet::from_range(500_000_000_000, 500_000_000_000));
    assert_eq!(holed, rows);
}

#[test]
fn a_block_is_a_bitmap_exactly_when_it_has_more_than_2047_runs() {
    // Every other key: `runs` runs of one key each.
    let alternate = |runs: RowKey| BlockRowSet::from_keys((0..runs).map(|i| 2 * i));
    let at_limit = alternate(MAX_RUNS as RowKey);
    assert!(matches!(at_limit.spans[0].body, Body::Runs(_)));
    let over = alternate(MAX_RUNS as RowKey + 1);
    assert!(matches!(over.spans[0].body, Body::Bits(_)));
    assert_eq!(over.size(), MAX_RUNS as u64 + 1);

    // One more key takes the run list over the limit, and taking it away brings it back.
    let extra = BlockRowSet::from_range(2 * MAX_RUNS as RowKey, 2 * MAX_RUNS as RowKey);
    let mut rows = at_limit.clone();
    rows.insert(&extra);
    assert_eq!(rows, over);
    rows.remove(&extra);
    assert_eq!(rows, at_limit);
    assert!(matches!(rows.spans[0].body, Body::Runs(_)));

    // Filling the gaps of a bitmap leaves one run, and a whole block when it is all of it.
    let odd = BlockRowSet::from_keys((0..=MAX_RUNS as RowKey).map(|i| 2 * i + 1));
    assert_eq!(
        over.union(&odd).spans[0].body,
        Body::One(Run {
            first: 0,
            last: 2 * MAX_RUNS as u16 + 1
        })
    );
    let half = BlockRowSet::from_keys((0..B / 2).map(|i| 2 * i));
    let other_half = BlockRowSet::from_keys((0..B / 2).map(|i| 2 * i + 1));
    assert_eq!(half.union(&other_half).spans[0].body, Body::Full(1));
    assert!(half.intersect(&other_half).is_empty());
    assert_eq!(half.union(&other_half).minus(&half), other_half);
}

#[test]
fn a_copy_shares_blocks_and_changes_leave_the_original() {
    let rows = BlockRowSet::from_keys((0..3 * B).filter(|k| k % 3 != 0));
    let before: Vec<RowRange> = rows.ranges().collect();
    let mut copy = rows.clone();
    let Body::Bits(original) = &rows.spans[2].body else {
        panic!("two of three keys in a block is a bitmap");
    };
    let Body::Bits(shared) = &copy.spans[2].body else {
        panic!("a copy has the same layout");
    };
    assert!(std::sync::Arc::ptr_eq(original, shared));

    // A change in the first block leaves the third block shared.
    copy.insert(&BlockRowSet::from_range(0, 0));
    let Body::Bits(shared) = &copy.spans[2].body else {
        panic!("the third block did not change");
    };
    assert!(std::sync::Arc::ptr_eq(original, shared));
    assert_eq!(copy.size(), rows.size() + 1);
    assert_eq!(rows.ranges().collect::<Vec<_>>(), before);
}

#[test]
fn the_last_key_of_the_key_space() {
    let rows = BlockRowSet::from_range(RowKey::MAX - B - 5, RowKey::MAX);
    assert_eq!(rows.size(), B as u64 + 6);
    assert_eq!(rows.last_row_key(), Some(RowKey::MAX));
    assert!(rows.contains(RowKey::MAX));
    assert_eq!(rows.find(RowKey::MAX), Some(B as u64 + 5));
    assert_eq!(rows.get(B as u64 + 5), Some(RowKey::MAX));
    assert_eq!(
        rows.ranges().collect::<Vec<_>>(),
        [RowRange::new(RowKey::MAX - B - 5, RowKey::MAX)]
    );
    assert_eq!(
        rows.minus(&BlockRowSet::from_range(0, RowKey::MAX - 1)),
        BlockRowSet::from_range(RowKey::MAX, RowKey::MAX)
    );
    assert_eq!(BlockRowSet::from_range(0, RowKey::MAX).size(), 1 << 63);
}

#[test]
fn intersecting_skips_the_spans_between_the_other_sets_blocks() {
    // A million scattered blocks against two keys far apart: the walk must not visit them all.
    let many = BlockRowSet::from_keys((0..1_000_000).map(|i| i * 3 * B + 17));
    let two = BlockRowSet::from_keys([17, 999_999 * 3 * B + 17]);
    let started = std::time::Instant::now();
    for _ in 0..2000 {
        assert_eq!(many.intersect(&two), two);
    }
    // A walk of every span would be 2000 x 1M steps; with the skip it is 2000 x 40.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}
