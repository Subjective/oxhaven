//! The row set against a sorted-set model, and the layout each set of keys must have. The
//! comparison of every operation with the vector-of-ranges reference is in
//! `crates/oxhaven-rowset/tests/blocks.rs`.

use super::container::{Body, Run, BLOCK_KEYS, MAX_RUNS};
use super::{RowKey, RowRange, RowSet, RowSetError};
use std::collections::BTreeSet;

const B: RowKey = BLOCK_KEYS as RowKey;

/// Small deterministic generator, so failures reproduce without a dependency.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }

    fn key_set(&mut self) -> BTreeSet<RowKey> {
        let mut s = BTreeSet::new();
        for _ in 0..self.next(6) {
            let first = self.next(60) as RowKey;
            let len = self.next(8) as RowKey;
            s.extend(first..=first + len);
        }
        s
    }
}

fn rs(model: &BTreeSet<RowKey>) -> RowSet {
    RowSet::from_keys(model.iter().copied())
}

fn keys(set: &RowSet) -> BTreeSet<RowKey> {
    set.iter().collect()
}

fn assert_canonical(set: &RowSet) {
    set.check_layout();
    let ranges: Vec<RowRange> = set.ranges().collect();
    assert_eq!(ranges.len(), set.range_count());
    for w in ranges.windows(2) {
        assert!(w[0].last() + 1 < w[1].first(), "not canonical: {set:?}");
    }
}

#[test]
fn matches_sorted_set_model() {
    let mut g = Lcg(42);
    for _ in 0..2000 {
        let (a, b) = (g.key_set(), g.key_set());
        let (ra, rb) = (rs(&a), rs(&b));
        assert_canonical(&ra);
        assert_eq!(keys(&ra), a);
        assert_eq!(ra.size(), a.len() as u64);
        assert_eq!(ra.first_row_key(), a.first().copied());
        assert_eq!(ra.last_row_key(), a.last().copied());

        let union = ra.union(&rb);
        assert_canonical(&union);
        assert_eq!(keys(&union), &a | &b, "{ra:?} union {rb:?}");
        let inter = ra.intersect(&rb);
        assert_canonical(&inter);
        assert_eq!(keys(&inter), &a & &b, "{ra:?} intersect {rb:?}");
        let minus = ra.minus(&rb);
        assert_canonical(&minus);
        assert_eq!(keys(&minus), &a - &b, "{ra:?} minus {rb:?}");
        assert_eq!(ra.overlaps(&rb), !a.is_disjoint(&b));
        assert_eq!(ra.is_subset_of(&rb), a.is_subset(&b));

        let sorted: Vec<RowKey> = a.iter().copied().collect();
        for probe in 0..80 {
            assert_eq!(ra.contains(probe), a.contains(&probe));
            assert_eq!(
                ra.find(probe),
                sorted.binary_search(&probe).ok().map(|p| p as u64)
            );
            assert_eq!(ra.get(probe as u64), sorted.get(probe as usize).copied());
            assert_eq!(
                ra.find_or_insertion(probe),
                sorted
                    .binary_search(&probe)
                    .map(|p| p as u64)
                    .map_err(|p| p as u64)
            );
        }

        let start = g.next(12);
        let end = start + g.next(12);
        let by_pos: BTreeSet<RowKey> = sorted
            .iter()
            .copied()
            .skip(start as usize)
            .take((end - start) as usize)
            .collect();
        let sub = ra.sub_set_by_position_range(start, end);
        assert_canonical(&sub);
        assert_eq!(sub, rs(&by_pos), "{ra:?} positions {start}..{end}");
        assert!(ra.sub_set_by_position_range(end, start).is_empty() || start == end);

        let positions = g.key_set();
        let picked: BTreeSet<RowKey> = positions
            .iter()
            .filter_map(|p| sorted.get(*p as usize).copied())
            .collect();
        assert_eq!(
            ra.sub_set_for_positions(&rs(&positions), false),
            rs(&picked)
        );
        let picked_rev: BTreeSet<RowKey> = positions
            .iter()
            .filter_map(|p| sorted.iter().rev().nth(*p as usize).copied())
            .collect();
        assert_eq!(
            ra.sub_set_for_positions(&rs(&positions), true),
            rs(&picked_rev)
        );

        let lo = g.next(70) as RowKey;
        let hi = lo + g.next(20) as RowKey - 5;
        let by_key: BTreeSet<RowKey> = if lo <= hi {
            a.range(lo..=hi).copied().collect()
        } else {
            BTreeSet::new()
        };
        assert_eq!(keys(&ra.sub_set_by_key_range(lo, hi)), by_key);
        assert_eq!(ra.overlaps_range(lo, hi), !by_key.is_empty());

        let sub = &a & &b;
        let expected: BTreeSet<RowKey> = sub
            .iter()
            .map(|k| sorted.binary_search(k).unwrap() as RowKey)
            .collect();
        assert_eq!(keys(&ra.invert(&rs(&sub)).unwrap()), expected);
        if !b.is_subset(&a) {
            assert_eq!(ra.invert(&rb), Err(RowSetError::NotASubset));
        }
    }
}

#[test]
fn empty_or_inverted_position_range_is_the_empty_set() {
    let set = RowSet::from_range(0, 9);
    assert_eq!(set.sub_set_by_position_range(2, 2), RowSet::empty());
    assert_eq!(set.sub_set_by_position_range(5, 3), RowSet::empty());
    assert_eq!(set.sub_set_by_position_range(20, 30), RowSet::empty());
}

#[test]
fn shift_moves_every_key_and_rejects_negative_results() {
    let set = RowSet::from_keys([3, 4, 9]);
    assert_eq!(set.shift(2).unwrap(), RowSet::from_keys([5, 6, 11]));
    assert_eq!(set.shift(-3).unwrap(), RowSet::from_keys([0, 1, 6]));
    assert_eq!(set.shift(-4), Err(RowSetError::NegativeKey));
    assert_eq!(RowSet::empty().shift(-4).unwrap(), RowSet::empty());
}

#[test]
fn adjacent_ranges_merge_and_flat_is_zero_based() {
    let set = RowSet::from_ranges([RowRange::new(5, 6), RowRange::new(0, 4)]);
    assert_eq!(set.ranges().collect::<Vec<_>>(), [RowRange::new(0, 6)]);
    assert_eq!(RowSet::flat(7), set);
    assert!(RowSet::flat(0).is_empty());
}

#[test]
fn in_place_operations_match_their_pure_forms() {
    let (a, b) = (RowSet::from_range(0, 9), RowSet::from_range(5, 14));
    let mut x = a.clone();
    x.insert(&b);
    assert_eq!(x, a.union(&b));
    let mut y = a.clone();
    y.remove(&b);
    assert_eq!(y, a.minus(&b));
    let mut z = a.clone();
    z.retain(&b);
    assert_eq!(z, a.intersect(&b));
}

#[test]
fn sub_set_by_key_range_with_negative_bounds() {
    let rs = RowSet::from_keys([0, 1, 2]);
    assert!(rs.sub_set_by_key_range(-2, -1).is_empty());
    assert!(rs.sub_set_by_key_range(-2, -2).is_empty());
    assert_eq!(rs.sub_set_by_key_range(-2, 1), RowSet::from_range(0, 1));
}

#[test]
fn overlaps_range_checks_the_inclusive_range() {
    let rows = RowSet::from_keys([5, 6, 20]);
    assert!(rows.overlaps_range(0, 5));
    assert!(rows.overlaps_range(6, 6));
    assert!(rows.overlaps_range(7, 20));
    assert!(rows.overlaps_range(-3, 5));
    assert!(!rows.overlaps_range(0, 4));
    assert!(!rows.overlaps_range(7, 19));
    assert!(!rows.overlaps_range(21, 30));
    assert!(!rows.overlaps_range(-3, -1));
    // An inverted range holds nothing, even when a range of `rows` spans both of its bounds.
    assert!(!rows.overlaps_range(6, 5));
}

#[test]
fn a_single_range_is_recognized_across_blocks() {
    assert_eq!(RowSet::empty().as_single_range(), None);
    assert_eq!(
        RowSet::from_range(3, 3 * B + 7).as_single_range(),
        Some(RowRange::new(3, 3 * B + 7))
    );
    assert_eq!(RowSet::from_keys([3, 5]).as_single_range(), None);
    assert_eq!(RowSet::from_range(3, 3 * B + 7).range_count(), 1);
    // Runs that meet at a block boundary are one range; a hole at the boundary makes two.
    let across = RowSet::from_ranges([RowRange::new(1, 5), RowRange::new(B - 4, B + 9)]);
    assert_eq!(across.range_count(), 2);
    let holed = across.minus(&RowSet::from_range(B, B));
    assert_eq!(holed.range_count(), 3);
}

#[test]
fn whole_blocks_are_one_span_however_many() {
    let rows = RowSet::flat(1_000_000_000_000);
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
    holed.remove(&RowSet::from_range(500_000_000_000, 500_000_000_000));
    assert_eq!(holed.size(), 999_999_999_999);
    assert_eq!(holed.spans.len(), 4);
    assert!(!holed.contains(500_000_000_000));
    assert_eq!(holed.get(500_000_000_000), Some(500_000_000_001));
    // Putting it back gives the original layout.
    holed.insert(&RowSet::from_range(500_000_000_000, 500_000_000_000));
    assert_eq!(holed, rows);
}

#[test]
fn a_block_is_a_bitmap_exactly_when_it_has_more_than_2047_runs() {
    // Every other key: `runs` runs of one key each.
    let alternate = |runs: RowKey| RowSet::from_keys((0..runs).map(|i| 2 * i));
    let at_limit = alternate(MAX_RUNS as RowKey);
    assert!(matches!(at_limit.spans[0].body, Body::Runs(_)));
    let over = alternate(MAX_RUNS as RowKey + 1);
    assert!(matches!(over.spans[0].body, Body::Bits(_)));
    assert_eq!(over.size(), MAX_RUNS as u64 + 1);

    // One more key takes the run list over the limit, and taking it away brings it back.
    let extra = RowSet::from_range(2 * MAX_RUNS as RowKey, 2 * MAX_RUNS as RowKey);
    let mut rows = at_limit.clone();
    rows.insert(&extra);
    assert_eq!(rows, over);
    rows.remove(&extra);
    assert_eq!(rows, at_limit);
    assert!(matches!(rows.spans[0].body, Body::Runs(_)));

    // Filling the gaps of a bitmap leaves one run, and a whole block when it is all of it.
    let odd = RowSet::from_keys((0..=MAX_RUNS as RowKey).map(|i| 2 * i + 1));
    assert_eq!(
        over.union(&odd).spans[0].body,
        Body::One(Run {
            first: 0,
            last: 2 * MAX_RUNS as u16 + 1
        })
    );
    let half = RowSet::from_keys((0..B / 2).map(|i| 2 * i));
    let other_half = RowSet::from_keys((0..B / 2).map(|i| 2 * i + 1));
    assert_eq!(half.union(&other_half).spans[0].body, Body::Full(1));
    assert!(half.intersect(&other_half).is_empty());
    assert_eq!(half.union(&other_half).minus(&half), other_half);
}

#[test]
fn a_copy_shares_blocks_and_changes_leave_the_original() {
    let rows = RowSet::from_keys((0..3 * B).filter(|k| k % 3 != 0));
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
    copy.insert(&RowSet::from_range(0, 0));
    let Body::Bits(shared) = &copy.spans[2].body else {
        panic!("the third block did not change");
    };
    assert!(std::sync::Arc::ptr_eq(original, shared));
    assert_eq!(copy.size(), rows.size() + 1);
    assert_eq!(rows.ranges().collect::<Vec<_>>(), before);
}

#[test]
fn the_last_key_of_the_key_space() {
    let rows = RowSet::from_range(RowKey::MAX - B - 5, RowKey::MAX);
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
        rows.minus(&RowSet::from_range(0, RowKey::MAX - 1)),
        RowSet::from_range(RowKey::MAX, RowKey::MAX)
    );
    assert_eq!(RowSet::from_range(0, RowKey::MAX).size(), 1 << 63);
}

#[test]
fn intersecting_skips_the_spans_between_the_other_sets_blocks() {
    // A million scattered blocks against two keys far apart: the walk must not visit them all.
    let many = RowSet::from_keys((0..1_000_000).map(|i| i * 3 * B + 17));
    let two = RowSet::from_keys([17, 999_999 * 3 * B + 17]);
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

#[test]
fn invert_rejects_a_range_whose_ends_are_present_but_not_its_middle() {
    let rows = RowSet::from_keys([1, 2, 5, 6]);
    assert_eq!(
        rows.invert(&RowSet::from_keys([2, 5])),
        Ok(RowSet::from_keys([1, 2]))
    );
    assert_eq!(
        rows.invert(&RowSet::from_range(2, 5)),
        Err(RowSetError::NotASubset)
    );
    // The same with the gap between two whole blocks.
    let rows = RowSet::from_ranges([RowRange::new(0, B - 1), RowRange::new(2 * B, 3 * B - 1)]);
    assert_eq!(
        rows.invert(&RowSet::from_keys([B - 1, 2 * B])),
        Ok(RowSet::from_range(B - 1, B))
    );
    assert_eq!(
        rows.invert(&RowSet::from_range(B - 1, 2 * B)),
        Err(RowSetError::NotASubset)
    );
}

#[test]
fn whole_blocks_side_by_side_become_one_span() {
    let (low, high) = (
        RowSet::from_range(0, 2 * B - 1),
        RowSet::from_range(2 * B, 5 * B - 1),
    );
    let both = RowSet::from_range(0, 5 * B - 1);
    assert_eq!(both.spans.len(), 1);
    for (a, b) in [(&low, &high), (&high, &low)] {
        assert_eq!(a.union(b), both);
        let mut joined = a.clone();
        joined.insert(b);
        assert_eq!(joined, both);
    }
    // What is left on either side of a removed block is two spans, and one again when the
    // block comes back from the other operand.
    let middle = RowSet::from_range(2 * B, 3 * B - 1);
    let holed = both.minus(&middle);
    assert_eq!(holed.spans.len(), 2);
    assert_eq!(holed.union(&middle), both);
    // Blocks that stay whole next to blocks that the other set completes.
    let evens = RowSet::from_keys((2 * B..3 * B).filter(|k| k % 2 == 0));
    let odds = RowSet::from_keys((2 * B..3 * B).filter(|k| k % 2 == 1));
    assert_eq!(holed.union(&evens).union(&odds), both);
    let mut rows = holed.clone();
    rows.insert(&evens);
    rows.insert(&odds);
    assert_eq!(rows, both);
    rows.check_layout();
}

#[test]
#[should_panic(expected = "beyond the key space")]
fn shifting_past_the_last_key_panics() {
    let rows = RowSet::from_range(RowKey::MAX - 2 * B, RowKey::MAX - 5);
    let _ = rows.shift(B);
}

/// The row set of a bitmap as it was built before there was a constructor for it: from the
/// runs of set bits, read a bit at a time.
fn from_bits_by_ranges(first: RowKey, bits: &[u64], length: usize) -> RowSet {
    let set = |i: usize| bits[i / 64] >> (i % 64) & 1 == 1;
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < length {
        if !set(i) {
            i += 1;
            continue;
        }
        let start = i;
        while i < length && set(i) {
            i += 1;
        }
        ranges.push(RowRange::new(
            first + start as RowKey,
            first + (i - 1) as RowKey,
        ));
    }
    RowSet::from_ranges(ranges)
}

/// Checks `from_bitmap` against the row set of the same runs, in keys and in layout, and
/// returns it.
fn assert_bitmap_is_its_runs(first: RowKey, bits: &[u64], length: usize) -> RowSet {
    let built = RowSet::from_bitmap(first, bits, length);
    let expected = from_bits_by_ranges(first, bits, length);
    // Equality is structural: the same spans, in the same forms, with the same counts.
    assert_eq!(built, expected, "first {first}, length {length}");
    assert_eq!(built.check_layout(), expected.check_layout());
    assert_canonical(&built);
    built
}

#[test]
fn a_bitmap_gives_the_row_set_of_its_runs() {
    let mut g = Lcg(7);
    // Where the bits start: on a block, in a word, in the middle and at the end of a block,
    // and far up the key space.
    let firsts: [RowKey; 9] = [
        0,
        1,
        63,
        64,
        B - 1,
        B,
        B + 12_345,
        5 * B + 64 * 100,
        (1 << 40) + 77,
    ];
    // How many bits: none, within a word, on a word, on a block, and a few blocks with an
    // end in a word and in a block.
    let lengths = [
        0, 1, 63, 64, 65, 1000, 65_535, 65_536, 65_537, 131_072, 200_000,
    ];
    // One stretch in `one_in` set, in stretches of up to `stretch` bits of the same value:
    // run lists, bitmaps, single runs and whole blocks all come out of these.
    let shapes: [(u64, u64); 7] = [
        (2, 1),
        (3, 2),
        (40, 1),
        (2, 300),
        (1000, 1),
        (2, 70_000),
        (10, 5000),
    ];
    let mut seen = super::SpanCounts::default();
    for (one_in, stretch) in shapes {
        for first in firsts {
            for length in lengths {
                // Two words more than the bits asked for, to show they are not read.
                let mut bits = vec![0u64; length / 64 + 3];
                let mut i = 0;
                while i < bits.len() * 64 {
                    let value = g.next(one_in) == 0;
                    for _ in 0..=g.next(stretch) {
                        if value && i < bits.len() * 64 {
                            bits[i / 64] |= 1 << (i % 64);
                        }
                        i += 1;
                    }
                }
                let built = assert_bitmap_is_its_runs(first, &bits, length);
                let counts = built.check_layout();
                seen.full += counts.full;
                seen.one += counts.one;
                seen.runs += counts.runs;
                seen.bits += counts.bits;
            }
        }
    }
    // Every form of span was built.
    assert!(
        seen.full > 0 && seen.one > 0 && seen.runs > 0 && seen.bits > 0,
        "{seen:?}"
    );
}

#[test]
fn empty_and_full_bitmaps_are_the_empty_set_and_a_range() {
    for first in [0, 5, 64, B - 3, B, 3 * B + 100] {
        for length in [0usize, 1, 64, 100, 65_536, 65_600, 3 * 65_536, 300_000] {
            let words = length / 64 + 1;
            assert!(RowSet::from_bitmap(first, &vec![0; words], length).is_empty());
            let full = assert_bitmap_is_its_runs(first, &vec![!0; words], length);
            let expected = match length {
                0 => RowSet::empty(),
                _ => RowSet::from_range(first, first + length as RowKey - 1),
            };
            assert_eq!(full, expected);
        }
    }
    // No words at all hold no bits.
    assert!(RowSet::from_bitmap(9, &[], 0).is_empty());
    // Whole blocks from a bitmap are one span, as from a range.
    let whole = RowSet::from_bitmap(2 * B, &vec![!0; 3 * 1024], 3 * 65_536);
    assert_eq!(whole.spans.len(), 1);
    assert_eq!(whole.spans[0].body, Body::Full(3));
}

#[test]
fn a_bitmap_block_is_a_bitmap_exactly_when_it_has_more_than_2047_runs() {
    // Every other bit: `runs` runs of one key each, from a key that is not on a word.
    let alternate = |runs: usize| {
        let mut bits = vec![0u64; 1024];
        for i in 0..runs {
            bits[2 * i / 64] |= 1 << (2 * i % 64);
        }
        RowSet::from_bitmap(B + 3, &bits, 65_000)
    };
    let at_limit = alternate(MAX_RUNS);
    assert!(matches!(at_limit.spans[0].body, Body::Runs(_)));
    let over = alternate(MAX_RUNS + 1);
    assert!(matches!(over.spans[0].body, Body::Bits(_)));
    assert_eq!(over.size(), MAX_RUNS as u64 + 1);
    assert_eq!(over.first_row_key(), Some(B + 3));
    assert_eq!(over.last_row_key(), Some(B + 3 + 2 * MAX_RUNS as RowKey));
}

#[test]
fn bitmaps_of_neighbouring_pieces_join_to_the_bitmap_of_both() {
    // Pieces of a mask, cut on and off the blocks, each a row set of its own, then united:
    // what a filter does with the pieces it judges on several threads.
    let mut g = Lcg(11);
    let bits: Vec<u64> = (0..4 * 1024)
        .map(|_| (g.next(1 << 31) << 32) | g.next(1 << 31))
        .collect();
    let length = bits.len() * 64;
    for first in [0, 100, B - 1] {
        let whole = assert_bitmap_is_its_runs(first, &bits, length);
        for piece in [65_536usize, 4096, 100_032] {
            let mut joined = RowSet::empty();
            let mut at = 0;
            while at < length {
                let end = (at + piece).min(length);
                // The piece's own words: it starts on a word of `bits`.
                let part = RowSet::from_bitmap(first + at as RowKey, &bits[at / 64..], end - at);
                joined.insert(&part);
                at = end;
            }
            assert_eq!(joined, whole, "first {first}, pieces of {piece}");
            joined.check_layout();
        }
    }
}

#[test]
#[should_panic(expected = "70 bits asked of 1 words")]
fn a_bitmap_shorter_than_its_length_panics() {
    let _ = RowSet::from_bitmap(0, &[!0], 70);
}

#[test]
#[should_panic(expected = "invalid bitmap of 2 keys from -1")]
fn a_bitmap_from_a_negative_key_panics() {
    let _ = RowSet::from_bitmap(-1, &[!0], 2);
}

#[test]
#[should_panic(expected = "invalid bitmap of 7 keys from")]
fn a_bitmap_past_the_last_key_panics() {
    let _ = RowSet::from_bitmap(RowKey::MAX - 5, &[!0], 7);
}

#[test]
fn a_bitmap_reaches_the_last_key_of_the_key_space() {
    let rows = assert_bitmap_is_its_runs(RowKey::MAX - 5, &[0b101101], 6);
    assert_eq!(rows.last_row_key(), Some(RowKey::MAX));
    assert_eq!(rows.size(), 4);
}
