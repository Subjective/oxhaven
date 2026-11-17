# Row sets for large tables

How Oxhaven stores the row keys of a table with many rows. Task 7.7
(`tasks/7.7-rowset-containers.md`) measured the first representation, a sorted vector of
ranges, and built the replacement; task 7.7b (`tasks/7.7b-rowset-swap.md`) made the replacement
the row set of the contracts, `oxhaven_contracts::rowset::RowSet`, which every crate uses.

## Decision

1. **The row set is a set of blocks**: the key space cut into blocks of 2^16 keys, with whole
   blocks counted, and every other block held as one run, a list of runs, or a bitmap,
   whichever is smallest. Blocks are immutable and shared between row sets. It is
   `oxhaven_contracts::rowset::RowSet` (`crates/oxhaven-contracts/src/rowset`).
2. **It has the operations of the first row set with the same names and results**, checked
   operation by operation against it. One signature differs: `ranges()` is an iterator, not a
   slice. `range_count`, `as_single_range` and `overlaps_range` answer what callers used the
   slice for without a walk of the ranges.
3. **The first row set is kept as a reference for tests**, as `oxhaven_testkit::RangeRowSet`.
   Nothing in the engine or the server uses it.

## What was slow

The first row set (now `RangeRowSet`) is a sorted vector of inclusive ranges, 16 bytes each. Its cost follows the
number of ranges, not the number of rows, so a stored table read whole (64 files of a million
rows: 64 ranges) costs nothing. The trouble starts when a large table is filtered, because the
result has about as many ranges as rows. Three things then cost O(ranges) each time they are
called:

- `size`, `get`, `find` and `sub_set_by_position_range` walk the ranges from the start, since
  nothing records how many keys precede a range;
- `insert` and `remove` rebuild the whole vector, and `insert` sorts it, to add one key;
- `clone`, which `TrackingRowSet::write` does once a cycle to keep the previous value, copies
  all of it.

`cargo run --release -p oxhaven-rowset --example rowset-bench` times this on five shapes of row
set. Storage-backed tables (task 7.1) are not built yet, so the shapes are made up to be what
they will produce: Java gives each file of a stored table its own 2^43 keys
(`RegionedColumnSource`), and a `where` keeps some of them. On the shape `filtered-half` (8
files of a million rows, half of the rows kept: 4.0M rows in 2.0M ranges) the vector of ranges
took, on this machine:

| operation | time |
| --- | ---: |
| `size` | 1.5 ms |
| `get`, `find` | 0.7 ms each |
| add one key | 64 ms |
| add or remove 1000 keys | 87 ms, 28 ms |
| `clone` | 2.6 ms |
| 100 rows by position (a viewport) | 0.74 ms |
| memory | 32 MB, 8 bytes a row |

Operators called these per key and per cycle: the keyed cross join calls `find` for each key
(`ops/crossjoin/keyed.rs`), `update_by` calls `get` and `find` around each affected position,
the Barrage producer calls `get` for the bounds of a viewport, and a ticking table inserts every
cycle. Set algebra between two large sets (`union` 216 ms, `intersect` 42 ms) is linear and
merely slow.

## The structure

A `RowSet` is a sorted vector of spans, and beside it the number of keys up to the end of
each span. A span starts at a block (a key divided by 65536) and is one of:

| form | holds | bytes |
| --- | --- | ---: |
| whole blocks | a count of consecutive blocks with every key | inline |
| one run | `first..=last` within a block | inline |
| run list | 2 to 2047 runs within a block | 4 per run |
| bitmap | a block with more runs than that | 8192 |

Each set of keys has exactly one layout: whole blocks are never stored as runs, neighbouring
spans of whole blocks are one span, and a block is a bitmap exactly when its run list would be
larger than the bitmap. So equality and hashing are structural, and a block is at most 8 KB
however its keys are arranged.

Run lists and bitmaps are behind `Arc` and never change in place. A copy of a row set copies
the span vector and shares the blocks; an operation's result shares every block it did not
change. `insert` and `remove` rebuild only the spans among the blocks of their argument.

The count of keys before each span makes `size` O(1) and turns `get` and `find` into a binary
search over spans and a scan of one block. `intersect` searches past the spans of one side that
lie between two spans of the other, so a small set against a large one costs the small one.

### Compared with Java

Java's `RspBitmap` is the model: the same 2^16-key blocks, spans of whole blocks, prefix array
of cardinalities and shared containers. Three things are not carried over.

- **RoaringBitmap's containers.** Java has array, run and bitmap containers and several special
  small ones, and an operation for each pair. Here a sorted `u16` array is a run list whose runs
  have one key, at 4 bytes a key instead of 2, so there are two forms and each binary operation
  is written once for run lists and once for bitmaps; a run list is turned into a bitmap when it
  meets one.
- **`SingleRange` and `SortedRanges`.** Java starts a row set in one of these and converts it
  to an `RspBitmap` when it grows. A small `RowSet` is a few inline spans with no
  allocation beyond the two vectors, so there is one representation and no conversion.
- **Reference counts by hand.** `RefCountedCow` and the containers' `shared` flag are `Arc`.

## Measurements

One run of `rowset-bench` (release build, 4 cores shared with other jobs, load average 6 to 7;
each time is the shortest of at least three runs). "ranges" is the vector of ranges, "blocks"
the row set. The benchmark checks that both give the same keys for every operation before it
prints a row.

`filtered-half`: 4.0M rows in 2.0M ranges, in 8 files.

| operation | ranges | blocks |
| --- | ---: | ---: |
| `size` | 1.5 ms | 20 ns |
| `get` x1000 | 670 ms | 0.52 ms |
| `find` x1000 | 715 ms | 0.33 ms |
| add 1000 keys | 87 ms | 0.28 ms |
| add one key, 100 times | 6363 ms | 0.22 ms |
| `clone` | 2.6 ms | 1.4 us |
| `union`, `intersect`, `minus` | 216, 42, 43 ms | 0.34 ms each |
| 100 viewports of 20 rows | 95 ms | 0.29 ms |
| `invert` of a 2M-row subset | 26 ms | 33 ms |
| walk every range | 1.5 ms | 15 ms |
| `shift` by 1 | 3.1 ms | 19 ms |
| build from sorted ranges | 14 ms | 21 ms |
| memory | 32.0 MB | 1.06 MB |

`runs`: 100M rows in 1M runs of 1 to 200 rows (a filter on clustered data; blocks are run lists).

| operation | ranges | blocks |
| --- | ---: | ---: |
| `size` | 0.71 ms | 21 ns |
| `get` x1000 | 343 ms | 0.12 ms |
| add 1000 keys | 24 ms | 0.77 ms |
| add one key, 100 times | 2410 ms | 0.13 ms |
| `clone` | 1.2 ms | 32 us |
| `union` | 82 ms | 14 ms |
| `intersect`, `minus` | 14, 15 ms | 12, 14 ms |
| walk every range | 0.71 ms | 4.0 ms |
| memory | 16.0 MB | 4.2 MB |

`filtered-1pct` (640K rows in 64 files, one row in a hundred kept) is like `runs`: positional
access 1000 times faster, changes 3.6 to 10000 times, set algebra 1.2 to 5.5 times, memory a
quarter.

A second run after the row set moved into the contracts (task 7.7b; the machine was busier)
gave the same picture. `filtered-half`: `get` x1000 993 ms against 0.55 ms, one key added 100
times 8216 ms against 0.27 ms, `clone` 2.96 ms against 1.6 us, a walk of every range 1.5 ms
against 18 ms, memory 32.0 MB against 1.06 MB.

### Where blocks are slower

- **Walking every range** costs about 4 ns a range for run lists and 7 ns for bitmaps, against
  under 1 ns for a slice. The same holds for what is built on the walk: `shift` by an amount
  that is not a multiple of 65536, which rebuilds the set as Java does (about 5 to 6 times
  slower),
  building from ranges (1.5 times), and `invert` of a large subset (1.3 to 1.5 times).
- **Small and dense row sets**, which most tables have, were never slow and are not faster.
  `small` (1000 rows in 100 runs) and `stored` (64M rows in 64 ranges) take nanoseconds to a
  few microseconds per operation in both. Depending on the operation, blocks are up to 4 times
  faster or up to 60 times slower (`shift` by 1 of 64 ranges: 53 ns against 3.3 us). The largest
  gap in absolute time is the removal of 1000 scattered keys from whole blocks, 2 us against
  49 us, which has to build a run list for each block it touches.
- **One row per block** is the worst case, and `scattered` measures it: a million rows spread
  over 2^50 keys. Every row is its own span of 40 bytes against 16 for a range, and nothing is
  shared or packed. Positional access and single-key changes are still 80 to 7000 times faster,
  but whole-set operations are slower: `clone` 12 ms against 1.3 ms, `minus` 42 against 13 ms,
  adding 1000 keys 71 against 38 ms, building 43 against 5 ms. A filter that keeps one row in
  65536 or fewer has this shape; it takes a table of 65 billion rows to make a million of them.

### In the engine

`cargo run -p oxhaven-engine --example fragmented-rows` builds an append-only table of 4M
longs, keeps the even ones (2.0M rows in 2.0M ranges), and works on that table. Before and
after the row set was replaced, on the same machine, both in the unoptimized `dev` profile (the
second of two runs each):

| step | ranges | blocks |
| --- | ---: | ---: |
| filter the 4M rows to the even ones | 2315 ms | 2464 ms |
| filter the 2M even rows again | 2567 ms | 2605 ms |
| 100 slices of 100 rows from the middle | 6952 ms | 5.3 ms |
| sort the even rows, descending | 3864 ms | 3765 ms |
| 20 cycles that append 10 rows each | 5231 ms | 32 ms |

The filters and the sort read every row and cost what they did. What depended on the number
of ranges is gone: a slice is a `get` and a sub set by position, and a cycle is an insert of
ten keys and a copy for the previous value.

## Making it the engine's row set

Done by task 7.7b, as one change with no new behavior:

1. `crates/oxhaven-rowset/src/blocks` became `crates/oxhaven-contracts/src/rowset`, the body
   of `rowset::RowSet`, with the name `RowSet` and every method kept.
2. `RowSet::ranges` is an iterator of `RowRange`. Callers that only walked the slice walk the
   iterator. Those that used it as a slice have a method each:
   - `range_count()`, where a buffer was sized by `ranges().len()`;
   - `as_single_range()`, where a slice pattern asked whether the rows are contiguous
     (`oxhaven-column/src/flat.rs`, hierarchical viewports);
   - `overlaps_range(first, last)`, which was `shift::has_key_in` and its `partition_point`;
   - `get` and `sub_set_by_position_range`, where code indexed ranges by position: the
     chunking of `ops/agg` and of column statistics, and the positional redirection of
     `ops/merge/flatten.rs`, which built a prefix sum over the ranges on every call.
3. The random builder adds a row set of more than 64 ranges by union, which shares its blocks,
   and copies the ranges of a smaller one as before.
4. The vector of ranges moved to `oxhaven_testkit::RangeRowSet`. The comparison of every
   operation with it is `crates/oxhaven-rowset/tests/blocks.rs`; the tests of the layout itself
   are unit tests of the contracts (`rowset/tests.rs`). `RowSet::check_layout`, hidden from
   the documentation, lets the comparison check the layout from outside the crate.
5. The row-set tests ported from Java (`crates/oxhaven-rowset/tests/rowset.rs`) run against
   the row set at Java's sizes. Two of them (`testMinusIndexOps`, `testRandomInsertMinus`) take
   37 and 22 seconds unoptimized at that size, so the quick seed budget runs the first fifth
   of their rounds and `OXHAVEN_TEST_SEEDS=full` all of them.

Two callers still copy ranges into a vector, both because they build a new row set from
changed ranges and `from_ranges` takes them in any order: `reverse` (`ops/merge/flatten.rs`),
whose mirrored ranges come out in decreasing order, and the builders, which hold ranges until
`build`.

The gate was the whole workspace at the full seed budget and `oracle/bin/ci-replay.sh` equal
to its baseline: row sets have no results of their own on the wire, so the recorded Deephaven
results of every operator are the differential test. No expectation of any test changed.

## Gate

```bash
cargo test -p oxhaven-contracts -p oxhaven-rowset
OXHAVEN_TEST_SEEDS=full cargo test --workspace
cargo run --release -p oxhaven-rowset --example rowset-bench
```

`crates/oxhaven-rowset/tests/blocks.rs` compares every operation with `RangeRowSet` on 300
seeds of generated key sets that reach all four span forms, and checks that every result is in
the one layout for its keys. The benchmark fails if any timed operation gives different keys
in the two representations.

## Not done

- A packed form for row sets with one row per block (Java's `SortedRanges` covers this case).
  The measurements say where it would matter; nothing in the engine produces that shape yet.
- `shift` of a bitmap by shifting its words, instead of rebuilding from ranges.
- A walk of the ranges from the end. `reverse` is the one caller that wants it.
- Operations Java has and the row set does not: search and reverse iterators, `RowSequence`,
  `invert` with a maximum position, serialization. They are listed in `tasks/1.2-rowset-tests.md`.
