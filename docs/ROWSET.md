# Row sets for large tables

How Oxhaven stores the row keys of a table with many rows. Written by task 7.7
(`tasks/7.7-rowset-containers.md`), which measured the present representation, built the
replacement in `crates/oxhaven-rowset/src/blocks`, and stopped before the contract change that
puts it to use.

## Decision

1. **The row set of a large table is `oxhaven_rowset::BlockRowSet`**: the key space cut into
   blocks of 2^16 keys, with whole blocks counted, and every other block held as one run, a list
   of runs, or a bitmap, whichever is smallest. Blocks are immutable and shared between row sets.
2. **It has the operations of the contract `RowSet` with the same names and results**, checked
   operation by operation against the contract type. One signature differs: `ranges()` is an
   iterator, not a slice.
3. **It is not yet the engine's row set.** The engine uses `oxhaven_contracts::rowset::RowSet`,
   which is protected, and whose `ranges() -> &[RowRange]` fixes the representation to a vector
   of ranges. "Making it the engine's row set" below is the contract change that remains.
   The owner approved the swap on 2026-11-17, as its own contract-change task started when no
   engine branch is open.

## What was slow

The contract `RowSet` is a sorted vector of inclusive ranges, 16 bytes each. Its cost follows the
number of ranges, not the number of rows, so a stored table read whole (64 files of a million
rows: 64 ranges) costs nothing. The trouble starts when a large table is filtered, because the
result has about as many ranges as rows. Three things then cost O(ranges) each time they are
called:

- `size`, `get`, `find` and `sub_set_by_position_range` walk the ranges from the start, since
  nothing records how many keys precede a range;
- `insert` and `remove` rebuild the whole vector, and `insert` sorts it, to add one key;
- `clone`, which `TrackingRowSet::write` does once a cycle to keep the previous value, copies
  all of it.

`cargo run --release -p oxhaven-rowset --bin rowset-bench` times this on five shapes of row
set. Storage-backed tables (task 7.1) are not built yet, so the shapes are made up to be what
they will produce: Java gives each file of a stored table its own 2^43 keys
(`RegionedColumnSource`), and a `where` keeps some of them. On the shape `filtered-half` (8
files of a million rows, half of the rows kept: 4.0M rows in 2.0M ranges) the contract row set
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

Operators call these per key and per cycle: the keyed cross join calls `find` for each key
(`ops/crossjoin/keyed.rs`), `update_by` calls `get` and `find` around each affected position,
the Barrage producer calls `get` for the bounds of a viewport, and a ticking table inserts every
cycle. Set algebra between two large sets (`union` 216 ms, `intersect` 42 ms) is linear and
merely slow.

## The structure

A `BlockRowSet` is a sorted vector of spans, and beside it the number of keys up to the end of
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
  to an `RspBitmap` when it grows. A small `BlockRowSet` is a few inline spans with no
  allocation beyond the two vectors, so there is one representation and no conversion.
- **Reference counts by hand.** `RefCountedCow` and the containers' `shared` flag are `Arc`.

## Measurements

One run of `rowset-bench` (release build, 4 cores shared with other jobs, load average 6 to 7;
each time is the shortest of at least three runs). "ranges" is the contract row set, "blocks"
the new one. The benchmark checks that both give the same keys for every operation before it
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

## Making it the engine's row set

This needs a change to `crates/oxhaven-contracts`, which task 7.7 may not make. It is one
change, with no new behavior:

1. Move `crates/oxhaven-rowset/src/blocks` into `oxhaven-contracts` as the body of
   `rowset::RowSet` (the contracts crate cannot depend on `oxhaven-rowset`), keeping the name
   `RowSet` and every method.
2. Change `RowSet::ranges` from `&[RowRange]` to an iterator of `RowRange`. Outside the
   contracts and the row-set crate there are about 35 callers. The `for` loops, 11 of them,
   compile as they are. The rest, in `oxhaven-engine` (`ops/merge`, `ops/crossjoin`,
   `ops/ungroup`, `ops/agg`), `oxhaven-column/src/flat.rs`, `oxhaven-server` and
   `oxhaven-testkit`, use the slice (`iter`, `len`, indexing, `partition_point`, a slice
   pattern) and need a small rewrite each; `ranges().len()` should become a method, since it
   is used to size buffers.
3. `oxhaven-rowset`'s builders and `shift::has_key_in` read `ranges()` as a slice and change
   with it. `has_key_in` becomes a `sub_set_by_key_range(..).is_empty()` or a method of its own.

The gate for that change is the whole workspace at the full seed budget and
`oracle/bin/ci-replay.sh` equal to its baseline: row sets have no results of their own on the
wire, so the recorded Java results of every operator are the differential test. The row-set
tests ported from Java (`crates/oxhaven-rowset/tests/rowset.rs`) then run against the new type,
and their sizes, cut down because the contract row set was slow, can go back to Java's.

## Gate of task 7.7

```bash
cargo test -p oxhaven-rowset
OXHAVEN_TEST_SEEDS=full cargo test -p oxhaven-rowset
cargo run --release -p oxhaven-rowset --bin rowset-bench
```

The first two run `blocks::tests`, which compares every operation with the contract row set on
300 seeds of generated key sets that reach all four span forms, and checks that every result is
in the one layout for its keys. The third fails if any timed operation gives different keys in
the two representations.

## Not done

- The contract change above, and with it any effect on the engine.
- A packed form for row sets with one row per block (Java's `SortedRanges` covers this case).
  The measurements say where it would matter; nothing in the engine produces that shape yet.
- `shift` of a bitmap by shifting its words, instead of rebuilding from ranges.
- Operations Java has and the contract does not: search and reverse iterators, `RowSequence`,
  `invert` with a maximum position, serialization. They are listed in `tasks/1.2-rowset-tests.md`.
