//! Times row-set operations on large tables, for the first representation (a sorted vector
//! of ranges, kept as `oxhaven_testkit::RangeRowSet`) and for the row set of the contracts.
//!
//! `docs/ROWSET.md` explains the workloads and records a run. Run it alone, in release mode:
//!
//! ```text
//! cargo run --release -p oxhaven-rowset --example rowset-bench            # every shape
//! cargo run --release -p oxhaven-rowset --example rowset-bench -- filtered # shapes whose name matches
//! ```
//!
//! Every operation is first checked to give the same keys in both representations, so a row of
//! the output never compares two different results.

use oxhaven_rowset::{RowKey, RowRange, RowSet, RowSetError};
use oxhaven_testkit::RangeRowSet;
use std::hint::black_box;
use std::time::{Duration, Instant};

/// The operations the benchmark needs, so that one body times both representations.
trait Rows: Clone + Sized {
    fn build(ranges: &[RowRange]) -> Self;
    fn keys(keys: &[RowKey]) -> Self;
    fn to_ranges(&self) -> Vec<RowRange>;
    /// Walks every range and adds up their sizes.
    fn walk(&self) -> u64;
    fn heap_bytes(&self) -> usize;
    fn size(&self) -> u64;
    fn get(&self, pos: u64) -> Option<RowKey>;
    fn find(&self, key: RowKey) -> Option<u64>;
    fn contains(&self, key: RowKey) -> bool;
    fn union(&self, other: &Self) -> Self;
    fn intersect(&self, other: &Self) -> Self;
    fn minus(&self, other: &Self) -> Self;
    fn insert(&mut self, other: &Self);
    fn remove(&mut self, other: &Self);
    fn invert(&self, keys: &Self) -> Result<Self, RowSetError>;
    fn by_position(&self, start: u64, end: u64) -> Self;
    fn by_key(&self, first: RowKey, last: RowKey) -> Self;
    fn for_positions(&self, positions: &Self) -> Self;
    fn shift(&self, delta: i64) -> Self;
}

macro_rules! forward {
    () => {
        fn size(&self) -> u64 {
            self.size()
        }
        fn get(&self, pos: u64) -> Option<RowKey> {
            self.get(pos)
        }
        fn find(&self, key: RowKey) -> Option<u64> {
            self.find(key)
        }
        fn contains(&self, key: RowKey) -> bool {
            self.contains(key)
        }
        fn union(&self, other: &Self) -> Self {
            self.union(other)
        }
        fn intersect(&self, other: &Self) -> Self {
            self.intersect(other)
        }
        fn minus(&self, other: &Self) -> Self {
            self.minus(other)
        }
        fn insert(&mut self, other: &Self) {
            self.insert(other)
        }
        fn remove(&mut self, other: &Self) {
            self.remove(other)
        }
        fn invert(&self, keys: &Self) -> Result<Self, RowSetError> {
            self.invert(keys)
        }
        fn by_position(&self, start: u64, end: u64) -> Self {
            self.sub_set_by_position_range(start, end)
        }
        fn by_key(&self, first: RowKey, last: RowKey) -> Self {
            self.sub_set_by_key_range(first, last)
        }
        fn for_positions(&self, positions: &Self) -> Self {
            self.sub_set_for_positions(positions, false)
        }
        fn shift(&self, delta: i64) -> Self {
            self.shift(delta).expect("the benchmark shifts upwards")
        }
    };
}

impl Rows for RangeRowSet {
    fn build(ranges: &[RowRange]) -> Self {
        Self::from_ranges(ranges.iter().copied())
    }
    fn keys(keys: &[RowKey]) -> Self {
        Self::from_keys(keys.iter().copied())
    }
    fn to_ranges(&self) -> Vec<RowRange> {
        self.ranges().to_vec()
    }
    fn walk(&self) -> u64 {
        self.ranges().iter().map(RowRange::size).sum()
    }
    fn heap_bytes(&self) -> usize {
        std::mem::size_of_val(self.ranges())
    }
    forward!();
}

impl Rows for RowSet {
    fn build(ranges: &[RowRange]) -> Self {
        Self::from_ranges(ranges.iter().copied())
    }
    fn keys(keys: &[RowKey]) -> Self {
        Self::from_keys(keys.iter().copied())
    }
    fn to_ranges(&self) -> Vec<RowRange> {
        self.ranges().collect()
    }
    fn walk(&self) -> u64 {
        self.ranges().map(|r| r.size()).sum()
    }
    fn heap_bytes(&self) -> usize {
        self.heap_bytes()
    }
    forward!();
}

/// splitmix64: fixed seeds, so every run times the same data.
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
}

/// The row keys of a table of large tables, as sorted ranges.
struct Shape {
    name: &'static str,
    what: &'static str,
    make: fn(&mut Rng) -> Vec<RowRange>,
}

/// Keeps each key of `first..first + len` with probability `1 / one_in`.
fn sample(rng: &mut Rng, first: RowKey, len: u64, one_in: u64, out: &mut Vec<RowRange>) {
    let mut open: Option<(RowKey, RowKey)> = None;
    for key in first..first + len as RowKey {
        if rng.below(one_in) != 0 {
            continue;
        }
        open = match open {
            Some((start, last)) if last + 1 == key => Some((start, key)),
            Some((start, last)) => {
                out.push(RowRange::new(start, last));
                Some((key, key))
            }
            None => Some((key, key)),
        };
    }
    if let Some((start, last)) = open {
        out.push(RowRange::new(start, last));
    }
}

/// Java gives each region (file) of a stored table its own slice of the key space.
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource
/// (`SUB_REGION_ROW_INDEX_ADDRESS_BITS`: 2^43 keys per region).
const REGION_KEYS: RowKey = 1 << 43;

const SHAPES: &[Shape] = &[
    Shape {
        name: "small",
        what: "1000 rows in 100 runs of 10 (a small table: most tables are)",
        make: |_| {
            (0..100)
                .map(|i| RowRange::new(i * 25, i * 25 + 9))
                .collect()
        },
    },
    Shape {
        name: "stored",
        what: "64 regions of 1M rows each, every row present (a stored table as read)",
        make: |_| {
            (0..64)
                .map(|r| RowRange::new(r * REGION_KEYS, r * REGION_KEYS + 999_999))
                .collect()
        },
    },
    Shape {
        name: "filtered-half",
        what: "8M rows in 8 regions, each row kept with probability 1/2 (where on a stored table)",
        make: |rng| {
            let mut out = Vec::new();
            for r in 0..8 {
                sample(rng, r * REGION_KEYS, 1_000_000, 2, &mut out);
            }
            out
        },
    },
    Shape {
        name: "filtered-1pct",
        what: "64M rows in 64 regions, each row kept with probability 1/100 (selective where)",
        make: |rng| {
            let mut out = Vec::new();
            for r in 0..64 {
                sample(rng, r * REGION_KEYS, 1_000_000, 100, &mut out);
            }
            out
        },
    },
    Shape {
        name: "runs",
        what: "1M runs of 1 to 200 rows with gaps of 1 to 200 keys (where on clustered data)",
        make: |rng| {
            let mut out = Vec::with_capacity(1_000_000);
            let mut next = 0;
            for _ in 0..1_000_000 {
                let first = next + 1 + rng.below(200) as RowKey;
                let last = first + rng.below(200) as RowKey;
                out.push(RowRange::new(first, last));
                next = last + 1;
            }
            out
        },
    },
    Shape {
        name: "scattered",
        what: "1M single rows spread over 2^50 keys (one row per block: the worst case for blocks)",
        make: |rng| {
            let mut keys: Vec<RowKey> = (0..1_000_000)
                .map(|_| rng.below(1 << 50) as RowKey)
                .collect();
            keys.sort_unstable();
            keys.dedup();
            keys.into_iter().map(|k| RowRange::new(k, k)).collect()
        },
    },
];

/// The shortest of several runs of `f`: at least three, and as many as fit in a fifth of a
/// second.
fn time<T>(mut f: impl FnMut() -> T) -> Duration {
    let started = Instant::now();
    let mut best = Duration::MAX;
    let mut runs = 0;
    while runs < 3 || (started.elapsed() < Duration::from_millis(200) && runs < 1000) {
        let t = Instant::now();
        black_box(f());
        best = best.min(t.elapsed());
        runs += 1;
    }
    best
}

/// The inputs of one shape, shared by both representations.
struct Inputs {
    ranges: Vec<RowRange>,
    other: Vec<RowRange>,
    /// 1000 keys of the set, 1000 keys near them that are mostly absent, 1000 positions.
    members: Vec<RowKey>,
    strangers: Vec<RowKey>,
    positions: Vec<u64>,
    /// 100 ranges of 20 positions each.
    viewports: Vec<RowRange>,
    size: u64,
    span: RowKey,
}

fn inputs(shape: &Shape) -> Inputs {
    let ranges = (shape.make)(&mut Rng(1));
    let other = (shape.make)(&mut Rng(2));
    let set = RowSet::build(&ranges);
    let size = set.size();
    // Cumulative sizes, so that picking members does not cost a walk of the ranges each.
    let mut ends = Vec::with_capacity(ranges.len());
    let mut total = 0u64;
    for r in &ranges {
        total += r.size();
        ends.push(total);
    }
    let key_at = |pos: u64| {
        let i = ends.partition_point(|&e| e <= pos);
        let before = if i == 0 { 0 } else { ends[i - 1] };
        ranges[i].first() + (pos - before) as RowKey
    };
    let mut rng = Rng(3);
    let positions: Vec<u64> = (0..1000).map(|_| rng.below(size)).collect();
    let members: Vec<RowKey> = positions.iter().map(|&p| key_at(p)).collect();
    let strangers: Vec<RowKey> = members
        .iter()
        .map(|&k| k + 1 + rng.below(3) as RowKey)
        .collect();
    let mut viewports: Vec<RowRange> = (0..100u64)
        .map(|i| {
            let start = (size / 100) * i + rng.below(size / 200);
            RowRange::new(start as RowKey, start as RowKey + 19)
        })
        .collect();
    viewports.sort_by_key(RowRange::first);
    let span = set.last_row_key().expect("shapes are not empty");
    Inputs {
        ranges,
        other,
        members,
        strangers,
        positions,
        viewports,
        size,
        span,
    }
}

/// Runs every operation on `S` and returns `(operation, time, result)` rows. The result is the
/// ranges of the outcome (or a number as a one-range list), for the cross-check.
fn run<S: Rows>(input: &Inputs) -> Vec<(&'static str, Duration, Vec<RowRange>)> {
    let set = S::build(&input.ranges);
    let other = S::build(&input.other);
    let members = S::keys(&input.members);
    let strangers = S::keys(&input.strangers);
    let viewports = S::build(&input.viewports);
    let common = set.intersect(&other);
    let number = |n: u64| vec![RowRange::new(n as RowKey, n as RowKey)];
    let mut rows = Vec::new();
    let mut add = |name: &'static str, t: Duration, result: Vec<RowRange>| {
        rows.push((name, t, result));
    };

    add("size", time(|| set.size()), number(set.size()));
    add(
        "get x1000",
        time(|| {
            input
                .positions
                .iter()
                .map(|&p| set.get(p).unwrap_or(0))
                .fold(0, RowKey::wrapping_add)
        }),
        number(input.positions.iter().filter_map(|&p| set.get(p)).count() as u64),
    );
    add(
        "find x1000",
        time(|| {
            input
                .members
                .iter()
                .map(|&k| set.find(k).unwrap_or(0))
                .fold(0, u64::wrapping_add)
        }),
        number(
            input
                .members
                .iter()
                .map(|&k| set.find(k).unwrap_or(0) % 1000)
                .sum(),
        ),
    );
    add(
        "contains x1000",
        time(|| input.strangers.iter().filter(|&&k| set.contains(k)).count()),
        number(input.strangers.iter().filter(|&&k| set.contains(k)).count() as u64),
    );
    add(
        "insert 1000 keys",
        time(|| {
            let mut s = set.clone();
            s.insert(&strangers);
            s
        }),
        {
            let mut s = set.clone();
            s.insert(&strangers);
            s.to_ranges()
        },
    );
    add(
        "remove 1000 keys",
        time(|| {
            let mut s = set.clone();
            s.remove(&members);
            s
        }),
        {
            let mut s = set.clone();
            s.remove(&members);
            s.to_ranges()
        },
    );
    add(
        "insert 1 key, 100 times",
        time(|| {
            let mut s = set.clone();
            for &k in &input.strangers[..100] {
                s.insert(&S::keys(&[k]));
            }
            s
        }),
        Vec::new(),
    );
    add("clone", time(|| set.clone()), Vec::new());
    add(
        "union",
        time(|| set.union(&other)),
        set.union(&other).to_ranges(),
    );
    add(
        "intersect",
        time(|| set.intersect(&other)),
        common.to_ranges(),
    );
    add(
        "minus",
        time(|| set.minus(&other)),
        set.minus(&other).to_ranges(),
    );
    add(
        "invert 1000 keys",
        time(|| set.invert(&members).expect("members are a subset")),
        set.invert(&members)
            .expect("members are a subset")
            .to_ranges(),
    );
    add(
        "invert a large subset",
        time(|| set.invert(&common).expect("an intersection is a subset")),
        set.invert(&common)
            .expect("an intersection is a subset")
            .to_ranges(),
    );
    let (start, end) = (input.size / 2, input.size / 2 + 100);
    add(
        "100 rows by position",
        time(|| set.by_position(start, end)),
        set.by_position(start, end).to_ranges(),
    );
    add(
        "100 viewports of 20 rows",
        time(|| set.for_positions(&viewports)),
        set.for_positions(&viewports).to_ranges(),
    );
    let (first, last) = (input.span / 3, input.span / 3 + 5_000_000);
    add(
        "5M keys by key range",
        time(|| set.by_key(first, last)),
        set.by_key(first, last).to_ranges(),
    );
    add(
        "shift by 1",
        time(|| set.shift(1)),
        set.shift(1).to_ranges(),
    );
    add(
        "shift by 2^16",
        time(|| set.shift(65536)),
        set.shift(65536).to_ranges(),
    );
    add("walk the ranges", time(|| set.walk()), number(set.walk()));
    add(
        "build from ranges",
        time(|| S::build(&input.ranges)),
        Vec::new(),
    );
    add(
        "memory (bytes)",
        Duration::from_nanos(set.heap_bytes() as u64),
        Vec::new(),
    );
    rows
}

fn show(d: Duration) -> String {
    let ns = d.as_nanos();
    match ns {
        0..=9_999 => format!("{ns} ns"),
        10_000..=9_999_999 => format!("{:.1} us", ns as f64 / 1e3),
        _ => format!("{:.1} ms", ns as f64 / 1e6),
    }
}

fn main() {
    let filter: Vec<String> = std::env::args().skip(1).collect();
    for shape in SHAPES {
        if !filter.is_empty() && !filter.iter().any(|f| shape.name.contains(f.as_str())) {
            continue;
        }
        let input = inputs(shape);
        println!("\n## {}: {}", shape.name, shape.what);
        println!("{} rows in {} ranges\n", input.size, input.ranges.len());
        println!("| operation | ranges | blocks | ratio |");
        println!("| --- | ---: | ---: | ---: |");
        let ranges = run::<RangeRowSet>(&input);
        let blocks = run::<RowSet>(&input);
        for ((name, a, result_a), (_, b, result_b)) in ranges.into_iter().zip(blocks) {
            assert_eq!(
                result_a, result_b,
                "{name} differs between the representations"
            );
            let ratio = a.as_nanos() as f64 / b.as_nanos().max(1) as f64;
            if name.starts_with("memory") {
                println!(
                    "| {name} | {} | {} | {ratio:.1} |",
                    a.as_nanos(),
                    b.as_nanos()
                );
            } else {
                println!("| {name} | {} | {} | {ratio:.1} |", show(a), show(b));
            }
        }
    }
}
