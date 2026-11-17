//! The typed kernels of the match and range filters against the generic path, and filters
//! over more rows than one piece on any number of threads.

use super::filters::{resolve, FilterSpec, MatchOptions, RangeOp, Resolved};
use super::literal::Key;
use crate::ops::select::update;
use crate::table::{empty_table, in_memory_table};
use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{Field, Schema};
use oxhaven_column::parallel::with_threads;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{definition, test_seeds};
use std::sync::Arc;

const PIECE: usize = 1 << 16;

/// A fixed stream of numbers (SplitMix64).
struct Stream(u64);

impl Stream {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// `rows` cells of every primitive type and `Instant`: small values around zero, so that
/// keys match, with nulls, the value Java reserves for null in the type, the type's bounds,
/// and for the floating point types NaN, both zeros and an infinity.
fn columns(rows: usize, seed: u64) -> Vec<(ColumnType, ArrayRef)> {
    fn cells<T>(
        stream: &mut Stream,
        rows: usize,
        special: &[T],
        small: impl Fn(i64) -> T,
    ) -> Vec<Option<T>>
    where
        T: Copy,
    {
        (0..rows)
            .map(|_| {
                let draw = stream.next();
                match draw % 16 {
                    0 => None,
                    1 => Some(special[(draw >> 8) as usize % special.len()]),
                    _ => Some(small(((draw >> 8) % 9) as i64 - 4)),
                }
            })
            .collect()
    }
    let s = &mut Stream(seed);
    vec![
        (
            ColumnType::Byte,
            Arc::new(Int8Array::from(cells(s, rows, &[i8::MIN, i8::MAX], |v| {
                v as i8
            }))),
        ),
        (
            ColumnType::Short,
            Arc::new(Int16Array::from(cells(
                s,
                rows,
                &[i16::MIN, i16::MAX],
                |v| v as i16,
            ))),
        ),
        (
            ColumnType::Char,
            Arc::new(UInt16Array::from(cells(s, rows, &[u16::MAX, 0], |v| {
                (v + 4) as u16
            }))),
        ),
        (
            ColumnType::Int,
            Arc::new(Int32Array::from(cells(
                s,
                rows,
                &[i32::MIN, i32::MAX],
                |v| v as i32,
            ))),
        ),
        (
            ColumnType::Long,
            Arc::new(Int64Array::from(cells(
                s,
                rows,
                &[i64::MIN, i64::MAX],
                |v| v,
            ))),
        ),
        (
            ColumnType::Instant,
            Arc::new(
                TimestampNanosecondArray::from(cells(s, rows, &[i64::MIN, i64::MAX], |v| v))
                    .with_data_type(ColumnType::Instant.arrow_type()),
            ),
        ),
        (
            ColumnType::Float,
            Arc::new(Float32Array::from(cells(
                s,
                rows,
                &[f32::NAN, -0.0, -f32::MAX, f32::INFINITY],
                |v| v as f32 / 2.0,
            ))),
        ),
        (
            ColumnType::Double,
            Arc::new(Float64Array::from(cells(
                s,
                rows,
                &[f64::NAN, -0.0, -f64::MAX, f64::NEG_INFINITY],
                |v| v as f64 / 2.0,
            ))),
        ),
    ]
}

/// Filters of every kind that has a typed kernel, with keys for a column of `column_type`.
fn filters(column_type: &ColumnType) -> Vec<Resolved> {
    let floating = matches!(column_type, ColumnType::Float | ColumnType::Double);
    let keys: Vec<Key> = if floating {
        [-1.5, 0.0, 2.0, f64::NAN, f64::INFINITY]
            .map(Key::Float)
            .into_iter()
            .chain([Key::Null])
            // The other zero, the infinity below everything, the values reserved for null
            // in `double` and in `float`, the largest value, and values between cells.
            .chain(
                [
                    -0.0,
                    f64::NEG_INFINITY,
                    -f64::MAX,
                    f64::from(-f32::MAX),
                    f64::MAX,
                    0.5,
                    -2.0,
                    1.25,
                    f64::MIN_POSITIVE,
                ]
                .map(Key::Float),
            )
            .collect()
    } else {
        [-3, 0, 2, 127, i64::MAX]
            .map(Key::Int)
            .into_iter()
            .chain([Key::Null])
            // The value reserved for null and the largest value of every type, and the
            // ends of the small values.
            .chain(
                [
                    i64::MIN,
                    i64::from(i32::MIN),
                    i64::from(i32::MAX),
                    -32768,
                    32767,
                    65535,
                    65534,
                    -128,
                    -4,
                    4,
                ]
                .map(Key::Int),
            )
            .collect()
    };
    let mut filters = Vec::new();
    for pivot in &keys {
        for op in [
            RangeOp::Less,
            RangeOp::LessOrEqual,
            RangeOp::Greater,
            RangeOp::GreaterOrEqual,
        ] {
            filters.push(Resolved::Range {
                column: 0,
                op,
                pivot: pivot.clone(),
            });
        }
        for (lower_inclusive, upper_inclusive) in
            [(true, true), (true, false), (false, true), (false, false)]
        {
            filters.push(Resolved::Between {
                column: 0,
                lower: pivot.clone(),
                upper: keys[2].clone(),
                lower_inclusive,
                upper_inclusive,
            });
            filters.push(Resolved::Between {
                column: 0,
                lower: keys[0].clone(),
                upper: pivot.clone(),
                lower_inclusive,
                upper_inclusive,
            });
        }
    }
    // Up to eight values are a typed pass each; more are judged cell by cell.
    for chosen in [
        &keys[..1],
        &keys[1..3],
        &keys[3..6],
        &keys[..6],
        &keys[5..6],
        &keys[5..12],
        &keys[6..],
        &keys[..],
    ] {
        for (inverted, nan_match) in [(false, false), (true, false), (false, true), (true, true)] {
            filters.push(Resolved::Match {
                column: 0,
                keys: chosen.to_vec(),
                options: MatchOptions {
                    inverted,
                    ignore_case: false,
                    nan_match,
                },
            });
        }
    }
    filters
}

#[test]
fn typed_kernels_agree_with_the_generic_path_for_every_primitive_type() {
    let mut compared = 0;
    for seed in test_seeds(0..20) {
        for (column_type, chunk) in columns(700, seed) {
            for (number, filter) in filters(&column_type).iter().enumerate() {
                let typed = filter
                    .primitive_kernel(&chunk)
                    .unwrap_or_else(|| panic!("no kernel for {column_type:?}, filter {number}"));
                let generic = filter
                    .generic_mask(&column_type, chunk.clone())
                    .expect("the generic path");
                assert!(
                    typed == generic,
                    "{column_type:?}, seed {seed}, filter {number} differs from the generic path"
                );
                compared += 1;
                // a slice that starts inside the chunk, as a piece does
                let slice = chunk.slice(13, 301);
                assert!(
                    filter.primitive_kernel(&slice).expect("a kernel")
                        == filter
                            .generic_mask(&column_type, slice)
                            .expect("the generic path"),
                    "{column_type:?}, seed {seed}, filter {number} differs on a slice"
                );
            }
        }
    }
    assert!(compared >= 8 * 2 * 100);
}

/// A key that does not fit the column, and a column that is not a primitive, are left to
/// the generic path, which reports them.
#[test]
fn the_kernel_leaves_what_it_cannot_judge_to_the_generic_path() {
    let (column_type, chunk) = columns(10, 1).remove(3);
    let filter = Resolved::Range {
        column: 0,
        op: RangeOp::Less,
        pivot: Key::Float(1.0),
    };
    assert!(filter.primitive_kernel(&chunk).is_none());
    assert!(filter.generic_mask(&column_type, chunk).is_err());
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let filter = Resolved::Match {
        column: 0,
        keys: vec![Key::Str("a".into())],
        options: MatchOptions::default(),
    };
    assert!(filter.primitive_kernel(&strings).is_none());
}

/// A table of `rows` rows built by formulas, as a client builds one.
fn table(rows: usize) -> Arc<dyn Table> {
    let empty: Arc<dyn Table> = empty_table(rows as u64);
    update(
        &empty,
        &[
            "A = (int)((ii * 31337) % 1003)",
            "B = ii % 11 == 0 ? NULL_DOUBLE : ((ii * 7919) % 1009) / 7.0",
            "K = (int)(ii % 10)",
            "S = `s` + (ii % 50)",
        ],
        &Compiler,
    )
    .expect("a table")
}

fn spec(text: &str) -> FilterSpec {
    super::factory::parse_filter(text).expect("a filter")
}

fn keys(table: &Arc<dyn Table>, texts: &[&str], selection: &RowSet, threads: usize) -> RowSet {
    with_threads(threads, || {
        let mut kept = selection.clone();
        for text in texts {
            let filter = resolve(&spec(text), table.definition(), &Compiler).expect("resolved");
            kept = filter.filter(&**table, &kept, false).expect("filtered");
        }
        kept
    })
}

const FILTERS: [&[&str]; 9] = [
    &["A >= 200 && A < 600"],
    &["A >= 200", "A < 600"],
    &["K = 3", "B > 50.0", "A < 500"],
    &["S in `s1`, `s42`, `s9`"],
    &["B < 20.5"],
    &["A not in 1, 2, 3, 500"],
    &["A > 100 || K == 2"],
    &["S.endsWith(`7`)"],
    &["i % 3 == 0 && A > K"],
];

/// The reference is the same filter applied to at most a piece of rows at a time, which is
/// one pass on the calling thread.
#[test]
fn many_rows_keep_what_one_pass_keeps_on_any_number_of_threads() {
    for rows in [PIECE, PIECE + 1, 2 * PIECE - 1, 3 * PIECE + 17] {
        let table = table(rows);
        let all = table.row_set();
        let scattered = RowSet::from_keys((0..rows as i64).filter(|key| key % 7 != 2));
        for texts in FILTERS {
            let mut expected = RowSet::empty();
            let mut expected_scattered = RowSet::empty();
            let mut first = 0;
            while first < rows as i64 {
                let last = (first + PIECE as i64 / 2).min(rows as i64) - 1;
                let part = RowSet::from_range(first, last);
                expected.insert(&keys(&table, texts, &part, 1));
                expected_scattered.insert(&keys(&table, texts, &part.intersect(&scattered), 1));
                first = last + 1;
            }
            assert!(!expected.is_empty() && expected.size() < rows as u64);
            for threads in [1, 2, 4] {
                assert!(
                    keys(&table, texts, &all, threads) == expected,
                    "{texts:?} over {rows} rows on {threads} threads"
                );
                assert!(
                    keys(&table, texts, &scattered, threads) == expected_scattered,
                    "{texts:?} over scattered rows of {rows} on {threads} threads"
                );
            }
        }
    }
}

/// Behavior: with up to three values a case-insensitive match throws on its null value
/// (see `matches`). The failure is the same whichever piece meets it.
#[test]
fn a_failure_in_a_piece_fails_the_filter_as_before() {
    let rows = 2 * PIECE + 3;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "S",
            arrow::datatypes::DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from_iter_values(
            (0..rows).map(|row| if row == rows - 1 { "last" } else { "x" }),
        ))],
    )
    .expect("a batch");
    let table: Arc<dyn Table> =
        in_memory_table(definition(&[("S", ColumnType::String)]), &[batch]).expect("a table");
    let filter = Resolved::Match {
        column: 0,
        keys: vec![Key::Str("X".into()), Key::Null],
        options: MatchOptions {
            ignore_case: true,
            ..MatchOptions::default()
        },
    };
    let short = RowSet::from_range(0, 9);
    let expected = filter.filter(&*table, &short, false);
    assert_eq!(
        expected.expect("every row matches the first value").size(),
        10
    );
    let all = table.row_set();
    for threads in [1, 2, 4] {
        let failure = with_threads(threads, || filter.filter(&*table, &all, false))
            .expect_err("the last row reaches the null value");
        assert!(
            failure.message.contains("NullPointerException"),
            "{}",
            failure.message
        );
    }
}

/// The row set of a piece against the rows picked one key at a time: selections that are one
/// range, runs with gaps, single keys and keys in several blocks; masks with long and short
/// kept stretches, all kept and none kept; pieces at the start, in the middle and at the end.
#[test]
fn the_row_set_of_a_piece_is_the_kept_keys_of_its_positions() {
    use super::filters::kept_of_piece;
    use arrow::buffer::BooleanBuffer;
    for seed in test_seeds(0..40) {
        let mut stream = Stream(seed);
        let step = [1, 1, 2, 7, 70_000][seed as usize % 5];
        let mut keys: Vec<i64> = Vec::new();
        let mut key = (stream.next() % 100) as i64;
        while keys.len() < 3000 {
            // A run of keys, then a gap, unless the selection is to be one range.
            let run = 1 + stream.next() % 40;
            for _ in 0..run {
                keys.push(key);
                key += 1;
            }
            key += (step - 1) * (1 + (stream.next() % 5) as i64);
        }
        let selection = RowSet::from_keys(keys.iter().copied());
        assert_eq!(selection.as_single_range().is_some(), step == 1);
        for (offset, length) in [(0, 1000), (1000, 1500), (2500, keys.len() - 2500), (17, 0)] {
            let density = stream.next() % 5;
            let mask: Vec<bool> = (0..length)
                .map(|_| match density {
                    0 => true,
                    1 => false,
                    2 => stream.next().is_multiple_of(2),
                    _ => !stream.next().is_multiple_of(50),
                })
                .collect();
            let expected = RowSet::from_keys(
                (0..length)
                    .filter(|&row| mask[row])
                    .map(|row| keys[offset + row]),
            );
            let kept = kept_of_piece(&selection, offset, &BooleanBuffer::from(mask));
            kept.check_layout();
            assert!(kept == expected, "seed {seed}, piece {offset}+{length}");
        }
    }
}

/// Whole pieces of 65536 rows, as a filter cuts them: rows that are one range (the mask is
/// the bitmap of the kept keys), rows close together in runs shorter and longer than a word
/// (each run's bits are put at its keys) and rows far apart (joined range by range); from a
/// first key on a block and inside one; masks of every density, read from the start of a
/// buffer and from a bit inside it. Against the kept keys picked one at a time.
#[test]
fn whole_pieces_keep_the_keys_of_their_set_bits() {
    use super::filters::kept_of_piece;
    use arrow::buffer::BooleanBuffer;
    let mut compared = 0_u64;
    for seed in test_seeds(0..30) {
        let mut stream = Stream(seed ^ 0x51ED);
        // The longest run of rows and the longest gap between runs, in keys.
        let (run, gap) =
            [(1, 0), (1, 12), (3, 2), (200, 90), (5000, 1), (2, 400)][seed as usize % 6];
        let start = [0, 65_536, 77, 65_535, 1 << 33][seed as usize % 5];
        let mut keys: Vec<i64> = Vec::new();
        let mut key = start;
        while keys.len() < 2 * PIECE + 1000 {
            for _ in 0..1 + stream.next() % run {
                keys.push(key);
                key += 1;
            }
            if gap > 0 {
                key += 1 + (stream.next() % gap) as i64;
            }
        }
        let selection = RowSet::from_keys(keys.iter().copied());
        assert_eq!(selection.as_single_range().is_some(), gap == 0);
        let density = stream.next() % 6;
        // Three bits more than the rows, so that a piece's mask can start at bit 3.
        let bits: Vec<bool> = (0..keys.len() + 3)
            .map(|_| match density {
                0 => true,
                1 => false,
                2 => stream.next().is_multiple_of(2),
                3 => stream.next().is_multiple_of(40),
                4 => !stream.next().is_multiple_of(40),
                _ => stream.next() % 1000 < 995,
            })
            .collect();
        let buffer = BooleanBuffer::from(bits.clone());
        for shift in [0, 3] {
            for (offset, length) in [
                (0, PIECE),
                (PIECE, PIECE),
                (2 * PIECE, keys.len() - 2 * PIECE),
                (100, 64),
                (PIECE - 1, 130),
            ] {
                let mask = buffer.slice(offset + shift, length);
                let expected = RowSet::from_keys(
                    (0..length)
                        .filter(|&row| bits[offset + shift + row])
                        .map(|row| keys[offset + row]),
                );
                let kept = kept_of_piece(&selection, offset, &mask);
                kept.check_layout();
                assert!(
                    kept == expected,
                    "seed {seed}, piece {offset}+{length}, mask from bit {shift}"
                );
                compared += 1;
            }
        }
    }
    assert!(compared >= oxhaven_testkit::at_least_for(300, 0..30));
}
