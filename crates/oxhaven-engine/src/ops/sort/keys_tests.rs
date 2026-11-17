//! Tests of the forms of sort keys against the row format and against Java's order.

use super::super::testing::static_table;
use super::*;
use arrow::array::{
    BooleanArray, Date32Array, DurationNanosecondArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, Time64NanosecondArray, TimestampNanosecondArray,
    UInt16Array,
};
use oxhaven_testkit::test_seeds;

/// A small random number generator, so that the tests need no dependency.
struct Random(u64);

impl Random {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }

    /// One of `pool`, or null one time in eight.
    fn pick<T: Copy>(&mut self, pool: &[T]) -> Option<T> {
        (self.below(8) != 0).then(|| pool[self.below(pool.len())])
    }

    fn column<T: Copy>(&mut self, size: usize, pool: &[T]) -> Vec<Option<T>> {
        (0..size).map(|_| self.pick(pool)).collect()
    }
}

const INTS: [i64; 7] = [-3, -1, 0, 1, 2, i64::MIN, i64::MAX];
const DOUBLES: [f64; 10] = [
    f64::NAN,
    -f64::NAN,
    -0.0,
    0.0,
    1.5,
    -1.5,
    f64::INFINITY,
    f64::NEG_INFINITY,
    f64::MIN_POSITIVE,
    -f64::MAX,
];
const TEXTS: [&str; 13] = [
    "",
    "a",
    "ab",
    "a\0",
    "\u{e000}",
    "\u{ffff}",
    "\u{10000}",
    "\u{10ffff}",
    "z",
    "\u{e000}\u{10000}",
    "a string of more than fifteen bytes, A",
    "a string of more than fifteen bytes, B",
    "a string of more than fifteen bytes",
];

fn narrow(values: &[Option<i64>], min: i64, max: i64) -> Vec<Option<i64>> {
    values
        .iter()
        .map(|v| v.map(|v| v.clamp(min, max)))
        .collect()
}

/// A column of every fixed-width type, of random values with many ties and nulls, and
/// the minimum and maximum of each integer type.
fn fixed_columns(random: &mut Random, size: usize) -> Vec<(ColumnType, ArrayRef)> {
    let mut longs = || random.column(size, &INTS);
    let i8s = narrow(&longs(), i8::MIN.into(), i8::MAX.into());
    let i16s = narrow(&longs(), i16::MIN.into(), i16::MAX.into());
    let i32s = narrow(&longs(), i32::MIN.into(), i32::MAX.into());
    let chars = narrow(&longs(), 0, 3);
    let times = narrow(&longs(), 0, 3);
    let days = narrow(&longs(), 0, 6);
    let (instants, local, durations, wide) = (longs(), longs(), longs(), longs());
    let doubles = random.column(size, &DOUBLES);
    let floats: Vec<Option<f32>> = random
        .column(size, &DOUBLES)
        .into_iter()
        .map(|v| v.map(|v| v as f32))
        .collect();
    let booleans = random.column(size, &[false, true]);
    let as_i8 =
        |values: &[Option<i64>]| Int8Array::from_iter(values.iter().map(|v| v.map(|v| v as i8)));
    let as_i32 = |values: &[Option<i64>]| -> Vec<Option<i32>> {
        values.iter().map(|v| v.map(|v| v as i32)).collect()
    };
    vec![
        (ColumnType::Byte, Arc::new(as_i8(&i8s))),
        (
            ColumnType::Short,
            Arc::new(Int16Array::from_iter(
                i16s.iter().map(|v| v.map(|v| v as i16)),
            )),
        ),
        (
            ColumnType::Int,
            Arc::new(Int32Array::from_iter(as_i32(&i32s))),
        ),
        (ColumnType::Long, Arc::new(Int64Array::from(wide))),
        (
            ColumnType::Char,
            Arc::new(UInt16Array::from_iter(
                chars.iter().map(|v| v.map(|v| (v * 0x5555) as u16)),
            )),
        ),
        (ColumnType::Double, Arc::new(Float64Array::from(doubles))),
        (ColumnType::Float, Arc::new(Float32Array::from(floats))),
        (ColumnType::Boolean, Arc::new(BooleanArray::from(booleans))),
        (
            ColumnType::Instant,
            Arc::new(TimestampNanosecondArray::from(instants).with_timezone("UTC")),
        ),
        (
            ColumnType::LocalDate,
            Arc::new(Date32Array::from_iter(as_i32(&i32s))),
        ),
        (
            ColumnType::LocalTime,
            Arc::new(Time64NanosecondArray::from_iter(
                times.iter().map(|v| v.map(|v| v * 1_000_000_000)),
            )),
        ),
        (
            ColumnType::LocalDateTime,
            Arc::new(TimestampNanosecondArray::from(local)),
        ),
        (
            ColumnType::Duration,
            Arc::new(DurationNanosecondArray::from(durations)),
        ),
        (ColumnType::DayOfWeek, Arc::new(as_i8(&days))),
    ]
}

/// The sorted positions of the rows of `table` by `columns`, with the form the sort
/// chooses and with the row format, and the keys of the rows in sorted order, kept by
/// appending them one by one as an incremental sort keeps them.
fn orders(table: &Arc<dyn Table>, columns: &[SortColumn]) -> (Vec<u32>, Vec<u32>, SortKeys) {
    let rows = table.row_set();
    let typed = KeyEncoder::new(&**table, columns).unwrap();
    assert!(!matches!(typed.form, Form::Rows(_)), "{columns:?}");
    let keys = typed.encode(&**table, &rows).unwrap();
    let order = keys.order();
    let mut appended = typed.empty(0, &keys);
    for &position in &order {
        appended.push_from(&keys, position as usize);
    }
    let general = KeyEncoder::build(&**table, columns, false).unwrap();
    let expected = general.encode(&**table, &rows).unwrap().order();
    (order, expected, appended)
}

/// Checks that keys appended in sorted order are in order, with ties exactly where the
/// unordered keys have them, and that each equals the key it was copied from.
fn check_appended(table: &Arc<dyn Table>, column: SortColumn, keys: &SortKeys, order: &[u32]) {
    let encoder = KeyEncoder::new(&**table, &[column]).unwrap();
    let unordered = encoder.encode(&**table, &table.row_set()).unwrap();
    for (index, pair) in order.windows(2).enumerate() {
        let between = keys.compare(index, keys, index + 1);
        assert_ne!(between, Ordering::Greater, "{index}");
        let original = unordered.compare(pair[0] as usize, &unordered, pair[1] as usize);
        assert_eq!(between, original, "{index}");
    }
    for (index, &position) in order.iter().enumerate() {
        let copied = keys.compare(index, &unordered, position as usize);
        assert_eq!(copied, Ordering::Equal, "{index}");
    }
}

#[test]
fn a_fixed_width_column_sorts_as_the_row_format_sorts_it() {
    for seed in test_seeds(0..40) {
        let mut random = Random(seed * 7919 + 1);
        let size = 1 + random.below(300);
        for (column_type, values) in fixed_columns(&mut random, size) {
            let table = static_table(&[("K", column_type.clone(), values)]);
            let mut sorts = vec![SortColumn::ascending("K"), SortColumn::descending("K")];
            if matches!(
                column_type,
                ColumnType::Byte
                    | ColumnType::Short
                    | ColumnType::Int
                    | ColumnType::Long
                    | ColumnType::Float
                    | ColumnType::Double
            ) {
                sorts.push(SortColumn::ascending("K").absolute());
                sorts.push(SortColumn::descending("K").absolute());
            }
            for column in sorts {
                let (order, expected, appended) = orders(&table, std::slice::from_ref(&column));
                assert_eq!(order, expected, "seed {seed}, {column_type:?}, {column:?}");
                check_appended(&table, column, &appended, &order);
            }
        }
    }
}

/// Java's order of strings: UTF-16 code units, with null least, as a stable sort.
fn java_order(values: &[Option<&str>], descending: bool) -> Vec<u32> {
    let key = |row: u32| values[row as usize].map(|v| v.encode_utf16().collect::<Vec<_>>());
    let mut order: Vec<u32> = (0..values.len() as u32).collect();
    order.sort_by(|&a, &b| {
        let order = key(a).cmp(&key(b));
        if descending {
            order.reverse()
        } else {
            order
        }
    });
    order
}

#[test]
fn a_string_column_sorts_by_utf16_code_units_in_both_forms() {
    for seed in test_seeds(0..40) {
        let mut random = Random(seed * 104_729 + 3);
        let size = 2 + random.below(300);
        let values = random.column(size, &TEXTS);
        let array: ArrayRef = Arc::new(StringArray::from(values.clone()));
        // A sliced array, whose offsets do not start at zero, as a chunk can be.
        let sliced = array.slice(1, size - 1);
        for (values, array) in [(&values[..], array), (&values[1..], sliced)] {
            let table = static_table(&[("S", ColumnType::String, array)]);
            for descending in [false, true] {
                let column = match descending {
                    false => SortColumn::ascending("S"),
                    true => SortColumn::descending("S"),
                };
                let (order, general, appended) = orders(&table, std::slice::from_ref(&column));
                let expected = java_order(values, descending);
                assert_eq!(order, expected, "seed {seed}, descending {descending}");
                assert_eq!(general, expected, "seed {seed}, descending {descending}");
                check_appended(&table, column, &appended, &order);
            }
        }
    }
}

#[test]
fn a_prefix_orders_short_strings_exactly_and_long_ones_by_their_start() {
    let short = ["", "\0", "a", "a\0", "a\0\0", "ab", "b"];
    for pair in short.windows(2) {
        assert!(
            prefix(pair[0].as_bytes()) < prefix(pair[1].as_bytes()),
            "{pair:?}"
        );
    }
    assert!(prefix(b"fifteen bytes..") < prefix(b"fifteen bytes..\0"));
    assert_eq!(prefix(b"fifteen bytes..A"), prefix(b"fifteen bytes..B"));
    assert_eq!(prefix(b"fifteen bytes..A").1 & 0xff, PREFIX_LONG);
}

#[test]
fn a_radix_sort_is_a_stable_sort_by_value() {
    for seed in test_seeds(0..10) {
        let mut random = Random(seed * 31337 + 5);
        let size = 1024 + random.below(5000);
        // Values that differ in a few bytes only, with many ties, and some that differ in
        // every byte.
        let values: Vec<u64> = (0..size)
            .map(|_| match random.below(4) {
                0 => random.0,
                _ => (random.below(50) as u64) << (8 * random.below(8)),
            })
            .collect();
        let mut pairs: Vec<(u64, u32)> = values.iter().copied().zip(0..).collect();
        let mut expected = pairs.clone();
        expected.sort_by_key(|pair| pair.0);
        radix_sort(&mut pairs);
        assert_eq!(pairs, expected, "seed {seed}");
    }
    // A large sort by a fixed-width key, as the row format sorts it.
    let size = 5000;
    let values = (0..size).map(|row: i32| row * 7919 % 1013 - 500);
    let table = static_table(&[(
        "K",
        ColumnType::Int,
        Arc::new(Int32Array::from_iter_values(values)),
    )]);
    for column in [SortColumn::ascending("K"), SortColumn::descending("K")] {
        let (order, expected, _) = orders(&table, std::slice::from_ref(&column));
        assert_eq!(order, expected, "{column:?}");
    }
}
