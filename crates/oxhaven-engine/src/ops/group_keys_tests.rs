//! Tests of [`GroupKeys`] against a model that numbers keys by first appearance.

use super::*;
use arrow::array::{
    BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use oxhaven_testkit::test_seeds;

/// A small deterministic generator.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The key of one cell by the grouping rule of PARITY.md, worked out without the table's
/// own code: two cells are one key when their strings are equal.
fn reference_cell(array: &ArrayRef, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    let double = |v: f64| {
        if v.is_nan() {
            "NaN".to_string()
        } else if v == 0.0 {
            "zero".to_string()
        } else {
            format!("{v:e}")
        }
    };
    Some(match array.data_type() {
        DataType::Float64 => double(array.as_primitive::<Float64Type>().value(row)),
        DataType::Float32 => double(array.as_primitive::<Float32Type>().value(row).into()),
        DataType::Utf8 => format!("'{}'", array.as_string::<i32>().value(row)),
        _ => arrow::util::display::array_value_to_string(array, row).expect("a value"),
    })
}

/// A column of `rows` values of `data_type` from a small domain, with nulls, NaNs of
/// both signs, both zeros and repeats.
fn column(data_type: &DataType, rows: usize, domain: u64, random: &mut Random) -> ArrayRef {
    let picked: Vec<Option<i64>> = (0..rows)
        .map(|_| {
            let value = random.below(domain + 1);
            (value != domain).then_some(value as i64 - (domain as i64) / 2)
        })
        .collect();
    let doubles = |v: &Option<i64>| {
        v.map(|v| match v {
            0 => -0.0,
            1 => 0.0,
            2 | 3 => f64::NAN,
            4 => -f64::NAN,
            v => v as f64 / 4.0,
        })
    };
    let each = picked.iter();
    match data_type {
        DataType::Int8 => Arc::new(Int8Array::from_iter(each.map(|v| v.map(|v| v as i8)))),
        DataType::Int16 => Arc::new(Int16Array::from_iter(each.map(|v| v.map(|v| v as i16)))),
        DataType::Int32 => Arc::new(Int32Array::from_iter(each.map(|v| v.map(|v| v as i32)))),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            each.map(|v| v.map(|v| v * 1_000_000_007)),
        )),
        DataType::UInt16 => Arc::new(UInt16Array::from_iter(each.map(|v| v.map(|v| v as u16)))),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(each.map(|v| v.map(|v| v % 2 == 0)))),
        DataType::Float64 => Arc::new(Float64Array::from_iter(each.map(doubles))),
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            each.map(|v| doubles(v).map(|v| v as f32)),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => Arc::new(
            TimestampNanosecondArray::from_iter(each.copied()).with_timezone_opt(zone.clone()),
        ),
        DataType::Utf8 => Arc::new(StringArray::from_iter(each.map(|v| {
            v.map(|v| format!("k{}", v.abs()).repeat(1 + (v.unsigned_abs() % 3) as usize))
        }))),
        other => panic!("no generator for {other}"),
    }
}

/// Feeds random chunks to a table and checks every slot against the model.
fn check(data_types: &[DataType], collide: bool, seed: u64) {
    let mut random = Random(0x9e37_79b9_7f4a_7c15 ^ (seed + 1).wrapping_mul(0x2545_f491_4f6c_dd1d));
    let mut keys = GroupKeys::new(data_types).expect("a key table");
    keys.collide = collide;
    let mut model: Vec<Vec<Option<String>>> = Vec::new();
    let domain = 4 + random.below(if collide { 60 } else { 3000 });
    for _ in 0..1 + random.below(6) {
        let rows = random.below(700) as usize;
        let columns: Vec<ArrayRef> = data_types
            .iter()
            .map(|data_type| column(data_type, rows, domain, &mut random))
            .collect();
        let (mut slots, mut created) = (vec![99], vec![99]);
        keys.find_or_create(&columns, &mut slots, &mut created)
            .expect("a lookup");
        assert_eq!(slots.remove(0), 99, "appends to `slots`");
        assert_eq!(created.remove(0), 99, "appends to `created`");
        let mut expected_created = Vec::new();
        for (row, &slot) in slots.iter().enumerate() {
            let key: Vec<Option<String>> = columns
                .iter()
                .map(|column| reference_cell(column, row))
                .collect();
            let expected = match model.iter().position(|known| *known == key) {
                Some(slot) => slot,
                None => {
                    model.push(key);
                    expected_created.push(row);
                    model.len() - 1
                }
            };
            assert_eq!(slot, expected, "row {row} of {data_types:?}, seed {seed}");
        }
        assert_eq!(created, expected_created, "seed {seed}");
        assert_eq!(keys.len(), model.len());
        // Every key just seen is found again without making a slot.
        let mut found = Vec::new();
        assert!(keys.find(&columns, &mut found).expect("a lookup"));
        assert_eq!(found, slots);
        assert_eq!(keys.len(), model.len());
    }
}

const TYPES: [DataType; 10] = [
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt16,
    DataType::Boolean,
    DataType::Float32,
    DataType::Float64,
    DataType::Timestamp(TimeUnit::Nanosecond, None),
    DataType::Utf8,
];

#[test]
fn one_key_column_numbers_keys_by_first_appearance() {
    for seed in test_seeds(0..40) {
        for data_type in &TYPES {
            check(std::slice::from_ref(data_type), false, seed);
        }
    }
}

#[test]
fn several_key_columns_number_keys_by_first_appearance() {
    let pairs = [
        [DataType::Int32, DataType::Utf8],
        [DataType::Float64, DataType::Boolean],
        [DataType::Utf8, DataType::Float32],
    ];
    for seed in test_seeds(0..40) {
        for pair in &pairs {
            check(pair, false, seed);
        }
    }
}

/// With every key hashed alike, each lookup compares keys of the whole cluster, and
/// growing the table moves them all: equal hashes must not make keys equal.
#[test]
fn keys_whose_hashes_collide_stay_apart() {
    for seed in test_seeds(0..20) {
        for data_type in &TYPES {
            check(std::slice::from_ref(data_type), true, seed);
        }
        check(&[DataType::Int32, DataType::Utf8], true, seed);
    }
}

#[test]
fn a_null_nan_and_negative_zero_key_are_each_one_key() {
    for data_type in [DataType::Float64, DataType::Float32] {
        let mut keys = GroupKeys::new(std::slice::from_ref(&data_type)).expect("a key table");
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            None,
            Some(f64::NAN),
            Some(0.0),
            Some(-f64::NAN),
            None,
            Some(1.0),
        ]));
        let values = arrow::compute::cast(&values, &data_type).expect("a cast");
        let (mut slots, mut created) = (Vec::new(), Vec::new());
        keys.find_or_create(&[values], &mut slots, &mut created)
            .expect("a lookup");
        assert_eq!(slots, vec![0, 1, 2, 0, 2, 1, 3]);
        assert_eq!(created, vec![0, 1, 2, 6]);
    }
}

#[test]
fn find_reports_a_key_without_a_slot() {
    for data_type in [DataType::Int32, DataType::Utf8, DataType::Float64] {
        let mut keys = GroupKeys::new(std::slice::from_ref(&data_type)).expect("a key table");
        let present = column(&data_type, 50, 8, &mut Random(5));
        let present = arrow::compute::filter(
            &present,
            &arrow::compute::is_not_null(&present).expect("a mask"),
        )
        .expect("the values");
        keys.find_or_create(
            std::slice::from_ref(&present),
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .expect("a lookup");
        let mut slots = Vec::new();
        assert!(keys.find(&[present], &mut slots).expect("a lookup"));
        let unseen: ArrayRef = match data_type {
            DataType::Int32 => Arc::new(Int32Array::from(vec![1_000])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![1_000.5])),
            _ => Arc::new(StringArray::from(vec!["never"])),
        };
        let before = keys.len();
        assert!(!keys.find(&[unseen], &mut Vec::new()).expect("a lookup"));
        // No null was added, so the null key has no slot either.
        let nulls = arrow::array::new_null_array(&data_type, 1);
        assert!(!keys
            .find(std::slice::from_ref(&nulls), &mut Vec::new())
            .expect("a lookup"));
        assert_eq!(keys.len(), before, "find makes no slot");
        keys.find_or_create(
            std::slice::from_ref(&nulls),
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .expect("a lookup");
        let mut slots = Vec::new();
        assert!(keys.find(&[nulls], &mut slots).expect("a lookup"));
        assert_eq!(slots, vec![before]);
    }
}

#[test]
fn a_table_grows_across_many_calls_and_keeps_its_slots() {
    let mut keys = GroupKeys::new(&[DataType::Int64]).expect("a key table");
    for round in 0..3 {
        for start in (0..100_000_i64).step_by(4096) {
            let chunk: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (start..start + 4096).map(|v| v * 31),
            ));
            let (mut slots, mut created) = (Vec::new(), Vec::new());
            keys.find_or_create(&[chunk], &mut slots, &mut created)
                .expect("a lookup");
            let expected: Vec<usize> = (start..start + 4096).map(|v| v as usize).collect();
            assert_eq!(slots, expected);
            assert_eq!(created.len(), if round == 0 { 4096 } else { 0 });
        }
    }
    assert_eq!(keys.len(), 102_400);
}
