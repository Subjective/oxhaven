//! [`KeyTable`] against what it replaced: the keys the joins compared cell by cell before
//! any of them hashed (`ops::join::keys::Cell`, whose equality is Java's), numbered in
//! the order they first appear. Every shape of key goes through both ways of making a
//! table, both ways of asking, and tables small and large.

use super::*;
use crate::ops::join::keys::{keys_of, Key};
use arrow::array::{
    Date32Array, DurationNanosecondArray, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, StructArray, Time64NanosecondArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{Field, Fields};
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

/// The kinds of key column.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    Char,
    Float,
    Double,
    Text,
    Instant,
    InstantWithZone,
    Date,
    Time,
    Duration,
    /// A struct of an instant and a zone name, as a `ZonedDateTime` is stored.
    Zoned,
    /// A struct of three ints, as a `Period` is stored.
    Period,
}

const KINDS: [Kind; 16] = [
    Kind::Boolean,
    Kind::Byte,
    Kind::Short,
    Kind::Int,
    Kind::Long,
    Kind::Char,
    Kind::Float,
    Kind::Double,
    Kind::Text,
    Kind::Instant,
    Kind::InstantWithZone,
    Kind::Date,
    Kind::Time,
    Kind::Duration,
    Kind::Zoned,
    Kind::Period,
];

/// The doubles that key equality treats specially, and some that it does not.
const DOUBLES: [f64; 10] = [
    0.0,
    -0.0,
    f64::NAN,
    f64::INFINITY,
    f64::NEG_INFINITY,
    1.5,
    -1.5,
    f64::MIN_POSITIVE,
    1e300,
    2.0,
];

/// A column of `rows` values of `kind` drawn from `domain` values, about one in seven
/// null.
fn column(kind: Kind, rows: usize, domain: u64, random: &mut Random) -> ArrayRef {
    let draw = |random: &mut Random| -> Option<i64> {
        (random.below(7) != 0).then(|| random.below(domain) as i64 - (domain / 2) as i64)
    };
    let ints =
        |random: &mut Random| -> Vec<Option<i64>> { (0..rows).map(|_| draw(random)).collect() };
    let double = |value: i64, random: &mut Random| -> f64 {
        let special = DOUBLES[value.unsigned_abs() as usize % DOUBLES.len()];
        match random.below(4) {
            // Another NaN, with a payload and maybe a sign.
            0 if special.is_nan() => f64::from_bits(f64::NAN.to_bits() | 1 | (random.next() << 63)),
            0 | 1 => special,
            _ => value as f64 / 4.0,
        }
    };
    match kind {
        Kind::Boolean => {
            let values = ints(random).into_iter().map(|v| v.map(|v| v % 2 == 0));
            Arc::new(values.collect::<BooleanArray>())
        }
        Kind::Byte => Arc::new(Int8Array::from_iter(
            ints(random).into_iter().map(|v| v.map(|v| v as i8)),
        )),
        Kind::Short => Arc::new(Int16Array::from_iter(
            ints(random).into_iter().map(|v| v.map(|v| v as i16)),
        )),
        Kind::Int => Arc::new(Int32Array::from_iter(
            ints(random).into_iter().map(|v| v.map(|v| v as i32)),
        )),
        Kind::Long => Arc::new(Int64Array::from_iter(
            ints(random)
                .into_iter()
                .map(|v| v.map(|v| v.wrapping_mul(0x1_0000_0001))),
        )),
        Kind::Char => Arc::new(UInt16Array::from_iter(
            ints(random).into_iter().map(|v| v.map(|v| v as u16)),
        )),
        Kind::Float => Arc::new(Float32Array::from_iter(
            ints(random)
                .into_iter()
                .map(|v| v.map(|v| double(v, random) as f32))
                .collect::<Vec<_>>(),
        )),
        Kind::Double => Arc::new(Float64Array::from_iter(
            ints(random)
                .into_iter()
                .map(|v| v.map(|v| double(v, random)))
                .collect::<Vec<_>>(),
        )),
        Kind::Text => Arc::new(StringArray::from_iter(ints(random).into_iter().map(|v| {
            v.map(|v| match v.rem_euclid(5) {
                0 => String::new(),
                1 => format!("k{v}"),
                _ => format!("a longer key {v} \u{e9}"),
            })
        }))),
        Kind::Instant => Arc::new(TimestampNanosecondArray::from(ints(random))),
        Kind::InstantWithZone => {
            Arc::new(TimestampNanosecondArray::from(ints(random)).with_timezone("Asia/Tokyo"))
        }
        Kind::Date => Arc::new(Date32Array::from_iter(
            ints(random).into_iter().map(|v| v.map(|v| v as i32)),
        )),
        Kind::Time => Arc::new(Time64NanosecondArray::from(ints(random))),
        Kind::Duration => Arc::new(DurationNanosecondArray::from(ints(random))),
        Kind::Zoned => {
            let outer = ints(random);
            let instants: ArrayRef = Arc::new(TimestampNanosecondArray::from(
                outer.iter().map(|v| v.unwrap_or(77)).collect::<Vec<_>>(),
            ));
            let zones: ArrayRef = Arc::new(StringArray::from_iter_values(
                (0..rows).map(|_| ["UTC", "Asia/Tokyo"][random.below(2) as usize]),
            ));
            let fields: Fields = vec![
                Field::new("instant", instants.data_type().clone(), true),
                Field::new("zone", DataType::Utf8, true),
            ]
            .into();
            let nulls = NullBuffer::from(outer.iter().map(Option::is_some).collect::<Vec<_>>());
            Arc::new(StructArray::new(fields, vec![instants, zones], Some(nulls)))
        }
        Kind::Period => {
            let outer = ints(random);
            let part = |random: &mut Random| -> ArrayRef {
                Arc::new(Int32Array::from_iter_values(
                    (0..rows).map(|_| random.below(3) as i32),
                ))
            };
            let parts = vec![part(random), part(random), part(random)];
            let fields: Fields = ["years", "months", "days"]
                .iter()
                .map(|name| Field::new(*name, DataType::Int32, true))
                .collect::<Vec<_>>()
                .into();
            let nulls = NullBuffer::from(outer.iter().map(Option::is_some).collect::<Vec<_>>());
            Arc::new(StructArray::new(fields, parts, Some(nulls)))
        }
    }
}

/// Numbers keys as the old paths did: by comparing whole keys, in order of appearance.
#[derive(Default)]
struct Model {
    slots: std::collections::BTreeMap<Key, u32>,
}

impl Model {
    fn find(&self, key: &Key) -> u32 {
        self.slots.get(key).copied().unwrap_or(NO_SLOT)
    }

    fn insert(&mut self, key: Key) -> u32 {
        let next = self.slots.len() as u32;
        *self.slots.entry(key).or_insert(next)
    }
}

/// Runs `kinds` as the key columns of one table through several chunks, and returns how
/// many rows shared a slot with an earlier row.
fn check(
    kinds: &[Kind],
    declared: bool,
    collide: bool,
    domain: u64,
    chunk_rows: u64,
    seed: u64,
) -> u64 {
    let what = format!("{kinds:?}, declared {declared}, collide {collide}, seed {seed}");
    let mut random = Random(0x9e37_79b9_7f4a_7c15 ^ (seed + 1).wrapping_mul(0x2545_f491_4f6c_dd1d));
    let mut table = None;
    let mut model = Model::default();
    let mut repeats = 0;
    for _ in 0..2 + random.below(4) {
        let rows = 1 + random.below(chunk_rows) as usize;
        let columns: Vec<ArrayRef> = kinds
            .iter()
            .map(|&kind| column(kind, rows, domain, &mut random))
            .collect();
        let table = table.get_or_insert_with(|| {
            let mut table = if declared {
                let types: Vec<DataType> = columns.iter().map(|c| c.data_type().clone()).collect();
                KeyTable::for_types(&types).expect("a key table")
            } else {
                KeyTable::new()
            };
            table.collide = collide;
            table
        });
        let keys = keys_of(&columns, rows);

        // Looked up first: only the keys of earlier chunks have slots.
        let expected: Vec<u32> = keys.iter().map(|key| model.find(key)).collect();
        assert_eq!(table.find(&columns, rows), expected, "find, {what}");
        let mut found: Vec<usize> = vec![5];
        let all = table.find_all(&columns, &mut found).expect("a lookup");
        assert_eq!(all, expected.iter().all(|slot| *slot != NO_SLOT), "{what}");
        let as_usize = |slot: &u32| {
            if *slot == NO_SLOT {
                usize::MAX
            } else {
                *slot as usize
            }
        };
        assert_eq!(found[0], 5, "appends, {what}");
        assert_eq!(
            found[1..],
            expected.iter().map(as_usize).collect::<Vec<_>>(),
            "{what}"
        );
        assert_eq!(
            table.len(),
            model.slots.len(),
            "a lookup makes no slot, {what}"
        );

        // Then inserted: half of the chunk one way, half the other.
        let before = model.slots.len() as u32;
        let expected: Vec<u32> = keys.into_iter().map(|key| model.insert(key)).collect();
        repeats += expected.iter().filter(|slot| **slot < before).count() as u64;
        let half = rows / 2;
        let head: Vec<ArrayRef> = columns.iter().map(|c| c.slice(0, half)).collect();
        let tail: Vec<ArrayRef> = columns.iter().map(|c| c.slice(half, rows - half)).collect();
        assert_eq!(
            table.insert(&head, half),
            expected[..half],
            "insert, {what}"
        );
        let (mut slots, mut created): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
        let first_new = table.len();
        table
            .find_or_create(&tail, &mut slots, &mut created)
            .expect("a lookup");
        let expected_tail: Vec<usize> = expected[half..].iter().map(as_usize).collect();
        assert_eq!(slots, expected_tail, "find_or_create, {what}");
        let mut next = first_new;
        let expected_created: Vec<usize> = (0..expected_tail.len())
            .filter(|&position| {
                let new = expected_tail[position] == next;
                next += usize::from(new);
                new
            })
            .collect();
        assert_eq!(created, expected_created, "created, {what}");
        assert_eq!(table.len(), model.slots.len(), "{what}");
        assert_eq!(table.find(&columns, rows), expected, "found again, {what}");
    }
    repeats
}

/// Each kind of column alone, in both kinds of table.
#[test]
fn one_key_column_of_every_kind_is_numbered_as_its_keys_compare() {
    for kind in KINDS {
        let mut repeats = 0;
        for seed in test_seeds(0..30) {
            for declared in [false, true] {
                repeats += check(&[kind], declared, false, 40, 300, seed);
            }
        }
        assert!(repeats > 100, "{kind:?}: {repeats} rows repeated a key");
    }
}

/// Pairs and triples of columns, which are encoded as rows.
#[test]
fn several_key_columns_are_numbered_as_their_keys_compare() {
    let mut repeats = 0;
    for seed in test_seeds(0..40) {
        let mut random = Random(seed * 2 + 1);
        for first in KINDS {
            let second = KINDS[random.below(KINDS.len() as u64) as usize];
            let third = KINDS[random.below(KINDS.len() as u64) as usize];
            for declared in [false, true] {
                repeats += check(&[first, second], declared, false, 4, 200, seed);
                repeats += check(&[second, first, third], declared, false, 3, 200, seed);
            }
        }
    }
    assert!(repeats > 1000, "{repeats} rows repeated a key");
}

/// With one hash for every key, a lookup passes every other key before it finds its own.
#[test]
fn keys_with_one_hash_are_numbered_as_their_keys_compare() {
    for seed in test_seeds(0..10) {
        for kinds in [
            &[Kind::Int][..],
            &[Kind::Double],
            &[Kind::Text],
            &[Kind::Float, Kind::Text],
        ] {
            for declared in [false, true] {
                check(kinds, declared, true, 30, 200, seed);
            }
        }
    }
}

/// Tables past the size from which buckets are read ahead, which also grow many times.
#[test]
fn a_table_larger_than_the_caches_is_numbered_as_its_keys_compare() {
    for seed in test_seeds(0..5) {
        for kinds in [
            &[Kind::Long][..],
            &[Kind::Double],
            &[Kind::Text],
            &[Kind::Int, Kind::Text],
        ] {
            let repeats = check(kinds, seed % 2 == 0, false, 200_000, 60_000, seed);
            assert!(repeats > 100, "{kinds:?}: {repeats} rows repeated a key");
        }
    }
}

/// A table made without types takes the columns of two tables whose timestamps differ in
/// their time zone annotation, alone and among other columns, and a `float` column's
/// keys are those of its values as doubles.
#[test]
fn an_annotation_is_not_part_of_a_key() {
    let plain: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(5), None, Some(6)]));
    let zoned: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![Some(6), Some(5), None]).with_timezone("Asia/Tokyo"),
    );
    let mut table = KeyTable::new();
    assert_eq!(table.insert(std::slice::from_ref(&plain), 3), [0, 1, 2]);
    assert_eq!(table.find(std::slice::from_ref(&zoned), 3), [2, 0, 1]);
    let ones: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 1]));
    let mut table = KeyTable::new();
    assert_eq!(table.insert(&[plain, ones.clone()], 3), [0, 1, 2]);
    assert_eq!(table.find(&[zoned, ones], 3), [2, 0, 1]);
}

/// What a table made for declared types answers for columns that are not of them.
#[test]
fn columns_of_other_types_than_declared_are_an_error() {
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let texts: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let mut slots: Vec<usize> = Vec::new();
    let mut table = KeyTable::for_types(&[DataType::Int32]).expect("a key table");
    let error = table
        .find_all(std::slice::from_ref(&texts), &mut slots)
        .expect_err("a string column");
    assert!(error
        .message
        .contains("where one of another type was declared"));
    let mut table = KeyTable::for_types(&[DataType::Utf8]).expect("a key table");
    let error = table
        .find_all(std::slice::from_ref(&ints), &mut slots)
        .expect_err("an int column");
    assert!(error.message.contains("is not a string array"));
    let mut table = KeyTable::for_types(&[DataType::Int32, DataType::Utf8]).expect("a table");
    let short: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let error = table
        .find_all(&[ints, short], &mut slots)
        .expect_err("columns of two lengths");
    assert!(error.message.contains("different lengths"));
}
