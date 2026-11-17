//! The tests task 9.1 wrote for the joins' key table (`ops/join/key_table.rs`), moved here
//! with the table that replaced it. Only the imports changed.

use super::*;
use arrow::array::{
    BooleanArray, Date32Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    StructArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::Field;

fn slots(table: &mut KeyTable, column: ArrayRef) -> Vec<u32> {
    let rows = column.len();
    table.insert(&[column], rows)
}

#[test]
fn slots_are_numbered_in_order_of_first_appearance() {
    let mut table = KeyTable::new();
    let first: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), Some(-1), Some(7), None]));
    assert_eq!(slots(&mut table, first), [0, 1, 0, 2]);
    let second: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(9), Some(-1)]));
    assert_eq!(
        table.find(std::slice::from_ref(&second), 3),
        [2, NO_SLOT, 1]
    );
    assert_eq!(table.len(), 3);
    assert_eq!(slots(&mut table, second), [2, 3, 1]);
    assert_eq!(table.len(), 4);
}

#[test]
fn find_in_an_empty_table_finds_nothing() {
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
    let texts: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
    assert_eq!(
        KeyTable::new().find(std::slice::from_ref(&ints), 2),
        [NO_SLOT; 2]
    );
    assert_eq!(
        KeyTable::new().find(std::slice::from_ref(&texts), 2),
        [NO_SLOT; 2]
    );
    assert_eq!(KeyTable::new().find(&[ints, texts], 2), [NO_SLOT; 2]);
    assert_eq!(KeyTable::new().find(&[], 2), [NO_SLOT; 2]);
    assert_eq!(KeyTable::new().len(), 0);
}

#[test]
fn without_key_columns_every_row_has_the_one_key() {
    let mut table = KeyTable::new();
    assert_eq!(table.insert(&[], 3), [0, 0, 0]);
    assert_eq!(table.find(&[], 2), [0, 0]);
    assert_eq!(table.len(), 1);
}

/// The null key and the value a null happens to be stored over are two keys, and so are
/// the null string and the empty string.
#[test]
fn null_is_a_key_of_its_own() {
    let mut table = KeyTable::new();
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(0), None, Some(0)]));
    assert_eq!(slots(&mut table, ints), [0, 1, 0, 1]);

    let mut table = KeyTable::new();
    let texts: ArrayRef = Arc::new(StringArray::from(vec![None, Some(""), Some("a"), None]));
    assert_eq!(slots(&mut table, texts), [0, 1, 2, 0]);
    let more: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("")]));
    assert_eq!(slots(&mut table, more), [2, 3, 1]);
}

/// Behavior: io.deephaven.util.compare.DoubleComparisons#eq and FloatComparisons#eq
#[test]
fn every_nan_is_one_key_and_the_zeros_are_one_key() {
    let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
    let doubles = vec![
        Some(f64::NAN),
        Some(other_nan),
        Some(0.0),
        Some(-0.0),
        None,
        Some(1.5),
        Some(f64::INFINITY),
        Some(-f64::NAN),
    ];
    let expected = [0, 0, 1, 1, 2, 3, 4, 0];
    let column: ArrayRef = Arc::new(Float64Array::from(doubles.clone()));
    assert_eq!(slots(&mut KeyTable::new(), column.clone()), expected);

    let floats: Vec<Option<f32>> = doubles.iter().map(|v| v.map(|v| v as f32)).collect();
    let narrow: ArrayRef = Arc::new(Float32Array::from(floats));
    assert_eq!(slots(&mut KeyTable::new(), narrow.clone()), expected);

    // The same among several key columns, where the rows are encoded.
    let ones: ArrayRef = Arc::new(Int32Array::from(vec![1; 8]));
    let mut table = KeyTable::new();
    assert_eq!(table.insert(&[ones.clone(), column], 8), expected);
    let mut table = KeyTable::new();
    assert_eq!(table.insert(&[narrow, ones], 8), expected);
}

#[test]
fn every_single_column_type_has_slots() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int8Array::from(vec![Some(-7), None, Some(7), Some(-7)])),
        Arc::new(Int16Array::from(vec![
            Some(300),
            None,
            Some(-300),
            Some(300),
        ])),
        Arc::new(UInt16Array::from(vec![
            Some(0xFFFE),
            None,
            Some(0),
            Some(0xFFFE),
        ])),
        Arc::new(Date32Array::from(vec![Some(-1), None, Some(1), Some(-1)])),
        Arc::new(
            TimestampNanosecondArray::from(vec![Some(5), None, Some(6), Some(5)])
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("y"),
            Some("x"),
        ])),
    ];
    for column in columns {
        let name = column.data_type().to_string();
        assert_eq!(slots(&mut KeyTable::new(), column), [0, 1, 2, 0], "{name}");
    }
}

/// A row of several columns is a key by all of them, and a null in one column is not
/// the null of another.
#[test]
fn several_columns_are_one_key() {
    let k: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(1),
        None,
        Some(1),
        None,
    ]));
    let s: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("b"),
        Some("a"),
        Some("a"),
        None,
    ]));
    let mut table = KeyTable::new();
    assert_eq!(table.insert(&[k.clone(), s.clone()], 5), [0, 1, 2, 0, 3]);
    assert_eq!(table.find(&[k.slice(2, 3), s.slice(2, 3)], 3), [2, 0, 3]);
    // The columns in the other order are other keys.
    assert_eq!(KeyTable::new().insert(&[s, k], 5), [0, 1, 2, 0, 3]);
}

/// Columns that differ only in their time zone annotation hold the same keys.
#[test]
fn a_time_zone_annotation_is_not_part_of_a_key() {
    let plain = TimestampNanosecondArray::from(vec![Some(5), Some(6)]);
    let zoned: ArrayRef = Arc::new(plain.clone().with_timezone("Asia/Tokyo"));
    let plain: ArrayRef = Arc::new(plain);
    let ones: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
    let mut table = KeyTable::new();
    assert_eq!(table.insert(&[ones.clone(), plain], 2), [0, 1]);
    assert_eq!(table.find(&[ones, zoned], 2), [0, 1]);
}

/// A struct column (a `ZonedDateTime` or a `Period`) is a key by its fields, and a null
/// struct is one key whatever is stored under it.
#[test]
fn a_struct_is_a_key_by_its_fields() {
    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let a: ArrayRef = Arc::new(Int64Array::from(vec![5, 5, 5, 7, 8]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "x", "x", "y"]));
    let nulls = NullBuffer::from(vec![true, true, true, false, false]);
    let column: ArrayRef = Arc::new(StructArray::new(fields.into(), vec![a, b], Some(nulls)));
    assert_eq!(slots(&mut KeyTable::new(), column), [0, 1, 0, 2, 2]);
}

#[test]
#[should_panic(expected = "a match column stored as")]
fn a_column_of_another_type_is_unported() {
    let column: ArrayRef = Arc::new(arrow::array::BinaryArray::from_vec(vec![b"a"]));
    slots(&mut KeyTable::new(), column);
}

/// Many keys, so that the table grows and rehashes from the stored keys.
#[test]
fn a_large_table_keeps_its_slots() {
    let mut table = KeyTable::new();
    let keys: ArrayRef = Arc::new(Int64Array::from_iter_values((0..5000).map(|i| i * 31)));
    let expected: Vec<u32> = (0..5000).collect();
    assert_eq!(slots(&mut table, keys.clone()), expected);
    assert_eq!(slots(&mut table, keys), expected);

    let mut table = KeyTable::new();
    let texts: Vec<String> = (0..5000).map(|i| format!("key{i}")).collect();
    let keys: ArrayRef = Arc::new(StringArray::from_iter_values(&texts));
    assert_eq!(slots(&mut table, keys.clone()), expected);
    assert_eq!(table.find(&[keys], 5000), expected);
}
