//! A hash table that numbers the distinct keys of rows, read a chunk of key columns at a
//! time, without a heap allocation per row.
//!
//! Each distinct key gets a slot: `0`, `1`, `2` and so on, in the order the keys were first
//! inserted. Two rows get one slot exactly when their keys are equal as Java's join hashers
//! compare them, which is the equality of [`Cell`](super::keys::Cell): a null equals only a
//! null, every NaN equals every NaN, and `-0.0` equals `0.0`. An operator keeps what it
//! knows about a key in vectors indexed by slot.
//!
//! Java generates one hasher class per combination of key types
//! (io.deephaven.engine.table.impl.by.typed.TypedHasherFactory). Here a key takes one of
//! three roads to one of two stores:
//!
//! | Key columns | A row's key is | Stored as |
//! | --- | --- | --- |
//! | one column of a primitive, boolean or time type | an `i64` | a `Vec<i64>` |
//! | one string column | the string's bytes, borrowed from the chunk | one byte buffer |
//! | anything else | the row's bytes in Arrow's row format, borrowed from the encoded chunk | one byte buffer |
//!
//! Hashes come from a fixed seed, so a table numbers the same keys the same way on every
//! run. The slot numbers do not depend on the hash at all.
//!
//! The table is not specific to joins: any operator that groups rows by key columns can
//! use it.

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array, Int64Array, StringArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Time64NanosecondType, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use arrow::row::{RowConverter, SortField};
use hashbrown::hash_table::Entry;
use hashbrown::HashTable;
use oxhaven_contracts::unported;
use std::sync::Arc;

/// What [`KeyTable::find`] answers for a row whose key the table does not have.
pub const NO_SLOT: u32 = u32::MAX;

/// The seeds are arbitrary and fixed (`PORTING.md`, rule 7).
const HASHER: RandomState = RandomState::with_seeds(
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
);

/// The keys that have a slot, hashed.
enum Store {
    /// No key has been inserted yet, so the kind of key is not known.
    Unknown,
    /// No key columns: every row has the one key, whose slot is 0.
    Unit,
    /// Each key with its slot. The key is in the table itself, so that looking one up
    /// touches one place in memory.
    Ints(HashTable<(i64, u32)>),
    /// Each slot with the place of its key: the key at place `n` is
    /// `data[ends[n - 1]..ends[n]]`.
    Bytes {
        slots: HashTable<(u32, u32)>,
        data: Vec<u8>,
        ends: Vec<usize>,
    },
}

/// The distinct keys seen so far, numbered.
pub struct KeyTable {
    store: Store,
    /// The number of slots given out.
    len: usize,
    /// The slot of the null key of a single key column, which is not in the store.
    null_slot: Option<u32>,
    /// A converter for each combination of column types met.
    converters: Vec<(Vec<DataType>, RowConverter)>,
}

/// One chunk of keys, ready to be hashed.
enum Chunk {
    Unit,
    Ints(Int64Array),
    Texts(StringArray),
    Rows(arrow::row::Rows),
}

impl Default for KeyTable {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyTable {
    pub fn new() -> Self {
        Self {
            store: Store::Unknown,
            len: 0,
            null_slot: None,
            converters: Vec::new(),
        }
    }

    /// The number of slots: every slot is below it.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no key has a slot.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The slot of each of the `rows` rows of the key `columns`, in row order. A key the
    /// table does not have gets the next free slot.
    ///
    /// # Panics
    /// With `unported!` for a column of a type that is not a key type, and if the columns
    /// are not of the types of the columns of earlier calls.
    pub fn insert(&mut self, columns: &[ArrayRef], rows: usize) -> Vec<u32> {
        self.slots(columns, rows, true)
    }

    /// As [`KeyTable::insert`], but no key gets a slot: a row whose key the table does
    /// not have gets [`NO_SLOT`].
    pub fn find(&mut self, columns: &[ArrayRef], rows: usize) -> Vec<u32> {
        self.slots(columns, rows, false)
    }

    fn slots(&mut self, columns: &[ArrayRef], rows: usize, insert: bool) -> Vec<u32> {
        if rows == 0 {
            return Vec::new();
        }
        let chunk = self.chunk(columns);
        if matches!(self.store, Store::Unknown) {
            if !insert {
                return vec![NO_SLOT; rows];
            }
            self.store = match chunk {
                Chunk::Unit => Store::Unit,
                Chunk::Ints(_) => Store::Ints(HashTable::new()),
                Chunk::Texts(_) | Chunk::Rows(_) => Store::Bytes {
                    slots: HashTable::new(),
                    data: Vec::new(),
                    ends: Vec::new(),
                },
            };
        }
        match chunk {
            Chunk::Unit => {
                self.len = 1;
                vec![0; rows]
            }
            Chunk::Ints(keys) => self.int_slots(&keys, insert),
            Chunk::Texts(keys) => {
                let texts = (0..keys.len()).map(|row| keys.value(row).as_bytes());
                self.byte_slots(texts, keys.nulls(), insert)
            }
            Chunk::Rows(keys) => self.byte_slots(keys.iter().map(|row| row.data()), None, insert),
        }
    }

    /// Brings the key columns into the form their kind of key is hashed in.
    fn chunk(&mut self, columns: &[ArrayRef]) -> Chunk {
        match columns {
            [] => Chunk::Unit,
            [column] => match as_ints(column) {
                Some(ints) => Chunk::Ints(ints),
                None => match column.data_type() {
                    DataType::Utf8 => Chunk::Texts(column.as_string::<i32>().clone()),
                    _ => Chunk::Rows(self.encode(columns)),
                },
            },
            _ => Chunk::Rows(self.encode(columns)),
        }
    }

    /// The rows of `columns` in Arrow's row format, in which two rows have the same bytes
    /// exactly when every column has equal values or nulls in both. Floating point columns
    /// are first brought to one NaN and one zero, and timestamps lose their time zone
    /// annotation, which is not part of their values.
    fn encode(&mut self, columns: &[ArrayRef]) -> arrow::row::Rows {
        let columns: Vec<ArrayRef> = columns.iter().map(canonical).collect();
        let types: Vec<DataType> = columns.iter().map(|c| c.data_type().clone()).collect();
        let known = self
            .converters
            .iter()
            .position(|(known, _)| *known == types);
        let index = known.unwrap_or_else(|| {
            let fields = types.iter().cloned().map(SortField::new).collect();
            let converter = match RowConverter::new(fields) {
                Ok(converter) => converter,
                Err(e) => panic!("the key columns passed the type check: {e}"),
            };
            self.converters.push((types, converter));
            self.converters.len() - 1
        });
        match self.converters[index].1.convert_columns(&columns) {
            Ok(rows) => rows,
            Err(e) => panic!("key columns of the converter's types: {e}"),
        }
    }

    /// The slot of the null key of a single key column.
    fn null_slot(&mut self, insert: bool) -> u32 {
        if self.null_slot.is_none() && insert {
            self.null_slot = Some(next_slot(&mut self.len));
        }
        self.null_slot.unwrap_or(NO_SLOT)
    }

    fn int_slots(&mut self, keys: &Int64Array, insert: bool) -> Vec<u32> {
        let mut slots = Vec::with_capacity(keys.len());
        for (row, &key) in keys.values().iter().enumerate() {
            if keys.is_null(row) {
                slots.push(self.null_slot(insert));
                continue;
            }
            let Store::Ints(table) = &mut self.store else {
                panic!("the kind of key of a table does not change");
            };
            let hash = HASHER.hash_one(key);
            let slot = if insert {
                let entry = table.entry(
                    hash,
                    |(stored, _)| *stored == key,
                    |(stored, _)| HASHER.hash_one(*stored),
                );
                match entry {
                    Entry::Occupied(found) => found.get().1,
                    Entry::Vacant(free) => {
                        let slot = next_slot(&mut self.len);
                        free.insert((key, slot));
                        slot
                    }
                }
            } else {
                let found = table.find(hash, |(stored, _)| *stored == key);
                found.map_or(NO_SLOT, |(_, slot)| *slot)
            };
            slots.push(slot);
        }
        slots
    }

    fn byte_slots<'a>(
        &mut self,
        keys: impl Iterator<Item = &'a [u8]>,
        nulls: Option<&NullBuffer>,
        insert: bool,
    ) -> Vec<u32> {
        fn stored<'s>(data: &'s [u8], ends: &[usize], index: u32) -> &'s [u8] {
            let index = index as usize;
            let start = if index == 0 { 0 } else { ends[index - 1] };
            &data[start..ends[index]]
        }
        let mut row_slots = Vec::with_capacity(keys.size_hint().0);
        for (row, key) in keys.enumerate() {
            if nulls.is_some_and(|nulls| nulls.is_null(row)) {
                row_slots.push(self.null_slot(insert));
                continue;
            }
            let Store::Bytes { slots, data, ends } = &mut self.store else {
                panic!("the kind of key of a table does not change");
            };
            // An entry is the slot of a key with the key's place in `ends`: the two differ
            // by one after the null key got its slot.
            let hash = HASHER.hash_one(key);
            let slot = if insert {
                let entry = slots.entry(
                    hash,
                    |(_, index)| stored(data, ends, *index) == key,
                    |(_, index)| HASHER.hash_one(stored(data, ends, *index)),
                );
                match entry {
                    Entry::Occupied(found) => found.get().0,
                    Entry::Vacant(free) => {
                        let slot = next_slot(&mut self.len);
                        let index = match u32::try_from(ends.len()) {
                            Ok(index) => index,
                            Err(_) => panic!("fewer keys than slots"),
                        };
                        free.insert((slot, index));
                        data.extend_from_slice(key);
                        ends.push(data.len());
                        slot
                    }
                }
            } else {
                let found = slots.find(hash, |(_, index)| stored(data, ends, *index) == key);
                found.map_or(NO_SLOT, |(slot, _)| *slot)
            };
            row_slots.push(slot);
        }
        row_slots
    }
}

/// Takes the next slot.
fn next_slot(len: &mut usize) -> u32 {
    match u32::try_from(*len) {
        Ok(slot) if slot != NO_SLOT => {
            *len += 1;
            slot
        }
        _ => panic!("more than {NO_SLOT} distinct keys"),
    }
}

/// The bits of `value`, with one pattern for every NaN and one for both zeros.
///
/// Behavior: the `eq` of io.deephaven.util.compare.DoubleComparisons and FloatComparisons
fn float_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

/// A single key column as `i64` keys, if it is of a type whose values are told apart by
/// 64 bits: the integers, `char`, `boolean`, the time types, and the floating point types
/// by their bits (a `float` widened to `double`, as [`Cell`](super::keys::Cell) has it).
fn as_ints(column: &ArrayRef) -> Option<Int64Array> {
    fn widen<T: ArrowPrimitiveType>(column: &ArrayRef) -> Int64Array
    where
        T::Native: Into<i64>,
    {
        column.as_primitive::<T>().unary(Into::into)
    }
    fn same<T: ArrowPrimitiveType<Native = i64>>(column: &ArrayRef) -> Int64Array {
        column.as_primitive::<T>().reinterpret_cast()
    }
    Some(match column.data_type() {
        DataType::Boolean => {
            let values = column.as_boolean();
            let ints = (0..values.len()).map(|row| i64::from(values.value(row)));
            Int64Array::new(ints.collect(), values.nulls().cloned())
        }
        DataType::Int8 => widen::<Int8Type>(column),
        DataType::Int16 => widen::<Int16Type>(column),
        DataType::Int32 => widen::<Int32Type>(column),
        DataType::Int64 => same::<Int64Type>(column),
        DataType::UInt16 => widen::<UInt16Type>(column),
        DataType::Date32 => widen::<Date32Type>(column),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => same::<TimestampNanosecondType>(column),
        DataType::Time64(TimeUnit::Nanosecond) => same::<Time64NanosecondType>(column),
        DataType::Duration(TimeUnit::Nanosecond) => same::<DurationNanosecondType>(column),
        DataType::Float32 => column
            .as_primitive::<Float32Type>()
            .unary(|value| float_bits(value.into()) as i64),
        DataType::Float64 => column
            .as_primitive::<Float64Type>()
            .unary(|value| float_bits(value) as i64),
        _ => return None,
    })
}

/// `column` as it is encoded among several key columns: a floating point column with one
/// NaN and one zero, every other column as it is.
///
/// # Panics
/// With `unported!` for a column of a type that joins do not take as a key.
fn canonical(column: &ArrayRef) -> ArrayRef {
    /// The types [`Cell`](super::keys::Cell) has an equality for. A `ZonedDateTime` and a
    /// `Period` are structs of them.
    fn is_key_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Date32
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Duration(TimeUnit::Nanosecond) => true,
            DataType::Struct(fields) => fields.iter().all(|field| {
                // No struct that is a column type has a floating point field, and one that
                // had would need its NaNs and zeros brought to one form here.
                !field.data_type().is_floating() && is_key_type(field.data_type())
            }),
            _ => false,
        }
    }
    match column.data_type() {
        DataType::Float32 => {
            let values: &Float32Array = column.as_primitive();
            Arc::new(values.unary::<_, Float32Type>(|value| {
                if value.is_nan() {
                    f32::NAN
                } else if value == 0.0 {
                    0.0
                } else {
                    value
                }
            }))
        }
        DataType::Float64 => {
            let values: &Float64Array = column.as_primitive();
            Arc::new(values.unary::<_, Float64Type>(|value| f64::from_bits(float_bits(value))))
        }
        // A time zone is an annotation of the column, not part of its values.
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            let values = column.as_primitive::<TimestampNanosecondType>();
            Arc::new(values.clone().with_timezone_opt(None::<Arc<str>>))
        }
        other if is_key_type(other) => column.clone(),
        other => unported!(
            "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#makeNaturalJoin",
            "a match column stored as {other}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Int16Array, Int32Array, Int8Array, StructArray,
        TimestampNanosecondArray, UInt16Array,
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
}
