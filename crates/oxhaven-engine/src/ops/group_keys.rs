//! Group keys to dense slots: the hash table under aggregations, `select_distinct` and
//! `head_by`/`tail_by`.
//!
//! [`GroupKeys`] gives each distinct key of one or more key columns a slot, `0, 1, 2, ...`
//! in the order keys are first seen, and finds the slot of a key again. A key keeps its
//! slot for the life of the table, also while it has no rows, which is what keeps a group
//! at its row of an aggregation's result.
//!
//! Keys are compared by the grouping rule of PARITY.md ("Keys"): null is a value equal
//! only to itself, every NaN is one key, and `-0.0` is the same key as `0.0`. Nothing is
//! allocated per row. There are three layouts, chosen by the key columns:
//!
//! - one column whose values fit in 64 bits (the integer types, `char`, `float`, `double`,
//!   `Boolean`, and the time types held as integers): the key is the value's bits;
//! - one string column: the keys' bytes, one after another in one buffer;
//! - anything else: each row encoded by Arrow's row format, the bytes kept the same way.
//!
//! The table is open-addressed with linear probing and holds slot numbers; the keys live
//! in arrays indexed by slot. Hashes come from `ahash` with fixed seeds (PORTING.md rule 7),
//! and only decide where a key is stored in the table, never an order anyone sees.

use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Time64NanosecondType, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use arrow::row::{RowConverter, SortField};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// An empty bucket of the open-addressed table.
const EMPTY: u32 = u32::MAX;

/// The fixed seeds of the hasher. Any constants do; these are digits of pi.
const SEEDS: [u64; 4] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];

/// How the keys of each slot are kept.
enum Layout {
    /// One key column of 64-bit-or-narrower values: each slot's value, canonical bits.
    Word { values: Vec<u64> },
    /// One string column: slot `s` is `bytes[ends[s - 1]..ends[s]]`.
    Text { bytes: Vec<u8>, ends: Vec<usize> },
    /// Any key columns, each row in Arrow's row format, kept as for `Text`.
    Rows {
        converter: RowConverter,
        bytes: Vec<u8>,
        ends: Vec<usize>,
    },
}

/// The slot of every key seen so far. See the module documentation.
///
/// Behavior: io.deephaven.engine.table.impl.by.OperatorAggregationStateManager, whose
/// `outputPosition` is the slot, with the key equality of the hashers that
/// io.deephaven.engine.table.impl.by.typed.TypedHasherFactory generates
/// (io.deephaven.util.compare.DoubleComparisons#eq and its siblings)
pub(crate) struct GroupKeys {
    layout: Layout,
    /// The hash of each slot's key, so that growing the table hashes nothing again.
    hashes: Vec<u64>,
    /// Slot numbers, or `EMPTY`; its length is a power of two.
    buckets: Vec<u32>,
    /// For `Word` and `Text`, the slot of the null key, which is not in `buckets`.
    null_slot: Option<u32>,
    hasher: ahash::RandomState,
    /// For tests: every key hashes alike, so every lookup probes past every other key.
    #[cfg(test)]
    collide: bool,
}

/// Whether `data_type` is a column type of [`Layout::Word`].
fn is_word(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Duration(TimeUnit::Nanosecond)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
    )
}

/// The canonical bits of a `float`: every NaN one value, `-0.0` as `0.0`.
fn float_bits(value: f32) -> u64 {
    if value.is_nan() {
        u64::from(f32::NAN.to_bits())
    } else if value == 0.0 {
        0
    } else {
        u64::from(value.to_bits())
    }
}

/// The canonical bits of a `double`: every NaN one value, `-0.0` as `0.0`.
fn double_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// What a lookup does with a key that has no slot.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Absent {
    /// Give it the next slot.
    Create,
    /// Report it.
    Missing,
}

/// The result of probing for one key.
enum Probe {
    Found(u32),
    /// The empty bucket where the key would go.
    Vacant(usize),
}

impl GroupKeys {
    /// A table for keys of columns of the Arrow types `data_types`, in that order.
    pub fn new(data_types: &[DataType]) -> Result<GroupKeys, TableFailure> {
        let layout = match data_types {
            [one] if is_word(one) => Layout::Word { values: Vec::new() },
            [DataType::Utf8] => Layout::Text {
                bytes: Vec::new(),
                ends: Vec::new(),
            },
            _ => {
                let fields = data_types
                    .iter()
                    .map(|data_type| SortField::new(data_type.clone()))
                    .collect();
                Layout::Rows {
                    converter: RowConverter::new(fields)
                        .map_err(|error| failure(error.to_string()))?,
                    bytes: Vec::new(),
                    ends: Vec::new(),
                }
            }
        };
        Ok(GroupKeys {
            layout,
            hashes: Vec::new(),
            buckets: vec![EMPTY; 16],
            null_slot: None,
            hasher: ahash::RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]),
            #[cfg(test)]
            collide: false,
        })
    }

    #[inline]
    fn hash(&self, key: impl std::hash::Hash) -> u64 {
        #[cfg(test)]
        if self.collide {
            return 7;
        }
        self.hasher.hash_one(key)
    }

    /// The number of slots handed out.
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// The slot of each row of `key_columns`, appended to `slots`, with a new slot for each
    /// key not seen before. The positions of the rows that made a new slot are appended to
    /// `created`, in slot order.
    pub fn find_or_create(
        &mut self,
        key_columns: &[ArrayRef],
        slots: &mut Vec<usize>,
        created: &mut Vec<usize>,
    ) -> Result<(), TableFailure> {
        let mut missing = false;
        self.lookup(key_columns, Absent::Create, slots, created, &mut missing)
    }

    /// The slot of each row of `key_columns`, appended to `slots`. Returns `false`, with
    /// `slots` in an unspecified state, if some key has no slot.
    pub fn find(
        &mut self,
        key_columns: &[ArrayRef],
        slots: &mut Vec<usize>,
    ) -> Result<bool, TableFailure> {
        let mut missing = false;
        self.lookup(
            key_columns,
            Absent::Missing,
            slots,
            &mut Vec::new(),
            &mut missing,
        )?;
        Ok(!missing)
    }

    fn lookup(
        &mut self,
        key_columns: &[ArrayRef],
        absent: Absent,
        slots: &mut Vec<usize>,
        created: &mut Vec<usize>,
        missing: &mut bool,
    ) -> Result<(), TableFailure> {
        let rows = key_columns.first().map_or(0, |column| column.len());
        if key_columns.iter().any(|column| column.len() != rows) {
            return Err(failure("group keys: key columns of different lengths"));
        }
        slots.reserve(rows);
        let mut first_slot = self.len();
        match &self.layout {
            Layout::Word { .. } => self.lookup_words(&key_columns[0], absent, slots, missing)?,
            Layout::Text { .. } => {
                let strings = key_columns[0].as_string_opt::<i32>().ok_or_else(|| {
                    failure("group keys: a string key column is not a string array")
                })?;
                let nulls = strings.logical_nulls();
                for row in 0..rows {
                    let key = match &nulls {
                        Some(nulls) if nulls.is_null(row) => None,
                        _ => Some(strings.value(row).as_bytes()),
                    };
                    slots.push(self.slot_of_bytes(key, absent, missing));
                }
            }
            Layout::Rows { converter, .. } => {
                let canonical: Vec<ArrayRef> = key_columns.iter().map(canonical_floats).collect();
                let encoded = converter
                    .convert_columns(&canonical)
                    .map_err(|error| failure(error.to_string()))?;
                for row in encoded.iter() {
                    slots.push(self.slot_of_bytes(Some(row.as_ref()), absent, missing));
                }
            }
        }
        if absent == Absent::Create {
            // The rows that made a slot are the first with each slot at or past the old
            // length; slots are made in row order, so they are found in slot order.
            let start = slots.len() - rows;
            for (position, &slot) in slots[start..].iter().enumerate() {
                if slot == first_slot {
                    created.push(position);
                    first_slot += 1;
                }
            }
        }
        Ok(())
    }

    fn lookup_words(
        &mut self,
        column: &ArrayRef,
        absent: Absent,
        slots: &mut Vec<usize>,
        missing: &mut bool,
    ) -> Result<(), TableFailure> {
        fn each<T: ArrowPrimitiveType>(
            keys: &mut GroupKeys,
            column: &ArrayRef,
            bits: impl Fn(T::Native) -> u64,
            absent: Absent,
            slots: &mut Vec<usize>,
            missing: &mut bool,
        ) {
            let values = column.as_primitive::<T>();
            match values.nulls().filter(|nulls| nulls.null_count() > 0) {
                None => {
                    for &value in values.values().iter() {
                        slots.push(keys.slot_of_word(Some(bits(value)), absent, missing));
                    }
                }
                Some(nulls) => {
                    for (row, &value) in values.values().iter().enumerate() {
                        let key = nulls.is_valid(row).then(|| bits(value));
                        slots.push(keys.slot_of_word(key, absent, missing));
                    }
                }
            }
        }
        let signed = |value: i64| value as u64;
        match column.data_type() {
            DataType::Boolean => {
                let values = column.as_boolean();
                for row in 0..values.len() {
                    let key = values.is_valid(row).then(|| u64::from(values.value(row)));
                    slots.push(self.slot_of_word(key, absent, missing));
                }
            }
            DataType::Int8 => {
                each::<Int8Type>(self, column, |v| signed(v.into()), absent, slots, missing)
            }
            DataType::Int16 => {
                each::<Int16Type>(self, column, |v| signed(v.into()), absent, slots, missing)
            }
            DataType::Int32 => {
                each::<Int32Type>(self, column, |v| signed(v.into()), absent, slots, missing)
            }
            DataType::Int64 => each::<Int64Type>(self, column, signed, absent, slots, missing),
            DataType::UInt16 => {
                each::<UInt16Type>(self, column, |v| v.into(), absent, slots, missing)
            }
            DataType::Float32 => each::<Float32Type>(self, column, float_bits, absent, slots, missing),
            DataType::Float64 => {
                each::<Float64Type>(self, column, double_bits, absent, slots, missing)
            }
            DataType::Date32 => {
                each::<Date32Type>(self, column, |v| signed(v.into()), absent, slots, missing)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                each::<Time64NanosecondType>(self, column, signed, absent, slots, missing)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                each::<DurationNanosecondType>(self, column, signed, absent, slots, missing)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                each::<TimestampNanosecondType>(self, column, signed, absent, slots, missing)
            }
            other => {
                return Err(failure(format!(
                    "group keys: a key column of Arrow type {other} where one of another type was declared"
                )))
            }
        }
        Ok(())
    }

    /// The slot of the null key, made if `absent` says so.
    fn null(&mut self, absent: Absent, missing: &mut bool) -> usize {
        if let Some(slot) = self.null_slot {
            return slot as usize;
        }
        if absent == Absent::Missing {
            *missing = true;
            return usize::MAX;
        }
        // The null key is not in `buckets`; its key storage holds a placeholder.
        let slot = self.hashes.len() as u32;
        match &mut self.layout {
            Layout::Word { values } => values.push(0),
            Layout::Text { bytes, ends } | Layout::Rows { bytes, ends, .. } => {
                ends.push(bytes.len())
            }
        }
        self.hashes.push(0);
        self.null_slot = Some(slot);
        slot as usize
    }

    #[inline]
    fn slot_of_word(&mut self, key: Option<u64>, absent: Absent, missing: &mut bool) -> usize {
        let Some(key) = key else {
            return self.null(absent, missing);
        };
        let hash = self.hash(key);
        let Layout::Word { values } = &self.layout else {
            unreachable!("a word key in a table of other keys")
        };
        let probe = probe(&self.buckets, hash, |slot| values[slot as usize] == key);
        match probe {
            Probe::Found(slot) => slot as usize,
            Probe::Vacant(_) if absent == Absent::Missing => {
                *missing = true;
                usize::MAX
            }
            Probe::Vacant(bucket) => {
                let Layout::Word { values } = &mut self.layout else {
                    unreachable!()
                };
                values.push(key);
                self.insert(bucket, hash)
            }
        }
    }

    #[inline]
    fn slot_of_bytes(&mut self, key: Option<&[u8]>, absent: Absent, missing: &mut bool) -> usize {
        let Some(key) = key else {
            return self.null(absent, missing);
        };
        let hash = self.hash(key);
        let (Layout::Text { bytes, ends } | Layout::Rows { bytes, ends, .. }) = &self.layout else {
            unreachable!("a byte key in a table of word keys")
        };
        let hashes = &self.hashes;
        let probe = probe(&self.buckets, hash, |slot| {
            let slot = slot as usize;
            let start = if slot == 0 { 0 } else { ends[slot - 1] };
            hashes[slot] == hash && &bytes[start..ends[slot]] == key
        });
        match probe {
            Probe::Found(slot) => slot as usize,
            Probe::Vacant(_) if absent == Absent::Missing => {
                *missing = true;
                usize::MAX
            }
            Probe::Vacant(bucket) => {
                let (Layout::Text { bytes, ends } | Layout::Rows { bytes, ends, .. }) =
                    &mut self.layout
                else {
                    unreachable!()
                };
                bytes.extend_from_slice(key);
                ends.push(bytes.len());
                self.insert(bucket, hash)
            }
        }
    }

    /// Makes the next slot, whose key storage the caller has just appended, at `bucket`.
    fn insert(&mut self, bucket: usize, hash: u64) -> usize {
        let slot = self.hashes.len();
        assert!(
            slot < EMPTY as usize,
            "group keys: more than 2^32 - 1 groups"
        );
        self.hashes.push(hash);
        self.buckets[bucket] = slot as u32;
        // At most half full, so that probes stay short.
        if 2 * (self.hashes.len() - usize::from(self.null_slot.is_some())) > self.buckets.len() {
            self.grow();
        }
        slot
    }

    fn grow(&mut self) {
        let mut buckets = vec![EMPTY; self.buckets.len() * 2];
        let mask = buckets.len() - 1;
        for slot in self.buckets.iter().copied().filter(|&slot| slot != EMPTY) {
            let mut at = self.hashes[slot as usize] as usize & mask;
            while buckets[at] != EMPTY {
                at = (at + 1) & mask;
            }
            buckets[at] = slot;
        }
        self.buckets = buckets;
    }
}

/// The slot whose key `matches`, or the empty bucket where a key of `hash` goes.
#[inline]
fn probe(buckets: &[u32], hash: u64, matches: impl Fn(u32) -> bool) -> Probe {
    let mask = buckets.len() - 1;
    let mut at = hash as usize & mask;
    loop {
        let slot = buckets[at];
        if slot == EMPTY {
            return Probe::Vacant(at);
        }
        if matches(slot) {
            return Probe::Found(slot);
        }
        at = (at + 1) & mask;
    }
}

/// `array` with every NaN made the same NaN and `-0.0` made `0.0`, so that equal keys
/// encode equally. Arrays of other types are returned as they are.
pub(crate) fn canonical_floats(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float64 => {
            let result: Float64Array = array
                .as_primitive::<Float64Type>()
                .unary(|v| f64::from_bits(double_bits(v)));
            Arc::new(result)
        }
        DataType::Float32 => {
            let result: Float32Array = array
                .as_primitive::<Float32Type>()
                .unary(|v| f32::from_bits(float_bits(v) as u32));
            Arc::new(result)
        }
        _ => array.clone(),
    }
}

#[cfg(test)]
#[path = "group_keys_tests.rs"]
mod tests;
