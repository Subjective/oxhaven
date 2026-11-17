//! The one hash table that numbers the distinct keys of rows: under the joins, the
//! aggregations, `select_distinct`, `head_by`/`tail_by` and the keyed cross join.
//!
//! [`KeyTable`] gives each distinct key of zero or more key columns a slot, `0, 1, 2, ...`
//! in the order keys are first inserted, and finds the slot of a key again. A key keeps
//! its slot for the life of the table, also while no row has it, which is what keeps a
//! group at its row of an aggregation's result. An operator keeps what it knows about a
//! key in vectors indexed by slot.
//!
//! Two rows get one slot exactly when their keys are equal as Java's typed hashers
//! compare them (PARITY.md, "Keys"): a null equals only a null, every NaN equals every
//! NaN, and `-0.0` equals `0.0`. Nothing is allocated per row. Java generates one hasher
//! class per combination of key types
//! (io.deephaven.engine.table.impl.by.typed.TypedHasherFactory); here a key takes one of
//! three roads:
//!
//! | Key columns | A row's key is | Kept as |
//! | --- | --- | --- |
//! | one column of a primitive, boolean or time type | the value's canonical 64 bits | in the hash table itself |
//! | one string column | the string's bytes, borrowed from the chunk | one byte buffer |
//! | anything else | the row in Arrow's row format, borrowed from the encoded chunk | one byte buffer |
//!
//! The table is open-addressed with linear probing, at most half full, and its buckets
//! hold a slot with the key's bits (first road) or the key's hash (the others), so that a
//! lookup of a word key reads one place in memory and a byte key is compared only when
//! the hashes agree. A chunk is hashed a block of rows at a time, and once the table is
//! larger than the processor's caches the bucket of a row a few rows ahead is read
//! early, so that the misses of several rows overlap.
//!
//! Hashes come from `ahash` with fixed seeds (PORTING.md rule 7), and only decide where
//! a key is stored in the table, never a slot number or an order anyone sees.
//!
//! This table replaced two: the one task 9.1 wrote for joins (hashbrown, keys widened to
//! `i64` arrays first) and the one task 9.2 wrote for aggregations (this layout, a row at
//! a time). `tasks/9.5-key-table-followups.md` has the measurements that chose between
//! them.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time64NanosecondType, TimeUnit, TimestampNanosecondType, UInt16Type,
};
use arrow::row::{RowConverter, SortField};
use oxhaven_contracts::table::TableFailure;
use oxhaven_contracts::unported;
use std::sync::Arc;

/// What [`KeyTable::find`] answers for a row whose key the table does not have.
pub const NO_SLOT: u32 = u32::MAX;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// One entry of the open-addressed table: a slot and, to compare against, its key's bits
/// for [`Layout::Word`] or its key's hash otherwise (from which growing the table
/// places it again without reading the key).
#[derive(Clone, Copy)]
struct Bucket {
    tag: u64,
    slot: u32,
}

/// The slot of an empty bucket.
const EMPTY: u32 = u32::MAX;

const VACANT: Bucket = Bucket {
    tag: 0,
    slot: EMPTY,
};

/// The fixed seeds of the hasher. Any constants do; these are digits of pi.
const SEEDS: [u64; 4] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];

/// Rows hashed at a time, before their buckets are probed.
const BLOCK: usize = 256;

/// How many rows ahead a bucket is read early, and from how many buckets on (16 bytes
/// each) that is worth doing.
const AHEAD: usize = 8;

/// Rows encoded in Arrow's row format at a time.
const ENCODED: usize = 4096;
const LARGE: usize = 1 << 17;

/// How the keys of each slot are kept.
enum Layout {
    /// No key has been inserted yet into a table made without column types, so the kind
    /// of key is not known.
    Unknown,
    /// No key columns: every row has the one key, whose slot is 0.
    Unit,
    /// One key column of 64-bit-or-narrower values, whose canonical bits are kept in the
    /// buckets.
    Word,
    /// One string column, or (`rows`) any key columns with each row in Arrow's row
    /// format: slot `s` is `bytes[ends[s - 1]..ends[s]]`.
    Bytes {
        rows: bool,
        bytes: Vec<u8>,
        ends: Vec<usize>,
    },
}

/// A number a slot is reported as: `u32` with [`NO_SLOT`] for the joins, `usize` with
/// `usize::MAX` for the operators that index vectors by it.
pub trait SlotNumber: Copy {
    fn of(slot: u32) -> Self;
}

impl SlotNumber for u32 {
    #[inline]
    fn of(slot: u32) -> u32 {
        slot
    }
}

impl SlotNumber for usize {
    #[inline]
    fn of(slot: u32) -> usize {
        if slot == NO_SLOT {
            usize::MAX
        } else {
            slot as usize
        }
    }
}

/// The slot of every key seen so far. See the module documentation.
///
/// Behavior: io.deephaven.engine.table.impl.by.OperatorAggregationStateManager, whose
/// `outputPosition` is the slot, and the state managers of the joins, with the key
/// equality of the hashers that
/// io.deephaven.engine.table.impl.by.typed.TypedHasherFactory generates
/// (io.deephaven.util.compare.DoubleComparisons#eq and its siblings)
pub struct KeyTable {
    layout: Layout,
    /// Whether the table was made for declared column types ([`KeyTable::for_types`]).
    /// One made without checks its columns as a join does: only the types a join takes
    /// as match columns, and timestamps without their time zone annotation, so that the
    /// columns of two tables can be looked up in one table.
    declared: bool,
    /// A converter for each combination of column types met; the declared types first.
    converters: Vec<(Vec<DataType>, RowConverter)>,
    /// The number of slots handed out.
    slots: usize,
    /// The open-addressed table; its length is a power of two.
    buckets: Vec<Bucket>,
    /// The slot of the null key of a single key column, which is not in `buckets`.
    null_slot: Option<u32>,
    hasher: ahash::RandomState,
    /// For tests: every key hashes alike, so every lookup probes past every other key.
    #[cfg(test)]
    collide: bool,
}

impl Default for KeyTable {
    fn default() -> Self {
        Self::new()
    }
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

/// The canonical bits of a `double`: every NaN one value, `-0.0` as `0.0`.
///
/// Behavior: the `eq` of io.deephaven.util.compare.DoubleComparisons and FloatComparisons
#[inline]
fn double_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// The values of one word key column, to be read as canonical bits a block at a time.
enum Words<'a> {
    Bool(&'a BooleanArray),
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    U16(&'a [u16]),
    F32(&'a [f32]),
    F64(&'a [f64]),
}

impl<'a> Words<'a> {
    fn of(column: &'a ArrayRef) -> Option<Words<'a>> {
        Some(match column.data_type() {
            DataType::Boolean => Words::Bool(column.as_boolean()),
            DataType::Int8 => Words::I8(column.as_primitive::<Int8Type>().values()),
            DataType::Int16 => Words::I16(column.as_primitive::<Int16Type>().values()),
            DataType::Int32 => Words::I32(column.as_primitive::<Int32Type>().values()),
            DataType::Date32 => Words::I32(column.as_primitive::<Date32Type>().values()),
            DataType::Int64 => Words::I64(column.as_primitive::<Int64Type>().values()),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Words::I64(column.as_primitive::<Time64NanosecondType>().values())
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Words::I64(column.as_primitive::<DurationNanosecondType>().values())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Words::I64(column.as_primitive::<TimestampNanosecondType>().values())
            }
            DataType::UInt16 => Words::U16(column.as_primitive::<UInt16Type>().values()),
            DataType::Float32 => Words::F32(column.as_primitive::<Float32Type>().values()),
            DataType::Float64 => Words::F64(column.as_primitive::<Float64Type>().values()),
            _ => return None,
        })
    }

    /// The canonical bits of rows `start..start + out.len()`. A `float` is widened to
    /// `double`, which keeps distinct values distinct.
    fn fill(&self, start: usize, out: &mut [u64]) {
        fn each<T: Copy>(values: &[T], start: usize, out: &mut [u64], bits: impl Fn(T) -> u64) {
            for (word, &value) in out.iter_mut().zip(&values[start..]) {
                *word = bits(value);
            }
        }
        match self {
            Words::Bool(values) => {
                for (offset, word) in out.iter_mut().enumerate() {
                    *word = u64::from(values.value(start + offset));
                }
            }
            Words::I8(values) => each(values, start, out, |v| i64::from(v) as u64),
            Words::I16(values) => each(values, start, out, |v| i64::from(v) as u64),
            Words::I32(values) => each(values, start, out, |v| i64::from(v) as u64),
            Words::I64(values) => each(values, start, out, |v| v as u64),
            Words::U16(values) => each(values, start, out, u64::from),
            Words::F32(values) => each(values, start, out, |v| double_bits(v.into())),
            Words::F64(values) => each(values, start, out, double_bits),
        }
    }
}

impl KeyTable {
    /// A table whose kind of key is that of the first columns inserted, and that takes
    /// the column types a join takes as match columns.
    pub fn new() -> Self {
        Self {
            layout: Layout::Unknown,
            declared: false,
            converters: Vec::new(),
            slots: 0,
            buckets: vec![VACANT; 16],
            null_slot: None,
            hasher: ahash::RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]),
            #[cfg(test)]
            collide: false,
        }
    }

    /// A table for keys of columns of the Arrow types `data_types`, in that order. An
    /// error for types Arrow's row format does not take.
    pub fn for_types(data_types: &[DataType]) -> Result<KeyTable, TableFailure> {
        let mut table = KeyTable::new();
        table.declared = true;
        table.layout = match data_types {
            [] => Layout::Unit,
            [one] if is_word(one) => Layout::Word,
            [DataType::Utf8] => bytes_layout(false),
            _ => {
                let fields = data_types
                    .iter()
                    .map(|data_type| SortField::new(data_type.clone()))
                    .collect();
                let converter =
                    RowConverter::new(fields).map_err(|error| failure(error.to_string()))?;
                table.converters.push((data_types.to_vec(), converter));
                bytes_layout(true)
            }
        };
        Ok(table)
    }

    /// The number of slots: every slot is below it.
    pub fn len(&self) -> usize {
        self.slots
    }

    /// Whether no key has a slot.
    pub fn is_empty(&self) -> bool {
        self.slots == 0
    }

    /// The slot of each of the `rows` rows of the key `columns`, in row order. A key the
    /// table does not have gets the next free slot.
    ///
    /// # Panics
    /// With `unported!` for a column of a type that is not a key type, and if the columns
    /// are not of the kind of the columns of earlier calls.
    pub fn insert(&mut self, columns: &[ArrayRef], rows: usize) -> Vec<u32> {
        let mut slots = Vec::with_capacity(rows);
        if let Err(error) = self.lookup(columns, rows, true, &mut slots) {
            panic!("{}", error.message);
        }
        slots
    }

    /// As [`KeyTable::insert`], but no key gets a slot: a row whose key the table does
    /// not have gets [`NO_SLOT`].
    pub fn find(&mut self, columns: &[ArrayRef], rows: usize) -> Vec<u32> {
        let mut slots = Vec::with_capacity(rows);
        if let Err(error) = self.lookup(columns, rows, false, &mut slots) {
            panic!("{}", error.message);
        }
        slots
    }

    /// The slot of each row of `key_columns`, appended to `slots`, with a new slot for each
    /// key not seen before. The positions of the rows that made a new slot are appended to
    /// `created`, in slot order.
    pub fn find_or_create<S: SlotNumber + PartialEq>(
        &mut self,
        key_columns: &[ArrayRef],
        slots: &mut Vec<S>,
        created: &mut Vec<usize>,
    ) -> Result<(), TableFailure> {
        let rows = row_count(key_columns)?;
        let mut first_slot = self.len();
        self.lookup(key_columns, rows, true, slots)?;
        // The rows that made a slot are the first with each slot at or past the old
        // length; slots are made in row order, so they are found in slot order.
        let start = slots.len() - rows;
        let last = self.len();
        for (position, &slot) in slots[start..].iter().enumerate() {
            if first_slot == last {
                break;
            }
            if slot == S::of(first_slot as u32) {
                created.push(position);
                first_slot += 1;
            }
        }
        Ok(())
    }

    /// The slot of each row of `key_columns`, appended to `slots`. Returns `false` if some
    /// key has no slot; its rows get the number of [`NO_SLOT`].
    pub fn find_all<S: SlotNumber>(
        &mut self,
        key_columns: &[ArrayRef],
        slots: &mut Vec<S>,
    ) -> Result<bool, TableFailure> {
        let rows = row_count(key_columns)?;
        self.lookup(key_columns, rows, false, slots)
    }

    #[inline]
    fn hash(&self, key: impl std::hash::Hash) -> u64 {
        #[cfg(test)]
        if self.collide {
            return 7;
        }
        self.hasher.hash_one(key)
    }

    /// Appends the slot of each row to `out`. Returns whether every row has a slot.
    fn lookup<S: SlotNumber>(
        &mut self,
        columns: &[ArrayRef],
        rows: usize,
        insert: bool,
        out: &mut Vec<S>,
    ) -> Result<bool, TableFailure> {
        if rows == 0 {
            return Ok(true);
        }
        out.reserve(rows);
        if matches!(self.layout, Layout::Unknown) {
            let layout = match columns {
                [] => Layout::Unit,
                [one] if is_word(one.data_type()) => Layout::Word,
                [one] if *one.data_type() == DataType::Utf8 => bytes_layout(false),
                _ => {
                    // Refuses what is not a key type, as every lookup of such columns does.
                    columns.iter().for_each(|column| drop(canonical(column)));
                    bytes_layout(true)
                }
            };
            if !insert {
                out.extend(std::iter::repeat_n(S::of(NO_SLOT), rows));
                return Ok(false);
            }
            self.layout = layout;
        }
        match &self.layout {
            Layout::Unknown => unreachable!("the layout was just chosen"),
            Layout::Unit => {
                if insert {
                    self.slots = 1;
                }
                let slot = if self.slots == 1 { 0 } else { NO_SLOT };
                out.extend(std::iter::repeat_n(S::of(slot), rows));
                Ok(slot != NO_SLOT)
            }
            Layout::Word => {
                let column = match columns {
                    [column] => column,
                    _ => return Err(failure("group keys: several key columns where one was")),
                };
                let words = Words::of(column).ok_or_else(|| {
                    failure(format!(
                        "group keys: a key column of Arrow type {} where one of another type was declared",
                        column.data_type()
                    ))
                })?;
                let nulls = column.nulls().filter(|nulls| nulls.null_count() > 0);
                Ok(self.word_slots(&words, nulls, rows, insert, out))
            }
            Layout::Bytes { rows: false, .. } => {
                let strings = match columns {
                    [column] => column.as_string_opt::<i32>(),
                    _ => None,
                }
                .ok_or_else(|| failure("group keys: a string key column is not a string array"))?;
                let nulls = strings.nulls().filter(|nulls| nulls.null_count() > 0);
                if strings.len() != rows {
                    return Err(failure("group keys: key columns of different lengths"));
                }
                let keys = (0..rows).map(|row| strings.value(row).as_bytes());
                Ok(self.byte_slots(keys, nulls, rows, insert, out))
            }
            Layout::Bytes { rows: true, .. } => {
                if columns.iter().any(|column| column.len() != rows) {
                    return Err(failure("group keys: key columns of different lengths"));
                }
                // Encoded a piece at a time, so that a piece's rows are still in the
                // processor's cache when they are hashed and compared.
                let mut all = true;
                for start in (0..rows).step_by(ENCODED) {
                    let count = ENCODED.min(rows - start);
                    let encoded = if count == rows {
                        self.encode(columns)?
                    } else {
                        let piece: Vec<ArrayRef> = columns
                            .iter()
                            .map(|column| column.slice(start, count))
                            .collect();
                        self.encode(&piece)?
                    };
                    let keys = encoded.iter().map(|row| row.data());
                    all &= self.byte_slots(keys, None, count, insert, out);
                }
                Ok(all)
            }
        }
    }

    /// The rows of `columns` in Arrow's row format, in which two rows have the same bytes
    /// exactly when every column has equal values or nulls in both. Floating point columns
    /// are first brought to one NaN and one zero.
    fn encode(&mut self, columns: &[ArrayRef]) -> Result<arrow::row::Rows, TableFailure> {
        let index = if self.declared {
            0
        } else {
            let types: Vec<DataType> = columns
                .iter()
                .map(|column| match column.data_type() {
                    // A time zone is an annotation of the column, not part of its values.
                    DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(*unit, None),
                    other => other.clone(),
                })
                .collect();
            let known = self
                .converters
                .iter()
                .position(|(known, _)| *known == types);
            match known {
                Some(index) => index,
                None => {
                    // Refuses what is not a key type before a converter is made for it.
                    columns.iter().for_each(|column| drop(canonical(column)));
                    let fields = types.iter().cloned().map(SortField::new).collect();
                    let converter = match RowConverter::new(fields) {
                        Ok(converter) => converter,
                        Err(e) => panic!("the key columns passed the type check: {e}"),
                    };
                    self.converters.push((types, converter));
                    self.converters.len() - 1
                }
            }
        };
        let columns: Vec<ArrayRef> = if self.declared {
            columns.iter().map(canonical_floats).collect()
        } else {
            columns.iter().map(canonical).collect()
        };
        self.converters[index]
            .1
            .convert_columns(&columns)
            .map_err(|error| failure(error.to_string()))
    }

    /// The slot of the null key of a single key column.
    fn null(&mut self, insert: bool) -> u32 {
        if let Some(slot) = self.null_slot {
            return slot;
        }
        if !insert {
            return NO_SLOT;
        }
        // The null key is not in `buckets`; byte storage holds an empty placeholder, so
        // that a slot is also its key's place there.
        if let Layout::Bytes { bytes, ends, .. } = &mut self.layout {
            ends.push(bytes.len());
        }
        let slot = self.next_slot();
        self.null_slot = Some(slot);
        slot
    }

    fn next_slot(&mut self) -> u32 {
        let slot = self.slots;
        assert!(
            slot < EMPTY as usize,
            "group keys: more than 2^32 - 1 groups"
        );
        self.slots += 1;
        slot as u32
    }

    /// Reads the bucket where a key of `hash` starts its probe, so that it is in the
    /// processor's cache when the key's turn comes.
    #[inline]
    fn touch(&self, hash: u64) {
        let at = hash as usize & (self.buckets.len() - 1);
        std::hint::black_box(self.buckets[at].slot);
    }

    fn word_slots<S: SlotNumber>(
        &mut self,
        words: &Words<'_>,
        nulls: Option<&NullBuffer>,
        rows: usize,
        insert: bool,
        out: &mut Vec<S>,
    ) -> bool {
        let mut keys = [0u64; BLOCK];
        let mut hashes = [0u64; BLOCK];
        let mut found = [0u32; BLOCK];
        let mut all = true;
        for start in (0..rows).step_by(BLOCK) {
            let count = BLOCK.min(rows - start);
            words.fill(start, &mut keys[..count]);
            if let Some(nulls) = nulls {
                for at in 0..count {
                    found[at] = if nulls.is_null(start + at) {
                        self.null(insert)
                    } else {
                        self.slot_of_word(keys[at], self.hash(keys[at]), insert)
                    };
                }
            } else if self.buckets.len() < LARGE {
                // The table is in the processor's caches: one pass.
                for (slot, &key) in found[..count].iter_mut().zip(&keys[..count]) {
                    *slot = self.slot_of_word(key, self.hash(key), insert);
                }
            } else {
                for (hash, key) in hashes[..count].iter_mut().zip(&keys[..count]) {
                    *hash = self.hash(key);
                }
                for at in 0..count {
                    if at + AHEAD < count {
                        self.touch(hashes[at + AHEAD]);
                    }
                    found[at] = self.slot_of_word(keys[at], hashes[at], insert);
                }
            }
            all &= insert || !found[..count].contains(&NO_SLOT);
            out.extend(found[..count].iter().map(|&slot| S::of(slot)));
        }
        all
    }

    #[inline]
    fn slot_of_word(&mut self, key: u64, hash: u64, insert: bool) -> u32 {
        let mask = self.buckets.len() - 1;
        let mut at = hash as usize & mask;
        // The bucket holds the key itself, so a lookup reads nothing else.
        loop {
            let bucket = self.buckets[at];
            if bucket.slot == EMPTY {
                break;
            }
            if bucket.tag == key {
                return bucket.slot;
            }
            at = (at + 1) & mask;
        }
        if !insert {
            return NO_SLOT;
        }
        self.place(at, key)
    }

    fn byte_slots<'k, S: SlotNumber>(
        &mut self,
        mut keys: impl Iterator<Item = &'k [u8]>,
        nulls: Option<&NullBuffer>,
        rows: usize,
        insert: bool,
        out: &mut Vec<S>,
    ) -> bool {
        let mut hashes = [0u64; BLOCK];
        let mut found = [0u32; BLOCK];
        let mut block: [&[u8]; BLOCK] = [&[]; BLOCK];
        let mut all = true;
        for start in (0..rows).step_by(BLOCK) {
            let count = BLOCK.min(rows - start);
            // Each key is taken from the chunk once.
            for (key, hash) in block[..count].iter_mut().zip(&mut hashes) {
                *key = keys.next().unwrap_or_default();
                *hash = self.hash(*key);
            }
            let ahead = self.buckets.len() >= LARGE;
            for at in 0..count {
                if ahead && at + AHEAD < count {
                    self.touch(hashes[at + AHEAD]);
                }
                found[at] = if nulls.is_some_and(|nulls| nulls.is_null(start + at)) {
                    self.null(insert)
                } else {
                    self.slot_of_bytes(block[at], hashes[at], insert)
                };
            }
            all &= insert || !found[..count].contains(&NO_SLOT);
            out.extend(found[..count].iter().map(|&slot| S::of(slot)));
        }
        all
    }

    #[inline]
    fn slot_of_bytes(&mut self, key: &[u8], hash: u64, insert: bool) -> u32 {
        let Layout::Bytes { bytes, ends, .. } = &mut self.layout else {
            unreachable!("a byte key in a table of word keys")
        };
        let mask = self.buckets.len() - 1;
        let mut at = hash as usize & mask;
        // The bucket holds the key's hash; the bytes are compared when the hashes agree.
        loop {
            let bucket = self.buckets[at];
            if bucket.slot == EMPTY {
                break;
            }
            if bucket.tag == hash {
                let slot = bucket.slot as usize;
                let start = if slot == 0 { 0 } else { ends[slot - 1] };
                if &bytes[start..ends[slot]] == key {
                    return bucket.slot;
                }
            }
            at = (at + 1) & mask;
        }
        if !insert {
            return NO_SLOT;
        }
        bytes.extend_from_slice(key);
        ends.push(bytes.len());
        self.place(at, hash)
    }

    /// Makes the next slot, whose key storage the caller has just appended, at the empty
    /// bucket `at` with `tag`.
    fn place(&mut self, at: usize, tag: u64) -> u32 {
        let slot = self.next_slot();
        self.buckets[at] = Bucket { tag, slot };
        // At most half full, so that probes stay short.
        let stored = self.slots - usize::from(self.null_slot.is_some());
        if 2 * stored > self.buckets.len() {
            self.grow();
        }
        slot
    }

    fn grow(&mut self) {
        let mut buckets = vec![VACANT; self.buckets.len() * 2];
        let mask = buckets.len() - 1;
        let words = matches!(self.layout, Layout::Word);
        for bucket in self.buckets.iter().filter(|bucket| bucket.slot != EMPTY) {
            let hash = if words {
                self.hash(bucket.tag)
            } else {
                bucket.tag
            };
            let mut at = hash as usize & mask;
            while buckets[at].slot != EMPTY {
                at = (at + 1) & mask;
            }
            buckets[at] = *bucket;
        }
        self.buckets = buckets;
    }
}

fn bytes_layout(rows: bool) -> Layout {
    Layout::Bytes {
        rows,
        bytes: Vec::new(),
        ends: Vec::new(),
    }
}

/// The number of rows of `key_columns`, which is none without columns.
fn row_count(key_columns: &[ArrayRef]) -> Result<usize, TableFailure> {
    let rows = key_columns.first().map_or(0, |column| column.len());
    if key_columns.iter().any(|column| column.len() != rows) {
        return Err(failure("group keys: key columns of different lengths"));
    }
    Ok(rows)
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
            let result: Float32Array = array.as_primitive::<Float32Type>().unary(|v| {
                if v.is_nan() {
                    f32::NAN
                } else if v == 0.0 {
                    0.0
                } else {
                    v
                }
            });
            Arc::new(result)
        }
        _ => array.clone(),
    }
}

/// `column` as a table made without declared types encodes it among several key columns:
/// a floating point column with one NaN and one zero, a timestamp without its time zone
/// annotation, every other column as it is.
///
/// # Panics
/// With `unported!` for a column of a type that joins do not take as a key.
fn canonical(column: &ArrayRef) -> ArrayRef {
    /// The types the joins' key cells (`ops::join::keys::Cell`) have an equality for. A
    /// `ZonedDateTime` and a `Period` are structs of them.
    fn is_key_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::Utf8 => true,
            DataType::Struct(fields) => fields.iter().all(|field| {
                // No struct that is a column type has a floating point field, and one that
                // had would need its NaNs and zeros brought to one form here.
                !field.data_type().is_floating() && is_key_type(field.data_type())
            }),
            other => is_word(other),
        }
    }
    match column.data_type() {
        DataType::Float32 | DataType::Float64 => canonical_floats(column),
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

/// The tests task 9.2 wrote for its table, which this one replaced, unchanged.
#[cfg(test)]
#[path = "group_keys_compat.rs"]
mod group_tests;

/// The tests task 9.1 wrote for its table, which this one replaced, unchanged.
#[cfg(test)]
#[path = "key_table_join_tests.rs"]
mod join_tests;

#[cfg(test)]
#[path = "key_table_tests.rs"]
mod tests;
