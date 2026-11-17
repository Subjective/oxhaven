//! `ComputeColumnStatistics`: a static table of one row that describes one column of a
//! table as it is when the request runs.
//!
//! What the row holds depends on the column's type: counts, sums, extremes, average and
//! standard deviation for a number, the extremes for an instant, and for anything else the
//! number of distinct values with the most frequent of them.

use super::prelude::*;
use arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, ListArray, StringArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampNanosecondType, UInt16Type,
};
use oxhaven_column::FlatSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use oxhaven_engine::table::{QueryTable, TableSpec};
use oxhaven_proto::grpc::ColumnStatisticsRequest;
use std::collections::HashSet;

/// The rows read at a time. Sums of floating point values are added up per chunk and then
/// to the total, so the size is part of the result.
///
/// Behavior: `io.deephaven.server.table.stats.ChunkedStatsKernel#CHUNK_SIZE`
const CHUNK_SIZE: u64 = 2048;

/// The unique values listed when the request names no limit, the most a request may ask
/// for, and the distinct values counted one by one before only their number is kept: the
/// defaults of the properties `ColumnStatistics.defaultUniqueLimit`, `.maxUniqueLimit` and
/// `.maxUniqueToCollect`.
///
/// Behavior: `io.deephaven.server.table.ops.ColumnStatisticsGrpcImpl`
const DEFAULT_UNIQUE_LIMIT: i32 = 20;
const MAX_UNIQUE_LIMIT: i32 = 200;
const MAX_UNIQUE_TO_COLLECT: usize = 1_000_000;

/// Behavior: `io.deephaven.server.table.ops.ColumnStatisticsGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = ColumnStatisticsRequest;

    fn result_id(request: &ColumnStatisticsRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &ColumnStatisticsRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        _context: &OpContext,
        request: &ColumnStatisticsRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let table = &sources[0];
        let Some(column) = table.definition().index_of(&request.column_name) else {
            return Err(Status::invalid_argument(
                "Table doesn't have a column with the specified name",
            )
            .into());
        };
        let unique_limit = match request.unique_value_limit {
            Some(limit) => limit.min(MAX_UNIQUE_LIMIT),
            None => DEFAULT_UNIQUE_LIMIT,
        };
        Ok(column_statistics(
            &**table,
            column,
            unique_limit,
            MAX_UNIQUE_TO_COLLECT,
        )?)
    }
}

/// The statistics of column `column` of `table` over its current rows, as a static table of
/// one row. `unique_limit` is the number of distinct values to list for a column that is
/// neither a number nor an instant, and `max_unique_to_collect` the number of distinct
/// values after which only their number is reported.
///
/// The caller holds the update graph's lock if `table` is refreshing, which is what Java's
/// snapshot gives: one consistent state. The result never follows the table.
///
/// Behavior: `io.deephaven.server.table.ops.ColumnStatisticsGrpcImpl#create`
pub fn column_statistics(
    table: &dyn Table,
    column: usize,
    unique_limit: i32,
    max_unique_to_collect: usize,
) -> Result<Arc<QueryTable>, ExportError> {
    let source = table.column(column);
    let rows = table.row_set();
    let mut stats = Stats::new(source.column_type(), max_unique_to_collect)?;
    for chunk_rows in chunks_of(&rows, CHUNK_SIZE) {
        stats.add(&source.chunk(&chunk_rows)?);
    }
    let size = i64::try_from(rows.size()).expect("a row set holds fewer than 2^63 rows");
    let mut columns = vec![
        ("COUNT", ColumnType::Long, long_cell(stats.count())),
        ("SIZE", ColumnType::Long, long_cell(size)),
    ];
    columns.extend(stats.finish(unique_limit)?);
    Ok(one_row(columns)?)
}

/// `rows` in order, cut into row sets of at most `size` rows.
fn chunks_of(rows: &RowSet, size: u64) -> impl Iterator<Item = RowSet> + '_ {
    let step = usize::try_from(size).unwrap_or(usize::MAX).max(1);
    (0..rows.size())
        .step_by(step)
        .map(move |start| rows.sub_set_by_position_range(start, start.saturating_add(size)))
}

type ResultColumn = (&'static str, ColumnType, ArrayRef);

/// The static table of one row with these columns, which is flat as Java's is.
///
/// Behavior: `io.deephaven.engine.util.TableTools#newTable`
fn one_row(columns: Vec<ResultColumn>) -> Result<Arc<QueryTable>, TableFailure> {
    let mut definitions = Vec::with_capacity(columns.len());
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(columns.len());
    for (name, column_type, values) in columns {
        definitions.push(ColumnDefinition::new(name, column_type.clone()));
        sources.push(Arc::new(FlatSource::new(column_type, values)?));
    }
    let definition = TableDefinition::new(definitions).map_err(|e| TableFailure {
        message: e.to_string(),
    })?;
    Ok(QueryTable::new_static(
        TableSpec::new(definition, RowSet::flat(1), sources).flat(),
    ))
}

/// The statistics of one column while its chunks are read.
enum Stats {
    Integer(IntegerStats),
    Floating(FloatingStats),
    Instant(InstantStats),
    Unique(UniqueStats),
}

impl Stats {
    /// Behavior: `ColumnStatisticsGrpcImpl#create`,
    /// `io.deephaven.server.table.stats.ChunkedNumericalStatsKernel#makeChunkedNumericalStats`
    fn new(column_type: &ColumnType, max_unique_to_collect: usize) -> Result<Self, Status> {
        Ok(match column_type {
            ColumnType::Byte | ColumnType::Short | ColumnType::Int | ColumnType::Long => {
                Stats::Integer(IntegerStats::new(column_type.clone()))
            }
            ColumnType::Float | ColumnType::Double => {
                Stats::Floating(FloatingStats::new(column_type.clone()))
            }
            ColumnType::Instant => Stats::Instant(InstantStats::default()),
            ColumnType::Boolean | ColumnType::Char | ColumnType::String => {
                Stats::Unique(UniqueStats::new(max_unique_to_collect))
            }
            // An array is equal only to itself in Java and is listed by its identity hash;
            // a vector is compared by its elements and listed by its text.
            other => {
                return Err(not_ported(
                    "io.deephaven.server.table.stats.ObjectChunkedStats#processChunks",
                    format_args!("the statistics of a column of type {other:?}"),
                ))
            }
        })
    }

    fn add(&mut self, chunk: &ArrayRef) {
        match self {
            Stats::Integer(stats) => stats.add(chunk),
            Stats::Floating(stats) => stats.add(chunk),
            Stats::Instant(stats) => stats.add(chunk),
            Stats::Unique(stats) => stats.add(chunk),
        }
    }

    /// The cells that are not null.
    fn count(&self) -> i64 {
        match self {
            Stats::Integer(stats) => stats.count,
            Stats::Floating(stats) => stats.count,
            Stats::Instant(stats) => stats.count,
            Stats::Unique(stats) => stats.count,
        }
    }

    /// The columns after `COUNT` and `SIZE`.
    fn finish(self, unique_limit: i32) -> Result<Vec<ResultColumn>, ExportError> {
        match self {
            Stats::Integer(stats) => Ok(stats.finish()),
            Stats::Floating(stats) => Ok(stats.finish()),
            Stats::Instant(stats) => Ok(stats.finish()),
            Stats::Unique(stats) => stats.finish(unique_limit),
        }
    }
}

/// Java's null of a `long` column; a result that lands on it is null.
const NULL_LONG: i64 = i64::MIN;
/// Java's null of a `double` column.
const NULL_DOUBLE: f64 = -f64::MAX;

fn long_cell(value: i64) -> ArrayRef {
    Arc::new(Int64Array::from(vec![(value != NULL_LONG).then_some(value)]))
}

fn double_cell(value: Option<f64>) -> ArrayRef {
    let value = value.filter(|value| *value != NULL_DOUBLE);
    Arc::new(Float64Array::from(vec![value]))
}

/// Behavior: `io.deephaven.server.table.stats.ChunkedNumericalStatsKernel#avg`
fn avg(count: i64, sum: f64) -> Option<f64> {
    (count != 0).then(|| sum / count as f64)
}

/// Behavior: `io.deephaven.server.table.stats.ChunkedNumericalStatsKernel#stdDev`
fn std_dev(count: i64, sum: f64, sum_of_squares: f64) -> Option<f64> {
    (count > 1)
        .then(|| ((sum_of_squares - sum * sum / count as f64) / (count - 1) as f64).sqrt())
}

/// A sum of `long`s that goes on as a sum of `double`s once it overflows. From then on the
/// terms of a chunk are added up by themselves and to the total at the end of the chunk.
#[derive(Default)]
struct ExactSum {
    exact: i64,
    floating: Option<f64>,
    chunk: f64,
}

impl ExactSum {
    /// Adds `term`, which is `None` if the term itself overflowed and is `approximate` as
    /// a `double`.
    fn add(&mut self, term: Option<i64>, approximate: f64) {
        if self.floating.is_some() {
            self.chunk += approximate;
            return;
        }
        match term.and_then(|term| self.exact.checked_add(term)) {
            Some(sum) => self.exact = sum,
            None => {
                self.floating = Some(self.exact as f64);
                self.chunk = approximate;
            }
        }
    }

    fn end_chunk(&mut self) {
        if let Some(total) = &mut self.floating {
            *total += self.chunk;
        }
        self.chunk = 0.0;
    }

    fn value(&self) -> f64 {
        self.floating.unwrap_or(self.exact as f64)
    }

    /// A `long` column while the sum is exact, a `double` column once it is not.
    fn column(&self, name: &'static str) -> ResultColumn {
        match self.floating {
            Some(total) => (name, ColumnType::Double, double_cell(Some(total))),
            None => (name, ColumnType::Long, long_cell(self.exact)),
        }
    }
}

/// Behavior: `io.deephaven.server.table.stats.IntegerChunkedNumericalStats#processChunks`
/// and its copies for `byte`, `short` and `long`
struct IntegerStats {
    column_type: ColumnType,
    count: i64,
    sum: ExactSum,
    abs_sum: ExactSum,
    sum_of_squares: ExactSum,
    /// Minimum, maximum, minimum and maximum of the absolute values.
    extremes: Option<[i64; 4]>,
}

impl IntegerStats {
    fn new(column_type: ColumnType) -> Self {
        IntegerStats {
            column_type,
            count: 0,
            sum: ExactSum::default(),
            abs_sum: ExactSum::default(),
            sum_of_squares: ExactSum::default(),
            extremes: None,
        }
    }

    fn add(&mut self, chunk: &ArrayRef) {
        fn widened<T>(values: impl Iterator<Item = Option<T>>, null: T) -> Vec<Option<i64>>
        where
            T: Into<i64> + PartialEq + Copy,
        {
            values
                .map(|value| value.filter(|value| *value != null).map(Into::into))
                .collect()
        }
        let chunk = chunk.as_ref();
        let values = match self.column_type {
            ColumnType::Byte => widened(chunk.as_primitive::<Int8Type>().iter(), i8::MIN),
            ColumnType::Short => widened(chunk.as_primitive::<Int16Type>().iter(), i16::MIN),
            ColumnType::Int => widened(chunk.as_primitive::<Int32Type>().iter(), i32::MIN),
            _ => widened(chunk.as_primitive::<Int64Type>().iter(), i64::MIN),
        };
        // Java squares a byte, a short and an int as an int, and a long as a long
        let squares_as_int = self.column_type != ColumnType::Long;
        for value in values.into_iter().flatten() {
            // the null of each type is its minimum, so the absolute value fits
            let abs = value.abs();
            self.extremes = Some(match self.extremes {
                None => [value, value, abs, abs],
                Some([min, max, abs_min, abs_max]) => [
                    min.min(value),
                    max.max(value),
                    abs_min.min(abs),
                    abs_max.max(abs),
                ],
            });
            self.count += 1;
            self.sum.add(Some(value), value as f64);
            self.abs_sum.add(Some(abs), abs as f64);
            let square = if squares_as_int {
                (value as i32).checked_mul(value as i32).map(i64::from)
            } else {
                value.checked_mul(value)
            };
            self.sum_of_squares
                .add(square, value as f64 * value as f64);
        }
        self.sum.end_chunk();
        self.abs_sum.end_chunk();
        self.sum_of_squares.end_chunk();
    }

    fn finish(self) -> Vec<ResultColumn> {
        let extreme = |name, index: usize| {
            let value = self.extremes.map(|extremes| extremes[index]);
            let cell: ArrayRef = match self.column_type {
                ColumnType::Byte => Arc::new(Int8Array::from(vec![value.map(|v| v as i8)])),
                ColumnType::Short => Arc::new(Int16Array::from(vec![value.map(|v| v as i16)])),
                ColumnType::Int => Arc::new(Int32Array::from(vec![value.map(|v| v as i32)])),
                _ => Arc::new(Int64Array::from(vec![value])),
            };
            (name, self.column_type.clone(), cell)
        };
        let double = |name, value| (name, ColumnType::Double, double_cell(value));
        vec![
            self.sum.column("SUM"),
            self.abs_sum.column("SUM_ABS"),
            self.sum_of_squares.column("SUM_SQRD"),
            extreme("MIN", 0),
            extreme("MAX", 1),
            extreme("MIN_ABS", 2),
            extreme("MAX_ABS", 3),
            double("AVG", avg(self.count, self.sum.value())),
            // Java divides the exact sum of the absolute values even after it overflowed,
            // which is then the sum up to the overflow
            double("AVG_ABS", avg(self.count, self.abs_sum.exact as f64)),
            double(
                "STD_DEV",
                std_dev(self.count, self.sum.value(), self.sum_of_squares.value()),
            ),
        ]
    }
}

/// Behavior: `io.deephaven.server.table.stats.DoubleChunkedNumericalStats#processChunks`,
/// `FloatChunkedNumericalStats#processChunks`
struct FloatingStats {
    column_type: ColumnType,
    count: i64,
    sum: f64,
    abs_sum: f64,
    sum_of_squares: f64,
    /// Minimum, maximum, minimum and maximum of the absolute values. A `float` widened to
    /// a `double` compares as it did.
    extremes: Option<[f64; 4]>,
}

impl FloatingStats {
    fn new(column_type: ColumnType) -> Self {
        FloatingStats {
            column_type,
            count: 0,
            sum: 0.0,
            abs_sum: 0.0,
            sum_of_squares: 0.0,
            extremes: None,
        }
    }

    fn add(&mut self, chunk: &ArrayRef) {
        let chunk = chunk.as_ref();
        let values: Vec<Option<f64>> = match self.column_type {
            ColumnType::Float => {
                let values = chunk.as_primitive::<Float32Type>().iter();
                values
                    .map(|value| value.filter(|v| *v != -f32::MAX).map(f64::from))
                    .collect()
            }
            _ => {
                let values = chunk.as_primitive::<Float64Type>().iter();
                values
                    .map(|value| value.filter(|v| *v != NULL_DOUBLE))
                    .collect()
            }
        };
        let (mut sum, mut abs_sum, mut sum_of_squares) = (0.0, 0.0, 0.0);
        for value in values.into_iter().flatten() {
            let abs = value.abs();
            // Java replaces an extreme only by a value that compares below or above it, so
            // a NaN stays if it came first and is passed over if it did not
            self.extremes = Some(match self.extremes {
                None => [value, value, abs, abs],
                Some([min, max, abs_min, abs_max]) => [
                    if value < min { value } else { min },
                    if value > max { value } else { max },
                    if abs < abs_min { abs } else { abs_min },
                    if abs > abs_max { abs } else { abs_max },
                ],
            });
            self.count += 1;
            sum += value;
            abs_sum += abs;
            sum_of_squares += value * value;
        }
        self.sum += sum;
        self.abs_sum += abs_sum;
        self.sum_of_squares += sum_of_squares;
    }

    fn finish(self) -> Vec<ResultColumn> {
        let extreme = |name, index: usize| {
            let value = self.extremes.map(|extremes| extremes[index]);
            let cell: ArrayRef = match self.column_type {
                ColumnType::Float => Arc::new(Float32Array::from(vec![value.map(|v| v as f32)])),
                _ => Arc::new(Float64Array::from(vec![value])),
            };
            (name, self.column_type.clone(), cell)
        };
        let double = |name, value| (name, ColumnType::Double, double_cell(value));
        vec![
            double("SUM", Some(self.sum)),
            double("SUM_ABS", Some(self.abs_sum)),
            double("SUM_SQRD", Some(self.sum_of_squares)),
            extreme("MIN", 0),
            extreme("MAX", 1),
            extreme("MIN_ABS", 2),
            extreme("MAX_ABS", 3),
            double("AVG", avg(self.count, self.sum)),
            double("AVG_ABS", avg(self.count, self.abs_sum)),
            double(
                "STD_DEV",
                std_dev(self.count, self.sum, self.sum_of_squares),
            ),
        ]
    }
}

/// Behavior: `io.deephaven.server.table.stats.DateTimeChunkedStats#processChunks`
#[derive(Default)]
struct InstantStats {
    count: i64,
    /// Earliest and latest, in nanoseconds since the epoch.
    extremes: Option<(i64, i64)>,
}

impl InstantStats {
    fn add(&mut self, chunk: &ArrayRef) {
        let values = chunk.as_primitive::<TimestampNanosecondType>().iter();
        for value in values.flatten().filter(|value| *value != NULL_LONG) {
            self.extremes = Some(match self.extremes {
                None => (value, value),
                Some((min, max)) => (min.min(value), max.max(value)),
            });
            self.count += 1;
        }
    }

    fn finish(self) -> Vec<ResultColumn> {
        let cell = |value: Option<i64>| -> ArrayRef {
            Arc::new(TimestampNanosecondArray::from(vec![value]).with_timezone("UTC"))
        };
        vec![
            (
                "MIN",
                ColumnType::Instant,
                cell(self.extremes.map(|(min, _)| min)),
            ),
            (
                "MAX",
                ColumnType::Instant,
                cell(self.extremes.map(|(_, max)| max)),
            ),
        ]
    }
}

/// A value of a column whose distinct values are counted.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Key {
    Boolean(bool),
    /// One UTF-16 code unit.
    Char(u16),
    Text(String),
}

/// A [`Key`] that borrows from the chunk it was read from, so that only a value seen for
/// the first time is copied.
#[derive(Clone, Copy)]
enum KeyRef<'a> {
    Boolean(bool),
    Char(u16),
    Text(&'a str),
}

impl KeyRef<'_> {
    /// Behavior: `java.lang.Boolean#hashCode`, `java.lang.String#hashCode`; a `char` is
    /// hashed as its number (`it.unimi.dsi.fastutil.chars.Char2LongOpenHashMap`)
    fn java_hash(self) -> i32 {
        match self {
            KeyRef::Boolean(true) => 1231,
            KeyRef::Boolean(false) => 1237,
            KeyRef::Char(unit) => i32::from(unit),
            KeyRef::Text(text) => text.encode_utf16().fold(0i32, |hash, unit| {
                hash.wrapping_mul(31).wrapping_add(i32::from(unit))
            }),
        }
    }

    fn is(self, key: &Key) -> bool {
        match (self, key) {
            (KeyRef::Boolean(a), Key::Boolean(b)) => a == *b,
            (KeyRef::Char(a), Key::Char(b)) => a == *b,
            (KeyRef::Text(a), Key::Text(b)) => a == b,
            _ => false,
        }
    }

    fn to_key(self) -> Key {
        match self {
            KeyRef::Boolean(value) => Key::Boolean(value),
            KeyRef::Char(unit) => Key::Char(unit),
            KeyRef::Text(text) => Key::Text(text.to_string()),
        }
    }
}

impl Key {
    /// Behavior: `java.util.Objects#toString(Object)` of a `Boolean`, a `Character` and a
    /// `String`
    fn text(&self) -> String {
        match self {
            Key::Boolean(value) => value.to_string(),
            // half of a surrogate pair has no text of its own in UTF-8
            Key::Char(unit) => char::from_u32((*unit).into())
                .unwrap_or(char::REPLACEMENT_CHARACTER)
                .to_string(),
            Key::Text(text) => text.clone(),
        }
    }
}

/// How often each distinct value occurred, kept the way the map Java counts in keeps it.
///
/// Java lists the values by falling count, and values of one count in the order its hash
/// map iterates them. That order is a result a client sees, so this is that map: open
/// addressing with linear probing over a table whose size is a power of two, a value
/// placed by the mixed hash of its Java hash code, the table doubled and refilled from its
/// top when three quarters of it are used, and read from its top down.
///
/// Behavior: `it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap#addTo`, `#rehash`,
/// `Object2LongOpenHashMap.MapEntrySet#fastForEach`, and the same of
/// `it.unimi.dsi.fastutil.chars.Char2LongOpenHashMap`; `it.unimi.dsi.fastutil.HashCommon#mix`
struct CountMap {
    /// Index into `entries`, by slot.
    slots: Vec<Option<usize>>,
    entries: Vec<Entry>,
    /// The entry of the `char` 0, which the map of `char`s keeps outside the table and
    /// reads first.
    zero: Option<usize>,
}

struct Entry {
    key: Key,
    hash: i32,
    count: i64,
}

impl CountMap {
    /// Java's table for the default expected size of 16 at the load factor 0.75.
    const INITIAL_SLOTS: usize = 32;

    fn new() -> Self {
        CountMap {
            slots: vec![None; Self::INITIAL_SLOTS],
            entries: Vec::new(),
            zero: None,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Behavior: `it.unimi.dsi.fastutil.HashCommon#mix`
    fn slot_of(hash: i32, slots: usize) -> usize {
        let mixed = (hash as u32).wrapping_mul(0x9E37_79B9);
        (mixed ^ (mixed >> 16)) as usize & (slots - 1)
    }

    /// The first free slot for `hash`, probing upwards and around.
    fn free_slot(slots: &[Option<usize>], hash: i32) -> usize {
        let mut slot = Self::slot_of(hash, slots.len());
        while slots[slot].is_some() {
            slot = (slot + 1) & (slots.len() - 1);
        }
        slot
    }

    /// Counts one occurrence of `key`; true if the value is new.
    fn add(&mut self, key: KeyRef<'_>) -> bool {
        let hash = key.java_hash();
        if matches!(key, KeyRef::Char(0)) {
            if let Some(entry) = self.zero {
                self.entries[entry].count += 1;
                return false;
            }
            self.zero = Some(self.entries.len());
        } else {
            let mask = self.slots.len() - 1;
            let mut slot = Self::slot_of(hash, self.slots.len());
            while let Some(entry) = self.slots[slot] {
                if key.is(&self.entries[entry].key) {
                    self.entries[entry].count += 1;
                    return false;
                }
                slot = (slot + 1) & mask;
            }
            self.slots[slot] = Some(self.entries.len());
        }
        self.entries.push(Entry {
            key: key.to_key(),
            hash,
            count: 1,
        });
        // Java grows the table when the size before this value had reached three quarters
        // of it, to the power of two that holds the new size plus one at that load
        let size_before = self.entries.len() - 1;
        if size_before >= self.slots.len() / 4 * 3 {
            let needed = ((self.entries.len() + 1) as f64 / 0.75).ceil() as usize;
            self.rehash(needed.next_power_of_two());
        }
        true
    }

    fn rehash(&mut self, size: usize) {
        let mut slots = vec![None; size];
        for entry in self.slots.iter().rev().flatten() {
            let slot = Self::free_slot(&slots, self.entries[*entry].hash);
            slots[slot] = Some(*entry);
        }
        self.slots = slots;
    }

    /// The entries in the order Java's map iterates them.
    fn in_java_order(&self) -> impl Iterator<Item = &Entry> {
        let table = self.slots.iter().rev().flatten();
        self.zero.iter().chain(table).map(|entry| &self.entries[*entry])
    }
}

/// Behavior: `io.deephaven.server.table.stats.ObjectChunkedStats#processChunks`,
/// `CharacterChunkedStats#processChunks`
struct UniqueStats {
    count: i64,
    counts: CountMap,
    /// The distinct values once there are more than `max_unique_to_collect` of them; from
    /// then on only their number is kept.
    uncounted: Option<HashSet<Key>>,
    max_unique_to_collect: usize,
}

impl UniqueStats {
    fn new(max_unique_to_collect: usize) -> Self {
        UniqueStats {
            count: 0,
            counts: CountMap::new(),
            uncounted: None,
            max_unique_to_collect,
        }
    }

    fn add(&mut self, chunk: &ArrayRef) {
        let chunk = chunk.as_ref();
        if let Some(values) = chunk.as_boolean_opt() {
            for value in values.iter().flatten() {
                self.add_key(KeyRef::Boolean(value));
            }
        } else if let Some(values) = chunk.as_primitive_opt::<UInt16Type>() {
            // 0xFFFF is Java's null of a char column
            for value in values.iter().flatten().filter(|unit| *unit != u16::MAX) {
                self.add_key(KeyRef::Char(value));
            }
        } else {
            for value in chunk.as_string::<i32>().iter().flatten() {
                self.add_key(KeyRef::Text(value));
            }
        }
    }

    fn add_key(&mut self, key: KeyRef<'_>) {
        self.count += 1;
        if let Some(uncounted) = &mut self.uncounted {
            uncounted.insert(key.to_key());
        } else if self.counts.add(key) && self.counts.len() > self.max_unique_to_collect {
            let counts = std::mem::replace(&mut self.counts, CountMap::new());
            self.uncounted = Some(counts.entries.into_iter().map(|entry| entry.key).collect());
        }
    }

    fn finish(self, unique_limit: i32) -> Result<Vec<ResultColumn>, ExportError> {
        let unique_values = |count: usize| {
            let count = i32::try_from(count).expect("Java counts the distinct values in an int");
            let cell: ArrayRef = Arc::new(Int32Array::from(vec![count]));
            ("UNIQUE_VALUES", ColumnType::Int, cell)
        };
        if let Some(uncounted) = &self.uncounted {
            return Ok(vec![unique_values(uncounted.len())]);
        }
        let mut listed: Vec<&Entry> = self.counts.in_java_order().collect();
        // stable, as Java's sort of the list is
        listed.sort_by_key(|entry| std::cmp::Reverse(entry.count));
        // Java allocates the arrays of this length, which a negative limit makes negative
        let shown = usize::try_from(unique_limit.min(listed.len() as i32)).map_err(|_| {
            ExportError::Internal(format!(
                "java.lang.NegativeArraySizeException: {unique_limit}"
            ))
        })?;
        listed.truncate(shown);
        let keys: StringArray = listed.iter().map(|entry| Some(entry.key.text())).collect();
        let counts: Int64Array = listed.iter().map(|entry| Some(entry.count)).collect();
        let keys_type = ColumnType::array(ColumnType::String);
        let counts_type = ColumnType::array(ColumnType::Long);
        Ok(vec![
            unique_values(self.counts.len()),
            (
                "UNIQUE_KEYS",
                keys_type.clone(),
                one_list(&keys_type, Arc::new(keys)),
            ),
            (
                "UNIQUE_COUNTS",
                counts_type.clone(),
                one_list(&counts_type, Arc::new(counts)),
            ),
        ])
    }
}

/// One cell of the array type `column_type` that holds `elements`.
fn one_list(column_type: &ColumnType, elements: ArrayRef) -> ArrayRef {
    let element = column_type
        .list_element()
        .expect("an array type has an element type");
    let field = Arc::new(Field::new("", element.arrow_type(), true));
    let offsets = OffsetBuffer::from_lengths([elements.len()]);
    Arc::new(ListArray::new(field, offsets, elements, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, UInt16Array};
    use arrow::util::display::array_value_to_string;
    use oxhaven_contracts::rowset::RowRange;
    use oxhaven_engine::table::in_memory_table;
    use oxhaven_testkit::{definition, doubles, ints, longs, strings};
    use ColumnType::{Double, Int, Long};

    fn sets(rows: &RowSet, size: u64) -> Vec<Vec<(i64, i64)>> {
        chunks_of(rows, size)
            .map(|chunk| {
                let ranges = chunk.ranges();
                ranges.map(|range| (range.first(), range.last())).collect()
            })
            .collect()
    }

    #[test]
    fn rows_are_cut_into_chunks_in_order_across_ranges() {
        let rows = RowSet::from_ranges([
            RowRange::new(0, 4),
            RowRange::new(10, 10),
            RowRange::new(20, 26),
        ]);
        assert_eq!(
            sets(&rows, 3),
            [
                vec![(0, 2)],
                vec![(3, 4), (10, 10)],
                vec![(20, 22)],
                vec![(23, 25)],
                vec![(26, 26)],
            ]
        );
        assert_eq!(sets(&rows, 100), [vec![(0, 4), (10, 10), (20, 26)]]);
        assert!(sets(&RowSet::empty(), 3).is_empty());
    }

    fn order(keys: &[&str]) -> Vec<String> {
        let mut map = CountMap::new();
        for key in keys {
            map.add(KeyRef::Text(key));
        }
        map.in_java_order().map(|entry| entry.key.text()).collect()
    }

    /// The orders are those of the Java server (42.9) for a string column with these
    /// values, each once, read from `UNIQUE_KEYS`.
    #[test]
    fn values_of_one_count_are_listed_in_the_order_of_javas_hash_map() {
        assert_eq!(order(&["a", "b", "c", "d"]), ["c", "a", "d", "b"]);
        assert_eq!(order(&["d", "c", "b", "a"]), ["c", "a", "d", "b"]);
    }

    #[test]
    fn the_sum_of_longs_goes_on_as_a_double_after_it_overflows() {
        let mut sum = ExactSum::default();
        sum.add(Some(i64::MAX), i64::MAX as f64);
        sum.end_chunk();
        assert_eq!(sum.floating, None);
        sum.add(Some(5), 5.0);
        sum.add(Some(7), 7.0);
        sum.end_chunk();
        // the total before the overflow, and the terms of the chunk from the overflow on
        assert_eq!(sum.exact, i64::MAX);
        assert_eq!(sum.floating, Some(i64::MAX as f64 + 12.0));
        // a term that is itself too large
        let mut squares = ExactSum::default();
        squares.add(Some(4), 4.0);
        squares.add(None, 1e30);
        squares.end_chunk();
        assert_eq!(squares.floating, Some(4.0 + 1e30));
    }

    type Described = Vec<(String, ColumnType, String)>;

    fn table_of(column_type: ColumnType, cells: ArrayRef) -> Arc<dyn Table> {
        let field = Field::new("X", column_type.arrow_type(), true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let batch = arrow::array::RecordBatch::try_new(schema, vec![cells]).unwrap();
        in_memory_table(definition(&[("X", column_type)]), &[batch]).unwrap()
    }

    /// The statistics of a column with these cells: each result column's name, type and
    /// value as text (a null as the empty text).
    fn stats_with(
        column_type: ColumnType,
        cells: ArrayRef,
        unique_limit: i32,
        max_unique_to_collect: usize,
    ) -> Described {
        let table = table_of(column_type, cells);
        let result = column_statistics(&*table, 0, unique_limit, max_unique_to_collect)
            .unwrap_or_else(|_| panic!("the statistics failed"));
        assert!(!result.is_refreshing());
        assert!(result.is_flat());
        assert_eq!(result.row_set().size(), 1);
        let columns = result.definition().columns().iter().enumerate();
        columns
            .map(|(index, column)| {
                let cell = result.column(index).chunk(&result.row_set()).unwrap();
                // an instant prints as its nanoseconds
                let cell = match column.column_type {
                    ColumnType::Instant => {
                        arrow::compute::cast(&cell, &arrow::datatypes::DataType::Int64).unwrap()
                    }
                    _ => cell,
                };
                let text = if cell.is_null(0) {
                    String::new()
                } else {
                    array_value_to_string(&cell, 0).unwrap()
                };
                (column.name.clone(), column.column_type.clone(), text)
            })
            .collect()
    }

    fn stats(column_type: ColumnType, cells: ArrayRef) -> Described {
        stats_with(
            column_type,
            cells,
            DEFAULT_UNIQUE_LIMIT,
            MAX_UNIQUE_TO_COLLECT,
        )
    }

    fn expect(actual: &Described, expected: &[(&str, ColumnType, &str)]) {
        let actual: Vec<(&str, ColumnType, &str)> = actual
            .iter()
            .map(|(name, column_type, text)| (name.as_str(), column_type.clone(), text.as_str()))
            .collect();
        assert_eq!(actual, expected);
    }

    fn value<'a>(result: &'a Described, name: &str) -> &'a str {
        let found = result.iter().find(|(column, _, _)| column == name);
        &found.unwrap().2
    }

    /// The expected values of this and the following tests are what the Java server
    /// (42.9) answered for the same column.
    #[test]
    fn an_int_column_has_exact_sums_until_one_overflows() {
        let cells: Int32Array = [
            Some(5),
            None,
            Some(-5),
            Some(5),
            Some(0),
            Some(5),
            Some(i32::MAX),
            None,
            Some(-7),
            Some(3),
        ]
        .into_iter()
        .collect();
        // the square of the largest int does not fit an int, so the sum of the squares is
        // a double although it would fit a long
        expect(
            &stats(Int, Arc::new(cells)),
            &[
                ("COUNT", Long, "8"),
                ("SIZE", Long, "10"),
                ("SUM", Long, "2147483653"),
                ("SUM_ABS", Long, "2147483677"),
                ("SUM_SQRD", Double, "4.6116860141324206e18"),
                ("MIN", Int, "-7"),
                ("MAX", Int, "2147483647"),
                ("MIN_ABS", Int, "0"),
                ("MAX_ABS", Int, "2147483647"),
                ("AVG", Double, "268435456.625"),
                ("AVG_ABS", Double, "268435459.625"),
                ("STD_DEV", Double, "759250124.3374133"),
            ],
        );
    }

    #[test]
    fn a_long_column_goes_on_in_doubles_and_averages_the_absolute_sum_before_the_overflow() {
        let big = i64::MAX;
        let cells: Int64Array = [
            Some(big),
            Some(-big),
            None,
            Some(big),
            Some(3_037_000_500),
            Some(big),
        ]
        .into_iter()
        .collect();
        let result = stats(Long, Arc::new(cells));
        let double = |name: &str| value(&result, name).parse::<f64>().unwrap();
        assert_eq!(result[2], ("SUM".to_string(), Double, "1.844674407674655e19".to_string()));
        assert_eq!(double("SUM_ABS"), 3.689348815045611e19);
        assert_eq!(double("SUM_SQRD"), 3.402823669209385e38);
        assert_eq!(value(&result, "MIN"), "-9223372036854775807");
        assert_eq!(value(&result, "MIN_ABS"), "3037000500");
        assert_eq!(double("AVG"), 3.6893488153493105e18);
        // the first value alone, divided by five
        assert_eq!(double("AVG_ABS"), 1.8446744073709553e18);
        assert_eq!(double("STD_DEV"), 8.249634742131643e18);
    }

    #[test]
    fn an_empty_or_all_null_number_column_has_zero_sums_and_null_extremes() {
        let empty = stats(Int, ints([]));
        let nulls: Int32Array = [None, None].into_iter().collect();
        let nulls = stats(Int, Arc::new(nulls));
        for (result, size) in [(&empty, "0"), (&nulls, "2")] {
            expect(
                result,
                &[
                    ("COUNT", Long, "0"),
                    ("SIZE", Long, size),
                    ("SUM", Long, "0"),
                    ("SUM_ABS", Long, "0"),
                    ("SUM_SQRD", Long, "0"),
                    ("MIN", Int, ""),
                    ("MAX", Int, ""),
                    ("MIN_ABS", Int, ""),
                    ("MAX_ABS", Int, ""),
                    ("AVG", Double, ""),
                    ("AVG_ABS", Double, ""),
                    ("STD_DEV", Double, ""),
                ],
            );
        }
        // one value has an average and no deviation
        let one = stats(Long, longs([7]));
        assert_eq!(value(&one, "AVG"), "7.0");
        assert_eq!(value(&one, "STD_DEV"), "");
    }

    #[test]
    fn a_nan_is_an_extreme_only_where_it_comes_first() {
        let first = stats(Double, doubles([f64::NAN, 1.0, -2.0]));
        for name in ["MIN", "MAX", "MIN_ABS", "MAX_ABS", "SUM", "AVG", "STD_DEV"] {
            assert_eq!(value(&first, name), "NaN", "{name}");
        }
        let later = stats(Double, doubles([1.0, f64::NAN, -2.0]));
        assert_eq!(value(&later, "MIN"), "-2.0");
        assert_eq!(value(&later, "MAX"), "1.0");
        assert_eq!(value(&later, "MIN_ABS"), "1.0");
        assert_eq!(value(&later, "MAX_ABS"), "2.0");
        assert_eq!(value(&later, "SUM"), "NaN");
        assert_eq!(value(&later, "COUNT"), "3");
    }

    #[test]
    fn a_float_column_has_float_extremes_and_sums_of_doubles() {
        let cells = Float32Array::from(vec![Some(0.5), None, Some(-3.25), Some(-f32::MAX)]);
        let result = stats(ColumnType::Float, Arc::new(cells));
        // the value Java reserves for null is not counted
        assert_eq!(value(&result, "COUNT"), "2");
        assert_eq!(value(&result, "SUM"), "-2.75");
        assert_eq!(value(&result, "SUM_SQRD"), "10.8125");
        let min = result.iter().find(|(name, _, _)| name == "MIN").unwrap();
        assert_eq!(min.1, ColumnType::Float);
        assert_eq!(min.2, "-3.25");
        assert_eq!(value(&result, "MIN_ABS"), "0.5");
    }

    #[test]
    fn floating_point_sums_are_added_up_chunk_by_chunk() {
        let result = stats(Double, doubles((0..5000).map(|i| 0.1 * f64::from(i))));
        assert_eq!(value(&result, "SUM"), "1249750.0");
        assert_eq!(value(&result, "SUM_SQRD"), "416541675.00000006");
        assert_eq!(value(&result, "STD_DEV"), "144.35200033252053");

        // one chunk of a large value, then small ones: added one by one each would be
        // lost in the total, added up per chunk they are not
        let cells = (0..5000).map(|i| if i < 2048 { 1e16 } else { 3.0 });
        let result = stats(Double, doubles(cells));
        assert_eq!(value(&result, "SUM"), "2.0480000000000012e19");
        assert_eq!(value(&result, "SUM_SQRD"), "2.048000000000104e35");
    }

    #[test]
    fn an_instant_column_has_its_earliest_and_latest() {
        let cells = TimestampNanosecondArray::from(vec![Some(5), None, Some(-3), Some(9)])
            .with_timezone("UTC");
        let result = stats(ColumnType::Instant, Arc::new(cells));
        assert_eq!(result.len(), 4);
        assert_eq!(value(&result, "COUNT"), "3");
        assert_eq!(value(&result, "SIZE"), "4");
        assert_eq!(value(&result, "MIN"), "-3");
        assert_eq!(value(&result, "MAX"), "9");
        assert_eq!(result[2].1, ColumnType::Instant);
        let cells = TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC");
        let result = stats(ColumnType::Instant, Arc::new(cells));
        assert_eq!(value(&result, "MIN"), "");
        assert_eq!(value(&result, "MAX"), "");
    }

    #[test]
    fn a_string_column_lists_its_most_frequent_values_up_to_the_limit() {
        let cells = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            None,
            Some("b"),
            Some("a"),
            Some(""),
            None,
            Some("b"),
            Some("a"),
        ]);
        let cells: ArrayRef = Arc::new(cells);
        expect(
            &stats(ColumnType::String, cells.clone()),
            &[
                ("COUNT", Long, "8"),
                ("SIZE", Long, "10"),
                ("UNIQUE_VALUES", Int, "3"),
                (
                    "UNIQUE_KEYS",
                    ColumnType::array(ColumnType::String),
                    "[a, b, ]",
                ),
                ("UNIQUE_COUNTS", ColumnType::array(Long), "[4, 3, 1]"),
            ],
        );
        let one = stats_with(ColumnType::String, cells.clone(), 1, MAX_UNIQUE_TO_COLLECT);
        assert_eq!(value(&one, "UNIQUE_VALUES"), "3");
        assert_eq!(value(&one, "UNIQUE_KEYS"), "[a]");
        assert_eq!(value(&one, "UNIQUE_COUNTS"), "[4]");
        let none = stats_with(ColumnType::String, cells, 0, MAX_UNIQUE_TO_COLLECT);
        assert_eq!(value(&none, "UNIQUE_KEYS"), "[]");
        assert_eq!(value(&none, "UNIQUE_COUNTS"), "[]");
    }

    #[test]
    fn values_of_one_count_keep_the_order_of_javas_map_after_it_has_grown() {
        // 70 values, the first ten twice and five of them three times
        let names: Vec<String> = (0..70).map(|i| format!("w{i}")).collect();
        let mut cells: Vec<&str> = names.iter().map(String::as_str).collect();
        cells.extend(names[..10].iter().map(String::as_str));
        for _ in 0..2 {
            cells.extend(names[20..25].iter().map(String::as_str));
        }
        let result = stats(ColumnType::String, strings(cells));
        assert_eq!(value(&result, "UNIQUE_VALUES"), "70");
        assert_eq!(
            value(&result, "UNIQUE_KEYS"),
            "[w21, w22, w20, w24, w23, w6, w7, w0, w4, w8, w9, w1, w5, w3, w2, w45, w29, w17, \
             w61, w68]"
        );
        assert_eq!(
            value(&result, "UNIQUE_COUNTS"),
            "[3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1]"
        );
    }

    #[test]
    fn strings_hash_as_their_utf16_units() {
        let cells = [
            "é", "😀", "日本", "", "a b", "Zz", "zZ", "Aa", "BB", "é", "😀", "日本",
        ];
        let result = stats(ColumnType::String, strings(cells));
        assert_eq!(
            value(&result, "UNIQUE_KEYS"),
            "[é, 日本, 😀, zZ, BB, Aa, Zz, a b, ]"
        );
    }

    #[test]
    fn a_boolean_and_a_char_column_list_their_values_as_text() {
        let cells = BooleanArray::from(vec![Some(true), Some(false), None, Some(false), Some(true)]);
        let result = stats(ColumnType::Boolean, Arc::new(cells));
        assert_eq!(value(&result, "COUNT"), "4");
        assert_eq!(value(&result, "UNIQUE_KEYS"), "[true, false]");
        assert_eq!(value(&result, "UNIQUE_COUNTS"), "[2, 2]");

        // 0xFFFF is null
        let units = [66u16, 48, 65, 0xFFFF, 67, 65, 48];
        let cells = UInt16Array::from_iter_values(units);
        let result = stats(ColumnType::Char, Arc::new(cells));
        assert_eq!(value(&result, "COUNT"), "6");
        assert_eq!(value(&result, "SIZE"), "7");
        assert_eq!(value(&result, "UNIQUE_VALUES"), "4");
        assert_eq!(value(&result, "UNIQUE_COUNTS"), "[2, 2, 1, 1]");
    }

    #[test]
    fn beyond_the_values_to_collect_only_their_number_is_reported() {
        let cells = strings(["a", "b", "a", "c", "d", "b", "e"]);
        let within = stats_with(ColumnType::String, cells.clone(), 20, 5);
        assert_eq!(within.len(), 5);
        assert_eq!(value(&within, "UNIQUE_VALUES"), "5");
        let beyond = stats_with(ColumnType::String, cells, 20, 3);
        expect(
            &beyond,
            &[
                ("COUNT", Long, "7"),
                ("SIZE", Long, "7"),
                ("UNIQUE_VALUES", Int, "5"),
            ],
        );
    }

    #[test]
    fn a_negative_limit_fails_where_values_are_listed() {
        let texts = table_of(ColumnType::String, strings(["a"]));
        let failure = column_statistics(&*texts, 0, -1, MAX_UNIQUE_TO_COLLECT).err();
        assert!(matches!(failure, Some(ExportError::Internal(_))));
        let numbers = table_of(Int, ints([1]));
        assert!(column_statistics(&*numbers, 0, -1, MAX_UNIQUE_TO_COLLECT).is_ok());
    }
}
