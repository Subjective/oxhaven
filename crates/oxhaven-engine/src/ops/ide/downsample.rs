//! `run_chart_downsample`: the rows of a table that a line chart of some width needs.
//!
//! The X axis is cut into buckets of equal width, a few more than the chart has pixels.
//! The result has the parent's columns and, of each bucket, the first and the last row,
//! for each Y column the rows of its largest and its smallest value, and the rows on
//! either side of each run of nulls. With a zoom range the buckets cover that range, and
//! everything below and above it is one bucket each.
//!
//! The result of Java depends on more than the parent's rows: the bucket width is fixed
//! when the result is made, a tie between two rows goes to the row that was seen first,
//! and a value that stops being an extreme is only replaced when its bucket is read again.
//! So this module keeps the state Java keeps and changes it as Java does, one upstream
//! change at a time, where another operator would compute its result from the parent.
//! That includes what Java gets wrong, where it can be reproduced:
//!
//! - A cycle that only removes rows fails the result if one of them was an extreme of its
//!   bucket and the bucket has rows left (`BucketState#rescanIfNeeded` reads the Y values
//!   through a context that only adds and modifies make).
//! - A result made of an empty table, or of rows that all have one X value, has a bucket
//!   width of zero and fails when a row arrives.
//! - Once a modified row has moved to another bucket, the modified rows after it are not
//!   looked at in that cycle (`DownsamplerListener#handleModified` returns).
//! - When a removed or modified row has the bin `0`, the bucket of the row before it in
//!   the same chunk is used.
//! - A modify of an `Object` column (`String`, `Instant`, `Boolean`) to null leaves the
//!   row an extreme of its bucket.
//! - The buckets below and above a zoom range are not shifted, and no bucket forgets the
//!   rows it last reported when it is shifted.
//!
//! Where what Java then does cannot be reproduced, because it reads rows that are not in
//! the table or reports a change that no row set can follow, the result fails here with a
//! message that starts `not ported:` and names the Java method.

use crate::table::{holds_values, QueryTable, TableSpec};
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::TableUpdate;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

/// The rows read at a time. Part of the result: a removed or modified row whose bin is `0`
/// takes the bucket of the row before it in its chunk.
///
/// Behavior: `RunChartDownsample#CHUNK_SIZE`
const CHUNK_SIZE: u64 = 1 << 14;

/// The numbers of buckets a pixel count is rounded up to.
///
/// Behavior: `RunChartDownsample#BUCKET_SIZES`
const BUCKET_SIZES: [i32; 4] = [500, 1000, 2000, 4000];

/// Java's null `long`, which is what the X value of a null cell is.
const NULL_LONG: i64 = i64::MIN;

const JAVA: &str = "io.deephaven.clientsupport.plotdownsampling";

/// What to downsample.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Downsample {
    /// The width of the chart in pixels.
    pub pixel_count: i32,
    /// The X values the chart shows in full, as nanoseconds since the epoch or as the
    /// values of a `long` column: first and last.
    pub zoom_range: Option<(i64, i64)>,
    /// An `Instant` or `long` column.
    pub x_column: String,
    pub y_columns: Vec<String>,
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The failure of a state that Java goes on from in a way that is not reproduced.
fn not_ported(method: &str, what: &str) -> TableFailure {
    failure(format!("not ported: {JAVA}.{method}: {what}"))
}

/// The downsampled `parent`: static if the parent is, and otherwise kept up to date.
///
/// An error is what Java throws: a pixel count that is not positive, no Y column, a zoom
/// range whose first value is not below its last, a column that does not exist, an X
/// column that is neither `Instant` nor `long`, a Y column that is a vector, an array or a
/// table, and a bucket width that is not positive when there are rows.
///
/// # Panics
/// By `unported!`, for a `long` X column that does not hold its values in memory. Java
/// takes a `long` column only if its source can be read as `long`
/// (`ColumnSource#allowsReinterpret`), which the array sources and some of the others can
/// and the formula columns of a view cannot.
///
/// Behavior: `RunChartDownsample#apply`, `RunChartDownsample#makeDownsampledQueryTable`,
/// `RunChartDownsample.DownsamplerListener`. The result of Java is memoized on the parent;
/// here every call makes its own, which has the same rows.
pub fn run_chart_downsample(
    parent: &Arc<dyn Table>,
    request: &Downsample,
) -> Result<Arc<dyn Table>, TableFailure> {
    // RunChartDownsample#<init>
    if request.pixel_count <= 0 {
        return Err(failure(format!(
            "Assertion failed: asserted pxCount > 0, instead pxCount == {}.",
            request.pixel_count
        )));
    }
    if request.y_columns.is_empty() {
        return Err(failure(
            "Assertion failed: asserted yColumnNames.length > 0, instead yColumnNames.length == 0.",
        ));
    }
    if let Some((first, last)) = request.zoom_range {
        if first >= last {
            return Err(failure(format!(
                "Assertion failed: asserted zoomRange[0] < zoomRange[1], instead zoomRange[0] \
                 == {first}, zoomRange[1] == {last}."
            )));
        }
    }
    let bins = BUCKET_SIZES
        .into_iter()
        .find(|&size| size >= request.pixel_count)
        .unwrap_or(request.pixel_count);

    // DownsamplerListener#<init>
    let definition = parent.definition().clone();
    let column = |name: &String| {
        definition.index_of(name).ok_or_else(|| {
            let names: Vec<&str> = definition
                .columns()
                .iter()
                .map(|column| column.name.as_str())
                .collect();
            failure(format!(
                "Unknown column names [{name}], available column names are [{}]",
                names.join(", ")
            ))
        })
    };
    let x = column(&request.x_column)?;
    match definition.columns()[x].column_type {
        ColumnType::Instant => {}
        ColumnType::Long => {
            if !holds_values(&parent.column(x)) {
                unported!(
                    "io.deephaven.engine.table.ColumnSource#allowsReinterpret",
                    "downsampling by the long column {}, which is not held in memory",
                    request.x_column
                );
            }
        }
        _ => {
            return Err(failure(format!(
                "Cannot use non-Instant, non-long x column {} in downsample",
                request.x_column
            )))
        }
    }
    let ys = request
        .y_columns
        .iter()
        .map(&column)
        .collect::<Result<Vec<usize>, _>>()?;
    let trackers = ys
        .iter()
        .map(|&y| Tracker::of(&definition.columns()[y].column_type))
        .collect::<Result<Vec<_>, _>>()?;

    let result = QueryTable::derive(parent, || {
        let mut state = State {
            zoom: request.zoom_range,
            nanos_per_px: 0,
            x,
            ys,
            head: request.zoom_range.map(|_| Bucket::new(0, None)),
            tail: request.zoom_range.map(|_| Bucket::new(1, None)),
            states: BTreeMap::new(),
            trackers,
            // 0 and 1 are the buckets outside a zoom range
            available: RowSet::from_range(2, i64::from(bins) + 1),
            next_slot: bins as usize + 2,
        };
        let rows = parent.row_set();
        state.rerange(&**parent, &rows, bins)?;
        state.handle_added(&**parent, &rows)?;
        let selected = state.selected()?;
        let columns = (0..definition.columns().len())
            .map(|index| parent.column(index))
            .collect();
        // `QueryTable#getSubTable(TrackingRowSet)` gives the result no attributes
        let spec = TableSpec::new(definition.clone(), selected, columns);
        let operator = Operator {
            parent: parent.clone(),
            state: Mutex::new(state),
        };
        Ok((spec, operator))
    })?;
    Ok(result)
}

/// One value of a Y column, of the kinds Java compares in one way each.
#[derive(Clone, PartialEq, Debug)]
enum Value {
    /// `byte`, `short`, `int`, `long` and `char`, and `Instant` and `Boolean`, whose
    /// `compareTo` is the order of their nanoseconds and of `false` before `true`.
    Integer(i64),
    /// `float` and `double`, compared as Java's `<` and `>` do: nothing is above or below
    /// a NaN.
    Float(f64),
    Text(String),
}

fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => Some(super::compare_java_strings(a, b)),
        _ => unreachable!("the values of one column are of one kind"),
    }
}

/// The values of a chunk of a Y column.
fn y_values(values: &ArrayRef, column_type: &ColumnType) -> Vec<Option<Value>> {
    fn integers<T: arrow::datatypes::ArrowPrimitiveType>(values: &ArrayRef) -> Vec<Option<Value>>
    where
        T::Native: Into<i64>,
    {
        let values = values.as_primitive::<T>();
        values
            .iter()
            .map(|value| value.map(|value| Value::Integer(value.into())))
            .collect()
    }
    match column_type {
        ColumnType::Boolean => {
            let values = values.as_boolean();
            values
                .iter()
                .map(|value| value.map(|value| Value::Integer(i64::from(value))))
                .collect()
        }
        ColumnType::Byte => integers::<Int8Type>(values),
        ColumnType::Char => integers::<UInt16Type>(values),
        ColumnType::Short => integers::<Int16Type>(values),
        ColumnType::Int => integers::<Int32Type>(values),
        ColumnType::Long => integers::<Int64Type>(values),
        ColumnType::Instant => integers::<TimestampNanosecondType>(values),
        ColumnType::Float => {
            let values = values.as_primitive::<Float32Type>();
            values
                .iter()
                .map(|value| value.map(|value| Value::Float(f64::from(value))))
                .collect()
        }
        ColumnType::Double => {
            let values = values.as_primitive::<Float64Type>();
            values
                .iter()
                .map(|value| value.map(Value::Float))
                .collect()
        }
        ColumnType::String => {
            let values = values.as_string::<i32>();
            values
                .iter()
                .map(|value| value.map(|value| Value::Text(value.to_string())))
                .collect()
        }
        _ => unreachable!("a tracker was made for the column"),
    }
}

/// The values of a chunk of the X column as Java's `long`s.
fn x_values(values: &ArrayRef, column_type: &ColumnType) -> Vec<i64> {
    let nullable: Vec<Option<i64>> = match column_type {
        ColumnType::Instant => values
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect(),
        _ => values.as_primitive::<Int64Type>().iter().collect(),
    };
    nullable
        .into_iter()
        .map(|value| value.unwrap_or(NULL_LONG))
        .collect()
}

/// Behavior: `io.deephaven.function.Numeric#lowerBin(long, long)`
fn lower_bin(value: i64, interval: i64) -> Result<i64, TableFailure> {
    if value == NULL_LONG || interval == NULL_LONG {
        return Ok(NULL_LONG);
    }
    if interval <= 0 {
        return Err(failure(format!("Interval is not positive: {interval}")));
    }
    let quotient = value / interval;
    let floor = if value % interval != 0 && value < 0 {
        quotient - 1
    } else {
        quotient
    };
    Ok(interval.wrapping_mul(floor))
}

/// `rows` in pieces of [`CHUNK_SIZE`].
fn chunks(rows: &RowSet) -> impl Iterator<Item = RowSet> + '_ {
    (0..rows.size())
        .step_by(CHUNK_SIZE as usize)
        .map(|start| rows.sub_set_by_position_range(start, start + CHUNK_SIZE))
}

/// A row set that single keys are added to in bulk.
struct Keys {
    set: RowSet,
    pending: Vec<RowKey>,
}

impl Keys {
    fn new() -> Self {
        Keys {
            set: RowSet::empty(),
            pending: Vec::new(),
        }
    }

    fn insert(&mut self, key: RowKey) {
        self.pending.push(key);
    }

    fn rows(&mut self) -> &mut RowSet {
        if !self.pending.is_empty() {
            self.set.insert(&RowSet::from_keys(self.pending.drain(..)));
        }
        &mut self.set
    }

    fn remove(&mut self, key: RowKey) {
        self.rows().remove(&RowSet::from_range(key, key));
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.set = RowSet::empty();
    }
}

/// The largest or the smallest value of a bucket.
#[derive(Clone, Default)]
struct Extreme {
    value: Option<Value>,
    /// The row that holds the value. `None` is Java's `NULL_LONG`: no value has been seen.
    index: Option<RowKey>,
    /// False when the row was removed or its value moved inwards: the bucket is read again
    /// before the cycle ends.
    valid: bool,
}

impl Extreme {
    fn set(&mut self, value: &Value, row: RowKey) {
        self.value = Some(value.clone());
        self.index = Some(row);
        self.valid = true;
    }

    fn shift(&mut self, shifted: &RowSetShiftData) {
        self.index = self.index.map(|index| shifted.apply_to_key(index));
    }
}

#[derive(Clone, Default)]
struct Slot {
    min: Extreme,
    max: Extreme,
}

/// The extremes of one Y column in every bucket, by the bucket's slot. A bucket that takes
/// the slot of one that has emptied finds that bucket's extremes, as in Java, where it
/// shows in which of two equal values is kept.
///
/// Behavior: `ValueTracker` and its subclasses
struct Tracker {
    /// The column is an `Object` column in Java, which `ObjectValueTracker` tracks, and
    /// not one of the primitive trackers. The two differ in `update`.
    object: bool,
    slots: Vec<Slot>,
}

impl Tracker {
    /// Behavior: `ValueTracker#of`
    fn of(column_type: &ColumnType) -> Result<Tracker, TableFailure> {
        let object = match column_type {
            ColumnType::Byte
            | ColumnType::Char
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double => false,
            // a Boolean column is a column of objects, so Java's refusal of boolean
            // chunks is never reached
            ColumnType::Boolean | ColumnType::String | ColumnType::Instant => true,
            _ => {
                return Err(failure(
                    "Comparable.class.isAssignableFrom(columnSource.getType())",
                ))
            }
        };
        Ok(Tracker {
            object,
            slots: Vec::new(),
        })
    }

    fn slot(&mut self, offset: usize) -> &mut Slot {
        if offset >= self.slots.len() {
            self.slots.resize(offset + 1, Slot::default());
        }
        &mut self.slots[offset]
    }

    /// Behavior: `DoubleValueTracker#append`, `ObjectValueTracker#append`
    fn append(
        &mut self,
        offset: usize,
        row: RowKey,
        value: Option<&Value>,
        nulls: Option<&mut Keys>,
    ) -> Result<(), TableFailure> {
        let Some(value) = value else {
            if let Some(nulls) = nulls {
                nulls.insert(row);
            }
            return Ok(());
        };
        let slot = self.slot(offset);
        let first = slot.max.index.is_none();
        if first != slot.min.index.is_none() {
            return Err(failure(format!(
                "Assertion failed: asserted first == minIndex({offset}) == \
                 QueryConstants.NULL_LONG"
            )));
        }
        let against = |extreme: &Extreme| extreme.value.as_ref().and_then(|old| compare(value, old));
        if first || against(&slot.max) == Some(Ordering::Greater) {
            slot.max.set(value, row);
        }
        if first || against(&slot.min) == Some(Ordering::Less) {
            slot.min.set(value, row);
        }
        Ok(())
    }

    /// Behavior: `ValueTracker#remove`
    fn remove(&mut self, offset: usize, row: RowKey) {
        let slot = self.slot(offset);
        if slot.max.index == Some(row) {
            slot.max.valid = false;
        }
        if slot.min.index == Some(row) {
            slot.min.valid = false;
        }
    }

    /// Behavior: `DoubleValueTracker#update` and the other primitive trackers,
    /// `ObjectValueTracker#update`
    fn update(
        &mut self,
        offset: usize,
        row: RowKey,
        value: Option<&Value>,
        nulls: Option<&mut Keys>,
    ) -> Result<(), TableFailure> {
        let object = self.object;
        let slot = self.slot(offset);
        let Some(value) = value else {
            let tracked = nulls.is_some();
            if let Some(nulls) = nulls {
                nulls.insert(row);
            }
            if object {
                // with nulls tracked the row stays the extreme it was
                if !tracked {
                    for extreme in [&mut slot.max, &mut slot.min] {
                        if extreme.index == Some(row) {
                            extreme.valid = false;
                            extreme.index = None;
                        }
                    }
                }
            } else {
                for extreme in [&mut slot.max, &mut slot.min] {
                    if extreme.index == Some(row) {
                        extreme.valid = false;
                    }
                }
            }
            return Ok(());
        };
        if let Some(nulls) = nulls {
            nulls.remove(row);
        }
        for (extreme, outwards) in [
            (&mut slot.max, Ordering::Greater),
            (&mut slot.min, Ordering::Less),
        ] {
            let order = match &extreme.value {
                Some(old) => compare(value, old),
                // `compareTo(null)`; the primitive trackers compare with the zero of a
                // new array and then take the value because there is no row yet
                None if object && extreme.index != Some(row) => {
                    return Err(failure("java.lang.NullPointerException"))
                }
                None => None,
            };
            if extreme.index == Some(row) {
                if order == Some(outwards) || order == Some(Ordering::Equal) {
                    // still the extreme, with a new value
                    extreme.value = Some(value.clone());
                } else {
                    // perhaps no longer; the old value stays for others to be compared
                    // with until the bucket is read again
                    extreme.valid = false;
                }
            } else if order == Some(outwards) || (!object && extreme.index.is_none()) {
                extreme.set(value, row);
            }
        }
        Ok(())
    }
}

/// One bucket: the rows whose X values fall in it.
///
/// Behavior: `BucketState`
struct Bucket {
    rows: Keys,
    /// The bucket's slot in every [`Tracker`].
    offset: usize,
    /// Per Y column, the rows whose value is null. Not kept for the two buckets outside
    /// a zoom range.
    nulls: Option<Vec<Keys>>,
    /// The rows last reported, until a row of the bucket is added, removed or modified.
    cached: Option<RowSet>,
}

impl Bucket {
    fn new(offset: usize, null_columns: Option<usize>) -> Bucket {
        Bucket {
            rows: Keys::new(),
            offset,
            nulls: null_columns.map(|count| (0..count).map(|_| Keys::new()).collect()),
            cached: None,
        }
    }

    /// Behavior: `BucketState#append`
    fn append(
        &mut self,
        trackers: &mut [Tracker],
        row: RowKey,
        values: &[Vec<Option<Value>>],
        at: usize,
    ) -> Result<(), TableFailure> {
        self.rows.insert(row);
        for (column, tracker) in trackers.iter_mut().enumerate() {
            let nulls = self.nulls.as_mut().map(|nulls| &mut nulls[column]);
            tracker.append(self.offset, row, values[column][at].as_ref(), nulls)?;
        }
        self.cached = None;
        Ok(())
    }

    /// Behavior: `BucketState#remove`
    fn remove(&mut self, trackers: &mut [Tracker], row: RowKey) {
        self.rows.remove(row);
        for (column, tracker) in trackers.iter_mut().enumerate() {
            if let Some(nulls) = &mut self.nulls {
                nulls[column].remove(row);
            }
            tracker.remove(self.offset, row);
        }
        self.cached = None;
    }

    /// Behavior: `BucketState#update`
    fn update(
        &mut self,
        trackers: &mut [Tracker],
        row: RowKey,
        values: &[Vec<Option<Value>>],
        at: usize,
    ) -> Result<(), TableFailure> {
        for (column, tracker) in trackers.iter_mut().enumerate() {
            let nulls = self.nulls.as_mut().map(|nulls| &mut nulls[column]);
            tracker.update(self.offset, row, values[column][at].as_ref(), nulls)?;
        }
        self.cached = None;
        Ok(())
    }

    /// Behavior: `BucketState#shift`, which leaves the rows last reported as they were
    fn shift(
        &mut self,
        trackers: &mut [Tracker],
        shifted: &RowSetShiftData,
    ) -> Result<(), TableFailure> {
        let apply = |keys: &mut Keys| {
            let rows = keys.rows();
            *rows = shifted
                .apply(rows)
                .map_err(|_| not_ported("BucketState#shift", "a shift onto a row of the bucket"))?;
            Ok::<(), TableFailure>(())
        };
        apply(&mut self.rows)?;
        for nulls in self.nulls.iter_mut().flatten() {
            apply(nulls)?;
        }
        for tracker in trackers {
            let slot = tracker.slot(self.offset);
            slot.max.shift(shifted);
            slot.min.shift(shifted);
        }
        Ok(())
    }

    /// Reads the bucket again for the columns that have lost an extreme.
    ///
    /// Behavior: `BucketState#rescanIfNeeded`
    fn rescan_if_needed(
        &mut self,
        trackers: &mut [Tracker],
        parent: &dyn Table,
        ys: &[usize],
        y_context: bool,
    ) -> Result<(), TableFailure> {
        let size = self.rows.rows().size();
        let mut columns = Vec::new();
        for (column, tracker) in trackers.iter_mut().enumerate() {
            let slot = tracker.slot(self.offset);
            if let Some(nulls) = &mut self.nulls {
                if nulls[column].rows().size() == size {
                    // all null: nothing to look for
                    for extreme in [&mut slot.max, &mut slot.min] {
                        extreme.valid = true;
                        extreme.index = None;
                    }
                    continue;
                }
            }
            if !slot.max.valid || !slot.min.valid {
                columns.push(column);
            }
        }
        if columns.is_empty() {
            return Ok(());
        }
        for &column in &columns {
            if let Some(nulls) = &mut self.nulls {
                nulls[column].clear();
            }
            let slot = trackers[column].slot(self.offset);
            slot.max.index = None;
            slot.min.index = None;
        }
        if !y_context {
            // Java reads through a context that only a cycle with added or modified rows
            // has made
            return Err(failure(
                "Assertion failed: asserted yContexts.get(yColIndex) != null, instead \
                 yContexts.get(yColIndex) == null.",
            ));
        }
        let rows = self.rows.rows().clone();
        for chunk in chunks(&rows) {
            let values = columns
                .iter()
                .map(|&column| {
                    let source = parent.column(ys[column]);
                    Ok(y_values(&source.chunk(&chunk)?, source.column_type()))
                })
                .collect::<Result<Vec<_>, TableFailure>>()?;
            for (at, row) in chunk.iter().enumerate() {
                for (read, &column) in columns.iter().enumerate() {
                    let nulls = self.nulls.as_mut().map(|nulls| &mut nulls[column]);
                    trackers[column].append(self.offset, row, values[read][at].as_ref(), nulls)?;
                }
            }
        }
        Ok(())
    }

    /// The rows of the bucket that the result holds.
    ///
    /// Behavior: `BucketState#makeRowSet`
    fn selected(&mut self, trackers: &mut [Tracker]) -> Result<RowSet, TableFailure> {
        if let Some(cached) = &self.cached {
            return Ok(cached.clone());
        }
        let rows = self.rows.rows().clone();
        let (Some(first), Some(last)) = (rows.first_row_key(), rows.last_row_key()) else {
            return Err(failure(
                "Assertion failed: asserted !rowSet.empty(), instead rowSet.empty() == true.",
            ));
        };
        let mut keys = vec![first, last];
        let garbage = || not_ported("BucketState#makeRowSet", "a bucket without the row of an extreme");
        match &mut self.nulls {
            Some(nulls) => {
                for (column, tracker) in trackers.iter_mut().enumerate() {
                    if nulls[column].rows().size() != rows.size() {
                        let slot = tracker.slot(self.offset);
                        keys.push(slot.max.index.ok_or_else(garbage)?);
                        keys.push(slot.min.index.ok_or_else(garbage)?);
                    }
                }
                // the rows on either side of each run of nulls, and the ends of the run
                for nulls in nulls {
                    let nulls = nulls.rows();
                    if nulls.is_empty() {
                        continue;
                    }
                    let positions = rows.invert(nulls).map_err(|_| {
                        not_ported("BucketState#makeRowSet", "a null row outside its bucket")
                    })?;
                    let key = |position: u64| rows.get(position);
                    for run in positions.ranges() {
                        let (first, last) = (run.first() as u64, run.last() as u64);
                        if first > 0 {
                            keys.extend(key(first - 1));
                            keys.extend(key(first));
                        }
                        if last < rows.size() - 1 {
                            keys.extend(key(last));
                            keys.extend(key(last + 1));
                        }
                    }
                }
            }
            None => {
                for tracker in trackers.iter_mut() {
                    let slot = tracker.slot(self.offset);
                    match (slot.max.index, slot.min.index) {
                        (Some(max), Some(min)) => keys.extend([max, min]),
                        // every value is null: the first and last row are enough
                        (None, None) => {}
                        _ => {
                            return Err(failure(
                                "Assertion failed: asserted max != QueryConstants.NULL_LONG",
                            ))
                        }
                    }
                }
            }
        }
        let selected = RowSet::from_keys(keys);
        self.cached = Some(selected.clone());
        Ok(selected)
    }
}

/// Which bucket a bin belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    /// Everything below a zoom range.
    Head,
    /// Everything above a zoom range.
    Tail,
    Bin(i64),
}

/// Behavior: the fields of `RunChartDownsample.DownsamplerListener`
struct State {
    zoom: Option<(i64, i64)>,
    /// The width of a bucket, in the units of the X column.
    nanos_per_px: i64,
    x: usize,
    ys: Vec<usize>,
    head: Option<Bucket>,
    tail: Option<Bucket>,
    states: BTreeMap<i64, Bucket>,
    trackers: Vec<Tracker>,
    /// The slots no bucket has, below `next_slot`.
    available: RowSet,
    next_slot: usize,
}

impl State {
    /// Behavior: `DownsamplerListener#rerange`
    fn rerange(&mut self, parent: &dyn Table, rows: &RowSet, bins: i32) -> Result<(), TableFailure> {
        let (first, last) = match self.zoom {
            Some(range) => range,
            None => match (rows.first_row_key(), rows.last_row_key()) {
                (Some(first), Some(last)) => {
                    let source = parent.column(self.x);
                    let ends = RowSet::from_keys([first, last]);
                    let values = x_values(&source.chunk(&ends)?, source.column_type());
                    (values[0], values[values.len() - 1])
                }
                // Java reads the row key -1, which is null
                _ => (NULL_LONG, NULL_LONG),
            },
        };
        // 10% more than the range, so that some can be lost without a new range
        self.nanos_per_px = (1.1 * last.wrapping_sub(first) as f64 / f64::from(bins)) as i64;
        Ok(())
    }

    /// Behavior: the choice in `DownsamplerListener#getOrCreateBucket` and `#getBucket`
    fn place(&self, bin: i64) -> Place {
        if let Some((first, last)) = self.zoom {
            if bin.wrapping_add(self.nanos_per_px) < first {
                return Place::Head;
            }
            if bin > last {
                return Place::Tail;
            }
        }
        Place::Bin(bin)
    }

    fn bucket(&mut self, place: Place) -> Option<(&mut Bucket, &mut [Tracker])> {
        let bucket = match place {
            Place::Head => self.head.as_mut(),
            Place::Tail => self.tail.as_mut(),
            Place::Bin(bin) => self.states.get_mut(&bin),
        }?;
        Some((bucket, &mut self.trackers))
    }

    /// Behavior: `DownsamplerListener#getOrCreateBucket`, `#nextPosition`
    fn bucket_or_new(&mut self, bin: i64) -> (&mut Bucket, &mut [Tracker]) {
        let place = self.place(bin);
        if let Place::Bin(bin) = place {
            if !self.states.contains_key(&bin) {
                let offset = match self.available.first_row_key() {
                    Some(reused) => {
                        self.available.remove(&RowSet::from_range(reused, reused));
                        reused as usize
                    }
                    None => {
                        self.next_slot += 1;
                        self.next_slot - 1
                    }
                };
                let bucket = Bucket::new(offset, Some(self.ys.len()));
                self.states.insert(bin, bucket);
            }
        }
        match self.bucket(place) {
            Some(found) => found,
            None => unreachable!("the bucket was made, and a zoom range has a head and a tail"),
        }
    }

    /// Behavior: `DownsamplerListener#getBucket`
    fn existing(&mut self, bin: i64) -> Result<Place, TableFailure> {
        let place = self.place(bin);
        match self.bucket(place) {
            Some(_) => Ok(place),
            None => Err(failure(format!("Failed to find state for bucket {bin}"))),
        }
    }

    fn y_chunks(
        &self,
        parent: &dyn Table,
        rows: &RowSet,
    ) -> Result<Vec<Vec<Option<Value>>>, TableFailure> {
        self.ys
            .iter()
            .map(|&y| {
                let source = parent.column(y);
                Ok(y_values(&source.chunk(rows)?, source.column_type()))
            })
            .collect()
    }

    fn x_chunk(&self, parent: &dyn Table, rows: &RowSet, prev: bool) -> Result<Vec<i64>, TableFailure> {
        let source = parent.column(self.x);
        let values = if prev {
            source.prev_chunk(rows)?
        } else {
            source.chunk(rows)?
        };
        Ok(x_values(&values, source.column_type()))
    }

    /// Behavior: `DownsamplerListener#handleAdded`
    fn handle_added(&mut self, parent: &dyn Table, added: &RowSet) -> Result<(), TableFailure> {
        for chunk in chunks(added) {
            let xs = self.x_chunk(parent, &chunk, false)?;
            let values = self.y_chunks(parent, &chunk)?;
            for (at, row) in chunk.iter().enumerate() {
                let bin = lower_bin(xs[at], self.nanos_per_px)?;
                let (bucket, trackers) = self.bucket_or_new(bin);
                bucket.append(trackers, row, &values, at)?;
            }
        }
        Ok(())
    }

    /// Behavior: `DownsamplerListener#handleRemoved`
    fn handle_removed(&mut self, parent: &dyn Table, removed: &RowSet) -> Result<(), TableFailure> {
        for chunk in chunks(removed) {
            let xs = self.x_chunk(parent, &chunk, true)?;
            let mut place = None;
            for (at, row) in chunk.iter().enumerate() {
                let bin = lower_bin(xs[at], self.nanos_per_px)?;
                // Java compares the bin with a last bin that it never sets, so a row of
                // bin 0 takes the bucket of the row before it
                let place = match place {
                    Some(place) if bin == 0 => place,
                    _ => *place.insert(self.existing(bin)?),
                };
                if let Some((bucket, trackers)) = self.bucket(place) {
                    bucket.remove(trackers, row);
                }
            }
        }
        Ok(())
    }

    /// Behavior: `DownsamplerListener#handleModified`
    fn handle_modified(&mut self, parent: &dyn Table, upstream: &TableUpdate) -> Result<(), TableFailure> {
        // Java reads the old X values at the new row keys. That is the old value of some
        // other row where the rows were shifted, and nothing where there was no row.
        if !upstream.shifted.is_empty() && !upstream.modified.is_subset_of(&parent.prev_row_set()) {
            return Err(not_ported(
                "RunChartDownsample.DownsamplerListener#handleModified",
                "a modified row at a row key that held no row before the shift",
            ));
        }
        for chunk in chunks(&upstream.modified) {
            let old_xs = self.x_chunk(parent, &chunk, true)?;
            let new_xs = self.x_chunk(parent, &chunk, false)?;
            let values = self.y_chunks(parent, &chunk)?;
            let mut place = None;
            for (at, row) in chunk.iter().enumerate() {
                let bin = lower_bin(old_xs[at], self.nanos_per_px)?;
                let new_bin = lower_bin(new_xs[at], self.nanos_per_px)?;
                let place = match place {
                    Some(place) if bin == 0 => place,
                    _ => *place.insert(self.existing(bin)?),
                };
                if bin != new_bin {
                    if let Some((bucket, trackers)) = self.bucket(place) {
                        bucket.remove(trackers, row);
                    }
                    let (bucket, trackers) = self.bucket_or_new(new_bin);
                    bucket.append(trackers, row, &values, at)?;
                    // Java returns here, and with that leaves the rest of the modified
                    // rows as they were
                    return Ok(());
                }
                if let Some((bucket, trackers)) = self.bucket(place) {
                    bucket.update(trackers, row, &values, at)?;
                }
            }
        }
        Ok(())
    }

    /// Behavior: `DownsamplerListener#handleShifts`, which leaves out the buckets outside
    /// a zoom range
    fn handle_shifts(&mut self, shifted: &RowSetShiftData) -> Result<(), TableFailure> {
        if shifted.is_empty() {
            return Ok(());
        }
        for bucket in self.states.values_mut() {
            bucket.shift(&mut self.trackers, shifted)?;
        }
        Ok(())
    }

    /// Behavior: `DownsamplerListener#performRescans`, `#releasePosition`
    fn perform_rescans(&mut self, parent: &dyn Table, y_context: bool) -> Result<(), TableFailure> {
        let bins: Vec<i64> = self.states.keys().copied().collect();
        for bin in bins {
            let Some(bucket) = self.states.get_mut(&bin) else {
                continue;
            };
            if bucket.rows.rows().is_empty() {
                let offset = bucket.offset as RowKey;
                self.states.remove(&bin);
                self.available.insert(&RowSet::from_range(offset, offset));
            } else {
                bucket.rescan_if_needed(&mut self.trackers, parent, &self.ys, y_context)?;
            }
        }
        Ok(())
    }

    /// The rows of the result.
    ///
    /// Behavior: `DownsamplerListener#indexFromStates`
    fn selected(&mut self) -> Result<RowSet, TableFailure> {
        let mut selected = RowSet::empty();
        let outside = [self.head.as_mut(), self.tail.as_mut()];
        for bucket in outside.into_iter().flatten() {
            if !bucket.rows.rows().is_empty() {
                selected.insert(&bucket.selected(&mut self.trackers)?);
            }
        }
        let mut keys = Vec::new();
        for bucket in self.states.values_mut() {
            let rows = bucket.selected(&mut self.trackers)?;
            keys.extend(rows.ranges().iter().copied());
        }
        selected.insert(&RowSet::from_ranges(keys));
        Ok(selected)
    }
}

struct Operator {
    parent: Arc<dyn Table>,
    state: Mutex<State>,
}

impl crate::table::UnaryOperator for Operator {
    /// Behavior: `DownsamplerListener#onUpdate`, `#notifyResultTable`
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let parent = &*self.parent;
        state.handle_removed(parent, &upstream.removed)?;
        state.handle_shifts(&upstream.shifted)?;
        state.handle_added(parent, &upstream.added)?;
        if !upstream.modified.is_empty() {
            state.handle_modified(parent, upstream)?;
        }
        // the Y values can be read again in a cycle that has read some already
        let y_context = !upstream.added.is_empty() || !upstream.modified.is_empty();
        state.perform_rescans(parent, y_context)?;

        let selected = state.selected()?;
        let last = result.row_set();
        let update = TableUpdate {
            added: selected.minus(&last),
            removed: last.minus(&selected),
            modified: selected.intersect(&last).intersect(&upstream.modified),
            shifted: upstream.shifted.clone(),
            modified_columns: upstream.modified_columns.clone(),
        };
        // Java makes its row set `selected` whatever the shifts say and whatever rows the
        // parent has. A table can follow that only when the two agree.
        const NOTIFY: &str = "RunChartDownsample.DownsamplerListener#notifyResultTable";
        if !selected.is_subset_of(&parent.row_set()) {
            return Err(not_ported(NOTIFY, "a result with rows its parent does not have"));
        }
        match update.clone().normalized() {
            None if selected == *last => {}
            Some(delivered) if delivered.apply_to(&last).as_ref() == Ok(&selected) => {}
            _ => {
                return Err(not_ported(
                    NOTIFY,
                    "a change of the result that its shifts do not lead to",
                ))
            }
        }
        Ok(update)
    }
}

#[cfg(test)]
#[path = "downsample_tests.rs"]
mod tests;
