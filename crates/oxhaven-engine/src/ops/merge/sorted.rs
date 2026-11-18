//! `merge_sorted`: the rows of several static tables, merged by a key column.
//!
//! Each table has a cursor on its first row. The row of the least cursor joins the result
//! and that cursor moves on, until every cursor is past its last row. Tables that are in
//! order by the key give a result in order by the key; other tables give whatever this
//! procedure gives.

use super::MergeError;
use crate::ops::agg;
use crate::table::{QueryTable, TableSpec};
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::interleave;
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_column::FlatSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use std::cmp::Ordering;
use std::sync::Arc;

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// The key column of one table, its rows in the order of the table's row set.
enum Keys {
    /// Boolean (false before true), byte, char, short, int, long and Instant keys.
    Whole(Vec<Option<i64>>),
    /// Float and double keys.
    Real(Vec<Option<f64>>),
    Text(ArrayRef),
    /// Keys of a vector or an array type, which Java compares and this does not.
    Unported(ColumnType),
}

impl Keys {
    fn of(column_type: &ColumnType, values: &ArrayRef) -> Keys {
        fn whole<T: Into<i64>>(values: impl Iterator<Item = Option<T>>) -> Keys {
            Keys::Whole(values.map(|value| value.map(Into::into)).collect())
        }
        match column_type {
            ColumnType::Boolean => whole(values.as_boolean().iter()),
            ColumnType::Byte => whole(values.as_primitive::<Int8Type>().iter()),
            ColumnType::Char => whole(values.as_primitive::<UInt16Type>().iter()),
            ColumnType::Short => whole(values.as_primitive::<Int16Type>().iter()),
            ColumnType::Int => whole(values.as_primitive::<Int32Type>().iter()),
            ColumnType::Long => whole(values.as_primitive::<Int64Type>().iter()),
            ColumnType::Instant => whole(values.as_primitive::<TimestampNanosecondType>().iter()),
            ColumnType::Float => Keys::Real(
                values
                    .as_primitive::<Float32Type>()
                    .iter()
                    .map(|value| value.map(f64::from))
                    .collect(),
            ),
            ColumnType::Double => Keys::Real(values.as_primitive::<Float64Type>().iter().collect()),
            ColumnType::String => Keys::Text(values.clone()),
            other => Keys::Unported(other.clone()),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Keys::Whole(values) => values[row].is_none(),
            Keys::Real(values) => values[row].is_none(),
            Keys::Text(values) => values.is_null(row),
            Keys::Unported(column_type) => unported!(
                "io.deephaven.engine.table.impl.util.MergeSortedHelper.TableCursor#compareTo",
                "a key column of type {column_type:?}"
            ),
        }
    }

    /// The order of two keys that are not null.
    ///
    /// Behavior: the `compareTo` of the boxed key: `java.lang.Double#compare` and
    /// `java.lang.Float#compare` (`-0.0` before `0.0`, every NaN equal and the greatest),
    /// `java.lang.String#compareTo` (by UTF-16 code unit), `java.lang.Boolean#compareTo`
    /// (false first), `java.time.Instant#compareTo`
    fn compare(&self, row: usize, other: &Keys, other_row: usize) -> Ordering {
        match (self, other) {
            (Keys::Whole(left), Keys::Whole(right)) => left[row].cmp(&right[other_row]),
            (Keys::Real(left), Keys::Real(right)) => {
                // Java compares the canonical NaN, which is the greatest value
                let canonical =
                    |value: Option<f64>| value.map(|v| if v.is_nan() { f64::NAN } else { v });
                match (canonical(left[row]), canonical(right[other_row])) {
                    (Some(left), Some(right)) => left.total_cmp(&right),
                    _ => panic!("a null key is refused before it is compared"),
                }
            }
            (Keys::Text(left), Keys::Text(right)) => left
                .as_string::<i32>()
                .value(row)
                .encode_utf16()
                .cmp(right.as_string::<i32>().value(other_row).encode_utf16()),
            _ => panic!("the key columns have one type"),
        }
    }
}

/// A table's position in the merge: its next row, as a position in its row set.
#[derive(Clone, Copy)]
struct Cursor {
    table: usize,
    row: usize,
}

/// The cursors that have a row left, least first.
///
/// Behavior: `java.util.PriorityQueue` of `MergeSortedHelper.TableCursor`. The comparisons
/// are made in Java's order because a comparison of a null key is an error, and whether a
/// null key is compared depends on that order: a null key in the only cursor left is never
/// compared and merges as any other value.
#[derive(Default)]
struct Queue {
    heap: Vec<Cursor>,
}

impl Queue {
    /// Behavior: io.deephaven.engine.table.impl.util.MergeSortedHelper.TableCursor#compareTo,
    /// without the cases of a finished cursor, which is never in the queue
    fn compare(keys: &[Keys], this: Cursor, other: Cursor) -> Result<Ordering, MergeError> {
        let (keys, other_keys) = (&keys[this.table], &keys[other.table]);
        if keys.is_null(this.row) {
            return Err(MergeError::NullKey(
                "Cannot invoke \"java.lang.Comparable.compareTo(Object)\" because \
                 \"this.currentKey\" is null"
                    .into(),
            ));
        }
        if other_keys.is_null(other.row) {
            return Err(MergeError::NullKey("the key compared with is null".into()));
        }
        Ok(keys
            .compare(this.row, other_keys, other.row)
            .then(this.table.cmp(&other.table)))
    }

    /// Behavior: `java.util.PriorityQueue#offer` and `#siftUpComparable`
    fn add(&mut self, keys: &[Keys], cursor: Cursor) -> Result<(), MergeError> {
        let mut at = self.heap.len();
        self.heap.push(cursor);
        while at > 0 {
            let parent = (at - 1) / 2;
            if Self::compare(keys, cursor, self.heap[parent])? != Ordering::Less {
                break;
            }
            self.heap[at] = self.heap[parent];
            at = parent;
        }
        self.heap[at] = cursor;
        Ok(())
    }

    /// Behavior: `java.util.PriorityQueue#poll` and `#siftDownComparable`
    fn poll(&mut self, keys: &[Keys]) -> Result<Option<Cursor>, MergeError> {
        let Some(&least) = self.heap.first() else {
            return Ok(None);
        };
        let moved = self.heap.pop().unwrap_or(least);
        let size = self.heap.len();
        if size > 0 {
            let mut at = 0;
            while at < size / 2 {
                let mut child = 2 * at + 1;
                let right = child + 1;
                if right < size
                    && Self::compare(keys, self.heap[child], self.heap[right])? == Ordering::Greater
                {
                    child = right;
                }
                if Self::compare(keys, moved, self.heap[child])? != Ordering::Greater {
                    break;
                }
                self.heap[at] = self.heap[child];
                at = child;
            }
            self.heap[at] = moved;
        }
        Ok(Some(least))
    }
}

/// `Class#toString` of a column's data type, as `Require#eq` prints it: the name of a
/// primitive, else `class` (`interface` for a vector) and the class's binary name.
fn java_class(column_type: &ColumnType) -> String {
    agg::java_class(column_type)
}

fn names(definition: &TableDefinition) -> String {
    let names: Vec<&str> = definition
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    names.join(", ")
}

/// The rows of `tables`, merged by `key_column`: at each step the next row is the one with
/// the least key among the tables' next rows, of the earliest table where keys are equal.
/// Where every table is in order by the key, so is the result. The tables must be static
/// and have the same column names, in any order, with the same types; the result has the
/// first table's columns, is static, and has the row keys `0..size` without being flat.
/// No tables give a table without columns or rows.
///
/// A null key is refused where it would be compared with the key of another table, and
/// only there: one table alone, or the last table with rows left, may have null keys.
///
/// # Panics
/// With `unported!` for a key column of a vector or array type, which Java compares, and
/// for columns of one name whose types differ where one of them is a vector or an array.
///
/// Behavior: io.deephaven.engine.util.TableTools#mergeSorted and
/// io.deephaven.engine.table.impl.util.MergeSortedHelper#mergeSortedHelper
pub fn merge_sorted(
    tables: &[Arc<dyn Table>],
    key_column: &str,
) -> Result<Arc<dyn Table>, MergeError> {
    let mut keys: Vec<Keys> = Vec::new();
    // For each table, the result's columns as chunks of its rows
    let mut chunks: Vec<Vec<ArrayRef>> = Vec::new();
    let mut sizes: Vec<usize> = Vec::new();
    let mut queue = Queue::default();
    let first_definition = tables.first().map(|table| table.definition().clone());
    for (index, table) in tables.iter().enumerate() {
        if table.is_refreshing() {
            return Err(MergeError::Unsupported(
                "mergeSorted does not yet support refreshing tables!".into(),
            ));
        }
        let definition = table.definition();
        let first = first_definition.as_ref().unwrap_or(definition);
        if index > 0 {
            let same_names = definition.columns().len() == first.columns().len()
                && definition
                    .columns()
                    .iter()
                    .all(|column| first.index_of(&column.name).is_some());
            if !same_names {
                return Err(MergeError::Runtime(format!(
                    "Incompatible column sources: [{}] and [{}]",
                    names(first),
                    names(definition)
                )));
            }
            for column in definition.columns() {
                let expected = match first.index_of(&column.name) {
                    Some(at) => &first.columns()[at].column_type,
                    None => panic!("the tables have the same column names"),
                };
                if *expected != column.column_type {
                    return Err(MergeError::Runtime(format!(
                        "Requirement failed: required source.getType() == \
                         innerSources.get(0).getType(), instead source.getType() == {}, \
                         innerSources.get(0).getType() == {}.",
                        java_class(&column.column_type),
                        java_class(expected)
                    )));
                }
            }
        }
        let Some(key) = definition.index_of(key_column) else {
            return Err(MergeError::NoSuchColumn(format!(
                "Unknown column names [{key_column}], available column names are [{}]",
                names(definition)
            )));
        };
        let rows = table.row_set();
        let read = |column: usize| table.column(column).chunk(&rows);
        let table_keys = Keys::of(&definition.columns()[key].column_type, &read(key)?);
        keys.push(table_keys);
        chunks.push(
            first
                .columns()
                .iter()
                .map(|column| match definition.index_of(&column.name) {
                    Some(at) => read(at),
                    None => panic!("the tables have the same column names"),
                })
                .collect::<Result<_, _>>()?,
        );
        sizes.push(rows.size() as usize);
        if !rows.is_empty() {
            let first_row = Cursor {
                table: index,
                row: 0,
            };
            queue.add(&keys, first_row)?;
        }
    }

    let mut order: Vec<(usize, usize)> = Vec::with_capacity(sizes.iter().sum());
    while let Some(least) = queue.poll(&keys)? {
        order.push((least.table, least.row));
        let next = Cursor {
            table: least.table,
            row: least.row + 1,
        };
        if next.row < sizes[next.table] {
            queue.add(&keys, next)?;
        }
    }

    let definition = match first_definition {
        Some(definition) => definition,
        None => TableDefinition::new(Vec::new()).map_err(failure)?,
    };
    let columns = definition
        .columns()
        .iter()
        .enumerate()
        .map(|(at, column)| {
            let parts: Vec<&dyn Array> = chunks.iter().map(|table| table[at].as_ref()).collect();
            let values = interleave(&parts, &order).map_err(failure)?;
            let source: Arc<dyn oxhaven_contracts::table::ColumnSource> =
                Arc::new(FlatSource::new(column.column_type.clone(), values)?);
            Ok(source)
        })
        .collect::<Result<Vec<_>, TableFailure>>()?;
    let rows = RowSet::flat(order.len() as u64);
    Ok(QueryTable::new_static(TableSpec::new(
        definition, rows, columns,
    )))
}
