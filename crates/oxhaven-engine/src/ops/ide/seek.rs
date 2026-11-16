//! `seek_row`: the position of the next row, from some row on, whose value in one column
//! is a given value. The grid's "go to row".
//!
//! A table that is sorted by the column first (it has the column in its `SortedColumns`
//! attribute) is searched by bisection, and every other table row by row, wrapping around
//! at the end. The two do not agree on everything, and neither does Java: bisection
//! ignores `contains`, compares a `String` with a `char` column by a cast that fails, and
//! with `insensitive` bisects rows that are sorted with regard to case.

use crate::table::marks;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use std::cmp::Ordering;

/// A value to look for, as the Java object the server makes of the request's literal.
#[derive(Clone, PartialEq, Debug)]
pub enum SeekValue {
    String(String),
    /// A `java.time.Instant`, as nanoseconds since the epoch.
    Instant(i64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
}

/// What to look for and from where.
#[derive(Clone, PartialEq, Debug)]
pub struct Seek {
    /// The position to start from. A negative one is 0.
    pub starting_row: i64,
    pub column: String,
    pub value: SeekValue,
    /// Compare text without regard to case.
    pub insensitive: bool,
    /// Look for text that contains the value.
    pub contains: bool,
    pub backward: bool,
}

/// The rows of a column that are read at a time by the search that goes row by row.
const CHUNK_SIZE: u64 = 4096;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn not_ported(what: &str) -> TableFailure {
    failure(format!(
        "not ported: io.deephaven.clientsupport.gotorow.SeekRow#seek: {what}"
    ))
}

/// A cell as the object Java reads from the column.
#[derive(Clone, PartialEq, Debug)]
enum Cell {
    Null,
    Char(u16),
    Value(SeekValue),
    /// An object of a class no seek value has: a vector, an array, a table.
    Other,
}

/// The cells of a chunk.
fn cells(values: &ArrayRef, column_type: &ColumnType) -> Vec<Cell> {
    fn primitive<T: arrow::datatypes::ArrowPrimitiveType>(
        values: &ArrayRef,
        cell: impl Fn(T::Native) -> Cell,
    ) -> Vec<Cell> {
        let values = values.as_primitive::<T>();
        values
            .iter()
            .map(|value| value.map_or(Cell::Null, &cell))
            .collect()
    }
    let value = Cell::Value;
    match column_type {
        ColumnType::Boolean => {
            let values = values.as_boolean();
            values
                .iter()
                .map(|cell| cell.map_or(Cell::Null, |cell| value(SeekValue::Boolean(cell))))
                .collect()
        }
        ColumnType::Byte => primitive::<Int8Type>(values, |v| value(SeekValue::Byte(v))),
        ColumnType::Char => primitive::<UInt16Type>(values, Cell::Char),
        ColumnType::Short => primitive::<Int16Type>(values, |v| value(SeekValue::Short(v))),
        ColumnType::Int => primitive::<Int32Type>(values, |v| value(SeekValue::Int(v))),
        ColumnType::Long => primitive::<Int64Type>(values, |v| value(SeekValue::Long(v))),
        ColumnType::Float => primitive::<Float32Type>(values, |v| value(SeekValue::Float(v))),
        ColumnType::Double => primitive::<Float64Type>(values, |v| value(SeekValue::Double(v))),
        ColumnType::Instant => {
            primitive::<TimestampNanosecondType>(values, |v| value(SeekValue::Instant(v)))
        }
        ColumnType::String => {
            let values = values.as_string::<i32>();
            values
                .iter()
                .map(|cell| {
                    cell.map_or(Cell::Null, |cell| {
                        value(SeekValue::String(cell.to_string()))
                    })
                })
                .collect()
        }
        _ => (0..values.len())
            .map(|at| {
                if values.is_null(at) {
                    Cell::Null
                } else {
                    Cell::Other
                }
            })
            .collect(),
    }
}

/// Behavior: `java.lang.Double#compare`, and `java.lang.Float#compare` of two floats made
/// doubles: `-0.0` is below `0.0`, and NaN is above everything and equal to itself.
fn compare_doubles(a: f64, b: f64) -> Ordering {
    let bits = |value: f64| {
        if value.is_nan() {
            f64::NAN.to_bits() as i64
        } else {
            value.to_bits() as i64
        }
    };
    a.partial_cmp(&b)
        .filter(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| bits(a).cmp(&bits(b)))
}

/// Behavior: `java.lang.Object#equals` of the boxed values: of one class and equal, where
/// a NaN equals a NaN and `0.0` does not equal `-0.0`.
fn equal(seek: &SeekValue, cell: &Cell) -> bool {
    match (seek, cell) {
        (SeekValue::Double(a), Cell::Value(SeekValue::Double(b))) => {
            compare_doubles(*a, *b) == Ordering::Equal
        }
        (SeekValue::Float(a), Cell::Value(SeekValue::Float(b))) => {
            compare_doubles(f64::from(*a), f64::from(*b)) == Ordering::Equal
        }
        (seek, Cell::Value(value)) => seek == value,
        _ => false,
    }
}

const CLASS_CAST: &str = "java.lang.ClassCastException";

struct Search<'a> {
    seek: &'a Seek,
    rows: &'a RowSet,
    source: &'a dyn ColumnSource,
}

impl Search<'_> {
    /// The cell at `position`, or null where the table has no such row: Java reads the
    /// row key `-1` there.
    fn cell(&self, position: i64) -> Result<Cell, TableFailure> {
        let key = u64::try_from(position)
            .ok()
            .and_then(|position| self.rows.get(position));
        let Some(key) = key else {
            return Ok(Cell::Null);
        };
        let values = self.source.chunk(&RowSet::from_range(key, key))?;
        Ok(cells(&values, self.source.column_type()).remove(0))
    }

    /// Behavior: `SeekRow#nullSafeCompare`
    fn compare(&self, cell: &Cell) -> Result<Ordering, TableFailure> {
        let mismatch = || failure(CLASS_CAST);
        let value = match cell {
            Cell::Null => return Ok(Ordering::Less),
            Cell::Char(_) | Cell::Other => return Err(mismatch()),
            Cell::Value(value) => value,
        };
        if self.seek.insensitive {
            let (SeekValue::String(cell), SeekValue::String(seek)) = (value, &self.seek.value)
            else {
                return Err(mismatch());
            };
            return Ok(super::compare_java_strings(
                &cell.to_lowercase(),
                &seek.to_lowercase(),
            ));
        }
        Ok(match (value, &self.seek.value) {
            (SeekValue::String(a), SeekValue::String(b)) => super::compare_java_strings(a, b),
            (SeekValue::Instant(a), SeekValue::Instant(b)) => a.cmp(b),
            (SeekValue::Byte(a), SeekValue::Byte(b)) => a.cmp(b),
            (SeekValue::Short(a), SeekValue::Short(b)) => a.cmp(b),
            (SeekValue::Int(a), SeekValue::Int(b)) => a.cmp(b),
            (SeekValue::Long(a), SeekValue::Long(b)) => a.cmp(b),
            (SeekValue::Float(a), SeekValue::Float(b)) => {
                compare_doubles(f64::from(*a), f64::from(*b))
            }
            (SeekValue::Double(a), SeekValue::Double(b)) => compare_doubles(*a, *b),
            (SeekValue::Boolean(a), SeekValue::Boolean(b)) => a.cmp(b),
            _ => return Err(mismatch()),
        })
    }

    /// The first or the last position in `start..=end` whose cell is the value, or `-1`.
    ///
    /// Behavior: `SeekRow#findEdgeOccurrence`
    fn edge(
        &self,
        mut start: i64,
        mut end: i64,
        find_first: bool,
        ascending: bool,
    ) -> Result<i64, TableFailure> {
        let mut result = -1;
        while start <= end {
            let mid = start + (end - start) / 2;
            let order = self.compare(&self.cell(mid)?)?;
            if order == Ordering::Equal {
                result = mid;
                if find_first {
                    end = mid - 1;
                } else {
                    start = mid + 1;
                }
            } else if (order == Ordering::Less) == ascending {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }
        Ok(result)
    }

    /// Behavior: the branch of `SeekRow#seek` for a column the table is sorted by
    fn sorted(&self, start: i64, ascending: bool) -> Result<i64, TableFailure> {
        let size = self.rows.size() as i64;
        let order = self.compare(&self.cell(start)?)?;
        // whether the value lies after the starting row
        let after = order != Ordering::Equal && (order == Ordering::Less) == ascending;
        if self.seek.backward {
            if order == Ordering::Equal && start > 0 {
                if self.compare(&self.cell(start - 1)?)? == Ordering::Equal {
                    return Ok(start - 1);
                }
            } else if order != Ordering::Equal && !after {
                return self.edge(0, start, false, ascending);
            }
            self.edge(start, size - 1, false, ascending)
        } else {
            if order == Ordering::Equal && start < size - 1 {
                if self.compare(&self.cell(start + 1)?)? == Ordering::Equal {
                    return Ok(start + 1);
                }
            } else if after {
                return self.edge(start, size - 1, true, ascending);
            }
            self.edge(0, start, true, ascending)
        }
    }

    /// The first position in `start..end`, or the last when seeking backward, whose cell
    /// is or contains the value.
    ///
    /// Behavior: `SeekRow#findRow`
    fn find(&self, start: i64, end: i64) -> Result<Option<i64>, TableFailure> {
        let (start, end) = (start.max(0) as u64, end.max(0) as u64);
        let range = self.rows.sub_set_by_position_range(start, end);
        let text = match &self.seek.value {
            SeekValue::String(text) if self.seek.insensitive => Some(text.to_lowercase()),
            SeekValue::String(text) => Some(text.clone()),
            _ => None,
        };
        let starts: Vec<u64> = (0..range.size()).step_by(CHUNK_SIZE as usize).collect();
        let pieces: Box<dyn Iterator<Item = u64>> = if self.seek.backward {
            Box::new(starts.into_iter().rev())
        } else {
            Box::new(starts.into_iter())
        };
        for piece in pieces {
            let chunk = range.sub_set_by_position_range(piece, piece + CHUNK_SIZE);
            let values = cells(&self.source.chunk(&chunk)?, self.source.column_type());
            let order: Box<dyn Iterator<Item = usize>> = if self.seek.backward {
                Box::new((0..values.len()).rev())
            } else {
                Box::new(0..values.len())
            };
            for at in order {
                if self.matches(&values[at], text.as_deref())? {
                    return Ok(Some((start + piece) as i64 + at as i64));
                }
            }
        }
        Ok(None)
    }

    /// Behavior: the body of the loop of `SeekRow#findRow`
    fn matches(&self, cell: &Cell, text: Option<&str>) -> Result<bool, TableFailure> {
        let Some(text) = text else {
            if self.seek.contains && *cell != Cell::Null {
                // `(String) value`, or for a String column `(String) useSeek`
                return Err(failure(CLASS_CAST));
            }
            return Ok(equal(&self.seek.value, cell));
        };
        // the value is compared as the text Java's `toString` gives it
        let value = match cell {
            Cell::Null => return Ok(false),
            Cell::Value(SeekValue::String(value)) => value.clone(),
            Cell::Char(unit) => String::from_utf16_lossy(&[*unit]),
            _ => {
                return Err(not_ported(
                    "the text of a value that is not a String or a char",
                ))
            }
        };
        let value = if self.seek.insensitive {
            value.to_lowercase()
        } else {
            value
        };
        Ok((self.seek.contains && value.contains(text)) || value == text)
    }
}

/// The position in `table` of the row that `seek` finds, or `-1` if there is none.
///
/// A refreshing table must be read under its update graph's lock, as for any read.
///
/// An error is an exception of Java's: a column that does not exist, a value of a class
/// that Java casts the column's values to and cannot (`contains` with anything but text,
/// `insensitive` with anything but text on a sorted column, a value of another class than
/// the sorted column's), and the cases refused as not ported: a starting row or a table
/// size beyond `i32::MAX`, and a sorted column in a table with row keys beyond it, which
/// Java casts to `int`.
///
/// Behavior: `io.deephaven.clientsupport.gotorow.SeekRow#seek`
pub fn seek_row(table: &dyn Table, seek: &Seek) -> Result<i64, TableFailure> {
    let Some(column) = table.definition().index_of(&seek.column) else {
        // Java's handler reads the type of a column definition that is null
        return Err(failure("java.lang.NullPointerException"));
    };
    let start = seek.starting_row.max(0);
    let rows = table.row_set();
    let limit = i64::from(i32::MAX);
    if start >= limit || rows.size() as i64 > limit {
        return Err(not_ported("a starting row or a size that is not an int"));
    }
    let source = table.column(column);
    let search = Search {
        seek,
        rows: &rows,
        source: &*source,
    };
    let sorted = marks(table)
        .sorted
        .into_iter()
        .find(|(name, _)| *name == seek.column);
    if let Some((_, descending)) = sorted {
        if rows.last_row_key().is_some_and(|last| last > limit) {
            return Err(not_ported(
                "a sorted column in a table with a row key that is not an int",
            ));
        }
        return search.sorted(start, !descending);
    }

    let size = rows.size() as i64;
    let ranges = if seek.backward {
        [(0, start), (start, size)]
    } else {
        [(start + 1, size), (0, start + 1)]
    };
    for (first, end) in ranges {
        if let Some(position) = search.find(first, end)? {
            return Ok(position);
        }
    }
    Ok(-1)
}

#[cfg(test)]
#[path = "seek_tests.rs"]
mod tests;
