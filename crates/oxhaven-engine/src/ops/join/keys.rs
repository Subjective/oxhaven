//! Join keys: the values of a row's match columns, in a form whose equality is the
//! equality Java's join hashers use.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, DurationNanosecondArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, StructArray,
    Time64NanosecondArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;

/// One match column's value.
///
/// Two cells of one column are equal exactly when Java's hasher for the column's type calls
/// them equal: a null equals only a null, every NaN equals every NaN, and `-0.0` equals
/// `0.0`. A `float` is widened to `double`, which keeps distinct values distinct.
///
/// Behavior: the `eq` of io.deephaven.util.compare.DoubleComparisons, FloatComparisons,
/// CharComparisons and ObjectComparisons as called by the hashers under
/// io.deephaven.engine.table.impl.naturaljoin.typed; `Instant` is compared as its
/// nanoseconds and `Boolean` as its byte (io.deephaven.engine.table.impl.BucketingContext).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    /// The bits of the value, with one pattern for every NaN and one for both zeros.
    Float(u64),
    Text(String),
    /// The fields of a `ZonedDateTime` (instant, zone) or a `Period` (years, months, days).
    Fields(Vec<Cell>),
}

/// The match column values of one row, in the order of the match columns. Empty for a join
/// without match columns, where every row has the same key.
pub(crate) type Key = Vec<Cell>;

fn float_cell(value: f64) -> Cell {
    let canonical = if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    };
    Cell::Float(canonical.to_bits())
}

/// Appends the cell of every row of `values` to that row's key.
fn append(keys: &mut [Key], values: &ArrayRef) {
    fn each<A: Array + 'static>(
        keys: &mut [Key],
        values: &ArrayRef,
        cell: impl Fn(&A, usize) -> Cell,
    ) {
        let Some(array) = values.as_any().downcast_ref::<A>() else {
            panic!("a {} array of another class", values.data_type());
        };
        for (row, key) in keys.iter_mut().enumerate() {
            key.push(if array.is_null(row) {
                Cell::Null
            } else {
                cell(array, row)
            });
        }
    }
    match values.data_type() {
        DataType::Boolean => each::<BooleanArray>(keys, values, |a, row| Cell::Bool(a.value(row))),
        DataType::Int8 => each::<Int8Array>(keys, values, |a, row| Cell::Int(a.value(row).into())),
        DataType::Int16 => {
            each::<Int16Array>(keys, values, |a, row| Cell::Int(a.value(row).into()))
        }
        DataType::Int32 => {
            each::<Int32Array>(keys, values, |a, row| Cell::Int(a.value(row).into()))
        }
        DataType::Int64 => each::<Int64Array>(keys, values, |a, row| Cell::Int(a.value(row))),
        DataType::UInt16 => {
            each::<UInt16Array>(keys, values, |a, row| Cell::Int(a.value(row).into()))
        }
        DataType::Float32 => {
            each::<Float32Array>(keys, values, |a, row| float_cell(a.value(row).into()))
        }
        DataType::Float64 => each::<Float64Array>(keys, values, |a, row| float_cell(a.value(row))),
        DataType::Utf8 => {
            each::<StringArray>(keys, values, |a, row| Cell::Text(a.value(row).to_string()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            each::<TimestampNanosecondArray>(keys, values, |a, row| Cell::Int(a.value(row)))
        }
        DataType::Date32 => {
            each::<Date32Array>(keys, values, |a, row| Cell::Int(a.value(row).into()))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            each::<Time64NanosecondArray>(keys, values, |a, row| Cell::Int(a.value(row)))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            each::<DurationNanosecondArray>(keys, values, |a, row| Cell::Int(a.value(row)))
        }
        // A `ZonedDateTime` or a `Period`: equal when every field is, which is
        // java.time.ZonedDateTime#equals (the instant and the zone) and
        // java.time.Period#equals (years, months and days).
        DataType::Struct(_) => {
            let Some(array) = values.as_any().downcast_ref::<StructArray>() else {
                panic!("a struct array of another class");
            };
            let mut fields = vec![Key::new(); keys.len()];
            for child in array.columns() {
                append(&mut fields, child);
            }
            for (row, (key, fields)) in keys.iter_mut().zip(fields).enumerate() {
                key.push(if array.is_null(row) {
                    Cell::Null
                } else {
                    Cell::Fields(fields)
                });
            }
        }
        other => unported!(
            "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#makeNaturalJoin",
            "a match column stored as {other}"
        ),
    }
}

/// The key of each of the `rows` rows of the key `columns`: the keys the joins compared
/// before they hashed, which the tests of the hash table compare it with.
#[cfg(test)]
pub(crate) fn keys_of(columns: &[ArrayRef], rows: usize) -> Vec<Key> {
    let mut keys = vec![Key::with_capacity(columns.len()); rows];
    for column in columns {
        append(&mut keys, column);
    }
    keys
}

/// The match columns of one side of a join.
pub(crate) struct KeyColumns {
    /// Indices of the match columns in the table's definition, in match order.
    pub(crate) columns: Vec<usize>,
}

impl KeyColumns {
    /// The match columns of `rows`, one array per column; with `use_prev`, as of the end of
    /// the previous cycle for `rows` of the previous key space. For hashing with a
    /// [`KeyTable`](crate::ops::key_table::KeyTable), which needs no key per row.
    pub(crate) fn chunks(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<Vec<ArrayRef>, TableFailure> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let chunk = |&column: &usize| {
            let source = table.column(column);
            if use_prev {
                source.prev_chunk(rows)
            } else {
                source.chunk(rows)
            }
        };
        self.columns.iter().map(chunk).collect()
    }

    /// The key of each row of `rows`, in row order; with `use_prev`, as of the end of the
    /// previous cycle for `rows` of the previous key space.
    pub(crate) fn read(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<Vec<Key>, TableFailure> {
        let mut keys = vec![Key::with_capacity(self.columns.len()); rows.size() as usize];
        if rows.is_empty() {
            return Ok(keys);
        }
        for &column in &self.columns {
            let source = table.column(column);
            let values = if use_prev {
                source.prev_chunk(rows)?
            } else {
                source.chunk(rows)?
            };
            append(&mut keys, &values);
        }
        Ok(keys)
    }
}

/// `key` as Java names a key in its error messages: the value, or `[a, b]` for several
/// match columns. Message text is not compared with Java (`PARITY.md` A5); an `Instant` is
/// written as its nanoseconds.
///
/// Behavior: io.deephaven.engine.table.impl.StaticNaturalJoinStateManager#extractKeyStringFromSourceTable
pub(crate) fn key_text(key: &[Cell], types: &[ColumnType]) -> String {
    let cells: Vec<String> = key
        .iter()
        .zip(types)
        .map(|(cell, column_type)| match cell {
            Cell::Null => "null".to_string(),
            Cell::Bool(value) => value.to_string(),
            Cell::Int(value) => match (column_type, u32::try_from(*value)) {
                (ColumnType::Char, Ok(code)) => {
                    char::from_u32(code).map_or_else(|| value.to_string(), String::from)
                }
                _ => value.to_string(),
            },
            Cell::Float(bits) => format!("{:?}", f64::from_bits(*bits)),
            Cell::Text(text) => text.clone(),
            Cell::Fields(fields) => {
                let texts: Vec<String> = fields
                    .iter()
                    .map(|field| key_text(std::slice::from_ref(field), &[ColumnType::Long]))
                    .collect();
                format!("({})", texts.join(", "))
            }
        })
        .collect();
    match cells.as_slice() {
        [single] => single.clone(),
        _ => format!("[{}]", cells.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn cells(values: ArrayRef) -> Vec<Cell> {
        let mut keys = vec![Key::new(); values.len()];
        append(&mut keys, &values);
        keys.into_iter().map(|mut key| key.remove(0)).collect()
    }

    #[test]
    fn nan_equals_nan_and_the_zeros_are_one_key() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        let found = cells(Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(other_nan),
            Some(0.0),
            Some(-0.0),
            None,
            Some(1.5),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ])));
        assert_eq!(found[0], found[1]);
        assert_eq!(found[2], found[3]);
        assert_eq!(found[4], Cell::Null);
        // Everything else is its own key.
        let distinct = [
            &found[0], &found[2], &found[4], &found[5], &found[6], &found[7],
        ];
        for (i, a) in distinct.iter().enumerate() {
            for b in &distinct[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn a_float_key_equals_the_double_of_the_same_value_only() {
        let floats = cells(Arc::new(Float32Array::from(vec![
            Some(0.1f32),
            Some(f32::NAN),
            Some(-0.0),
            None,
        ])));
        assert_eq!(floats[0], float_cell(f64::from(0.1f32)));
        assert_ne!(floats[0], float_cell(0.1));
        assert_eq!(floats[1], float_cell(f64::NAN));
        assert_eq!(floats[2], float_cell(0.0));
        assert_eq!(floats[3], Cell::Null);
    }

    /// Behavior: java.time.ZonedDateTime#equals: one instant in two zones is two keys.
    #[test]
    fn a_zoned_date_time_key_is_its_instant_and_its_zone() {
        let keys = cells(crate::ops::zoned::tests::zoned(&[
            Some((5, "UTC")),
            Some((5, "Asia/Tokyo")),
            Some((5, "UTC")),
            None,
        ]));
        assert_eq!(keys[0], keys[2]);
        assert_ne!(keys[0], keys[1]);
        assert_eq!(keys[3], Cell::Null);
    }

    #[test]
    fn every_key_type_has_cells_and_null_is_its_own_cell() {
        let columns: Vec<(ArrayRef, Cell)> = vec![
            (
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Cell::Bool(true),
            ),
            (
                Arc::new(Int8Array::from(vec![Some(-7), None])),
                Cell::Int(-7),
            ),
            (
                Arc::new(Int16Array::from(vec![Some(300), None])),
                Cell::Int(300),
            ),
            (
                Arc::new(Int32Array::from(vec![Some(70000), None])),
                Cell::Int(70000),
            ),
            (
                Arc::new(Int64Array::from(vec![Some(1 << 40), None])),
                Cell::Int(1 << 40),
            ),
            (
                Arc::new(UInt16Array::from(vec![Some(0xFFFE), None])),
                Cell::Int(0xFFFE),
            ),
            (
                Arc::new(StringArray::from(vec![Some(""), None])),
                Cell::Text(String::new()),
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![Some(-5), None]).with_timezone("UTC")),
                Cell::Int(-5),
            ),
        ];
        for (values, first) in columns {
            assert_eq!(cells(values), vec![first, Cell::Null]);
        }
    }

    #[test]
    fn key_text_is_the_value_or_a_bracketed_list() {
        let types = [ColumnType::Int, ColumnType::String];
        let key = vec![Cell::Int(4), Cell::Text("a".into())];
        assert_eq!(key_text(&key, &types), "[4, a]");
        assert_eq!(key_text(&key[..1], &types[..1]), "4");
        assert_eq!(key_text(&[Cell::Null], &types[..1]), "null");
        assert_eq!(key_text(&[Cell::Int(97)], &[ColumnType::Char]), "a");
        assert_eq!(key_text(&[float_cell(1.0)], &[ColumnType::Double]), "1.0");
        assert_eq!(key_text(&[], &[]), "[]");
    }
}
