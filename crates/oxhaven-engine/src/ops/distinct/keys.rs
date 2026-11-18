//! Which key columns `select_distinct` takes. The keys themselves go into
//! [`KeyTable`](crate::ops::key_table::KeyTable), which compares them by the grouping
//! rule of PARITY.md.

use crate::ops::key_table::is_ported_key;
use arrow::datatypes::{DataType, TimeUnit};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

/// Refuses a key column of a type that `select_distinct` does not group by.
///
/// Taken: the booleans, integers, floats, strings, the time types, the structs of a
/// `ZonedDateTime` or a `Period` (whose fields are checked alike), `BigDecimal` and
/// `BigInteger`, and the vectors [`is_ported_key`] takes. A `ZonedDateTime` or a `Period`
/// is one key when every field is, which is java.time.ZonedDateTime#equals (the instant
/// and the zone) and java.time.Period#equals (years, months and days). A `BigDecimal` is
/// one key with another of the same value and scale (seen on Deephaven 42.9: `3`, `3.0`
/// and `3.000` are three keys), a vector with another of equal elements (an empty vector
/// is not a null one). Vectors of `float` or `double` (owner items 7.6 and 7.13) and
/// arrays (7.7) are refused by name.
///
/// Behavior: the `eq` of io.deephaven.util.compare.ByteComparisons, CharComparisons,
/// ShortComparisons, IntComparisons, LongComparisons, FloatComparisons, DoubleComparisons
/// and ObjectComparisons, which the hashers generated for
/// io.deephaven.engine.table.impl.by.typed.TypedHasherFactory compare keys with;
/// java.math.BigDecimal#equals, io.deephaven.vector.IntVector#equals and
/// ObjectVector#equals
pub(super) fn check(column_type: &ColumnType) -> Result<(), TableFailure> {
    if !is_ported_key(column_type) {
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
            "a key column of type {column_type:?}: Java keys vectors of float or double by \
             the class of the vector objects (-0.0 and 0.0), and arrays and vectors of \
             arrays by the identity of the arrays"
        );
    }
    check_stored(&column_type.arrow_type())
}

fn check_stored(data_type: &DataType) -> Result<(), TableFailure> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::UInt16
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::Date32
        | DataType::Time64(TimeUnit::Nanosecond)
        | DataType::Duration(TimeUnit::Nanosecond)
        | DataType::Utf8
        | DataType::Binary => Ok(()),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| check_stored(field.data_type())),
        DataType::List(element) => check_stored(element.data_type()),
        other => Err(TableFailure {
            message: format!("a key column of the Arrow type {other} is not supported"),
        }),
    }
}
