//! Which key columns `select_distinct` takes. The keys themselves go into
//! [`GroupKeys`](crate::ops::group_keys::GroupKeys), which compares them by the grouping
//! rule of PARITY.md.

use arrow::datatypes::{DataType, TimeUnit};
use oxhaven_contracts::table::TableFailure;

/// Refuses a key column of an Arrow type that `select_distinct` does not group by:
/// anything but the booleans, integers, floats, strings, the time types and the structs of
/// a `ZonedDateTime` or a `Period` (whose fields are checked alike). A `ZonedDateTime` or a
/// `Period` is one key when every field is, which is java.time.ZonedDateTime#equals (the
/// instant and the zone) and java.time.Period#equals (years, months and days).
///
/// Behavior: the `eq` of io.deephaven.util.compare.ByteComparisons, CharComparisons,
/// ShortComparisons, IntComparisons, LongComparisons, FloatComparisons, DoubleComparisons
/// and ObjectComparisons, which the hashers generated for
/// io.deephaven.engine.table.impl.by.typed.TypedHasherFactory compare keys with
pub(super) fn check(data_type: &DataType) -> Result<(), TableFailure> {
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
        | DataType::Utf8 => Ok(()),
        DataType::Struct(fields) => fields.iter().try_for_each(|field| check(field.data_type())),
        // Java takes a vector or an array as a key and compares it as an object; when two
        // of them are one key is not ported.
        DataType::List(_) => oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
            "a key column stored as {}",
            data_type
        ),
        other => Err(TableFailure {
            message: format!("a key column of the Arrow type {other} is not supported"),
        }),
    }
}
