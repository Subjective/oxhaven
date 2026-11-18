//! Which key columns `select_distinct` takes. The keys themselves go into
//! [`KeyTable`](crate::ops::key_table::KeyTable), which compares them by the grouping
//! rule of PARITY.md.

use crate::ops::agg::keys::groupable_vector;
use arrow::datatypes::{DataType, TimeUnit};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

/// Refuses a key column that `select_distinct` does not group by. A vector column is a
/// key when an aggregation groups by it (`groupable_vector`: its elements are integers,
/// `char`s, `Boolean`s, `String`s, `Instant`s or vectors of these), and two vectors are
/// then one key by their contents, as they are one group of an aggregation. Any other
/// column is checked by its Arrow type ([`check_stored`]).
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#selectDistinct, which groups with
/// the hashers of io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make as
/// every aggregation does
pub(super) fn check(column_type: &ColumnType) -> Result<(), TableFailure> {
    if column_type.list_element().is_some() {
        if groupable_vector(column_type) {
            return Ok(());
        }
        // Java takes any vector or array as a key and compares it as an object: vectors
        // of float or double by a rule that depends on their class (-0.0 and 0.0), arrays
        // by identity.
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
            "a key column of type {column_type:?}"
        );
    }
    check_stored(&column_type.arrow_type())
}

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
        | DataType::Utf8 => Ok(()),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| check_stored(field.data_type())),
        other => Err(TableFailure {
            message: format!("a key column of the Arrow type {other} is not supported"),
        }),
    }
}
