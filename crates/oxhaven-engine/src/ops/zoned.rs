//! Java's order and equality of `java.time.ZonedDateTime` cells, for the operations that
//! sort, filter or key on them.
//!
//! A cell is a struct of the instant in nanoseconds and the zone's id
//! (`oxhaven_contracts::schema::ColumnType::ZonedDateTime`). Two cells are equal when both
//! are equal (`ZonedDateTime#equals` compares the local date and time, the offset and the
//! zone, which the instant and the zone decide). They order by the instant, then by the
//! local date and time, which for one instant is the order of the zones' offsets, then by
//! the zone's id.

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Int32Array, Int64Array, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, TimestampNanosecondType};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The instants and zone ids of a chunk of a `ZonedDateTime` column.
pub(crate) fn parts(
    values: &ArrayRef,
) -> Result<(&StructArray, &[i64], &StringArray), TableFailure> {
    let cells = values
        .as_struct_opt()
        .ok_or_else(|| failure("a ZonedDateTime column must hold a struct array"))?;
    let instants = cells
        .column(0)
        .as_primitive_opt::<TimestampNanosecondType>()
        .ok_or_else(|| failure("the instants of a ZonedDateTime column must be timestamps"))?;
    let zones = cells
        .column(1)
        .as_string_opt::<i32>()
        .ok_or_else(|| failure("the zones of a ZonedDateTime column must be strings"))?;
    Ok((cells, instants.values(), zones))
}

/// The offset from UTC, in seconds, of the zone `id` at the instant `nanos`.
///
/// The ids are those a cell can hold: `UTC`, `Z` and the region ids of the IANA database
/// (what the formulas make; `oxhaven_expr`'s `zone_of_id`).
///
/// Behavior: java.time.zone.ZoneRules#getOffset(java.time.Instant)
pub(crate) fn offset_seconds(id: &str, nanos: i64) -> Result<i32, TableFailure> {
    use chrono::{Offset, TimeZone};
    if matches!(id, "UTC" | "Z") {
        return Ok(0);
    }
    let zone: chrono_tz::Tz = id
        .parse()
        .map_err(|_| failure(format!("time zone {id} is not one this server knows")))?;
    let utc = chrono::DateTime::from_timestamp(nanos.div_euclid(1_000_000_000), 0)
        .ok_or_else(|| failure("an instant outside the range of the time zone rules"))?
        .naive_utc();
    Ok(zone.offset_from_utc_datetime(&utc).fix().local_minus_utc())
}

/// The Arrow type of [`sort_key`]'s result.
pub(crate) fn sort_key_type() -> DataType {
    DataType::Struct(sort_key_fields())
}

fn sort_key_fields() -> Fields {
    vec![
        Field::new("instant", DataType::Int64, true),
        Field::new("offset", DataType::Int32, true),
        Field::new("zone", DataType::Binary, true),
    ]
    .into()
}

/// A struct whose Arrow row format orders like `ZonedDateTime#compareTo`: the instant, then
/// the offset (for one instant the later local date and time has the greater offset), then
/// the zone's id as big-endian UTF-16 code units, which order as `String#compareTo`. A null
/// cell stays null.
///
/// Behavior: java.time.chrono.ChronoZonedDateTime#compareTo
pub(crate) fn sort_key(values: &ArrayRef) -> Result<ArrayRef, TableFailure> {
    let (cells, instants, zones) = parts(values)?;
    let mut offsets = Vec::with_capacity(cells.len());
    for (row, &nanos) in instants.iter().enumerate() {
        offsets.push(if cells.is_valid(row) && zones.is_valid(row) {
            offset_seconds(zones.value(row), nanos)?
        } else {
            0
        });
    }
    let ids: BinaryArray = (0..cells.len())
        .map(|row| {
            Some(
                zones
                    .value(row)
                    .encode_utf16()
                    .flat_map(u16::to_be_bytes)
                    .collect::<Vec<u8>>(),
            )
        })
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::new(instants.to_vec().into(), None)),
        Arc::new(Int32Array::from(offsets)),
        Arc::new(ids),
    ];
    let key = StructArray::try_new(sort_key_fields(), columns, cells.nulls().cloned())
        .map_err(|error| failure(error.to_string()))?;
    Ok(Arc::new(key))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::TimestampNanosecondArray;
    use arrow::row::{RowConverter, SortField};
    use oxhaven_contracts::schema::ColumnType;

    pub(crate) fn zoned(cells: &[Option<(i64, &str)>]) -> ArrayRef {
        let DataType::Struct(fields) = ColumnType::ZonedDateTime.arrow_type() else {
            unreachable!()
        };
        let instants: TimestampNanosecondArray = cells
            .iter()
            .map(|cell| Some(cell.map_or(0, |(nanos, _)| nanos)))
            .collect::<TimestampNanosecondArray>()
            .with_timezone("UTC");
        let zones: StringArray = cells
            .iter()
            .map(|cell| Some(cell.map_or("UTC", |(_, zone)| zone)))
            .collect();
        let nulls = cells.iter().map(Option::is_some).collect::<Vec<_>>();
        Arc::new(StructArray::new(
            fields,
            vec![Arc::new(instants), Arc::new(zones)],
            Some(nulls.into()),
        ))
    }

    /// Seen on Java 42.9: one instant in four zones sorts as New York (its local time is
    /// the earliest), then `Europe/London` before `UTC` (equal offsets, ids in order),
    /// then Tokyo; a null sorts first.
    #[test]
    fn one_instant_orders_by_offset_then_zone_id() {
        let instant = 1_704_067_200_000_000_000; // 2024-01-01T00:00:00Z
        let cells = zoned(&[
            Some((instant, "UTC")),
            Some((instant, "America/New_York")),
            Some((instant, "Asia/Tokyo")),
            Some((instant, "Europe/London")),
            None,
            Some((instant - 1, "Asia/Tokyo")),
        ]);
        let key = sort_key(&cells).unwrap();
        let converter = RowConverter::new(vec![SortField::new(sort_key_type())]).unwrap();
        let rows = converter.convert_columns(&[key]).unwrap();
        let mut order: Vec<usize> = (0..rows.num_rows()).collect();
        order.sort_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));
        assert_eq!(order, [4, 5, 1, 3, 0, 2]);
    }

    #[test]
    fn offsets_follow_the_zone_rules_at_the_instant() {
        let winter = 1_704_067_200_000_000_000;
        let summer = 1_719_792_000_000_000_000; // 2024-07-01T00:00:00Z
        assert_eq!(offset_seconds("America/New_York", winter), Ok(-5 * 3600));
        assert_eq!(offset_seconds("America/New_York", summer), Ok(-4 * 3600));
        assert_eq!(offset_seconds("Z", summer), Ok(0));
        assert!(offset_seconds("Nowhere/Else", summer).is_err());
    }
}
