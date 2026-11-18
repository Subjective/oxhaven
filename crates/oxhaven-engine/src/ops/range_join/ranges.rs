//! The match columns of a range join, and the range of every left row.

use super::search::{bucket_ranges, Ordered, RangeValue, Runs, Utf16};
use super::{RangeJoinError, RangeJoinMatch};
use crate::ops::join::keys::{Key, KeyColumns};
use crate::ops::join::JoinMatch;
use crate::ops::key_table::{groupable_vector, is_ported_key};
use crate::ops::sort::ComparableCells;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType};
use oxhaven_contracts::table::Table;
use oxhaven_contracts::unported;
use std::collections::BTreeMap;

/// The match columns, as indices into the tables' definitions.
pub(super) struct MatchColumns {
    left_exact: KeyColumns,
    right_exact: KeyColumns,
    left_start: usize,
    right_range: usize,
    left_end: usize,
}

/// What Java says of two match columns whose types differ.
///
/// Behavior: io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation#validateMatchCompatibility
/// with io.deephaven.engine.table.ColumnDefinition#hasCompatibleDataType. Java names the
/// classes; the text is not compared with Java (`PARITY.md` A5).
fn incompatible(left: &ColumnDefinition, right: &ColumnDefinition) -> Option<String> {
    (left.column_type != right.column_type).then(|| {
        format!(
            "left table column \"{}\" (data type {:?}), is incompatible with right table column \
             \"{}\" (data type {:?})",
            left.name, left.column_type, right.name, right.column_type
        )
    })
}

/// The columns `exact` and `range` name, or the refusal of the first of the two that names
/// a column a table does not have or a pair of columns of differing types.
///
/// Behavior: io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation#validateExactMatchColumns
/// and `#validateRangeMatchColumns`
pub(super) fn match_columns(
    left: &dyn Table,
    right: &dyn Table,
    exact: &[JoinMatch],
    range: &RangeJoinMatch,
) -> Result<MatchColumns, RangeJoinError> {
    let (left_definition, right_definition) = (left.definition(), right.definition());
    let left_column = |name: &str| left_definition.index_of(name);
    let right_column = |name: &str| right_definition.index_of(name);
    let left_columns = left_definition.columns();
    let right_columns = right_definition.columns();

    let mut issues = Vec::new();
    let (mut left_exact, mut right_exact) = (Vec::new(), Vec::new());
    for pair in exact {
        let found = (left_column(&pair.left), right_column(&pair.right));
        if found.0.is_none() {
            issues.push(format!("left table has no column \"{}\"", pair.left));
        }
        if found.1.is_none() {
            issues.push(format!("right table has no column \"{}\"", pair.right));
        }
        if let (Some(l), Some(r)) = found {
            issues.extend(incompatible(&left_columns[l], &right_columns[r]));
            left_exact.push(l);
            right_exact.push(r);
        }
    }
    if !issues.is_empty() {
        return Err(RangeJoinError::InvalidExactMatches(issues));
    }
    if let Some(&l) = left_exact
        .iter()
        .find(|&&l| !is_ported_key(&left_columns[l].column_type))
    {
        unported!(
            "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
            "an exact match column of type {:?}: Java keys vectors of float or double by \
             the class of the vector objects (-0.0 and 0.0), and arrays and vectors of \
             arrays by the identity of the arrays",
            left_columns[l].column_type
        );
    }

    let start = left_column(range.left_start());
    let values = right_column(range.right_range());
    let end = left_column(range.left_end());
    if start.is_none() {
        issues.push(format!(
            "left start column {} is missing",
            range.left_start()
        ));
    }
    if values.is_none() {
        issues.push(format!(
            "right range column {} is missing",
            range.right_range()
        ));
    }
    if end.is_none() {
        // Java's text names the start column here as well.
        issues.push(format!("left start column {} is missing", range.left_end()));
    }
    for bound in [start, end] {
        if let (Some(bound), Some(values)) = (bound, values) {
            issues.extend(incompatible(&left_columns[bound], &right_columns[values]));
        }
    }
    let (Some(left_start), Some(right_range), Some(left_end), true) =
        (start, values, end, issues.is_empty())
    else {
        return Err(RangeJoinError::InvalidRangeMatch(issues));
    };
    // An array, a `Period`, a `ZoneId`, a column of objects or of tables is neither
    // primitive nor `Comparable` (seen on Deephaven 42.9: "Invalid range value type class
    // java.time.Period, must be primitive or comparable"). A vector is `Comparable`.
    let range_type = &left_columns[left_start].column_type;
    match range_type {
        ColumnType::Array(_)
        | ColumnType::Period
        | ColumnType::ZoneId
        | ColumnType::Object(_)
        | ColumnType::Table(_) => {
            return Err(RangeJoinError::InvalidRangeValueType(range_type.clone()));
        }
        // Java orders `BigDecimal` range values by `compareTo` but takes a left bound for
        // equal to a right value by `equals`: seen, right values `1 3.0 3 5` and the
        // bounds `3 <= D <= 9` match `3 5` and not `3.0` (owner item 7.18).
        ColumnType::BigDecimal => unported!(
            "io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelObject#eq",
            "a range join on BigDecimal values, which Java compares by compareTo and equals"
        ),
        vector @ ColumnType::Vector(_) if !groupable_vector(vector) => unported!(
            "io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelObject#eq",
            "a range join on values of type {vector:?}: Java compares vectors of float or \
             double by compareTo and equals, which disagree on -0.0 (owner item 7.6), and \
             fails to compare vectors of arrays, periods or zones"
        ),
        _ => {}
    }
    Ok(MatchColumns {
        left_exact: KeyColumns {
            columns: left_exact,
        },
        right_exact: KeyColumns {
            columns: right_exact,
        },
        left_start,
        right_range,
        left_end,
    })
}

/// The values of a range column, in the form the kernel of its type compares.
enum Values {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Text(Vec<Option<Utf16>>),
    Ordered(Vec<Option<Ordered>>),
}

/// Behavior: io.deephaven.engine.table.impl.rangejoin.RangeSearchKernel#makeRangeSearchKernel
/// for the chunk type io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive
/// gives the column: `Boolean` is searched as its byte (false 0, true 1) and `Instant` as
/// its nanoseconds. The kernels of the integer types are one algorithm, as are those of
/// `float` and `double`. The other classes are objects compared by `compareTo` (and
/// `equals`, which agrees for the classes that reach here): their cells in the sort's row
/// format, whose bytes order as `compareTo` does.
fn values(column_type: &ColumnType, array: &ArrayRef) -> Result<Values, RangeJoinError> {
    fn cells<A: Array + 'static, T>(
        array: &ArrayRef,
        cell: impl Fn(&A, usize) -> T,
    ) -> Vec<Option<T>> {
        let Some(typed) = array.as_any().downcast_ref::<A>() else {
            panic!("a {} array of another class", array.data_type());
        };
        (0..typed.len())
            .map(|row| (!typed.is_null(row)).then(|| cell(typed, row)))
            .collect()
    }
    Ok(match column_type {
        ColumnType::Boolean => Values::Int(cells::<BooleanArray, _>(array, |a, row| {
            a.value(row).into()
        })),
        ColumnType::Byte => Values::Int(cells::<Int8Array, _>(array, |a, row| a.value(row).into())),
        ColumnType::Short => {
            Values::Int(cells::<Int16Array, _>(array, |a, row| a.value(row).into()))
        }
        ColumnType::Int => Values::Int(cells::<Int32Array, _>(array, |a, row| a.value(row).into())),
        ColumnType::Long => Values::Int(cells::<Int64Array, _>(array, |a, row| a.value(row))),
        ColumnType::Char => {
            Values::Int(cells::<UInt16Array, _>(array, |a, row| a.value(row).into()))
        }
        ColumnType::Instant => {
            Values::Int(cells::<TimestampNanosecondArray, _>(array, |a, row| {
                a.value(row)
            }))
        }
        ColumnType::Float => Values::Float(cells::<Float32Array, _>(array, |a, row| {
            a.value(row).into()
        })),
        ColumnType::Double => Values::Float(cells::<Float64Array, _>(array, |a, row| a.value(row))),
        ColumnType::String => Values::Text(cells::<StringArray, _>(array, |a, row| {
            Utf16::from(a.value(row))
        })),
        other => match ComparableCells::new(other) {
            Some(comparable) => {
                let rows = comparable.rows(array)?;
                Values::Ordered(
                    (0..array.len())
                        .map(|row| {
                            (!array.is_null(row)).then(|| Ordered(rows.row(row).as_ref().into()))
                        })
                        .collect(),
                )
            }
            None => unported!(
                "io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation#validateRangeMatchColumns",
                "a range column of type {other:?}"
            ),
        },
    })
}

/// The range of every left row.
pub(super) struct Ranges {
    /// For each left row, by position: the bucket of its range and, as positions in that
    /// bucket, the first row of the range and the row after its last. `None` for a row
    /// whose bounds make no range.
    pub(super) cells: Vec<Option<(usize, usize, usize)>>,
    /// The row keys of the right rows of each bucket, in the right table's order, without
    /// the rows whose range value is null or NaN. The first bucket is empty: it is the
    /// bucket of the left rows no right row matches.
    pub(super) buckets: Vec<Vec<RowKey>>,
}

/// The bucket that has no right rows.
const NO_RIGHT_ROWS: usize = 0;

/// Behavior: `StaticRangeJoinPhase1` and `StaticRangeJoinPhase2` of
/// io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation. Java groups each table by
/// its exact match columns and joins the groups (`exposeGroupRowSets`, then `naturalJoin`);
/// the keys here are equal as those of a natural join are. Only the buckets of left rows
/// are looked at, so right rows out of order in a bucket no left row has are not refused.
fn ranges_of<T: RangeValue + Clone>(
    range: &RangeJoinMatch,
    starts: &[Option<T>],
    ends: &[Option<T>],
    left_keys: &[Key],
    right_values: &[Option<T>],
    right_keys: &[Key],
    right_row_keys: &[RowKey],
) -> Result<Ranges, RangeJoinError> {
    // The right rows of each bucket, as positions in the right table.
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new()];
    let mut bucket_of: BTreeMap<&Key, usize> = BTreeMap::new();
    for (row, value) in right_values.iter().enumerate() {
        // Behavior: the filter of `StaticRangeJoinPhase1#filterAndGroupRightTable`, which
        // for floats is io.deephaven.engine.table.impl.rangejoin.ValidFloatingPointFilter
        if !value.as_ref().is_some_and(|value| !value.is_nan()) {
            continue;
        }
        let bucket = *bucket_of.entry(&right_keys[row]).or_insert_with(|| {
            buckets.push(Vec::new());
            buckets.len() - 1
        });
        buckets[bucket].push(row);
    }

    // The left rows of each bucket, in the order the buckets first appear.
    let mut groups: Vec<(usize, Vec<usize>)> = Vec::new();
    let mut group_of: BTreeMap<&Key, usize> = BTreeMap::new();
    for (row, key) in left_keys.iter().enumerate() {
        let group = *group_of.entry(key).or_insert_with(|| {
            let bucket = bucket_of.get(key).copied().unwrap_or(NO_RIGHT_ROWS);
            groups.push((bucket, Vec::new()));
            groups.len() - 1
        });
        groups[group].1.push(row);
    }

    let mut cells = vec![None; left_keys.len()];
    for (bucket, rows) in &groups {
        let runs = if *bucket == NO_RIGHT_ROWS {
            None
        } else {
            let right_rows = &buckets[*bucket];
            let values: Vec<T> = right_rows
                .iter()
                .filter_map(|&row| right_values[row].clone())
                .collect();
            let runs = Runs::of(&values)
                .map_err(|at| RangeJoinError::OutOfOrder(right_row_keys[right_rows[at]]))?;
            Some(runs)
        };
        let bucket_starts: Vec<Option<T>> = rows.iter().map(|&row| starts[row].clone()).collect();
        let bucket_ends: Vec<Option<T>> = rows.iter().map(|&row| ends[row].clone()).collect();
        let found = bucket_ranges(
            range.start_rule(),
            range.end_rule(),
            &bucket_starts,
            &bucket_ends,
            runs.as_ref(),
        );
        for (&row, found) in rows.iter().zip(found) {
            cells[row] = found.map(|(first, after)| {
                if after < first {
                    // Java would make a vector of negative length. No bounds are known
                    // that lead here.
                    unported!(
                        "io.deephaven.engine.table.impl.sources.aggregate.RangeAggregateColumnSource#get",
                        "a range that ends at position {after}, before its start {first}"
                    );
                }
                (*bucket, first, after)
            });
        }
    }
    let buckets = buckets
        .iter()
        .map(|rows| rows.iter().map(|&row| right_row_keys[row]).collect())
        .collect();
    Ok(Ranges { cells, buckets })
}

/// The range of every row of `left` among the rows of `right`, or the refusal of a bucket
/// whose right rows are not ascending in their range values.
pub(super) fn ranges(
    left: &dyn Table,
    right: &dyn Table,
    columns: &MatchColumns,
    range: &RangeJoinMatch,
) -> Result<Ranges, RangeJoinError> {
    let (left_rows, right_rows) = (left.row_set(), right.row_set());
    let read = |table: &dyn Table, column: usize| {
        let rows = table.row_set();
        let source = table.column(column);
        let cells = source.chunk(&rows)?;
        values(source.column_type(), &cells)
    };
    let starts = read(left, columns.left_start)?;
    let ends = read(left, columns.left_end)?;
    let right_values = read(right, columns.right_range)?;
    let left_keys = columns.left_exact.read(left, &left_rows, false)?;
    let right_keys = columns.right_exact.read(right, &right_rows, false)?;
    let right_row_keys: Vec<RowKey> = right_rows.iter().collect();
    match (starts, ends, right_values) {
        (Values::Int(starts), Values::Int(ends), Values::Int(values)) => ranges_of(
            range,
            &starts,
            &ends,
            &left_keys,
            &values,
            &right_keys,
            &right_row_keys,
        ),
        (Values::Float(starts), Values::Float(ends), Values::Float(values)) => ranges_of(
            range,
            &starts,
            &ends,
            &left_keys,
            &values,
            &right_keys,
            &right_row_keys,
        ),
        (Values::Text(starts), Values::Text(ends), Values::Text(values)) => ranges_of(
            range,
            &starts,
            &ends,
            &left_keys,
            &values,
            &right_keys,
            &right_row_keys,
        ),
        (Values::Ordered(starts), Values::Ordered(ends), Values::Ordered(values)) => ranges_of(
            range,
            &starts,
            &ends,
            &left_keys,
            &values,
            &right_keys,
            &right_row_keys,
        ),
        _ => unreachable!("the range columns were checked to be of one type"),
    }
}
