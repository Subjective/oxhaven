//! Match, range and between filters over a chunk of a primitive type, judged in the
//! column's own type: one loop over the values with no value widened to a `long`, no
//! `Option` per cell and no branch, packed 64 rows at a time by `oxhaven_column::bits`.
//!
//! What a filter keeps is first reduced to a [`Kept`]: whether it keeps nulls, which
//! stretch of the values, and whether NaN. The loop then asks each value only that. A cell
//! is null when it holds the value Java reserves for null in its type or when its validity
//! bit is clear; the loop sees the first, and the validity bits are applied afterwards.
//!
//! Behavior: as `matches`, `in_range` and `between` of `filters.rs`, which the tests of
//! `kernel_tests.rs` compare these with for every type, operator and kind of cell.

use super::filters::{MatchOptions, RangeOp};
use arrow::array::PrimitiveArray;
use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use oxhaven_column::bits::pack;
use oxhaven_column::sentinel::{
    NULL_BYTE, NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_INT, NULL_LONG, NULL_SHORT,
};

/// The cells a filter keeps: the null ones or not, the values `from..=to` (none when
/// `from` is above `to`), and for a floating point column NaN or not.
#[derive(Clone, Copy, PartialEq, Debug)]
struct Kept<T> {
    nulls: bool,
    from: T,
    to: T,
    nan: bool,
}

impl Kept<i128> {
    const fn of(nulls: bool, from: i128, to: i128) -> Self {
        Self {
            nulls,
            from,
            to,
            nan: false,
        }
    }
}

/// No value, and every value but NaN, as the ends of a stretch of doubles.
const NO_DOUBLES: (f64, f64) = (f64::INFINITY, f64::NEG_INFINITY);
const ALL_DOUBLES: (f64, f64) = (f64::NEG_INFINITY, f64::INFINITY);

impl Kept<f64> {
    const fn of(nulls: bool, (from, to): (f64, f64), nan: bool) -> Self {
        Self {
            nulls,
            from,
            to,
            nan,
        }
    }
}

/// The doubles above `value`, which is not NaN.
fn above(value: f64) -> (f64, f64) {
    if value == f64::INFINITY {
        NO_DOUBLES
    } else {
        (value.next_up(), f64::INFINITY)
    }
}

/// The doubles below `value`, which is not NaN.
fn below(value: f64) -> (f64, f64) {
    if value == f64::NEG_INFINITY {
        NO_DOUBLES
    } else {
        (f64::NEG_INFINITY, value.next_down())
    }
}

/// What `column <op> pivot` keeps of an integer column. A null is below every value and
/// equal to a null pivot.
fn int_range(op: RangeOp, pivot: Option<i64>) -> Kept<i128> {
    let (lowest, highest) = (i128::MIN, i128::MAX);
    match (pivot.map(i128::from), op) {
        (None, RangeOp::Less) => Kept::<i128>::of(false, highest, lowest),
        (None, RangeOp::LessOrEqual) => Kept::<i128>::of(true, highest, lowest),
        (None, RangeOp::Greater) => Kept::<i128>::of(false, lowest, highest),
        (None, RangeOp::GreaterOrEqual) => Kept::<i128>::of(true, lowest, highest),
        (Some(pivot), RangeOp::Less) => Kept::<i128>::of(true, lowest, pivot - 1),
        (Some(pivot), RangeOp::LessOrEqual) => Kept::<i128>::of(true, lowest, pivot),
        (Some(pivot), RangeOp::Greater) => Kept::<i128>::of(false, pivot + 1, highest),
        (Some(pivot), RangeOp::GreaterOrEqual) => Kept::<i128>::of(false, pivot, highest),
    }
}

/// What a range from `lower` to `upper` keeps of an integer column: what is above the
/// lower end, or equal to it when that is included, and likewise below the upper end.
fn int_between(
    lower: Option<i64>,
    upper: Option<i64>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> Kept<i128> {
    // Above a null end is every value; a null is above nothing and equal to a null end.
    let (nulls_above, from) = match lower {
        None => (lower_inclusive, i128::MIN),
        Some(lower) => (false, i128::from(lower) + i128::from(!lower_inclusive)),
    };
    // Below a null end is nothing; a null is below every value and equal to a null end.
    let (nulls_below, to) = match upper {
        None => (upper_inclusive, i128::MIN),
        Some(upper) => (true, i128::from(upper) - i128::from(!upper_inclusive)),
    };
    Kept::<i128>::of(nulls_above && nulls_below, from, to)
}

/// What `column <op> pivot` keeps of a floating point column: the order of
/// `DoubleComparisons#compare` (null lowest, NaN highest and equal to itself, the zeros
/// equal), and no NaN for `>` and `>=`.
fn float_range(op: RangeOp, pivot: Option<f64>) -> Kept<f64> {
    match (pivot, op) {
        (None, RangeOp::Less) => Kept::<f64>::of(false, NO_DOUBLES, false),
        (None, RangeOp::LessOrEqual) => Kept::<f64>::of(true, NO_DOUBLES, false),
        (None, RangeOp::Greater) => Kept::<f64>::of(false, ALL_DOUBLES, false),
        (None, RangeOp::GreaterOrEqual) => Kept::<f64>::of(true, ALL_DOUBLES, false),
        (Some(pivot), op) if pivot.is_nan() => match op {
            RangeOp::Less => Kept::<f64>::of(true, ALL_DOUBLES, false),
            RangeOp::LessOrEqual => Kept::<f64>::of(true, ALL_DOUBLES, true),
            RangeOp::Greater | RangeOp::GreaterOrEqual => Kept::<f64>::of(false, NO_DOUBLES, false),
        },
        (Some(pivot), RangeOp::Less) => Kept::<f64>::of(true, below(pivot), false),
        (Some(pivot), RangeOp::LessOrEqual) => {
            Kept::<f64>::of(true, (f64::NEG_INFINITY, pivot), false)
        }
        (Some(pivot), RangeOp::Greater) => Kept::<f64>::of(false, above(pivot), false),
        (Some(pivot), RangeOp::GreaterOrEqual) => {
            Kept::<f64>::of(false, (pivot, f64::INFINITY), false)
        }
    }
}

/// What a range from `lower` to `upper` keeps of a floating point column, in the order of
/// [`float_range`]. Unlike `>`, a lower end keeps the NaN above it.
fn float_between(
    lower: Option<f64>,
    upper: Option<f64>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> Kept<f64> {
    let above_lower = match lower {
        None => Kept::<f64>::of(lower_inclusive, ALL_DOUBLES, true),
        Some(lower) if lower.is_nan() => Kept::<f64>::of(false, NO_DOUBLES, lower_inclusive),
        Some(lower) if lower_inclusive => Kept::<f64>::of(false, (lower, f64::INFINITY), true),
        Some(lower) => Kept::<f64>::of(false, above(lower), true),
    };
    let below_upper = match upper {
        None => Kept::<f64>::of(upper_inclusive, NO_DOUBLES, false),
        Some(upper) if upper.is_nan() => Kept::<f64>::of(true, ALL_DOUBLES, upper_inclusive),
        Some(upper) if upper_inclusive => Kept::<f64>::of(true, (f64::NEG_INFINITY, upper), false),
        Some(upper) => Kept::<f64>::of(true, below(upper), false),
    };
    Kept {
        nulls: above_lower.nulls && below_upper.nulls,
        // Neither end of a stretch is NaN, so the larger and the smaller are the plain ones.
        from: above_lower.from.max(below_upper.from),
        to: above_lower.to.min(below_upper.to),
        nan: above_lower.nan && below_upper.nan,
    }
}

/// `mask`, which is right for every cell whose validity bit is set, with the cells whose
/// bit is clear kept or not.
fn with_validity<P: ArrowPrimitiveType>(
    array: &PrimitiveArray<P>,
    mask: BooleanBuffer,
    nulls_kept: bool,
) -> BooleanBuffer {
    match array.nulls().filter(|nulls| nulls.null_count() > 0) {
        None => mask,
        Some(nulls) if nulls_kept => &mask | &!nulls.inner(),
        Some(nulls) => &mask & nulls.inner(),
    }
}

/// The cells of an integer column that `kept` names.
fn int_interval<P>(
    array: &PrimitiveArray<P>,
    null_value: P::Native,
    kept: Kept<i128>,
) -> BooleanBuffer
where
    P: ArrowPrimitiveType,
    P::Native: Into<i128> + TryFrom<i128>,
{
    // The stretch within the values of the type; an empty one is from one to zero.
    let lowest: i128 = P::Native::MIN_TOTAL_ORDER.into();
    let highest: i128 = P::Native::MAX_TOTAL_ORDER.into();
    let ends = (
        P::Native::try_from(kept.from.max(lowest)),
        P::Native::try_from(kept.to.min(highest)),
    );
    let (from, to) = match ends {
        (Ok(from), Ok(to)) if from <= to => (from, to),
        _ => (P::Native::ONE, P::Native::ZERO),
    };
    let nulls = kept.nulls;
    let mask = pack(array.values(), |value| {
        let null = value == null_value;
        (((value >= from) & (value <= to)) & !null) | (null & nulls)
    });
    with_validity(array, mask, nulls)
}

/// The cells of a floating point column that `kept` names, each value compared as the
/// `double` that `widen` makes of it.
fn float_interval<P: ArrowPrimitiveType>(
    array: &PrimitiveArray<P>,
    null_value: P::Native,
    widen: impl Fn(P::Native) -> f64,
    kept: Kept<f64>,
) -> BooleanBuffer {
    let Kept {
        nulls,
        from,
        to,
        nan,
    } = kept;
    let mask = pack(array.values(), |value| {
        let null = value == null_value;
        let wide = widen(value);
        // NaN is in no stretch and is not the null value.
        (((wide >= from) & (wide <= to)) & !null) | (null & nulls) | (wide.is_nan() & nan)
    });
    with_validity(array, mask, nulls)
}

/// The cells `ints` names of a chunk of an integer type, `char` or `Instant`, or the cells
/// `floats` names of a chunk of `float` or `double`. `None` for a chunk of another type.
fn interval(chunk: &ArrayRef, ints: Kept<i128>, floats: Kept<f64>) -> Option<BooleanBuffer> {
    macro_rules! int {
        ($arrow:ty, $null:expr) => {
            int_interval(chunk.as_primitive::<$arrow>(), $null, ints)
        };
    }
    Some(match chunk.data_type() {
        DataType::Int8 => int!(Int8Type, NULL_BYTE),
        DataType::Int16 => int!(Int16Type, NULL_SHORT),
        DataType::Int32 => int!(Int32Type, NULL_INT),
        DataType::Int64 => int!(Int64Type, NULL_LONG),
        DataType::UInt16 => int!(UInt16Type, NULL_CHAR),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            int!(TimestampNanosecondType, NULL_LONG)
        }
        DataType::Float32 => float_interval(
            chunk.as_primitive::<Float32Type>(),
            NULL_FLOAT,
            f64::from,
            floats,
        ),
        DataType::Float64 => float_interval(
            chunk.as_primitive::<Float64Type>(),
            NULL_DOUBLE,
            |value| value,
            floats,
        ),
        _ => return None,
    })
}

/// Whether each cell of a primitive chunk is `<op> pivot`; the pivot of the chunk's kind
/// of type is the one read.
pub(super) fn range_mask(
    chunk: &ArrayRef,
    op: RangeOp,
    int_pivot: Option<i64>,
    float_pivot: Option<f64>,
) -> Option<BooleanBuffer> {
    interval(
        chunk,
        int_range(op, int_pivot),
        float_range(op, float_pivot),
    )
}

/// Whether each cell of a primitive chunk is between two ends, each included or not.
pub(super) fn between_mask(
    chunk: &ArrayRef,
    int_ends: (Option<i64>, Option<i64>),
    float_ends: (Option<f64>, Option<f64>),
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> Option<BooleanBuffer> {
    interval(
        chunk,
        int_between(int_ends.0, int_ends.1, lower_inclusive, upper_inclusive),
        float_between(float_ends.0, float_ends.1, lower_inclusive, upper_inclusive),
    )
}

/// A match on more values than this is left to the caller: each value is a pass over the
/// chunk.
const MATCH_KEYS: usize = 8;

/// The cells of an integer column that equal one of `keys`, a null cell equal to a null key.
fn int_match<P>(
    array: &PrimitiveArray<P>,
    null_value: P::Native,
    keys: &[Option<i64>],
) -> Option<BooleanBuffer>
where
    P: ArrowPrimitiveType,
    P::Native: TryFrom<i64>,
{
    let nulls = keys.contains(&None);
    // A key that is not a value of the type equals no cell, and neither does the value
    // reserved for null: a cell that holds it is null.
    let mut values: Vec<P::Native> = keys
        .iter()
        .filter_map(|key| P::Native::try_from((*key)?).ok())
        .filter(|key| *key != null_value)
        .collect();
    values.dedup();
    if values.len() > MATCH_KEYS {
        return None;
    }
    let cells = array.values();
    let mut mask = match values.first().copied() {
        None => pack(cells, |value| (value == null_value) & nulls),
        Some(key) => pack(cells, |value| {
            (value == key) | ((value == null_value) & nulls)
        }),
    };
    for key in values.iter().skip(1).copied() {
        mask = &mask | &pack(cells, |value| value == key);
    }
    Some(with_validity(array, mask, nulls))
}

/// The cells of a floating point column that equal one of `keys`: as IEEE 754 says, so
/// the zeros are equal and NaN equals nothing, or with `nan_match` NaN equals NaN.
fn float_match<P: ArrowPrimitiveType>(
    array: &PrimitiveArray<P>,
    null_value: P::Native,
    widen: impl Fn(P::Native) -> f64,
    keys: &[Option<f64>],
    nan_match: bool,
) -> Option<BooleanBuffer> {
    let nulls = keys.contains(&None);
    let nan = nan_match && keys.iter().flatten().any(|key| key.is_nan());
    let reserved = widen(null_value);
    let mut values: Vec<f64> = keys
        .iter()
        .flatten()
        .copied()
        .filter(|key| !key.is_nan() && *key != reserved)
        .collect();
    values.dedup();
    if values.len() > MATCH_KEYS {
        return None;
    }
    let cells = array.values();
    let mut mask = match values.first().copied() {
        None => pack(cells, |value| {
            let wide = widen(value);
            ((value == null_value) & nulls) | (wide.is_nan() & nan)
        }),
        Some(key) => pack(cells, |value| {
            let wide = widen(value);
            (wide == key) | ((value == null_value) & nulls) | (wide.is_nan() & nan)
        }),
    };
    for key in values.iter().skip(1).copied() {
        mask = &mask | &pack(cells, |value| widen(value) == key);
    }
    Some(with_validity(array, mask, nulls))
}

/// Whether each cell of a primitive chunk equals one of the keys of its kind of type, or
/// with `options.inverted` none of them. `None` for a chunk of another type and for more
/// than [`MATCH_KEYS`] values.
pub(super) fn match_mask(
    chunk: &ArrayRef,
    int_keys: &[Option<i64>],
    float_keys: &[Option<f64>],
    options: MatchOptions,
) -> Option<BooleanBuffer> {
    macro_rules! int {
        ($arrow:ty, $null:expr) => {
            int_match(chunk.as_primitive::<$arrow>(), $null, int_keys)
        };
    }
    let mask = match chunk.data_type() {
        DataType::Int8 => int!(Int8Type, NULL_BYTE),
        DataType::Int16 => int!(Int16Type, NULL_SHORT),
        DataType::Int32 => int!(Int32Type, NULL_INT),
        DataType::Int64 => int!(Int64Type, NULL_LONG),
        DataType::UInt16 => int!(UInt16Type, NULL_CHAR),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            int!(TimestampNanosecondType, NULL_LONG)
        }
        DataType::Float32 => float_match(
            chunk.as_primitive::<Float32Type>(),
            NULL_FLOAT,
            f64::from,
            float_keys,
            options.nan_match,
        ),
        DataType::Float64 => float_match(
            chunk.as_primitive::<Float64Type>(),
            NULL_DOUBLE,
            |value| value,
            float_keys,
            options.nan_match,
        ),
        _ => None,
    }?;
    Some(if options.inverted { !&mask } else { mask })
}
