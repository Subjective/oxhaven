//! The functions whose value changes from one call to the next: the clock and the random
//! numbers. A formula that calls one has no value to compare with Java's; its type, and
//! what makes it fail, are Java's.

use arrow::array::{
    ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    TimestampNanosecondArray,
};
use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cell::Cell;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A function of `io.deephaven.function.Random`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RandomFn {
    /// `random()`.
    Uniform,
    /// `randomBool()`.
    Bool,
    /// `randomInt(int, int)`.
    Int,
    /// `randomLong(long, long)`.
    Long,
    /// `randomFloat(float, float)`.
    Float,
    /// `randomDouble(double, double)`.
    Double,
    /// `randomGaussian(double, double)`.
    Gaussian,
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn clock_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64)
}

/// `DateTimeUtils#now()`: the time of the system clock, the same in every row of one
/// evaluation.
pub(crate) fn now(rows: usize) -> ArrayRef {
    let nanos = clock_nanos() as i64;
    Arc::new(
        TimestampNanosecondArray::from_value(nanos, rows)
            .with_data_type(ColumnType::Instant.arrow_type()),
    )
}

thread_local! {
    /// The state of this thread's generator, as Java's `ThreadLocalRandom` is one per
    /// thread. Zero until first used.
    static STATE: Cell<u64> = const { Cell::new(0) };
}

/// The next 64 random bits: SplitMix64, the mixing function `ThreadLocalRandom` uses too.
fn next_bits() -> u64 {
    STATE.with(|state| {
        let mut seed = state.get();
        if seed == 0 {
            // The address of the cell tells the threads of one process apart.
            seed = (clock_nanos() ^ ((state as *const Cell<u64> as u64) << 17)) | 1;
        }
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        state.set(seed);
        let mut mixed = seed;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    })
}

/// A double in `[0, 1)` with 53 random bits, as `Random#nextDouble()`.
fn next_unit() -> f64 {
    (next_bits() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
}

/// A value in `[0, bound)` for a positive `bound`.
fn next_below(bound: u64) -> u64 {
    // Rejection keeps the values uniform.
    let zone = u64::MAX - u64::MAX % bound;
    loop {
        let bits = next_bits();
        if bits < zone {
            return bits % bound;
        }
    }
}

/// `ThreadLocalRandom#nextDouble(double, double)`: the bound must be greater than the
/// origin, and a result that rounds up to the bound is the value below it.
fn next_between(origin: f64, bound: f64) -> Result<f64, TableFailure> {
    if !(origin < bound && (bound - origin).is_finite()) {
        return Err(failure(
            "java.lang.IllegalArgumentException: bound must be greater than origin",
        ));
    }
    let value = next_unit() * (bound - origin) + origin;
    Ok(if value >= bound {
        bound.next_down()
    } else {
        value
    })
}

/// A standard normal value, by the polar method `Random#nextGaussian` describes.
fn next_gaussian() -> f64 {
    loop {
        let a = 2.0 * next_unit() - 1.0;
        let b = 2.0 * next_unit() - 1.0;
        let square = a * a + b * b;
        if square < 1.0 && square != 0.0 {
            return a * (-2.0 * square.ln() / square).sqrt();
        }
    }
}

/// One value of `function` per row. The arguments have the function's parameter types and
/// hold Java's values for null, which the functions of `io.deephaven.function.Random` read
/// as the numbers they are.
///
/// Behavior: `io.deephaven.function.Random#random`, `#randomBool`, `#randomInt`
/// (`min + nextInt(max - min)`, which fails unless the difference, computed as an `int`, is
/// positive), `#randomLong` (`nextLong(min, max)`, which fails unless `min < max`),
/// `#randomFloat`, `#randomDouble` and `#randomGaussian` (`mean + std * nextGaussian()`).
pub(crate) fn random(
    function: RandomFn,
    arguments: &[ArrayRef],
    rows: usize,
) -> Result<ArrayRef, TableFailure> {
    let doubles = |index: usize| arguments[index].as_primitive::<Float64Type>();
    Ok(match function {
        RandomFn::Uniform => Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|_| next_unit()),
        )),
        RandomFn::Bool => Arc::new(BooleanArray::from_iter(
            (0..rows).map(|_| Some(next_bits() >> 63 == 1)),
        )),
        RandomFn::Int => {
            let (min, max) = (
                arguments[0].as_primitive::<Int32Type>(),
                arguments[1].as_primitive::<Int32Type>(),
            );
            let values = (0..rows)
                .map(|row| {
                    let bound = max.value(row).wrapping_sub(min.value(row));
                    if bound <= 0 {
                        return Err(failure(
                            "java.lang.IllegalArgumentException: bound must be positive",
                        ));
                    }
                    Ok(min.value(row).wrapping_add(next_below(bound as u64) as i32))
                })
                .collect::<Result<Vec<i32>, _>>()?;
            Arc::new(Int32Array::from(values))
        }
        RandomFn::Long => {
            let (min, max) = (
                arguments[0].as_primitive::<Int64Type>(),
                arguments[1].as_primitive::<Int64Type>(),
            );
            let values = (0..rows)
                .map(|row| {
                    let (min, max) = (min.value(row), max.value(row));
                    if min >= max {
                        return Err(failure(
                            "java.lang.IllegalArgumentException: bound must be greater than origin",
                        ));
                    }
                    let span = max.wrapping_sub(min) as u64;
                    Ok(min.wrapping_add(next_below(span) as i64))
                })
                .collect::<Result<Vec<i64>, _>>()?;
            Arc::new(Int64Array::from(values))
        }
        RandomFn::Float => {
            let (min, max) = (
                arguments[0].as_primitive::<Float32Type>(),
                arguments[1].as_primitive::<Float32Type>(),
            );
            let values = (0..rows)
                .map(|row| {
                    next_between(min.value(row).into(), max.value(row).into())
                        .map(|value| value as f32)
                })
                .collect::<Result<Vec<f32>, _>>()?;
            Arc::new(Float32Array::from(values))
        }
        RandomFn::Double => {
            let values = (0..rows)
                .map(|row| next_between(doubles(0).value(row), doubles(1).value(row)))
                .collect::<Result<Vec<f64>, _>>()?;
            Arc::new(Float64Array::from(values))
        }
        RandomFn::Gaussian => {
            Arc::new(Float64Array::from_iter_values((0..rows).map(|row| {
                doubles(0).value(row) + doubles(1).value(row) * next_gaussian()
            })))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn doubles(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn ints(values: &[i32]) -> ArrayRef {
        Arc::new(Int32Array::from(values.to_vec()))
    }

    #[test]
    fn random_numbers_lie_in_their_range_and_differ() {
        let rows = 2_000;
        let unit = random(RandomFn::Uniform, &[], rows).unwrap();
        let unit = unit.as_primitive::<Float64Type>();
        assert!(unit.values().iter().all(|value| (0.0..1.0).contains(value)));
        let mean = unit.values().iter().sum::<f64>() / rows as f64;
        assert!((0.4..0.6).contains(&mean), "{mean}");

        let between = random(
            RandomFn::Double,
            &[doubles(&vec![-2.0; rows]), doubles(&vec![3.0; rows])],
            rows,
        )
        .unwrap();
        let between = between.as_primitive::<Float64Type>();
        assert!(between.values().iter().all(|v| (-2.0..3.0).contains(v)));
        assert!(between.values().iter().any(|v| *v < 0.0));
        assert!(between.values().iter().any(|v| *v > 1.0));

        let whole = random(
            RandomFn::Int,
            &[ints(&vec![5; rows]), ints(&vec![8; rows])],
            rows,
        )
        .unwrap();
        let whole = whole.as_primitive::<Int32Type>();
        for wanted in 5..8 {
            assert!(whole.values().contains(&wanted), "{wanted}");
        }
        assert!(whole.values().iter().all(|value| (5..8).contains(value)));

        let coins = random(RandomFn::Bool, &[], rows).unwrap();
        let heads = coins.as_boolean().true_count();
        assert!((rows / 3..rows * 2 / 3).contains(&heads), "{heads}");

        let bell = random(
            RandomFn::Gaussian,
            &[doubles(&vec![10.0; rows]), doubles(&vec![2.0; rows])],
            rows,
        )
        .unwrap();
        let bell = bell.as_primitive::<Float64Type>();
        let mean = bell.values().iter().sum::<f64>() / rows as f64;
        assert!((9.5..10.5).contains(&mean), "{mean}");
    }

    #[test]
    fn an_empty_range_fails_as_in_java() {
        let fails = |function, a: ArrayRef, b: ArrayRef| {
            let error = random(function, &[a, b], 1).unwrap_err();
            assert!(
                error.message.contains("IllegalArgumentException"),
                "{}",
                error.message
            );
        };
        fails(RandomFn::Double, doubles(&[1.0]), doubles(&[0.0]));
        fails(RandomFn::Double, doubles(&[1.0]), doubles(&[1.0]));
        fails(RandomFn::Double, doubles(&[f64::NAN]), doubles(&[1.0]));
        fails(RandomFn::Int, ints(&[5]), ints(&[5]));
        // The difference is computed as an `int`.
        fails(RandomFn::Int, ints(&[-2]), ints(&[i32::MAX]));
        let longs = |value: i64| Arc::new(Int64Array::from(vec![value])) as ArrayRef;
        fails(RandomFn::Long, longs(3), longs(3));
        assert!(random(RandomFn::Long, &[longs(i64::MIN), longs(i64::MAX)], 1).is_ok());
    }

    #[test]
    fn now_is_the_time_of_the_clock_in_every_row() {
        let before = clock_nanos() as i64;
        let values = now(3);
        let after = clock_nanos() as i64;
        let values = values.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        assert_eq!(values.len(), 3);
        assert!(values.values().iter().all(|v| (before..=after).contains(v)));
        assert_eq!(*values.data_type(), ColumnType::Instant.arrow_type());
    }
}
