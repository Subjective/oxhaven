//! `java.util.Random`, for what a formula of `deephaven.plot.express.data` does with one:
//! `new Random(seed).nextInt(bound)`, a number that depends on the seed alone.
//!
//! In Java the object could also be a cell of a column, be kept, and give another number
//! each time it is asked. No column type of the contracts holds an object of a class, so
//! only the two calls together are computed, and every other use is refused by the name of
//! the constructor or the method. `new Random()` takes its seed from the clock and is
//! refused with them.

use crate::eval::numeric::with_null_values;
use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{Int32Type, Int64Type};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

/// The class; a formula may leave the package out, which Java imports.
pub(crate) const CLASS: &str = "java.util.Random";

/// What Java's validator answers a client whose table operation makes one.
///
/// Behavior: `io.deephaven.server.table.validation.ParsingColumnExpressionValidator#validateInvocations`,
/// as Java 42.9 logged it.
pub(crate) const REFUSAL: &str =
    "User expressions are not permitted to instantiate class java.util.Random(long)";

/// Whether `created`, the name after `new`, is the class.
pub(crate) fn is_class(created: &str) -> bool {
    created == "Random" || created == CLASS
}

const MULTIPLIER: i64 = 0x5_DEEC_E66D;
const MASK: i64 = (1 << 48) - 1;

/// The generator's state.
struct Random(i64);

impl Random {
    /// Behavior: `java.util.Random#Random(long)`
    fn new(seed: i64) -> Random {
        Random((seed ^ MULTIPLIER) & MASK)
    }

    /// Behavior: `java.util.Random#next`
    fn next(&mut self, bits: u32) -> i32 {
        self.0 = (self.0.wrapping_mul(MULTIPLIER).wrapping_add(0xB)) & MASK;
        (self.0 >> (48 - bits)) as i32
    }

    /// The first number below `bound`, which is positive.
    ///
    /// Behavior: `java.util.Random#nextInt(int)`
    fn next_int(&mut self, bound: i32) -> i32 {
        let mut r = self.next(31);
        let m = bound - 1;
        if bound & m == 0 {
            return ((i64::from(bound) * i64::from(r)) >> 31) as i32;
        }
        let mut u = r;
        loop {
            r = u % bound;
            // the sum wraps where `u` is in the last, incomplete run of `bound` numbers
            if u.wrapping_sub(r).wrapping_add(m) >= 0 {
                return r;
            }
            u = self.next(31);
        }
    }
}

/// `new Random(seed).nextInt(bound)` for a `long` seed and an `int` bound, each as the
/// generated code holds it: a null is the value Java reserves for null. A null seed is so
/// the seed zero, whose low 48 bits it has, and a null bound fails as a negative one does.
pub(crate) fn evaluate(arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let seeds = with_null_values(&arguments[0]);
    let bounds = with_null_values(&arguments[1]);
    let seeds = seeds.as_primitive::<Int64Type>().values();
    let bounds = bounds.as_primitive::<Int32Type>().values();
    let numbers = seeds
        .iter()
        .zip(bounds.iter())
        .map(|(seed, bound)| {
            if *bound <= 0 {
                return Err(TableFailure {
                    message: "java.lang.IllegalArgumentException: bound must be positive"
                        .to_string(),
                });
            }
            Ok(Random::new(*seed).next_int(*bound))
        })
        .collect::<Result<Vec<i32>, _>>()?;
    Ok(Arc::new(Int32Array::from(numbers)))
}
