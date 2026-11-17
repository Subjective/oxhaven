//! Comparison operators. The handling of null and NaN is written once, in [`decide`]; the
//! comparison domains differ only in how two present values are ordered.

use super::failure;
use super::numeric::convert;
use crate::types::{CompareDomain, CompareOp};
use arrow::array::{Array, ArrayAccessor, AsArray, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;

/// A value that takes part in a comparison.
trait Comparand: Copy {
    /// Whether the value is NaN, which is neither equal to nor ordered against anything.
    fn is_nan(self) -> bool {
        false
    }
}

impl Comparand for i64 {}
impl Comparand for bool {}
impl Comparand for &str {}
impl Comparand for f64 {
    fn is_nan(self) -> bool {
        f64::is_nan(self)
    }
}

fn accepts(op: CompareOp, ordering: Ordering) -> bool {
    match op {
        CompareOp::Equal => ordering == Ordering::Equal,
        CompareOp::NotEqual => ordering != Ordering::Equal,
        CompareOp::Less => ordering == Ordering::Less,
        CompareOp::LessOrEqual => ordering != Ordering::Greater,
        CompareOp::Greater => ordering == Ordering::Greater,
        CompareOp::GreaterOrEqual => ordering != Ordering::Less,
    }
}

/// The result of `left op right` for one row, where `None` is null and `order` orders two
/// present values that are not NaN.
///
/// Behavior: `QueryLanguageFunctionUtils#eq`, `#less`, `#lessEquals`, `#greater`,
/// `#greaterEquals` and `#compareTo`, for every pair of types:
///
/// - A NaN operand makes every operator false, also against null (`#less(double,double)`
///   tests `Double.isNaN` before anything else), except `!=`, which is `!eq(a, b)`
///   (`QueryLanguageParser#visit(BinaryExpr)`) and so true.
/// - Otherwise null equals null and is less than every other value (`#compareTo(int,int)`,
///   `#compareTo(Comparable,Comparable)`), so `null < 1`, `null <= null` and `1 > null` are
///   true.
fn decide<L: Comparand, R: Comparand>(
    op: CompareOp,
    left: Option<L>,
    right: Option<R>,
    order: impl Fn(L, R) -> Ordering,
) -> bool {
    if left.is_some_and(L::is_nan) || right.is_some_and(R::is_nan) {
        return op == CompareOp::NotEqual;
    }
    let ordering = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => order(a, b),
    };
    accepts(op, ordering)
}

fn rows<L, R>(
    op: CompareOp,
    left: L,
    right: R,
    order: impl Fn(L::Item, R::Item) -> Ordering,
) -> BooleanArray
where
    L: ArrayAccessor,
    R: ArrayAccessor,
    L::Item: Comparand,
    R::Item: Comparand,
{
    if left.null_count() == 0 && right.null_count() == 0 {
        // Without nulls the operator is all that differs between rows; naming it in each
        // arm lets the compiler make one loop per operator.
        macro_rules! present {
            ($op:expr) => {
                BooleanBuffer::collect_bool(left.len(), |row| {
                    decide($op, Some(left.value(row)), Some(right.value(row)), &order)
                })
            };
        }
        let values = match op {
            CompareOp::Equal => present!(CompareOp::Equal),
            CompareOp::NotEqual => present!(CompareOp::NotEqual),
            CompareOp::Less => present!(CompareOp::Less),
            CompareOp::LessOrEqual => present!(CompareOp::LessOrEqual),
            CompareOp::Greater => present!(CompareOp::Greater),
            CompareOp::GreaterOrEqual => present!(CompareOp::GreaterOrEqual),
        };
        return BooleanArray::new(values, None);
    }
    let values = BooleanBuffer::collect_bool(left.len(), |row| {
        let a = left.is_valid(row).then(|| left.value(row));
        let b = right.is_valid(row).then(|| right.value(row));
        decide(op, a, b, &order)
    });
    BooleanArray::new(values, None)
}

/// Behavior: `Double.compare`, as called by `QueryLanguageFunctionUtils#compareTo(double,double)`,
/// for operands that are not NaN: numeric order, except that `-0.0` is less than `0.0`.
fn java_double_compare(a: f64, b: f64) -> Ordering {
    match a.partial_cmp(&b) {
        Some(Ordering::Equal) | None => b.is_sign_negative().cmp(&a.is_sign_negative()),
        Some(ordering) => ordering,
    }
}

/// Behavior: Java's `==` on floating point operands that are not NaN, as used by
/// `QueryLanguageFunctionUtils#eq(double,double)`: `-0.0` equals `0.0`.
fn ieee_order(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Behavior: `QueryLanguageFunctionUtils#compareTo(long,double)` and `#compareTo(long,float)`
/// after their null checks. A `float` widens to `double` exactly, and the subtraction is
/// exact in either type, so one function serves both.
fn compare_long_to_double(a: i64, b: f64) -> Ordering {
    if b.is_nan() || b > i64::MAX as f64 {
        return Ordering::Less;
    }
    if b < i64::MIN as f64 {
        return Ordering::Greater;
    }
    // Truncates toward zero and saturates, as Java's cast does.
    let truncated = b as i64;
    match truncated.cmp(&a) {
        Ordering::Greater => Ordering::Less,
        Ordering::Less => Ordering::Greater,
        Ordering::Equal => {
            let fraction = b - truncated as f64;
            if fraction == 0.0 {
                Ordering::Equal
            } else if fraction > 0.0 {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
    }
}

/// Behavior: `String#compareTo`: by UTF-16 code unit.
fn utf16_order(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Evaluates `left op right` in `domain`. The result has no nulls.
pub(crate) fn compare(
    op: CompareOp,
    domain: CompareDomain,
    left: &dyn Array,
    right: &dyn Array,
) -> Result<BooleanArray, TableFailure> {
    Ok(match domain {
        CompareDomain::Integral => {
            rows(op, &convert::<i64>(left), &convert::<i64>(right), |a, b| {
                a.cmp(&b)
            })
        }
        CompareDomain::Floating { through_float } => {
            // Java converts both operands to float, then compares; comparing the floats
            // as doubles gives the same answer.
            let double = |array| {
                if through_float {
                    convert::<f64>(&convert::<f32>(array))
                } else {
                    convert::<f64>(array)
                }
            };
            let order: fn(f64, f64) -> Ordering = if op.is_equality() {
                ieee_order
            } else {
                java_double_compare
            };
            rows(op, &double(left), &double(right), order)
        }
        CompareDomain::LongWithFloating => {
            if left.data_type() == &DataType::Int64 {
                rows(
                    op,
                    &convert::<i64>(left),
                    &convert::<f64>(right),
                    compare_long_to_double,
                )
            } else {
                // `compareTo(double a, long b)` is `-compareTo(b, a)`.
                rows(op, &convert::<f64>(left), &convert::<i64>(right), |a, b| {
                    compare_long_to_double(b, a).reverse()
                })
            }
        }
        CompareDomain::Boolean => rows(op, left.as_boolean(), right.as_boolean(), |a, b| a.cmp(&b)),
        CompareDomain::String => rows(
            op,
            left.as_string::<i32>(),
            right.as_string::<i32>(),
            utf16_order,
        ),
        // `QueryLanguageFunctionUtils#eq(Object,Object)` on values of two classes: the
        // same reference, which only two nulls are, or `equals`, which is false. A
        // primitive is boxed on the way in, so even its null value is an object.
        CompareDomain::Unrelated { primitive } => {
            let values = BooleanBuffer::collect_bool(left.len(), |row| {
                let both_null =
                    is_null(left, primitive[0], row) && is_null(right, primitive[1], row);
                both_null == (op == CompareOp::Equal)
            });
            BooleanArray::new(values, None)
        }
        CompareDomain::UnrelatedOrder { primitive } => {
            let mut values = Vec::with_capacity(left.len());
            for row in 0..left.len() {
                let ordering =
                    match (
                        is_null(left, primitive[0], row),
                        is_null(right, primitive[1], row),
                    ) {
                        (true, true) => Ordering::Equal,
                        (true, false) => Ordering::Less,
                        (false, true) => Ordering::Greater,
                        (false, false) => return Err(failure(
                            "java.lang.ClassCastException: compareTo of a value of another class",
                        )),
                    };
                values.push(accepts(op, ordering));
            }
            BooleanArray::from(values)
        }
        CompareDomain::Fields => fields_equal(op, left, right),
    })
}

/// Whether the cell at `row` is Java's `null`: a primitive is boxed first, so it never is.
fn is_null(array: &dyn Array, primitive: bool, row: usize) -> bool {
    !primitive && array.is_null(row)
}

/// Behavior: `java.time.Period#equals` and `java.time.ZonedDateTime#equals`, of values held
/// as Arrow structs whose fields fix the value (`Period`: years, months and days;
/// `ZonedDateTime`: the instant and the zone id), through
/// `QueryLanguageFunctionUtils#eq(Object, Object)`: two nulls are equal.
fn fields_equal(op: CompareOp, left: &dyn Array, right: &dyn Array) -> BooleanArray {
    let (left, right) = (left.as_struct(), right.as_struct());
    let mut equal = vec![true; left.len()];
    for (l, r) in left.columns().iter().zip(right.columns()) {
        let same = arrow::compute::kernels::cmp::not_distinct(l, r)
            .expect("the fields of one struct type compare");
        for (row, value) in equal.iter_mut().enumerate() {
            *value &= same.value(row);
        }
    }
    let values = BooleanBuffer::collect_bool(left.len(), |row| {
        let is_equal = match (left.is_valid(row), right.is_valid(row)) {
            (true, true) => equal[row],
            (false, false) => true,
            _ => false,
        };
        is_equal == (op == CompareOp::Equal)
    });
    BooleanArray::new(values, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected values traced through `QueryLanguageFunctionUtils#compareTo(long,double)`.
    #[test]
    fn long_against_double_is_exact() {
        use Ordering::*;
        let two_to_53 = 9_007_199_254_740_992_i64;
        let cases = [
            (1, 1.0, Equal),
            (1, 1.5, Less),
            (2, 1.5, Greater),
            (-1, -1.5, Greater),
            (-2, -1.5, Less),
            (0, -0.0, Equal),
            // (double) (2^53 + 1) == 2^53, yet the long is greater.
            (two_to_53 + 1, two_to_53 as f64, Greater),
            (two_to_53, two_to_53 as f64, Equal),
            (i64::MAX, f64::INFINITY, Less),
            (i64::MAX, 1e19, Less),
            (i64::MIN + 1, -1e19, Greater),
            (i64::MIN + 1, f64::NEG_INFINITY, Greater),
            // 2^63 is not above (double) Long.MAX_VALUE, which is also 2^63, and the cast
            // saturates to Long.MAX_VALUE.
            (i64::MAX, 9_223_372_036_854_775_808.0, Equal),
            (i64::MAX - 1, 9_223_372_036_854_775_808.0, Less),
            (5, f64::NAN, Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare_long_to_double(a, b), expected, "{a} vs {b}");
        }
    }

    /// Expected values are those of `Double.compare`.
    #[test]
    fn double_compare_orders_negative_zero_first() {
        use Ordering::*;
        let cases = [
            (1.0, 2.0, Less),
            (2.0, 1.0, Greater),
            (1.0, 1.0, Equal),
            (-0.0, 0.0, Less),
            (0.0, -0.0, Greater),
            (0.0, 0.0, Equal),
            (-0.0, -0.0, Equal),
            (f64::NEG_INFINITY, f64::INFINITY, Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(java_double_compare(a, b), expected, "{a} vs {b}");
        }
    }

    /// Expected values are those of `String#compareTo`, which compares `char` values.
    #[test]
    fn strings_order_by_utf16_code_unit() {
        use Ordering::*;
        assert_eq!(utf16_order("a", "b"), Less);
        assert_eq!(utf16_order("a", "ab"), Less);
        assert_eq!(utf16_order("b", "ab"), Greater);
        assert_eq!(utf16_order("", ""), Equal);
        // U+1F600 is the surrogate pair D83D DE00, below U+FF5E, although its code point
        // and its UTF-8 encoding are above.
        assert_eq!(utf16_order("\u{1F600}", "\u{FF5E}"), Less);
        assert!("\u{1F600}" > "\u{FF5E}");
    }
}
