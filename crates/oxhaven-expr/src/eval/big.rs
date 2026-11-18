//! Comparisons of `BigDecimal` and `BigInteger` values with each other and with numbers.

use super::failure;
use super::numeric::convert;
use super::text::double_to_string;
use crate::types::BigOperand;
use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use oxhaven_column::java_values;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;

/// A number as Java's `BigDecimal#compareTo` sees it: its sign, its decimal digits with no
/// zeros at either end (none for zero), and the power of ten of the place just before the
/// first digit, so that the value is `0.d1d2... × 10^exponent`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Decimal {
    negative: bool,
    digits: Vec<u8>,
    exponent: i64,
}

/// A value a comparison reads: a number, or NaN, which compares with nothing.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BigValue {
    Number(Decimal),
    NaN,
}

impl Decimal {
    /// The number `digits × 10^-scale`, where `digits` is an optional `-` and ASCII digits.
    fn of_text(text: &str, scale: i64) -> Decimal {
        let (negative, magnitude) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let digits: Vec<u8> = magnitude.bytes().map(|b| b - b'0').collect();
        Decimal::normalized(negative, digits, -scale)
    }

    /// The integer of `digits` times `10^exponent`, with the zeros at both ends of the
    /// digits removed.
    fn normalized(negative: bool, digits: Vec<u8>, exponent: i64) -> Decimal {
        let (Some(start), Some(last)) = (
            digits.iter().position(|d| *d != 0),
            digits.iter().rposition(|d| *d != 0),
        ) else {
            return Decimal {
                negative: false,
                digits: Vec::new(),
                exponent: 0,
            };
        };
        Decimal {
            negative,
            exponent: exponent + (digits.len() - start) as i64,
            digits: digits[start..=last].to_vec(),
        }
    }

    /// Behavior: `java.math.BigDecimal#valueOf(long)` and `BigInteger#valueOf(long)`
    fn of_long(value: i64) -> Decimal {
        Decimal::of_text(&value.to_string(), 0)
    }

    /// The number Java's `BigDecimal.valueOf(double)` makes: that of the text of
    /// `Double#toString`, which is not the double's exact value (`0.1` is one tenth).
    ///
    /// Behavior: `java.math.BigDecimal#valueOf(double)`
    fn of_double(value: f64) -> Decimal {
        let text = double_to_string(value);
        let (mantissa, power) = match text.split_once('E') {
            Some((mantissa, power)) => (mantissa, power.parse::<i64>().unwrap_or(0)),
            None => (text.as_str(), 0),
        };
        let (negative, mantissa) = match mantissa.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, mantissa),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits: Vec<u8> = whole
            .bytes()
            .chain(fraction.bytes())
            .map(|b| b - b'0')
            .collect();
        Decimal::normalized(negative, digits, power - fraction.len() as i64)
    }

    fn signum(&self) -> i8 {
        match (self.digits.is_empty(), self.negative) {
            (true, _) => 0,
            (false, true) => -1,
            (false, false) => 1,
        }
    }

    /// Behavior: `java.math.BigDecimal#compareTo` and `java.math.BigInteger#compareTo`,
    /// which compare values and not scales.
    pub(crate) fn compare(&self, other: &Decimal) -> Ordering {
        let (a, b) = (self.signum(), other.signum());
        if a != b || a == 0 {
            return a.cmp(&b);
        }
        let magnitude = self
            .exponent
            .cmp(&other.exponent)
            .then_with(|| self.digits.cmp(&other.digits));
        if a < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

/// The values of one side of a comparison: each cell as a number, NaN, or `None` for null.
///
/// A primitive operand is converted as the overloads of `QueryLanguageFunctionUtils#compareTo`
/// with a `BigDecimal` or `BigInteger` convert it: an integer or `char` by
/// `BigDecimal#valueOf(long)` or `BigInteger#valueOf(long)`, a `float` widened to `double`,
/// and a `double` by `BigDecimal#valueOf(double)`. A `BigInteger` is compared through
/// `new BigDecimal(BigInteger)` where the other side is a decimal, which keeps its value.
///
/// Behavior: `QueryLanguageFunctionUtils#compareTo(BigDecimal, double)`,
/// `#compareTo(BigInteger, float)`, `#compareTo(BigInteger, long)`,
/// `#compareTo(BigDecimal, BigInteger)` and the other overloads with a big number
pub(crate) fn values(
    operand: BigOperand,
    array: &dyn Array,
) -> Result<Vec<Option<BigValue>>, TableFailure> {
    match operand {
        BigOperand::Decimal | BigOperand::Integer => {
            let cells = array
                .as_binary_opt::<i32>()
                .ok_or_else(|| failure("a big number column needs binary cells"))?;
            cells
                .iter()
                .map(|cell| {
                    cell.map(|cell| {
                        let (scale, unscaled) = match operand {
                            BigOperand::Decimal => java_values::big_decimal_parts(cell)
                                .ok_or_else(|| failure("a BigDecimal cell without a number"))?,
                            _ => (0, cell),
                        };
                        let text = java_values::big_integer_text(unscaled);
                        Ok(BigValue::Number(Decimal::of_text(&text, scale.into())))
                    })
                    .transpose()
                })
                .collect()
        }
        BigOperand::Primitive => Ok(match array.data_type() {
            DataType::Float32 | DataType::Float64 => convert::<f64>(array)
                .iter()
                .map(|value| {
                    value.map(|v| match v.is_nan() {
                        true => BigValue::NaN,
                        false => BigValue::Number(Decimal::of_double(v)),
                    })
                })
                .collect(),
            _ => convert::<i64>(array)
                .iter()
                .map(|value| value.map(|v| BigValue::Number(Decimal::of_long(v))))
                .collect(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(t: &str, scale: i64) -> Decimal {
        Decimal::of_text(t, scale)
    }

    #[test]
    fn values_compare_whatever_their_scale() {
        assert_eq!(text("150", 2).compare(&text("15", 1)), Ordering::Equal);
        assert_eq!(text("0", 3).compare(&text("-0", 0)), Ordering::Equal);
        assert_eq!(
            text("1", -20).compare(&text("100000000000000000000", 0)),
            Ordering::Equal
        );
        assert_eq!(text("-150", 2).compare(&text("-2", 0)), Ordering::Greater);
        assert_eq!(text("-150", 2).compare(&text("1", 1)), Ordering::Less);
        assert_eq!(text("999", 3).compare(&text("1", 0)), Ordering::Less);
        assert_eq!(text("1001", 3).compare(&text("1", 0)), Ordering::Greater);
    }

    #[test]
    fn a_double_is_the_number_of_its_java_text() {
        assert_eq!(
            Decimal::of_double(0.1).compare(&text("1", 1)),
            Ordering::Equal
        );
        assert_eq!(
            Decimal::of_double(1e20).compare(&text("1", -20)),
            Ordering::Equal
        );
        assert_eq!(
            Decimal::of_double(-0.0).compare(&text("0", 0)),
            Ordering::Equal
        );
        assert_eq!(
            Decimal::of_double(-1.5).compare(&text("-15", 1)),
            Ordering::Equal
        );
        // Double.toString(4.9E-324), not the exact value of the smallest double
        assert_eq!(
            Decimal::of_double(f64::from_bits(1)).compare(&text("49", 325)),
            Ordering::Equal
        );
        // a float is widened first: 0.1f is 0.10000000149011612
        let widened = Decimal::of_double(f64::from(0.1f32));
        assert_eq!(
            widened.compare(&text("10000000149011612", 17)),
            Ordering::Equal
        );
        assert_eq!(
            widened.compare(&text("1000000014901161193847656250", 28)),
            Ordering::Greater
        );
    }
}
