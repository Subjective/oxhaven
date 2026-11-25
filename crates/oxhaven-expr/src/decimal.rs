//! The numbers of `java.math`: a decimal (`BigDecimal`), an unscaled integer and a scale,
//! and the integer of any size (`BigInteger`), with the arithmetic Java's formulas reach.
//!
//! Written from the documented behaviour of `java.math.BigDecimal`, `java.math.BigInteger`
//! and `java.math.RoundingMode` and checked against values Java 42.9 computed
//! (`tests/formulas/big_numbers.rs`); the JDK's source was not used.

use num_bigint::{BigInt, Sign};
use num_integer::Integer;
use num_traits::{One, Signed, Zero};
use oxhaven_column::java_values;
use std::cmp::Ordering;

/// The most decimal digits a power of ten that an operation needs may have. Java computes
/// such a power whatever its size, and for one of millions of digits it runs out of memory
/// or does not finish (`PARITY.md` A12); here the operation fails instead.
const MAX_POWER: u64 = 1_000_000;

/// Why an operation on big numbers fails: the text of the Java exception it throws.
pub(crate) type Failure = String;

fn arithmetic(message: &str) -> Failure {
    format!("java.lang.ArithmeticException: {message}")
}

/// A `java.math.BigDecimal`: `unscaled × 10^-scale`. Two of one value and different scales
/// are different decimals, as `BigDecimal#equals` says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Decimal {
    pub(crate) unscaled: BigInt,
    pub(crate) scale: i32,
}

/// `java.math.RoundingMode`, by the number its constant has in `BigDecimal` (`ROUND_UP` is
/// 0, `ROUND_UNNECESSARY` 7), which is also its ordinal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rounding {
    Up,
    Down,
    Ceiling,
    Floor,
    HalfUp,
    HalfDown,
    HalfEven,
    Unnecessary,
}

impl Rounding {
    /// Behavior: `java.math.RoundingMode#valueOf(int)`, which `BigDecimal#setScale(int, int)`
    /// and `#divide(BigDecimal, int, int)` call: "Invalid rounding mode" for any other number.
    pub(crate) fn of_number(number: i64) -> Result<Rounding, Failure> {
        Ok(match number {
            0 => Rounding::Up,
            1 => Rounding::Down,
            2 => Rounding::Ceiling,
            3 => Rounding::Floor,
            4 => Rounding::HalfUp,
            5 => Rounding::HalfDown,
            6 => Rounding::HalfEven,
            7 => Rounding::Unnecessary,
            _ => {
                return Err("java.lang.IllegalArgumentException: Invalid rounding mode".into());
            }
        })
    }

    /// The constant of `java.math.RoundingMode` named `name`, as its number.
    pub(crate) fn number_of_name(name: &str) -> Option<i32> {
        let number = match name {
            "UP" => 0,
            "DOWN" => 1,
            "CEILING" => 2,
            "FLOOR" => 3,
            "HALF_UP" => 4,
            "HALF_DOWN" => 5,
            "HALF_EVEN" => 6,
            "UNNECESSARY" => 7,
            _ => return None,
        };
        Some(number)
    }
}

/// `10^exponent`, or a failure when it has more digits than [`MAX_POWER`].
fn ten_to(exponent: u64) -> Result<BigInt, Failure> {
    if exponent > MAX_POWER {
        return Err(format!(
            "a power of ten of {exponent} digits, which Java does not finish computing"
        ));
    }
    Ok(num_traits::pow(BigInt::from(10), exponent as usize))
}

/// The scale `value` names, or Java's failure where it leaves the range of an `int`.
fn scale_of(value: i64) -> Result<i32, Failure> {
    i32::try_from(value).map_err(|_| arithmetic(if value > 0 { "Underflow" } else { "Overflow" }))
}

/// `numerator / denominator` rounded to an integer as `rounding` says; the denominator is
/// not zero.
///
/// Behavior: `java.math.RoundingMode`: the documented result of each mode for a quotient
/// that is not an integer; `UNNECESSARY` throws "Rounding necessary".
fn divide_rounded(
    numerator: &BigInt,
    denominator: &BigInt,
    rounding: Rounding,
) -> Result<BigInt, Failure> {
    let (quotient, remainder) = numerator.div_rem(denominator);
    if remainder.is_zero() {
        return Ok(quotient);
    }
    let negative = (numerator.sign() == Sign::Minus) != (denominator.sign() == Sign::Minus);
    // The distance from the truncated quotient against one half: twice the remainder
    // against the divisor.
    let half = (remainder.abs() * 2u32).cmp(&denominator.abs());
    let away = match rounding {
        Rounding::Up => true,
        Rounding::Down => false,
        Rounding::Ceiling => !negative,
        Rounding::Floor => negative,
        Rounding::HalfUp => half != Ordering::Less,
        Rounding::HalfDown => half == Ordering::Greater,
        Rounding::HalfEven => match half {
            Ordering::Less => false,
            Ordering::Greater => true,
            Ordering::Equal => quotient.is_odd(),
        },
        Rounding::Unnecessary => return Err(arithmetic("Rounding necessary")),
    };
    Ok(match (away, negative) {
        (false, _) => quotient,
        (true, false) => quotient + 1u32,
        (true, true) => quotient - 1u32,
    })
}

/// The number of decimal digits of `value`'s magnitude, one for zero.
fn digits(value: &BigInt) -> u64 {
    if value.is_zero() {
        return 1;
    }
    value.magnitude().to_str_radix(10).len() as u64
}

/// The low 64 bits of `value` in two's complement.
fn low_bits(value: &BigInt) -> i64 {
    let magnitude = value.magnitude().iter_u64_digits().next().unwrap_or(0);
    let bits = match value.sign() {
        Sign::Minus => magnitude.wrapping_neg(),
        _ => magnitude,
    };
    bits as i64
}

/// The `double` nearest the number `text`, a plain or scientific decimal: Rust's parsing
/// rounds correctly, as `BigDecimal#doubleValue` and `BigInteger#doubleValue` do.
fn nearest_double(unscaled: &BigInt, scale: i32) -> f64 {
    format!("{unscaled}e{}", -i64::from(scale))
        .parse()
        .unwrap_or(f64::NAN)
}

fn nearest_float(unscaled: &BigInt, scale: i32) -> f32 {
    format!("{unscaled}e{}", -i64::from(scale))
        .parse()
        .unwrap_or(f32::NAN)
}

impl Decimal {
    pub(crate) fn new(unscaled: BigInt, scale: i32) -> Decimal {
        Decimal { unscaled, scale }
    }

    /// Behavior: `java.math.BigDecimal#valueOf(long)`
    pub(crate) fn of_long(value: i64) -> Decimal {
        Decimal::new(BigInt::from(value), 0)
    }

    /// Behavior: `java.math.BigDecimal#BigDecimal(BigInteger)`
    pub(crate) fn of_integer(value: BigInt) -> Decimal {
        Decimal::new(value, 0)
    }

    /// The number of the text `Double#toString` gives for `value`, which is not the
    /// double's exact value (`0.1` is one tenth, with scale 1); NaN and the infinities have
    /// no number.
    ///
    /// Behavior: `java.math.BigDecimal#valueOf(double)`
    pub(crate) fn of_double(value: f64) -> Result<Decimal, Failure> {
        let text = crate::eval::double_to_string(value);
        Decimal::parse(&text).ok_or_else(|| {
            format!(
                "java.lang.NumberFormatException: Character {} is neither a decimal digit \
                 number, decimal point, nor \"e\" notation exponential mark.",
                text.trim_start_matches('-').chars().next().unwrap_or('?')
            )
        })
    }

    /// The exact value of `value`, a finite double: `m × 2^e` is `m × 5^-e × 10^e` for a
    /// negative `e`, so its scale is the number of binary places.
    ///
    /// Behavior: `java.math.BigDecimal#BigDecimal(double)`, which throws for NaN and the
    /// infinities ("Infinite or NaN").
    pub(crate) fn of_double_exactly(value: f64) -> Result<Decimal, Failure> {
        if !value.is_finite() {
            return Err("java.lang.NumberFormatException: Infinite or NaN".into());
        }
        let bits = value.to_bits();
        let negative = bits >> 63 == 1;
        let exponent = ((bits >> 52) & 0x7ff) as i64;
        let fraction = bits & ((1 << 52) - 1);
        let (mut mantissa, mut power) = match exponent {
            0 => (fraction, -1074),
            _ => (fraction | (1 << 52), exponent - 1075),
        };
        if mantissa == 0 {
            return Ok(Decimal::of_long(0));
        }
        while mantissa % 2 == 0 {
            mantissa /= 2;
            power += 1;
        }
        let mut unscaled = BigInt::from(mantissa);
        let scale = if power >= 0 {
            unscaled <<= power as usize;
            0
        } else {
            unscaled *= num_traits::pow(BigInt::from(5), (-power) as usize);
            -power
        };
        if negative {
            unscaled = -unscaled;
        }
        Ok(Decimal::new(unscaled, scale as i32))
    }

    /// The decimal of the text `text`: an optional sign, digits with at most one `.` and at
    /// least one digit, and an optional exponent `e` or `E` with an optional sign. The scale
    /// is the number of digits after the point less the exponent. `None` for any other
    /// text, a scale out of the range of an `int`, and digits that are not ASCII, which
    /// Java reads too (`Character#digit`) and this parser does not.
    ///
    /// Behavior: `java.math.BigDecimal#BigDecimal(String)`
    pub(crate) fn parse(text: &str) -> Option<Decimal> {
        let (mantissa, exponent) = match text.find(['e', 'E']) {
            Some(at) => (&text[..at], Some(&text[at + 1..])),
            None => (text, None),
        };
        let (negative, mantissa) = match mantissa.as_bytes().first() {
            Some(b'-') => (true, &mantissa[1..]),
            Some(b'+') => (false, &mantissa[1..]),
            _ => (false, mantissa),
        };
        let (whole, fraction) = match mantissa.split_once('.') {
            Some((whole, fraction)) => (whole, fraction),
            None => (mantissa, ""),
        };
        let all_digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
        if whole.len() + fraction.len() == 0 || !all_digits(whole) || !all_digits(fraction) {
            return None;
        }
        let exponent: i64 = match exponent {
            None => 0,
            Some(exponent) => {
                let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
                if digits.is_empty() || !all_digits(digits) || digits.len() > 10 {
                    return None;
                }
                exponent.parse().ok()?
            }
        };
        let scale = i32::try_from(fraction.len() as i64 - exponent).ok()?;
        let mut unscaled: BigInt = format!("{whole}{fraction}").parse().ok()?;
        if negative {
            unscaled = -unscaled;
        }
        Some(Decimal::new(unscaled, scale))
    }

    /// The decimal of a `BigDecimal` cell (`ColumnType::BigDecimal`).
    pub(crate) fn of_cell(cell: &[u8]) -> Option<Decimal> {
        let (scale, unscaled) = java_values::big_decimal_parts(cell)?;
        Some(Decimal::new(BigInt::from_signed_bytes_be(unscaled), scale))
    }

    /// The `BigDecimal` cell of this decimal.
    pub(crate) fn cell(&self) -> Vec<u8> {
        java_values::big_decimal_cell(self.scale, &integer_bytes(&self.unscaled))
    }

    /// This decimal at `scale`, which is at least its own: the value does not change.
    fn widened(&self, scale: i32) -> Result<BigInt, Failure> {
        let by = i64::from(scale) - i64::from(self.scale);
        Ok(&self.unscaled * ten_to(by as u64)?)
    }

    /// Behavior: `java.math.BigDecimal#add`: exact, with the larger scale.
    pub(crate) fn add(&self, other: &Decimal) -> Result<Decimal, Failure> {
        let scale = self.scale.max(other.scale);
        Ok(Decimal::new(
            self.widened(scale)? + other.widened(scale)?,
            scale,
        ))
    }

    /// Behavior: `java.math.BigDecimal#subtract`: exact, with the larger scale.
    pub(crate) fn subtract(&self, other: &Decimal) -> Result<Decimal, Failure> {
        self.add(&other.negate())
    }

    /// Behavior: `java.math.BigDecimal#multiply`: exact, with the sum of the scales.
    pub(crate) fn multiply(&self, other: &Decimal) -> Result<Decimal, Failure> {
        let scale = scale_of(i64::from(self.scale) + i64::from(other.scale))?;
        Ok(Decimal::new(&self.unscaled * &other.unscaled, scale))
    }

    /// Behavior: `java.math.BigDecimal#negate`
    pub(crate) fn negate(&self) -> Decimal {
        Decimal::new(-&self.unscaled, self.scale)
    }

    /// Behavior: `java.math.BigDecimal#abs`
    pub(crate) fn abs(&self) -> Decimal {
        Decimal::new(self.unscaled.abs(), self.scale)
    }

    /// Behavior: `java.math.BigDecimal#signum`
    pub(crate) fn signum(&self) -> i32 {
        match self.unscaled.sign() {
            Sign::Minus => -1,
            Sign::NoSign => 0,
            Sign::Plus => 1,
        }
    }

    /// Behavior: `java.math.BigDecimal#precision`: the digits of the unscaled value, one for
    /// zero.
    pub(crate) fn precision(&self) -> i32 {
        digits(&self.unscaled) as i32
    }

    /// The fails of a division by `divisor`, which Java checks first: "Division undefined"
    /// for zero by zero, "Division by zero" otherwise.
    fn check_divisor(&self, divisor: &Decimal) -> Result<(), Failure> {
        if !divisor.unscaled.is_zero() {
            return Ok(());
        }
        Err(arithmetic(if self.unscaled.is_zero() {
            "Division undefined"
        } else {
            "Division by zero"
        }))
    }

    /// `self / divisor` rounded to `scale`.
    ///
    /// Behavior: `java.math.BigDecimal#divide(BigDecimal, int, RoundingMode)`
    pub(crate) fn divide_to_scale(
        &self,
        divisor: &Decimal,
        scale: i32,
        rounding: Rounding,
    ) -> Result<Decimal, Failure> {
        self.check_divisor(divisor)?;
        // self / divisor × 10^scale = a × 10^(scale + ds - s) / d
        let shift = i64::from(scale) + i64::from(divisor.scale) - i64::from(self.scale);
        let (numerator, denominator) = if shift >= 0 {
            (
                &self.unscaled * ten_to(shift as u64)?,
                divisor.unscaled.clone(),
            )
        } else {
            let power = -shift as u64;
            if power > digits(&self.unscaled) + 1 {
                // The quotient is below a tenth in size: only its sign and whether it is
                // zero decide the rounding, as a smaller power that keeps both does.
                let small = digits(&self.unscaled) + 1;
                (
                    self.unscaled.clone(),
                    &divisor.unscaled * ten_to(small)? * 10u32,
                )
            } else {
                (self.unscaled.clone(), &divisor.unscaled * ten_to(power)?)
            }
        };
        Ok(Decimal::new(
            divide_rounded(&numerator, &denominator, rounding)?,
            scale,
        ))
    }

    /// `self / divisor` exactly, with the scale nearest to `self.scale - divisor.scale` that
    /// holds it; "Non-terminating decimal expansion" when no scale does.
    ///
    /// Behavior: `java.math.BigDecimal#divide(BigDecimal)`
    pub(crate) fn divide_exactly(&self, divisor: &Decimal) -> Result<Decimal, Failure> {
        self.check_divisor(divisor)?;
        let preferred = i64::from(self.scale) - i64::from(divisor.scale);
        if self.unscaled.is_zero() {
            let saturated = preferred.clamp(i32::MIN.into(), i32::MAX.into()) as i32;
            return Ok(Decimal::new(BigInt::zero(), saturated));
        }
        let common = self.unscaled.gcd(&divisor.unscaled);
        let (mut numerator, mut denominator) = (
            &self.unscaled / &common,
            (&divisor.unscaled / &common).abs(),
        );
        if divisor.unscaled.is_negative() {
            numerator = -numerator;
        }
        // The denominator must be 2^a × 5^b; then 10^max(a, b) is a multiple of it.
        let (mut twos, mut fives) = (0_u64, 0_u64);
        while denominator.is_even() {
            denominator /= 2u32;
            twos += 1;
        }
        while (&denominator % 5u32).is_zero() {
            denominator /= 5u32;
            fives += 1;
        }
        if !denominator.is_one() {
            return Err(arithmetic(
                "Non-terminating decimal expansion; no exact representable decimal result.",
            ));
        }
        let places = twos.max(fives);
        numerator *= num_traits::pow(BigInt::from(2), (places - twos) as usize);
        numerator *= num_traits::pow(BigInt::from(5), (places - fives) as usize);
        let exact = Decimal::new(numerator, scale_of(preferred + places as i64)?);
        Ok(exact.stripped_to(preferred))
    }

    /// The same value with the zeros at the end of the unscaled value removed, one place at
    /// a time while the scale stays above `floor`.
    fn stripped_to(mut self, floor: i64) -> Decimal {
        let ten = BigInt::from(10);
        while i64::from(self.scale) > floor && self.scale > i32::MIN && !self.unscaled.is_zero() {
            let (quotient, remainder) = self.unscaled.div_rem(&ten);
            if !remainder.is_zero() {
                break;
            }
            self.unscaled = quotient;
            self.scale -= 1;
        }
        self
    }

    /// The integer part of `self / divisor`, with the scale nearest to `self.scale -
    /// divisor.scale` that holds it: that scale itself where it is not negative, and
    /// otherwise no lower than the zeros at the end of the integer allow.
    ///
    /// Behavior: `java.math.BigDecimal#divideToIntegralValue(BigDecimal)`
    pub(crate) fn divide_to_integral(&self, divisor: &Decimal) -> Result<Decimal, Failure> {
        self.check_divisor(divisor)?;
        let preferred = i64::from(self.scale) - i64::from(divisor.scale);
        let integer = self.divide_to_scale(divisor, 0, Rounding::Down)?.unscaled;
        // Seen on Java 42.9: `0 % 0.1` is `0`, `20 % 97` (`2E+1 % 97`) `2E+1`, so a zero
        // integer part has the preferred scale.
        if integer.is_zero() {
            let saturated = preferred.clamp(i32::MIN.into(), i32::MAX.into()) as i32;
            return Ok(Decimal::new(integer, saturated));
        }
        if preferred >= 0 {
            let scale = scale_of(preferred)?;
            return Ok(Decimal::new(integer * ten_to(preferred as u64)?, scale));
        }
        Ok(Decimal::new(integer, 0).stripped_to(preferred))
    }

    /// `self - self.divideToIntegralValue(divisor) × divisor`.
    ///
    /// Behavior: `java.math.BigDecimal#remainder(BigDecimal)`
    pub(crate) fn remainder(&self, divisor: &Decimal) -> Result<Decimal, Failure> {
        let integral = self.divide_to_integral(divisor)?;
        self.subtract(&integral.multiply(divisor)?)
    }

    /// Behavior: `java.math.BigDecimal#setScale(int, RoundingMode)`
    pub(crate) fn set_scale(&self, scale: i32, rounding: Rounding) -> Result<Decimal, Failure> {
        if scale >= self.scale {
            return Ok(Decimal::new(self.widened(scale)?, scale));
        }
        self.divide_to_scale(&Decimal::of_long(1), scale, rounding)
    }

    /// Behavior: `java.math.BigDecimal#stripTrailingZeros`: zero is `0` with scale 0.
    pub(crate) fn strip_trailing_zeros(&self) -> Decimal {
        if self.unscaled.is_zero() {
            return Decimal::of_long(0);
        }
        self.clone().stripped_to(i64::MIN)
    }

    /// Behavior: `java.math.BigDecimal#movePointLeft`: the scale grows by `places`, and is
    /// then at least zero.
    pub(crate) fn move_point_left(&self, places: i32) -> Result<Decimal, Failure> {
        let moved = Decimal::new(
            self.unscaled.clone(),
            scale_of(i64::from(self.scale) + i64::from(places))?,
        );
        if moved.scale < 0 {
            return moved.set_scale(0, Rounding::Unnecessary);
        }
        Ok(moved)
    }

    /// Behavior: `java.math.BigDecimal#movePointRight`: the scale shrinks by `places`, and is
    /// then at least zero.
    pub(crate) fn move_point_right(&self, places: i32) -> Result<Decimal, Failure> {
        let moved = Decimal::new(
            self.unscaled.clone(),
            scale_of(i64::from(self.scale) - i64::from(places))?,
        );
        if moved.scale < 0 {
            return moved.set_scale(0, Rounding::Unnecessary);
        }
        Ok(moved)
    }

    /// Behavior: `java.math.BigDecimal#scaleByPowerOfTen`
    pub(crate) fn scale_by_power_of_ten(&self, places: i32) -> Result<Decimal, Failure> {
        Ok(Decimal::new(
            self.unscaled.clone(),
            scale_of(i64::from(self.scale) - i64::from(places))?,
        ))
    }

    /// Behavior: `java.math.BigDecimal#ulp`: one unit of the last place, at this scale.
    pub(crate) fn ulp(&self) -> Decimal {
        Decimal::new(BigInt::one(), self.scale)
    }

    /// Behavior: `java.math.BigDecimal#pow(int)`: exact, with `exponent` times the scale;
    /// "Invalid operation" for an exponent outside 0 to 999999999.
    pub(crate) fn pow(&self, exponent: i32) -> Result<Decimal, Failure> {
        if !(0..=999_999_999).contains(&exponent) {
            return Err(arithmetic("Invalid operation"));
        }
        let scale = scale_of(i64::from(self.scale) * i64::from(exponent))?;
        Ok(Decimal::new(integer_pow(&self.unscaled, exponent)?, scale))
    }

    /// Behavior: `java.math.BigDecimal#compareTo`: by value, whatever the scales.
    pub(crate) fn compare(&self, other: &Decimal) -> Ordering {
        let (a, b) = (self.signum(), other.signum());
        if a != b || a == 0 {
            return a.cmp(&b);
        }
        let scale = self.scale.max(other.scale);
        // Widening by a huge power to compare is not needed: the sizes decide first.
        let magnitude = |d: &Decimal| digits(&d.unscaled) as i64 - i64::from(d.scale);
        let (left, right) = (magnitude(self), magnitude(other));
        if left != right {
            let ordering = left.cmp(&right);
            return if a < 0 { ordering.reverse() } else { ordering };
        }
        let widen = |d: &Decimal| {
            let by = (i64::from(scale) - i64::from(d.scale)) as usize;
            &d.unscaled * num_traits::pow(BigInt::from(10), by)
        };
        widen(self).cmp(&widen(other))
    }

    /// The integer part, rounded toward zero.
    ///
    /// Behavior: `java.math.BigDecimal#toBigInteger`
    pub(crate) fn to_integer(&self) -> Result<BigInt, Failure> {
        if self.scale <= 0 {
            return Ok(&self.unscaled * ten_to(-i64::from(self.scale) as u64)?);
        }
        if u64::try_from(self.scale).unwrap_or(0) > digits(&self.unscaled) {
            return Ok(BigInt::zero());
        }
        Ok(&self.unscaled / ten_to(self.scale as u64)?)
    }

    /// The low 64 bits of the integer part.
    ///
    /// Behavior: `java.math.BigDecimal#longValue`, `#intValue`, and `Number#shortValue`
    /// and `#byteValue`, which narrow `intValue`.
    pub(crate) fn low_bits(&self) -> i64 {
        // 10^k is a multiple of 2^64 from k = 64 on: those low bits are zero.
        if self.scale <= -64 {
            return 0;
        }
        self.to_integer()
            .map(|integer| low_bits(&integer))
            .unwrap_or(0)
    }

    /// Behavior: `java.math.BigDecimal#doubleValue`: the nearest `double`.
    pub(crate) fn to_double(&self) -> f64 {
        nearest_double(&self.unscaled, self.scale)
    }

    /// Behavior: `java.math.BigDecimal#floatValue`: the nearest `float`.
    pub(crate) fn to_float(&self) -> f32 {
        nearest_float(&self.unscaled, self.scale)
    }

    /// Behavior: `java.math.BigDecimal#toString`
    pub(crate) fn text(&self) -> String {
        java_values::big_decimal_text(self.scale, &integer_bytes(&self.unscaled))
    }

    /// Behavior: `java.math.BigDecimal#toPlainString`: no exponent; a negative scale is
    /// zeros at the end, a positive one places after the point.
    pub(crate) fn plain_text(&self) -> String {
        let digits = self.unscaled.magnitude().to_str_radix(10);
        let sign = if self.unscaled.is_negative() { "-" } else { "" };
        match self.scale.cmp(&0) {
            Ordering::Equal => format!("{sign}{digits}"),
            Ordering::Less => {
                if self.unscaled.is_zero() {
                    return "0".repeat(1 + (-i64::from(self.scale)) as usize);
                }
                format!(
                    "{sign}{digits}{}",
                    "0".repeat((-i64::from(self.scale)) as usize)
                )
            }
            Ordering::Greater => {
                let scale = self.scale as usize;
                if digits.len() > scale {
                    let (whole, fraction) = digits.split_at(digits.len() - scale);
                    format!("{sign}{whole}.{fraction}")
                } else {
                    format!("{sign}0.{}{digits}", "0".repeat(scale - digits.len()))
                }
            }
        }
    }
}

/// The bytes of `value` as `BigInteger#toByteArray` gives them: two's complement, most
/// significant first, in the fewest bytes, `[0]` for zero.
pub(crate) fn integer_bytes(value: &BigInt) -> Vec<u8> {
    java_values::minimal(&value.to_signed_bytes_be()).to_vec()
}

/// The integer of a `BigInteger` cell.
pub(crate) fn integer_of_cell(cell: &[u8]) -> BigInt {
    BigInt::from_signed_bytes_be(cell)
}

/// `value^exponent` for an exponent that is not negative, or a failure where the result
/// would have more than [`MAX_POWER`] digits.
fn integer_pow(value: &BigInt, exponent: i32) -> Result<BigInt, Failure> {
    let size = digits(value) * exponent as u64;
    if value.magnitude() > &num_bigint::BigUint::one() && size > MAX_POWER {
        return Err(format!(
            "a power of {size} digits, which Java does not finish computing"
        ));
    }
    Ok(num_traits::pow(value.clone(), exponent as usize))
}

/// The methods of `java.math.BigInteger` that compute with integers.
pub(crate) mod integer {
    use super::*;

    /// Behavior: `java.math.BigInteger#divide`: rounded toward zero.
    pub(crate) fn divide(a: &BigInt, b: &BigInt) -> Result<BigInt, Failure> {
        if b.is_zero() {
            return Err(arithmetic("BigInteger divide by zero"));
        }
        Ok(a / b)
    }

    /// Behavior: `java.math.BigInteger#remainder`: the sign of the dividend.
    pub(crate) fn remainder(a: &BigInt, b: &BigInt) -> Result<BigInt, Failure> {
        if b.is_zero() {
            return Err(arithmetic("BigInteger divide by zero"));
        }
        Ok(a % b)
    }

    /// Behavior: `java.math.BigInteger#mod`: never negative; "BigInteger: modulus not
    /// positive" for a modulus that is not above zero.
    pub(crate) fn modulo(a: &BigInt, m: &BigInt) -> Result<BigInt, Failure> {
        if !m.is_positive() {
            return Err(arithmetic("BigInteger: modulus not positive"));
        }
        Ok(a.mod_floor(m))
    }

    /// Behavior: `java.math.BigInteger#pow`: "Negative exponent" below zero.
    pub(crate) fn pow(a: &BigInt, exponent: i32) -> Result<BigInt, Failure> {
        if exponent < 0 {
            return Err(arithmetic("Negative exponent"));
        }
        integer_pow(a, exponent)
    }

    /// Behavior: `java.math.BigInteger#gcd`: never negative, zero for two zeros.
    pub(crate) fn gcd(a: &BigInt, b: &BigInt) -> BigInt {
        a.gcd(b)
    }

    /// Behavior: `java.math.BigInteger#bitLength`: the bits of the shortest two's complement
    /// form without the sign bit.
    pub(crate) fn bit_length(a: &BigInt) -> i32 {
        let magnitude = if a.is_negative() {
            (-a) - 1u32
        } else {
            a.clone()
        };
        magnitude.bits() as i32
    }

    /// Behavior: `java.math.BigInteger#signum`
    pub(crate) fn signum(a: &BigInt) -> i32 {
        match a.sign() {
            Sign::Minus => -1,
            Sign::NoSign => 0,
            Sign::Plus => 1,
        }
    }

    /// Behavior: `java.math.BigInteger#longValue`, `#intValue`, and `Number#shortValue` and
    /// `#byteValue`: the low bits.
    pub(crate) fn low_bits(a: &BigInt) -> i64 {
        super::low_bits(a)
    }

    /// Behavior: `java.math.BigInteger#doubleValue`: the nearest `double`.
    pub(crate) fn to_double(a: &BigInt) -> f64 {
        nearest_double(a, 0)
    }

    /// Behavior: `java.math.BigInteger#floatValue`: the nearest `float`.
    pub(crate) fn to_float(a: &BigInt) -> f32 {
        nearest_float(a, 0)
    }

    /// The integer of the text `text`: an optional sign and ASCII digits; `None` for any
    /// other text, and for digits that are not ASCII, which Java reads too.
    ///
    /// Behavior: `java.math.BigInteger#BigInteger(String)`
    pub(crate) fn parse(text: &str) -> Option<BigInt> {
        let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let value: BigInt = digits.parse().ok()?;
        Some(if text.starts_with('-') { -value } else { value })
    }
}

#[cfg(test)]
fn small(value: &BigInt) -> Option<i64> {
    num_traits::ToPrimitive::to_i64(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(text: &str) -> Decimal {
        Decimal::parse(text).expect(text)
    }

    #[test]
    fn text_reads_as_java_reads_it() {
        assert_eq!(d("1.50"), Decimal::new(BigInt::from(150), 2));
        assert_eq!(d("-2.345"), Decimal::new(BigInt::from(-2345), 3));
        assert_eq!(d("1E+3"), Decimal::new(BigInt::from(1), -3));
        assert_eq!(d("1.0E-7"), Decimal::new(BigInt::from(10), 8));
        assert_eq!(d(".5"), Decimal::new(BigInt::from(5), 1));
        assert_eq!(d("5."), Decimal::new(BigInt::from(5), 0));
        assert_eq!(d("+0.000"), Decimal::new(BigInt::from(0), 3));
        for bad in [
            "", ".", "1e", "e5", "1.2.3", "--1", "1e+", "١", "NaN", "Infinity",
        ] {
            assert_eq!(Decimal::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_double_is_the_number_of_its_java_text() {
        assert_eq!(Decimal::of_double(0.1).unwrap(), d("0.1"));
        assert_eq!(Decimal::of_double(1e20).unwrap(), d("1.0E+20"));
        assert_eq!(Decimal::of_double(3.0).unwrap(), d("3.0"));
        assert_eq!(
            Decimal::of_double(f64::from(0.1f32)).unwrap(),
            d("0.10000000149011612")
        );
        assert!(Decimal::of_double(f64::NAN)
            .unwrap_err()
            .contains("Character N"));
        assert!(Decimal::of_double(f64::INFINITY)
            .unwrap_err()
            .contains("Character I"));
        assert_eq!(
            Decimal::of_double_exactly(0.1).unwrap().text(),
            "0.1000000000000000055511151231257827021181583404541015625"
        );
        assert_eq!(Decimal::of_double_exactly(-2.5).unwrap(), d("-2.5"));
        assert_eq!(
            Decimal::of_double_exactly(1e20).unwrap(),
            d("100000000000000000000")
        );
    }

    #[test]
    fn division_rounds_to_the_scale_asked() {
        let cases = [
            ("1.50", "7", 8, Rounding::HalfUp, "0.21428571"),
            ("-2.345", "3", 8, Rounding::HalfUp, "-0.78166667"),
            ("1E+3", "7", -3, Rounding::HalfUp, "0E+3"),
            ("1E-7", "-0.3", 7, Rounding::HalfUp, "-3E-7"),
            ("-0.5", "2E+1", 3, Rounding::HalfUp, "-0.025"),
            ("-0.5", "2E+1", 2, Rounding::HalfUp, "-0.03"),
            ("-0.5", "2E+1", 2, Rounding::HalfDown, "-0.02"),
            ("-0.5", "2E+1", 2, Rounding::HalfEven, "-0.02"),
            ("-0.5", "2E+1", 0, Rounding::Floor, "-1"),
            ("1E-7", "-0.3", 0, Rounding::Floor, "-1"),
            ("1E-7", "-0.3", 4, Rounding::Up, "-0.0001"),
            ("1", "3", 0, Rounding::Ceiling, "1"),
            ("1", "1E+30", -2, Rounding::Ceiling, "1E+2"),
            ("-1", "1E+30", -2, Rounding::Ceiling, "0E+2"),
            ("1", "1E+30", 2, Rounding::HalfUp, "0.00"),
        ];
        for (a, b, scale, rounding, expected) in cases {
            let quotient = d(a).divide_to_scale(&d(b), scale, rounding).unwrap();
            assert_eq!(
                quotient.text(),
                expected,
                "{a} / {b} at {scale} {rounding:?}"
            );
        }
        assert!(d("1").divide_to_scale(&d("0.00"), 2, Rounding::Up).is_err());
        assert!(d("1")
            .divide_to_scale(&d("3"), 2, Rounding::Unnecessary)
            .unwrap_err()
            .contains("Rounding necessary"));
    }

    #[test]
    fn the_remainder_has_the_scale_java_gives_it() {
        for (a, b, expected) in [
            ("-2.345", "0.10", "-0.045"),
            ("1E+3", "7.000", "6.000"),
            ("1E+3", "2", "0E+2"),
            ("1E+3", "97", "3E+1"),
            ("-7", "0.10", "0.0"),
            ("-5", "2E+1", "-5"),
            ("9223372036854775808", "-0.3", "0.2"),
            (
                "12345678901234567890.123456789",
                "1.0E+20",
                "12345678901234567890.123456789",
            ),
            ("1.50", "2", "1.50"),
        ] {
            assert_eq!(d(a).remainder(&d(b)).unwrap().text(), expected, "{a} % {b}");
        }
    }

    #[test]
    fn exact_division_takes_the_nearest_scale_that_holds_it() {
        for (a, b, expected) in [
            ("-2.345", "0.10", "-23.45"),
            ("0", "-7", "0"),
            ("12", "3", "4"),
            ("1", "8", "0.125"),
            ("1.00", "4", "0.25"),
            ("100", "4", "25"),
            ("1E+3", "2E+1", "5E+1"),
        ] {
            assert_eq!(
                d(a).divide_exactly(&d(b)).unwrap().text(),
                expected,
                "{a} / {b}"
            );
        }
        assert!(d("1").divide_exactly(&d("3")).is_err());
    }

    #[test]
    fn numbers_convert_as_java_converts_them() {
        let big = d("12345678901234567890.123456789");
        assert_eq!(big.low_bits() as i32, -350287150);
        assert_eq!(big.low_bits(), -6101065172474983726);
        assert_eq!(big.to_double(), 1.2345678901234567E19);
        assert_eq!(d("1E+64").low_bits(), 0);
        assert_eq!(d("-0.5").low_bits(), 0);
        assert_eq!(d("1E+3").low_bits() as i8, -24);
        assert_eq!(small(&d("-2.345").to_integer().unwrap()), Some(-2));
        assert_eq!(d("1E-7").plain_text(), "0.0000001");
        assert_eq!(d("1E+3").plain_text(), "1000");
        assert_eq!(d("0E+2").plain_text(), "000");
        assert_eq!(d("-2.345").plain_text(), "-2.345");
        assert_eq!(d("1E+3").move_point_left(2).unwrap().text(), "10");
        assert_eq!(d("1E-7").move_point_right(2).unwrap().text(), "0.00001");
        assert_eq!(d("0").scale_by_power_of_ten(3).unwrap().text(), "0E+3");
        assert_eq!(
            d("-2.345").set_scale(2, Rounding::HalfUp).unwrap().text(),
            "-2.35"
        );
        assert_eq!(
            d("1E+3").set_scale(-1, Rounding::Ceiling).unwrap().text(),
            "1.00E+3"
        );
        assert_eq!(d("0.000").strip_trailing_zeros().text(), "0");
        assert_eq!(
            d("12345678901234567890.123456789").compare(&d("3")),
            Ordering::Greater
        );
        assert_eq!(d("-1.50").compare(&d("-1.5")), Ordering::Equal);
        assert_eq!(d("1E+1000").compare(&d("-1E+2000")), Ordering::Greater);
    }

    #[test]
    fn integers_compute_as_java_computes_them() {
        let i = |v: i64| BigInt::from(v);
        assert_eq!(integer::modulo(&i(-7), &i(2)).unwrap(), i(1));
        assert!(integer::modulo(&i(-7), &i(-2)).is_err());
        assert_eq!(integer::remainder(&i(-7), &i(2)).unwrap(), i(-1));
        assert_eq!(integer::divide(&i(-7), &i(2)).unwrap(), i(-3));
        assert_eq!(integer::bit_length(&i(-7)), 3);
        assert_eq!(integer::bit_length(&i(-8)), 3);
        assert_eq!(integer::gcd(&i(0), &i(5)), i(5));
        let two_63 = integer::parse("9223372036854775808").unwrap();
        assert_eq!(integer::low_bits(&two_63), i64::MIN);
        assert_eq!(integer::to_float(&two_63), 9.223372E18);
        assert_eq!(integer::parse("-12"), Some(i(-12)));
        assert_eq!(integer::parse("s0"), None);
    }
}
