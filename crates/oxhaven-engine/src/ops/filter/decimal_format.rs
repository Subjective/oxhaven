//! A double as Java's `new DecimalFormat("##0.################")` formats it.
//!
//! Java's formatter does not round the double's exact value. It starts from the decimal
//! digits of the binary to decimal conversion that `Double.toString` used before JDK 19,
//! and rounds those to sixteen fraction digits. That conversion is not always the shortest
//! decimal that reads back as the double, so neither the exact value nor the shortest
//! digits give Java's text.
//!
//! Nothing here comes from the JDK's source. The rules were found as a black box (task
//! 10.10): from the documentation of `DecimalFormat` and `Double.toString`, from the
//! free-format algorithm of Steele and White ("How to print floating-point numbers
//! accurately", 1990), and from what JDK 21 prints for several million doubles. They are:
//!
//! 1. A whole double from 2^52 and below 2^63 keeps the digits of its exact value, less
//!    its last one or two where the gap to the next double is 64 or more
//!    ([`whole_digits`]).
//! 2. Any other double gets free-format digits ([`free_format`]): digits of the exact value
//!    are written until what remains is less than half the gap to the next double, or
//!    the digits written and rounded up are less than half a gap above the value. Both
//!    comparisons are strict, the first digit is written without them, a mantissa of a
//!    single bit counts half the gap, and in one binade a sum that the JDK holds in 64
//!    bits wraps ([`jdk_sum_wraps`]).
//! 3. The digits are rounded half even to sixteen fraction digits, except that when they
//!    end exactly on the five, and are not the double's exact value, the side of the
//!    digits on which the value lies decides ([`Decimal::rounded`]).

use std::cmp::Ordering;

/// The fraction digits of the pattern `##0.################`.
const FRACTION_DIGITS: i32 = 16;

/// Behavior: java.text.DecimalFormat#format(double) with the pattern
/// `##0.################`, as called by
/// io.deephaven.server.table.ops.filter.FilterFactory#generateNumericConditionFilter
pub(super) fn decimal_format(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let size = value.abs();
    if size.is_infinite() {
        return format!("{sign}\u{221E}");
    }
    if size == 0.0 {
        return format!("{sign}0");
    }
    let text = formatter_digits(size).rounded(FRACTION_DIGITS).plain();
    format!("{sign}{text}")
}

/// Decimal digits standing for a double: `0.d1d2d3... * 10^point`, without leading or
/// trailing zeros (no digits at all is zero).
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Decimal {
    /// Each digit as a number from 0 to 9.
    digits: Vec<u8>,
    point: i32,
    /// The digits compared with the double's exact value: `Greater` when they were
    /// rounded up, `Less` when digits of the value were left off.
    side: Ordering,
}

impl Decimal {
    fn new(mut digits: Vec<u8>, mut point: i32, side: Ordering) -> Self {
        let leading = digits.iter().take_while(|d| **d == 0).count();
        digits.drain(..leading);
        point -= leading as i32;
        while digits.last() == Some(&0) {
            digits.pop();
        }
        Self {
            digits,
            point,
            side,
        }
    }

    /// The digits rounded to `fraction_digits` digits after the decimal point.
    ///
    /// The digit after the last one kept decides: below five down, above five up, a five
    /// with digits after it up. A five that ends the digits is a tie only if the digits
    /// are the double's exact value, and then goes to the even digit (no digit kept counts
    /// as even). Otherwise the value itself is above or below the tie and goes that way.
    fn rounded(self, fraction_digits: i32) -> Self {
        let kept = self.point + fraction_digits;
        if kept >= self.digits.len() as i32 {
            return self;
        }
        if kept < 0 {
            return Self::new(Vec::new(), 0, Ordering::Less);
        }
        let kept = kept as usize;
        let up = match (self.digits[kept].cmp(&5), self.digits.len() - kept) {
            (Ordering::Less, _) => false,
            (Ordering::Greater, _) | (Ordering::Equal, 2..) => true,
            (Ordering::Equal, _) => match self.side {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => kept > 0 && self.digits[kept - 1] % 2 == 1,
            },
        };
        let mut digits = self.digits;
        digits.truncate(kept);
        let mut point = self.point;
        if up && !add_one(&mut digits) {
            digits.insert(0, 1);
            point += 1;
        }
        Self::new(
            digits,
            point,
            if up {
                Ordering::Greater
            } else {
                Ordering::Less
            },
        )
    }

    /// The digits written without an exponent: `12.5`, `0.001`, `1200`, `0`.
    pub(super) fn plain(&self) -> String {
        let text =
            |digits: &[u8]| -> String { digits.iter().map(|d| char::from(b'0' + d)).collect() };
        let count = self.digits.len() as i32;
        if count == 0 {
            "0".to_string()
        } else if self.point <= 0 {
            format!(
                "0.{}{}",
                "0".repeat(-self.point as usize),
                text(&self.digits)
            )
        } else if self.point >= count {
            format!(
                "{}{}",
                text(&self.digits),
                "0".repeat((self.point - count) as usize)
            )
        } else {
            let (whole, fraction) = self.digits.split_at(self.point as usize);
            format!("{}.{}", text(whole), text(fraction))
        }
    }
}

/// Adds one to the last digit, carrying. False when every digit was a nine (or there was
/// none): the digits are then all zero and the carry is the caller's.
fn add_one(digits: &mut [u8]) -> bool {
    for digit in digits.iter_mut().rev() {
        if *digit == 9 {
            *digit = 0;
        } else {
            *digit += 1;
            return true;
        }
    }
    false
}

/// The digits Java's formatter starts from, for a finite positive double.
pub(super) fn formatter_digits(size: f64) -> Decimal {
    let bits = size.to_bits();
    let (biased, fraction) = ((bits >> 52) as i32, bits & ((1 << 52) - 1));
    // size = mantissa * 2^exponent
    let (mantissa, exponent) = match biased {
        0 => (fraction, -1074),
        _ => (fraction | 1 << 52, biased - 1075),
    };
    if biased != 0 && (0..DROPPED.len() as i32).contains(&exponent) {
        whole_digits(mantissa, exponent as usize)
    } else {
        free_format(mantissa, exponent)
    }
}

/// How many of the last digits of a whole double `mantissa * 2^exponent` below 2^63 the
/// formatter leaves off, by `exponent`: none while doubles are less than 64 apart, one up
/// to 256 apart, two for 512 and 1024. Observed; any rule "the largest power of ten no
/// more than the gap over c" with c between 3.2 and 5.12 gives this table.
const DROPPED: [u32; 11] = [0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2];

/// The digits of a double that is a whole number from 2^52 and below 2^63: those of the
/// number itself, rounded half up at the digits left off (no tie arises: the number is a
/// multiple of 64 where one digit goes, and of 512 where two go).
fn whole_digits(mantissa: u64, exponent: usize) -> Decimal {
    let value = mantissa << exponent;
    let dropped = DROPPED[exponent];
    let unit = 10u64.pow(dropped);
    let (mut kept, rest) = (value / unit, value % unit);
    let side = if rest == 0 {
        Ordering::Equal
    } else if 2 * rest >= unit {
        kept += 1;
        Ordering::Greater
    } else {
        Ordering::Less
    };
    let digits: Vec<u8> = kept.to_string().bytes().map(|d| d - b'0').collect();
    let point = digits.len() as i32 + dropped as i32;
    Decimal::new(digits, point, side)
}

/// Free-format digits of `mantissa * 2^exponent`, after Steele and White's (FPP)^2 with
/// the differences that JDK 21's output shows.
///
/// The value is held as `rest / scale` in units of the current digit, and half the gap
/// to the next double as `gap / scale`. Each step writes one digit and leaves the rest;
/// it stops when the rest is below the half gap (`low`: the digits so far read back as
/// the double) or within the half gap of the next digit up (`high`: so do the digits
/// rounded up). As observed:
///
/// - both tests are strict, so a decimal exactly half a gap away is not taken though it
///   reads back as the double (`1e23` gets `9999999999999999e7`, not `1e23`);
/// - the first digit written is not tested, which shows only in subnormals of a few
///   bits (the least double gets `49e-325`, not `5e-324`);
/// - a mantissa of a single bit counts half the gap on both sides, whether or not the
///   double below is closer (it is for a normal power of two, and is not for a subnormal);
/// - when both tests hold the last digit goes to the nearer decimal, and to the even
///   digit when the rest is exactly a half.
fn free_format(mantissa: u64, exponent: i32) -> Decimal {
    let narrow = u32::from(mantissa.is_power_of_two());
    // value = rest / scale and half gap = gap / scale, with scale a power of two
    let mut rest = Natural::from(mantissa).shifted(exponent.max(0) as u32 + 1 + narrow);
    let mut scale = Natural::from(1).shifted((-exponent).max(0) as u32 + 1 + narrow);
    let mut gap = Natural::from(1).shifted(exponent.max(0) as u32);

    // Bring the value into [0.1, 1) of `scale * 10^point`: first by an estimate from the
    // binary size that may be one or two short, then exactly.
    let binary_size = 64 - mantissa.leading_zeros() as i32 + exponent;
    let mut point = (binary_size * 30103).div_euclid(100_000) - 1;
    if point > 0 {
        scale.times_ten_to(point as u32);
    } else {
        rest.times_ten_to(-point as u32);
        gap.times_ten_to(-point as u32);
    }
    while rest >= scale {
        scale.times_ten_to(1);
        point += 1;
    }
    while rest.times(10) < scale {
        rest.times_ten_to(1);
        gap.times_ten_to(1);
        point -= 1;
    }
    // A value within half a gap of the next power of ten starts with a zero digit, so
    // that rounding up can reach that power.
    if rest.plus(&gap) >= scale {
        scale.times_ten_to(1);
        point += 1;
    }

    let mut digits = Vec::new();
    let (low, high) = loop {
        rest.times_ten_to(1);
        gap.times_ten_to(1);
        let mut digit = 0;
        while rest >= scale {
            rest.subtract(&scale);
            digit += 1;
        }
        let first = digits.is_empty();
        digits.push(digit);
        if first {
            continue;
        }
        let above = rest.plus(&gap);
        let low = rest < gap;
        let high = above > scale && !jdk_sum_wraps(exponent, &above, &scale);
        if low || high {
            break (low, high);
        }
    };
    let up = high
        && (!low
            || match rest.times(2).cmp(&scale) {
                Ordering::Less => false,
                Ordering::Greater => true,
                Ordering::Equal => digits[digits.len() - 1] % 2 == 1,
            });
    if up && !add_one(&mut digits) {
        digits.insert(0, 1);
        point += 1;
    }
    let side = match (up, rest.is_zero()) {
        (true, _) => Ordering::Greater,
        (false, true) => Ordering::Equal,
        (false, false) => Ordering::Less,
    };
    Decimal::new(digits, point, side)
}

/// The binary exponent of the doubles from 2^84 and below 2^85, as `mantissa * 2^32`.
const WRAPPING_EXPONENT: i32 = 32;
/// 5^13 and 5^12, whose product 5^25 is the JDK's unit for those doubles.
const FIVE_TO_25: [u32; 2] = [1_220_703_125, 244_140_625];

/// Whether the JDK's test "the digits rounded up are within half a gap" fails because its
/// sum does not fit a signed 64-bit integer.
///
/// Observed only in the binade from 2^84 (1,643 of 60,000 random doubles there, none in
/// any other binade), where every case is explained by this model: the JDK holds the rest
/// and the half gap as integers in units of `10 / 5^25` of a digit, and their sum
/// (`above`, here in units of `1 / scale`) wraps to a negative number when it passes
/// `i64::MAX`, at the seventeenth digit. The digits then stay as they are, where the
/// rule asks for them to be rounded up.
fn jdk_sum_wraps(exponent: i32, above: &Natural, scale: &Natural) -> bool {
    if exponent != WRAPPING_EXPONENT {
        return false;
    }
    // above / scale * 10 * 5^25 > i64::MAX
    let sum = above.times(10).times(FIVE_TO_25[0]).times(FIVE_TO_25[1]);
    let mut limit = scale.clone().shifted(63);
    limit.subtract(scale);
    sum > limit
}

/// A natural number of any size: 32-bit limbs, least significant first, no leading zero
/// limb. Only what the digit generation needs.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Natural(Vec<u32>);

impl Natural {
    fn from(value: u64) -> Self {
        let mut limbs = vec![value as u32, (value >> 32) as u32];
        while limbs.last() == Some(&0) {
            limbs.pop();
        }
        Self(limbs)
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    /// `self * 2^bits`.
    fn shifted(mut self, bits: u32) -> Self {
        if self.is_zero() {
            return self;
        }
        let within = bits % 32;
        if within > 0 {
            let mut carry = 0;
            for limb in &mut self.0 {
                let wide = u64::from(*limb) << within | carry;
                *limb = wide as u32;
                carry = wide >> 32;
            }
            if carry > 0 {
                self.0.push(carry as u32);
            }
        }
        self.0
            .splice(0..0, std::iter::repeat_n(0, (bits / 32) as usize));
        self
    }

    /// `self * factor`.
    fn times(&self, factor: u32) -> Self {
        let mut limbs = Vec::with_capacity(self.0.len() + 1);
        let mut carry = 0;
        for limb in &self.0 {
            let wide = u64::from(*limb) * u64::from(factor) + carry;
            limbs.push(wide as u32);
            carry = wide >> 32;
        }
        if carry > 0 {
            limbs.push(carry as u32);
        }
        if factor == 0 {
            limbs.clear();
        }
        Self(limbs)
    }

    /// Multiplies by `10^power`.
    fn times_ten_to(&mut self, power: u32) {
        for _ in 0..power / 9 {
            *self = self.times(1_000_000_000);
        }
        *self = self.times(10u32.pow(power % 9));
    }

    /// `self + other`.
    fn plus(&self, other: &Self) -> Self {
        let mut limbs = Vec::with_capacity(self.0.len().max(other.0.len()) + 1);
        let mut carry = 0;
        for at in 0..self.0.len().max(other.0.len()) {
            let wide = u64::from(self.0.get(at).copied().unwrap_or(0))
                + u64::from(other.0.get(at).copied().unwrap_or(0))
                + carry;
            limbs.push(wide as u32);
            carry = wide >> 32;
        }
        if carry > 0 {
            limbs.push(carry as u32);
        }
        Self(limbs)
    }

    /// Subtracts `other`, which is not above `self`.
    fn subtract(&mut self, other: &Self) {
        debug_assert!(&*self >= other);
        let mut borrow = 0;
        for (at, limb) in self.0.iter_mut().enumerate() {
            let take = i64::from(other.0.get(at).copied().unwrap_or(0)) + borrow;
            let left = i64::from(*limb) - take;
            *limb = left.rem_euclid(1 << 32) as u32;
            borrow = i64::from(left < 0);
        }
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }
}

impl Ord for Natural {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.iter().rev().cmp(other.0.iter().rev()))
    }
}

impl PartialOrd for Natural {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn double(bits: u64) -> f64 {
        f64::from_bits(bits)
    }

    /// Every digit the formatter has for a positive double: what Java's `DecimalFormat`
    /// prints with 340 fraction digits, which rounds nothing.
    fn every_digit(value: f64) -> String {
        formatter_digits(value).plain()
    }

    /// The same for a subnormal of the given bits, without the zeros: the digits and how
    /// many zeros follow the decimal point.
    fn subnormal(bits: u64) -> (String, usize) {
        let text = every_digit(double(bits));
        let digits = text.trim_start_matches("0.").trim_start_matches('0');
        (digits.to_string(), text.len() - 2 - digits.len())
    }

    /// `java_decimal_format.txt` holds what JDK 21 printed for doubles chosen to reach
    /// every rule of this module, and every kind of double an earlier guess at the rules
    /// got wrong: the bits, `new DecimalFormat("##0.################").format(x)`, and the
    /// format with 340 fraction digits in place of sixteen.
    #[test]
    fn doubles_format_as_the_jdk_formats_them() {
        let mut lines = 0;
        for line in include_str!("java_decimal_format.txt").lines() {
            let fields: Vec<&str> = line.split(' ').collect();
            let value = double(u64::from_str_radix(fields[0], 16).unwrap());
            assert_eq!(decimal_format(value), fields[1], "{line}");
            let unsigned = fields[2].trim_start_matches('-');
            assert_eq!(every_digit(value.abs()), unsigned, "{line}");
            lines += 1;
        }
        assert!(lines > 3500, "{lines} lines");
    }

    /// `java_formats.txt` holds the bits and JDK 21's format of 1,981 doubles: random
    /// ones of many digits, doubles from 2^53, and ties at the sixteenth fraction digit.
    #[test]
    fn the_first_sample_formats_as_the_jdk_formats_it() {
        let mut lines = 0;
        for line in include_str!("java_formats.txt").lines() {
            let (bits, text) = line.split_once(' ').unwrap();
            let value = double(u64::from_str_radix(bits, 16).unwrap());
            assert_eq!(decimal_format(value), text, "{line}");
            lines += 1;
        }
        assert_eq!(lines, 1981);
    }

    #[test]
    fn values_without_digits_and_signs() {
        assert_eq!(decimal_format(f64::NAN), "NaN");
        assert_eq!(decimal_format(f64::INFINITY), "\u{221E}");
        assert_eq!(decimal_format(f64::NEG_INFINITY), "-\u{221E}");
        assert_eq!(decimal_format(0.0), "0");
        assert_eq!(decimal_format(-0.0), "-0");
        assert_eq!(decimal_format(-1e-20), "-0");
        assert_eq!(decimal_format(-2.5), "-2.5");
    }

    #[test]
    fn ordinary_doubles_keep_their_shortest_digits() {
        assert_eq!(decimal_format(0.3), "0.3");
        assert_eq!(decimal_format(4.35), "4.35");
        assert_eq!(decimal_format(1e-5), "0.00001");
        assert_eq!(decimal_format(123456.0), "123456");
        assert_eq!(decimal_format(1.0 / 3.0), "0.3333333333333333");
        assert_eq!(decimal_format(1e22), "10000000000000000000000");
        assert_eq!(decimal_format(1e21), "1000000000000000000000");
    }

    #[test]
    fn whole_doubles_below_2_63_keep_the_digits_of_the_number() {
        // 2^53 and the next double but one: every digit.
        assert_eq!(decimal_format(9007199254740992.0), "9007199254740992");
        assert_eq!(decimal_format(18014398509481984.0), "18014398509481984");
        // Doubles 32 apart keep every digit; 64 apart lose one, rounded half up.
        assert_eq!(decimal_format(175644876539316128.0), "175644876539316128");
        assert_eq!(decimal_format(519341046697967488.0), "519341046697967490");
        assert_eq!(decimal_format(530846696713713024.0), "530846696713713020");
        // 256 apart lose one (2^60 is 1152921504606846976), 512 and 1024 apart two.
        assert_eq!(decimal_format(1152921504606846976.0), "1152921504606846980");
        assert_eq!(decimal_format(3584204650828788736.0), "3584204650828788700");
        assert_eq!(decimal_format(7412997258725693440.0), "7412997258725693400");
        // From 2^63 the digits are free-format again.
        assert_eq!(decimal_format(9223372036854775808.0), "9223372036854776000");
        assert_eq!(
            decimal_format(-9223372036854775808.0),
            "-9223372036854776000"
        );
    }

    #[test]
    fn a_decimal_exactly_half_a_gap_away_is_not_taken() {
        // 1e23 is 99999999999999991611392 and the gap is 2^24: 10^23 is 8388608 above.
        assert_eq!(decimal_format(1e23), "99999999999999990000000");
        // 50608000000000004194304, with a gap of 2^23: 50608e18 is 4194304 below.
        assert_eq!(decimal_format(5.0608e22), "50608000000000004000000");
        assert_eq!(decimal_format(2.1e22), "20999999999999998000000");
        assert_eq!(decimal_format(9.5e21), "9500000000000001000000");
    }

    #[test]
    fn the_first_digit_is_written_without_a_test() {
        // The least double is 4.94e-324: `5` alone would read back as it.
        assert_eq!(subnormal(1), ("49".to_string(), 323));
        // 2.96e-323 gets `3` through its second digit; 4.94e-323 and 8.89e-323 keep two.
        assert_eq!(subnormal(6), ("3".to_string(), 322));
        assert_eq!(subnormal(10), ("49".to_string(), 322));
        assert_eq!(subnormal(18), ("89".to_string(), 322));
        // 9.88e-324 and 9.88e-323 are within half a gap of the next power of ten.
        assert_eq!(subnormal(2), ("1".to_string(), 322));
        assert_eq!(subnormal(20), ("1".to_string(), 321));
        assert_eq!(decimal_format(double(1)), "0");
    }

    #[test]
    fn a_mantissa_of_one_bit_counts_half_the_gap() {
        // 2^-97 and 2^-83: seventeen digits where sixteen read back.
        assert_eq!(
            every_digit(double(0x3d20_0000_0000_0000)),
            "0.000000000000028421709430404007"
        );
        assert_eq!(
            every_digit(double(0x3e00_0000_0000_0000)),
            "0.00000000046566128730773926"
        );
        assert_eq!(
            decimal_format(double(0x3e00_0000_0000_0000)),
            "0.0000000004656613"
        );
        // 2^-30, where the shortest digits fit the half gap too.
        assert_eq!(
            every_digit(double(0x3e10_0000_0000_0000)),
            "0.0000000009313225746154785"
        );
        // Subnormals of one bit, whose neighbours are equally far on both sides, and one
        // of two bits between them.
        assert_eq!(subnormal(32), ("158".to_string(), 321));
        assert_eq!(subnormal(128), ("632".to_string(), 321));
        assert_eq!(subnormal(34), ("17".to_string(), 321));
    }

    #[test]
    fn a_last_digit_half_way_goes_to_the_even_digit() {
        // 2174429028029277.25, 576558572665954.75 and 886107453436175.25, exactly.
        assert_eq!(
            decimal_format(double(0x431e_e686_d738_4d75)),
            "2174429028029277.2"
        );
        assert_eq!(
            decimal_format(double(0x4300_6304_0753_e316)),
            "576558572665954.8"
        );
        assert_eq!(
            decimal_format(double(0x4309_2f47_bff5_887a)),
            "886107453436175.2"
        );
    }

    #[test]
    fn a_sum_past_64_bits_keeps_the_digit_in_the_binade_of_2_84() {
        // 26805587305293154973908992: 973908992 is within half a gap (2^31) of 10^9,
        // and nearer to it than to zero.
        assert_eq!(
            decimal_format(double(0x4536_2c4d_ccaf_cbca)),
            "26805587305293154000000000"
        );
        assert_eq!(
            decimal_format(double(0x4530_0000_8221_0000)),
            "19342822490609965000000000"
        );
        // 2^84 itself, 19342813113834066795298816, is rounded up: 795298816 is too far
        // from 10^9 for the sum to wrap.
        assert_eq!(
            decimal_format(double(0x4530_0000_0000_0000)),
            "19342813113834067000000000"
        );
        // The same mantissa as the first, a binade up: 53611174610586309947817984.
        assert_eq!(
            decimal_format(double(0x4546_2c4d_ccaf_cbca)),
            "53611174610586310000000000"
        );
    }

    #[test]
    fn a_five_that_ends_the_digits_follows_the_value() {
        // 7.4999999999999995967e-16 has the digits 75: the value is below the five.
        assert_eq!(
            decimal_format(double(0x3ccb_0587_6e5b_0120)),
            "0.0000000000000007"
        );
        // 8.5000000000000000689e-16 has the digits 85: above.
        assert_eq!(
            decimal_format(double(0x3cce_9fdd_c156_1258)),
            "0.0000000000000009"
        );
        assert_eq!(decimal_format(0.12345678901234565), "0.1234567890123456");
        // 5e-17 is below its digit, and no digit is kept.
        assert_eq!(decimal_format(5e-17), "0");
        // Exactly on the five, 1 / 2^17 and 3 / 2^17: to the even digit.
        assert_eq!(decimal_format(0.00000762939453125), "0.0000076293945312");
        assert_eq!(decimal_format(0.00002288818359375), "0.0000228881835938");
        // Past the five, with a carry through every digit, and before it.
        assert_eq!(decimal_format(6e-17), "0.0000000000000001");
        assert_eq!(
            decimal_format(0.999_999_999_999_999_9),
            "0.9999999999999999"
        );
        assert_eq!(decimal_format(double(0x3fb9_9999_9999_9999)), "0.1");
        assert_eq!(decimal_format(4e-17), "0");
        assert_eq!(decimal_format(9e-18), "0");
    }

    #[test]
    fn naturals_compare_add_and_subtract_across_limbs() {
        let big = Natural::from(u64::MAX).shifted(70);
        let one = Natural::from(1);
        assert!(big > one && one < big && big == big.clone());
        let mut sum = big.plus(&one);
        assert!(sum > big);
        sum.subtract(&one);
        assert_eq!(sum, big);
        let mut power = Natural::from(1);
        power.times_ten_to(20);
        assert_eq!(
            power,
            Natural::from(10_000_000_000).times(1_000_000_000).times(10)
        );
        let mut borrow = Natural::from(1).shifted(64);
        borrow.subtract(&one);
        assert_eq!(borrow, Natural::from(u64::MAX));
        assert!(Natural::from(0).is_zero() && Natural::from(0).shifted(5).is_zero());
    }
}
