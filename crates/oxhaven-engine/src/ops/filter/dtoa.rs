//! The decimal digits `java.text.DecimalFormat` formats a `double` from: those of the JDK's
//! original binary to decimal conversion, which is not always the shortest decimal that
//! reads back (`Double#toString` stopped using it in JDK 19; the formatter did not).

/// The digits of a positive finite double as the conversion leaves them, with what the
/// formatter asks it about them.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct JavaDigits {
    /// Decimal digits, as ASCII; there may be zeros at either end.
    pub digits: Vec<u8>,
    /// The value is `0.digits * 10^exponent`.
    pub exponent: i32,
    /// Whether the last digit was rounded up.
    pub rounded_up: bool,
    /// Whether the digits are the exact value.
    pub exact: bool,
}

/// `N_5_BITS[i]`: the number of bits of `5^i`.
const FIVE_BITS: [i32; 27] = [
    0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56,
    59, 61,
];

/// `insignificantDigitsForPow2`: how many decimal digits of a whole number below 2^63 the
/// conversion drops, by the power of two that one unit in its last place is.
fn insignificant_digits(power: i32) -> u32 {
    const DIGITS: [u32; 64] = [
        0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8,
        9, 9, 9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15, 15,
        16, 16, 16, 17, 17, 17, 18, 18, 18, 19,
    ];
    if power > 1 && power < 64 {
        DIGITS[power as usize]
    } else {
        0
    }
}

/// Behavior: jdk.internal.math.FloatingDecimal.BinaryToASCIIBuffer#estimateDecExp, whose
/// floating point estimate of the decimal exponent the digits depend on.
// Java's literals, which are not the nearest doubles to log10(2) and its kin.
#[allow(clippy::approx_constant)]
fn estimate_decimal_exponent(fraction: u64, binary_exponent: i32) -> i32 {
    let d2 = f64::from_bits((1023u64 << 52) | (fraction & ((1 << 52) - 1)));
    let d = (d2 - 1.5) * 0.289_529_654
        + 0.176_091_259
        + f64::from(binary_exponent) * 0.301_029_995_663_981;
    let bits = d.to_bits();
    let exponent = ((bits >> 52) & 0x7FF) as i32 - 1023;
    let negative = bits >> 63 != 0;
    if (0..52).contains(&exponent) {
        let mask = ((1u64 << 52) - 1) >> exponent;
        let r = (((bits & ((1 << 52) - 1)) | (1 << 52)) >> (52 - exponent)) as i32;
        if negative {
            if mask & bits == 0 {
                -r
            } else {
                -r - 1
            }
        } else {
            r
        }
    } else if exponent < 0 {
        if bits & !(1 << 63) == 0 || !negative {
            0
        } else {
            -1
        }
    } else {
        d as i32
    }
}

/// The digits of a positive finite nonzero double.
///
/// Behavior: jdk.internal.math.FloatingDecimal#getBinaryToASCIIConverter(double) and
/// jdk.internal.math.FloatingDecimal.BinaryToASCIIBuffer#dtoa with `isCompatibleFormat`,
/// its `developLongDigits` and `roundup`, including the overflow of its `int` and `long`
/// arithmetic.
pub(super) fn java_digits(value: f64) -> JavaDigits {
    let bits = value.to_bits();
    let mut fraction = bits & ((1 << 52) - 1);
    let mut binary_exponent = ((bits >> 52) & 0x7FF) as i32;
    let significant_bits;
    if binary_exponent == 0 {
        let leading = fraction.leading_zeros() as i32;
        let shift = leading - 11;
        fraction <<= shift;
        binary_exponent = 1 - shift;
        significant_bits = 64 - leading;
    } else {
        fraction |= 1 << 52;
        significant_bits = 53;
    }
    binary_exponent -= 1023;
    let tail_zeros = fraction.trailing_zeros() as i32;
    let fraction_bits = 53 - tail_zeros;
    let tiny_bits = (fraction_bits - binary_exponent - 1).max(0);
    if tiny_bits == 0 && (-21..=62).contains(&binary_exponent) {
        // a whole number below 2^63
        let insignificant = if binary_exponent > significant_bits {
            insignificant_digits(binary_exponent - significant_bits - 1)
        } else {
            0
        };
        let whole = if binary_exponent >= 52 {
            fraction << (binary_exponent - 52)
        } else {
            fraction >> (52 - binary_exponent)
        };
        let unit = 10u64.pow(insignificant);
        let mut kept = whole / unit;
        if insignificant > 0 && whole % unit >= unit >> 1 {
            kept += 1;
        }
        let text = kept.to_string();
        return JavaDigits {
            digits: text.trim_end_matches('0').as_bytes().to_vec(),
            exponent: text.len() as i32 + insignificant as i32,
            rounded_up: false,
            exact: false,
        };
    }
    let mut decimal_exponent = estimate_decimal_exponent(fraction, binary_exponent);
    let b5 = (-decimal_exponent).max(0);
    let mut b2 = b5 + tiny_bits + binary_exponent;
    let s5 = decimal_exponent.max(0);
    let mut s2 = s5 + tiny_bits;
    let m5 = b5;
    let mut m2 = b2 - significant_bits;
    let fraction = fraction >> tail_zeros;
    b2 -= fraction_bits - 1;
    let common = b2.min(s2);
    b2 -= common;
    s2 -= common;
    m2 -= common;
    if fraction_bits == 1 {
        m2 -= 1;
    }
    if m2 < 0 {
        b2 -= m2;
        s2 -= m2;
        m2 = 0;
    }
    let five_bits = |n: i32| FIVE_BITS.get(n as usize).copied().unwrap_or(n * 3);
    let b_bits = fraction_bits + b2 + five_bits(b5);
    let ten_s_bits = s2 + 1 + five_bits(s5 + 1);
    let mut digits = Vec::new();
    let (low, high, low_digit_difference, exact);
    if b_bits < 64 && ten_s_bits < 64 {
        // Java's `int` arithmetic when both fit in 32 bits, else `long`, overflow and all.
        let narrow = b_bits < 32 && ten_s_bits < 32;
        let wrap = |x: i128| {
            if narrow {
                i128::from(x as i32)
            } else {
                i128::from(x as i64)
            }
        };
        let width = if narrow { 32 } else { 64 };
        let shl = |x: i128, by: i32| wrap(x << (by & (width - 1)));
        let pow5 = |n: i32| wrap(5i128.pow(n as u32));
        let mut b = shl(wrap(wrap(i128::from(fraction)) * pow5(b5)), b2);
        let s = shl(pow5(s5), s2);
        let mut m = shl(pow5(m5), m2);
        let tens = wrap(s * 10);
        let mut q = b / s;
        b = wrap(10 * (b % s));
        m = wrap(m * 10);
        let mut is_low = b < m;
        let mut is_high = wrap(b + m) > tens;
        if q == 0 && !is_high {
            decimal_exponent -= 1;
        } else {
            digits.push(b'0'.wrapping_add(q as u8));
        }
        if !(-3..8).contains(&decimal_exponent) {
            is_low = false;
            is_high = false;
        }
        while !is_low && !is_high {
            q = b / s;
            b = wrap(10 * (b % s));
            m = wrap(m * 10);
            if m > 0 {
                is_low = b < m;
                is_high = wrap(b + m) > tens;
            } else {
                is_low = true;
                is_high = true;
            }
            digits.push(b'0'.wrapping_add(q as u8));
        }
        low = is_low;
        high = is_high;
        low_digit_difference = wrap(wrap(b << 1) - tens).signum() as i32;
        exact = b == 0;
    } else {
        // Exact arithmetic; here `high` holds when B + M reaches 10 S.
        let mut b = Big::of(fraction).times_power(5, b5).shifted(b2);
        let s = Big::of(1).times_power(5, s5).shifted(s2);
        let mut m = Big::of(1).times_power(5, m5).shifted(m2).times(10);
        let ten_s = s.times(10);
        let mut q = b.digit_step(&s);
        let mut is_low = b < m;
        let mut is_high = ten_s <= b.plus(&m);
        if q == 0 && !is_high {
            decimal_exponent -= 1;
        } else {
            digits.push(b'0' + q);
        }
        if !(-3..8).contains(&decimal_exponent) {
            is_low = false;
            is_high = false;
        }
        while !is_low && !is_high {
            q = b.digit_step(&s);
            m = m.times(10);
            is_low = b < m;
            is_high = ten_s <= b.plus(&m);
            digits.push(b'0' + q);
        }
        low = is_low;
        high = is_high;
        low_digit_difference = if high && low {
            b = b.shifted(1);
            b.cmp(&ten_s) as i32
        } else {
            0
        };
        exact = b.is_zero();
    }
    let mut result = JavaDigits {
        digits,
        exponent: decimal_exponent + 1,
        rounded_up: false,
        exact,
    };
    let odd = result.digits.last().is_some_and(|d| d & 1 == 1);
    if high && (!low || low_digit_difference > 0 || (low_digit_difference == 0 && odd)) {
        result.round_up();
    }
    result
}

impl JavaDigits {
    /// Behavior: jdk.internal.math.FloatingDecimal.BinaryToASCIIBuffer#roundup, which marks
    /// the digits rounded up unless every digit was a nine.
    fn round_up(&mut self) {
        let mut at = self.digits.len() - 1;
        while self.digits[at] == b'9' && at > 0 {
            self.digits[at] = b'0';
            at -= 1;
        }
        if self.digits[at] == b'9' {
            self.exponent += 1;
            self.digits[0] = b'1';
            return;
        }
        self.digits[at] += 1;
        self.rounded_up = true;
    }

    /// The text `toJavaFormatString` gives the digits, which the formatter reads back.
    ///
    /// Behavior: jdk.internal.math.FloatingDecimal.BinaryToASCIIBuffer#getChars
    fn java_format(&self) -> String {
        let text = |digits: &[u8]| String::from_utf8_lossy(digits).into_owned();
        let (n, e) = (self.digits.len() as i32, self.exponent);
        if e > 0 && e < 8 {
            let shown = n.min(e) as usize;
            let mut out = text(&self.digits[..shown]);
            if (shown as i32) < e {
                out.push_str(&"0".repeat((e - shown as i32) as usize));
                out.push_str(".0");
            } else if (shown as i32) < n {
                out.push('.');
                out.push_str(&text(&self.digits[shown..]));
            } else {
                out.push_str(".0");
            }
            out
        } else if e <= 0 && e > -3 {
            format!("0.{}{}", "0".repeat((-e) as usize), text(&self.digits))
        } else {
            let rest = if n > 1 {
                text(&self.digits[1..])
            } else {
                "0".to_string()
            };
            format!("{}.{rest}E{}", char::from(self.digits[0]), e - 1)
        }
    }
}

/// A positive finite double formatted with at most `fraction_digits` fraction digits,
/// half even, as `java.text.DecimalFormat` does with a pattern like `##0.####`: the integer
/// digits (`0` for none) and the fraction digits without trailing zeros; and whether the
/// result is zero.
///
/// Behavior: java.text.DigitList#set(boolean, double, int, boolean) with `fixedPoint`, its
/// `round` and `shouldRoundUp` in `HALF_EVEN`, and java.text.DecimalFormat#subformat
pub(super) fn format_fixed(value: f64, fraction_digits: i32) -> String {
    let converted = java_digits(value);
    // DigitList#set reads the text back: the digits from the first that is not zero, and
    // where the point is.
    let text = converted.java_format();
    let (mantissa, exponent) = match text.split_once('E') {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().unwrap_or(0)),
        None => (text.as_str(), 0),
    };
    let mut digits: Vec<u8> = Vec::new();
    let mut point: Option<i32> = None;
    let mut zeros_after_point = 0;
    for c in mantissa.bytes() {
        if c == b'.' {
            point = Some(digits.len() as i32);
        } else if digits.is_empty() && c == b'0' {
            if point.is_some() {
                zeros_after_point += 1;
            }
        } else {
            digits.push(c);
        }
    }
    let mut decimal_at = point.unwrap_or(digits.len() as i32);
    if !digits.is_empty() {
        decimal_at += exponent - zeros_after_point;
    }
    let should_round_up = |digits: &[u8], at: usize| -> bool {
        match digits[at] {
            b'6'..=b'9' => true,
            b'5' if at == digits.len() - 1 => {
                if converted.rounded_up {
                    false
                } else if !converted.exact {
                    true
                } else {
                    at > 0 && digits[at - 1] % 2 == 1
                }
            }
            b'5' => digits[at + 1..].iter().any(|d| *d != b'0'),
            _ => false,
        }
    };
    if -decimal_at > fraction_digits {
        digits.clear();
    } else if -decimal_at == fraction_digits {
        if !digits.is_empty() && should_round_up(&digits, 0) {
            digits = vec![b'1'];
            decimal_at += 1;
        } else {
            digits.clear();
        }
    } else {
        while digits.len() > 1 && digits.last() == Some(&b'0') {
            digits.pop();
        }
        let maximum = fraction_digits + decimal_at;
        if maximum >= 0 && (maximum as usize) < digits.len() {
            let mut count = maximum as usize;
            if should_round_up(&digits, count) {
                loop {
                    if count == 0 {
                        digits[0] = b'1';
                        decimal_at += 1;
                        count = 1;
                        break;
                    }
                    count -= 1;
                    digits[count] += 1;
                    if digits[count] <= b'9' {
                        count += 1;
                        break;
                    }
                }
            }
            digits.truncate(count);
            while digits.len() > 1 && digits.last() == Some(&b'0') {
                digits.pop();
            }
        }
    }
    if digits.is_empty() {
        return "0".to_string();
    }
    let digit = |at: i32| -> char {
        if at >= 0 && (at as usize) < digits.len() {
            char::from(digits[at as usize])
        } else {
            '0'
        }
    };
    let mut out: String = if decimal_at > 0 {
        (0..decimal_at).map(digit).collect()
    } else {
        "0".to_string()
    };
    if (digits.len() as i32) > decimal_at {
        out.push('.');
        out.extend((decimal_at.min(0)..0).map(|_| '0'));
        out.extend((decimal_at.max(0)..digits.len() as i32).map(digit));
    }
    out
}

/// An unsigned integer of any size, for the exact arithmetic of the conversion.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Big(Vec<u32>);

impl Big {
    fn of(value: u64) -> Big {
        let mut big = Big(vec![value as u32, (value >> 32) as u32]);
        big.trim();
        big
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn times(&self, factor: u32) -> Big {
        let mut carry = 0u64;
        let mut out: Vec<u32> = self
            .0
            .iter()
            .map(|limb| {
                let product = u64::from(*limb) * u64::from(factor) + carry;
                carry = product >> 32;
                product as u32
            })
            .collect();
        if carry > 0 {
            out.push(carry as u32);
        }
        let mut big = Big(out);
        big.trim();
        big
    }

    fn times_power(&self, base: u32, power: i32) -> Big {
        (0..power).fold(self.clone(), |big, _| big.times(base))
    }

    fn shifted(&self, by: i32) -> Big {
        let (words, bits) = ((by / 32) as usize, by % 32);
        let mut out = vec![0u32; words];
        let mut carry = 0u32;
        for limb in &self.0 {
            out.push((limb << bits) | carry);
            carry = if bits == 0 { 0 } else { limb >> (32 - bits) };
        }
        out.push(carry);
        let mut big = Big(out);
        big.trim();
        big
    }

    fn plus(&self, other: &Big) -> Big {
        let mut out = Vec::new();
        let mut carry = 0u64;
        for at in 0..self.0.len().max(other.0.len()) {
            let sum = u64::from(*self.0.get(at).unwrap_or(&0))
                + u64::from(*other.0.get(at).unwrap_or(&0))
                + carry;
            out.push(sum as u32);
            carry = sum >> 32;
        }
        out.push(carry as u32);
        let mut big = Big(out);
        big.trim();
        big
    }

    /// `self - other`, for `other <= self`.
    fn minus(&self, other: &Big) -> Big {
        let mut out = Vec::new();
        let mut borrow = 0i64;
        for at in 0..self.0.len() {
            let mut difference =
                i64::from(self.0[at]) - i64::from(*other.0.get(at).unwrap_or(&0)) - borrow;
            borrow = 0;
            if difference < 0 {
                difference += 1 << 32;
                borrow = 1;
            }
            out.push(difference as u32);
        }
        let mut big = Big(out);
        big.trim();
        big
    }

    /// Behavior: jdk.internal.math.FDBigInteger#quoRemIteration: `q = self / divisor`,
    /// `self = 10 * (self % divisor)`, for a quotient below ten.
    fn digit_step(&mut self, divisor: &Big) -> u8 {
        let mut q = 0;
        while *self >= *divisor {
            *self = self.minus(divisor);
            q += 1;
        }
        *self = self.times(10);
        q
    }
}

impl PartialOrd for Big {
    fn partial_cmp(&self, other: &Big) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Big {
    fn cmp(&self, other: &Big) -> std::cmp::Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.iter().rev().cmp(other.0.iter().rev()))
    }
}
