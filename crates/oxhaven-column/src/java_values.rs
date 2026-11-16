//! The cells of the column types that are a Java object of `java.math` or `java.time`:
//! how a `BigInteger` and a `BigDecimal` are laid out in an Arrow `Binary` value
//! (`oxhaven_contracts::schema::ColumnType::BigDecimal`), and the text Java's `toString`
//! gives each of them and a `LocalDate`, `LocalTime` and `LocalDateTime`.
//!
//! Nothing here computes with the numbers beyond what the text and Java's wire layout
//! need: a division by a small number.

/// The fewest bytes that hold the two's complement integer `bytes` (most significant
/// first) with its sign: what `java.math.BigInteger#toByteArray` returns for it. Empty
/// input is not an integer and comes back empty.
pub fn minimal(bytes: &[u8]) -> &[u8] {
    let mut bytes = bytes;
    while let [first, second, ..] = bytes {
        let redundant =
            (*first == 0x00 && second & 0x80 == 0) || (*first == 0xff && second & 0x80 != 0);
        if !redundant {
            break;
        }
        bytes = &bytes[1..];
    }
    bytes
}

/// The cell of a `BigInteger` column for `value`.
pub fn big_integer_cell(value: i128) -> Vec<u8> {
    minimal(&value.to_be_bytes()).to_vec()
}

/// The cell of a `BigInteger` column for the unsigned `value`.
///
/// Behavior: `io.deephaven.parquet.base.materializers.BigIntegerFromUnsignedLongMaterializer#convertValue`
pub fn big_integer_cell_unsigned(value: u64) -> Vec<u8> {
    big_integer_cell(i128::from(value))
}

/// The cell of a `BigDecimal` column for `unscaled * 10^-scale`, `unscaled` being a two's
/// complement integer, most significant byte first, of at least one byte.
pub fn big_decimal_cell(scale: i32, unscaled: &[u8]) -> Vec<u8> {
    let unscaled = minimal(unscaled);
    let mut cell = Vec::with_capacity(4 + unscaled.len());
    cell.extend_from_slice(&scale.to_le_bytes());
    cell.extend_from_slice(unscaled);
    cell
}

/// The scale and the unscaled integer of a `BigDecimal` cell; `None` for bytes that are
/// not one.
pub fn big_decimal_parts(cell: &[u8]) -> Option<(i32, &[u8])> {
    let (scale, unscaled) = cell.split_first_chunk::<4>()?;
    (!unscaled.is_empty()).then(|| (i32::from_le_bytes(*scale), unscaled))
}

/// A two's complement integer as a sign and a magnitude in 32-bit limbs, most significant
/// first, without leading zero limbs: zero is no limb.
fn magnitude(bytes: &[u8]) -> (bool, Vec<u32>) {
    let negative = bytes.first().is_some_and(|first| first & 0x80 != 0);
    let mut bytes = bytes.to_vec();
    if negative {
        // the magnitude of a negative number: invert, add one
        let mut carry = true;
        for byte in bytes.iter_mut().rev() {
            let (inverted, overflow) = (!*byte).overflowing_add(u8::from(carry));
            *byte = inverted;
            carry = overflow;
        }
    }
    let mut limbs = Vec::with_capacity(bytes.len() / 4 + 1);
    let lead = bytes.len() % 4;
    if lead > 0 {
        let first = bytes[..lead].iter();
        limbs.push(first.fold(0u32, |limb, byte| limb << 8 | u32::from(*byte)));
    }
    for chunk in bytes[lead..].chunks_exact(4) {
        limbs.push(u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    let zeros = limbs.iter().take_while(|limb| **limb == 0).count();
    limbs.drain(..zeros);
    (negative, limbs)
}

/// Divides `limbs` by `divisor` in place and returns the remainder.
fn divide(limbs: &mut Vec<u32>, divisor: u32) -> u32 {
    let mut remainder = 0u64;
    for limb in limbs.iter_mut() {
        let dividend = remainder << 32 | u64::from(*limb);
        *limb = (dividend / u64::from(divisor)) as u32;
        remainder = dividend % u64::from(divisor);
    }
    let zeros = limbs.iter().take_while(|limb| **limb == 0).count();
    limbs.drain(..zeros);
    remainder as u32
}

/// The decimal digits of a magnitude; `0` for zero.
fn digits(mut limbs: Vec<u32>) -> String {
    let mut groups = Vec::new();
    while !limbs.is_empty() {
        groups.push(divide(&mut limbs, 1_000_000_000));
    }
    let Some(first) = groups.pop() else {
        return "0".to_string();
    };
    let mut text = first.to_string();
    for group in groups.iter().rev() {
        text.push_str(&format!("{group:09}"));
    }
    text
}

/// The two's complement bytes, in the fewest bytes, of a sign and a magnitude.
fn twos_complement(negative: bool, limbs: &[u32]) -> Vec<u8> {
    let mut bytes = vec![0u8];
    for limb in limbs {
        bytes.extend_from_slice(&limb.to_be_bytes());
    }
    if negative {
        let mut carry = true;
        for byte in bytes.iter_mut().rev() {
            let (inverted, overflow) = (!*byte).overflowing_add(u8::from(carry));
            *byte = inverted;
            carry = overflow;
        }
    }
    minimal(&bytes).to_vec()
}

/// Behavior: `java.math.BigInteger#toString()`
pub fn big_integer_text(cell: &[u8]) -> String {
    let (negative, limbs) = magnitude(cell);
    let digits = digits(limbs);
    if negative {
        format!("-{digits}")
    } else {
        digits
    }
}

/// The number of decimal digits of the unscaled integer, one for zero.
///
/// Behavior: `java.math.BigDecimal#precision`
pub fn big_decimal_precision(unscaled: &[u8]) -> usize {
    digits(magnitude(unscaled).1).len()
}

/// Behavior: `java.math.BigDecimal#toString()`: plain digits when the scale is not
/// negative and the number is not smaller than `1E-6`, scientific notation otherwise
pub fn big_decimal_text(scale: i32, unscaled: &[u8]) -> String {
    let (negative, limbs) = magnitude(unscaled);
    let coefficient = digits(limbs);
    let sign = if negative { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{coefficient}");
    }
    let length = coefficient.len() as i64;
    let adjusted = -i64::from(scale) + (length - 1);
    if scale >= 0 && adjusted >= -6 {
        let pad = i64::from(scale) - length;
        if pad >= 0 {
            let zeros = "0".repeat(pad as usize);
            format!("{sign}0.{zeros}{coefficient}")
        } else {
            let (whole, fraction) = coefficient.split_at((-pad) as usize);
            format!("{sign}{whole}.{fraction}")
        }
    } else {
        let (first, rest) = coefficient.split_at(1);
        let point = if rest.is_empty() { "" } else { "." };
        let exponent = match adjusted {
            0 => String::new(),
            positive if positive > 0 => format!("E+{positive}"),
            negative => format!("E{negative}"),
        };
        format!("{sign}{first}{point}{rest}{exponent}")
    }
}

/// The cell of the same number without the zeros at the end of its unscaled integer:
/// `1.50` (15 and scale 1 once stripped) for `1.500`, and zero with scale 0 for any zero.
/// It is what Java sends a client for a `BigDecimal`.
///
/// Behavior: `java.math.BigDecimal#stripTrailingZeros`
pub fn strip_trailing_zeros(scale: i32, unscaled: &[u8]) -> Vec<u8> {
    let (negative, mut limbs) = magnitude(unscaled);
    if limbs.is_empty() {
        return big_decimal_cell(0, &[0]);
    }
    let mut scale = scale;
    loop {
        let mut quotient = limbs.clone();
        if divide(&mut quotient, 10) != 0 || scale == i32::MIN {
            break;
        }
        limbs = quotient;
        scale -= 1;
    }
    big_decimal_cell(scale, &twos_complement(negative, &limbs))
}

/// The year, month and day of the day `days` after 1970-01-01, in the proleptic Gregorian
/// calendar.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Behavior: `java.time.LocalDate#toString`: the year in at least four digits, with a
/// `+` when it has more
pub fn local_date_text(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    let year = if year.abs() < 1000 {
        let sign = if year < 0 { "-" } else { "" };
        format!("{sign}{:04}", year.abs())
    } else if year > 9999 {
        format!("+{year}")
    } else {
        year.to_string()
    };
    format!("{year}-{month:02}-{day:02}")
}

/// Behavior: `java.time.LocalTime#toString`: the seconds only when they or the nanoseconds
/// are not zero, and the fraction in three, six or nine digits
pub fn local_time_text(nanos_of_day: i64) -> String {
    let seconds = nanos_of_day / 1_000_000_000;
    let fraction = nanos_of_day % 1_000_000_000;
    let (hour, minute, second) = (seconds / 3_600, seconds / 60 % 60, seconds % 60);
    let mut text = format!("{hour:02}:{minute:02}");
    if second > 0 || fraction > 0 {
        text.push_str(&format!(":{second:02}"));
        if fraction > 0 {
            if fraction % 1_000_000 == 0 {
                text.push_str(&format!(".{:03}", fraction / 1_000_000));
            } else if fraction % 1_000 == 0 {
                text.push_str(&format!(".{:06}", fraction / 1_000));
            } else {
                text.push_str(&format!(".{fraction:09}"));
            }
        }
    }
    text
}

/// The text of the date and time `nanos` nanoseconds after 1970-01-01T00:00.
///
/// Behavior: `java.time.LocalDateTime#toString`
pub fn local_date_time_text(nanos: i64) -> String {
    const DAY: i64 = 86_400_000_000_000;
    let date = local_date_text(nanos.div_euclid(DAY));
    let time = local_time_text(nanos.rem_euclid(DAY));
    format!("{date}T{time}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `BigDecimal` text from the decimal digits of its unscaled integer.
    fn decimal(unscaled: i128, scale: i32) -> String {
        big_decimal_text(scale, &big_integer_cell(unscaled))
    }

    #[test]
    fn an_integer_is_held_in_the_bytes_of_to_byte_array() {
        // expected: `BigInteger.valueOf(n).toByteArray()`
        assert_eq!(big_integer_cell(0), [0]);
        assert_eq!(big_integer_cell(1), [1]);
        assert_eq!(big_integer_cell(-1), [0xff]);
        assert_eq!(big_integer_cell(127), [0x7f]);
        assert_eq!(big_integer_cell(128), [0x00, 0x80]);
        assert_eq!(big_integer_cell(-128), [0x80]);
        assert_eq!(big_integer_cell(-129), [0xff, 0x7f]);
        assert_eq!(big_integer_cell(256), [0x01, 0x00]);
        // 2^64 - 1 needs a ninth byte for its sign
        let largest = big_integer_cell_unsigned(u64::MAX);
        assert_eq!(largest.len(), 9);
        assert_eq!(largest[0], 0);
        assert_eq!(minimal(&[0xff, 0xff, 0xfe]), [0xfe]);
        assert_eq!(minimal(&[0x00, 0x00, 0x7f]), [0x7f]);
        assert_eq!(minimal(&[]), [0u8; 0]);
    }

    #[test]
    fn an_integer_prints_in_decimal_whatever_its_size() {
        for value in [
            0i128,
            1,
            -1,
            999_999_999,
            1_000_000_000,
            -1_000_000_000,
            i128::from(u64::MAX),
            i128::MAX,
            i128::MIN,
            -4_294_967_296,
            1_000_000_007_000_000_000,
        ] {
            assert_eq!(
                big_integer_text(&big_integer_cell(value)),
                value.to_string()
            );
        }
        // 2^200, which no primitive holds
        let mut power = vec![1u8];
        power.extend([0u8; 25]);
        assert_eq!(
            big_integer_text(&power),
            "1606938044258990275541962092341162602522202993782792835301376"
        );
        // bytes that are not minimal are the same number
        assert_eq!(big_integer_text(&[0, 0, 0, 0, 0, 5]), "5");
        assert_eq!(
            big_integer_text(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xfb]),
            "-5"
        );
    }

    #[test]
    fn a_decimal_prints_as_java_prints_it() {
        // expected: `new BigDecimal(BigInteger.valueOf(unscaled), scale).toString()`
        assert_eq!(decimal(150, 2), "1.50");
        assert_eq!(decimal(-150, 2), "-1.50");
        assert_eq!(decimal(15, 0), "15");
        assert_eq!(decimal(0, 0), "0");
        assert_eq!(decimal(0, 2), "0.00");
        assert_eq!(decimal(5, 3), "0.005");
        assert_eq!(decimal(-5, 1), "-0.5");
        assert_eq!(decimal(123, 3), "0.123");
        assert_eq!(decimal(1, 6), "0.000001");
        // smaller than 1E-6: scientific
        assert_eq!(decimal(1, 7), "1E-7");
        assert_eq!(decimal(123, 9), "1.23E-7");
        assert_eq!(decimal(0, 7), "0E-7");
        // a negative scale: scientific
        assert_eq!(decimal(15, -1), "1.5E+2");
        assert_eq!(decimal(1, -3), "1E+3");
        assert_eq!(decimal(-123, -2), "-1.23E+4");
        assert_eq!(decimal(123_456, 2), "1234.56");
    }

    #[test]
    fn precision_counts_the_digits_of_the_unscaled_integer() {
        for (value, digits) in [
            (0i128, 1),
            (9, 1),
            (-9, 1),
            (10, 2),
            (-150, 3),
            (i128::MAX, 39),
        ] {
            assert_eq!(big_decimal_precision(&big_integer_cell(value)), digits);
        }
    }

    #[test]
    fn stripping_zeros_lowers_the_scale_and_keeps_the_number() {
        let strip = |unscaled: i128, scale: i32| {
            let cell = strip_trailing_zeros(scale, &big_integer_cell(unscaled));
            let (scale, unscaled) = big_decimal_parts(&cell).unwrap();
            (big_integer_text(unscaled), scale)
        };
        // expected: `stripTrailingZeros()` of the same number
        assert_eq!(strip(150, 2), ("15".to_string(), 1));
        assert_eq!(strip(-1500, 2), ("-15".to_string(), 0));
        assert_eq!(strip(100, 0), ("1".to_string(), -2));
        assert_eq!(strip(123, 5), ("123".to_string(), 5));
        assert_eq!(strip(0, 7), ("0".to_string(), 0));
        assert_eq!(strip(0, -3), ("0".to_string(), 0));
        // the layout Java sends: the scale, least significant byte first, then the bytes
        assert_eq!(
            strip_trailing_zeros(2, &big_integer_cell(150)),
            [1, 0, 0, 0, 15]
        );
        assert_eq!(
            big_decimal_cell(-2, &[0, 0, 1]),
            [0xfe, 0xff, 0xff, 0xff, 1]
        );
        assert_eq!(big_decimal_parts(&[1, 0, 0, 0]), None);
        assert_eq!(big_decimal_parts(&[1, 0]), None);
    }

    #[test]
    fn dates_print_with_at_least_four_digits_of_year() {
        // expected: `LocalDate.ofEpochDay(n).toString()`
        assert_eq!(local_date_text(0), "1970-01-01");
        assert_eq!(local_date_text(19_724), "2024-01-02");
        assert_eq!(local_date_text(-1), "1969-12-31");
        assert_eq!(local_date_text(19_782), "2024-02-29");
        assert_eq!(local_date_text(-719_162), "0001-01-01");
        assert_eq!(local_date_text(-719_528), "0000-01-01");
        assert_eq!(local_date_text(-719_529), "-0001-12-31");
        assert_eq!(local_date_text(2_932_896), "9999-12-31");
        assert_eq!(local_date_text(2_932_897), "+10000-01-01");
        assert_eq!(local_date_text(i64::from(i32::MIN)), "-5877641-06-23");
        assert_eq!(local_date_text(i64::from(i32::MAX)), "+5881580-07-11");
    }

    #[test]
    fn times_print_only_the_parts_that_are_not_zero() {
        // expected: `LocalTime.ofNanoOfDay(n).toString()`
        assert_eq!(local_time_text(0), "00:00");
        assert_eq!(local_time_text(3_723_000_000_000), "01:02:03");
        assert_eq!(local_time_text(3_720_000_000_000), "01:02");
        assert_eq!(local_time_text(3_720_500_000_000), "01:02:00.500");
        assert_eq!(local_time_text(1_000), "00:00:00.000001");
        assert_eq!(local_time_text(1), "00:00:00.000000001");
        assert_eq!(local_time_text(86_399_999_999_999), "23:59:59.999999999");
    }

    #[test]
    fn a_date_and_time_is_the_two_texts_around_a_t() {
        // expected: `LocalDateTime.ofEpochSecond(s, n, ZoneOffset.UTC).toString()`
        assert_eq!(local_date_time_text(0), "1970-01-01T00:00");
        assert_eq!(local_date_time_text(-1), "1969-12-31T23:59:59.999999999");
        assert_eq!(
            local_date_time_text(1_704_157_323_000_000_000),
            "2024-01-02T01:02:03"
        );
        assert_eq!(
            local_date_time_text(i64::MAX),
            "2262-04-11T23:47:16.854775807"
        );
        assert_eq!(
            local_date_time_text(i64::MIN),
            "1677-09-21T00:12:43.145224192"
        );
    }
}
