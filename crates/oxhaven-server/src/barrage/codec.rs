//! Barrage's wire encodings of row sets, shift data and column sets.
//!
//! A row set travels as a stream of commands over the differences between consecutive range
//! boundaries: the start of a range is a positive difference, the end of a range of more
//! than one key is a negative one. The encoder makes the same choices as Java's, so the
//! bytes are the same.

use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use std::collections::BTreeSet;

const SHORT_VALUE: u8 = 0b0000_0001;
const INT_VALUE: u8 = 0b0000_0010;
const LONG_VALUE: u8 = 0b0000_0011;
const BYTE_VALUE: u8 = 0b0000_0100;
const VALUE_MASK: u8 = 0b0000_0111;

const OFFSET: u8 = 0b0000_1000;
const SHORT_ARRAY: u8 = 0b0001_0000;
const BYTE_ARRAY: u8 = 0b0001_1000;
const END: u8 = 0b0010_0000;
const CMD_MASK: u8 = 0b0011_1000;

/// The bytes are not an encoded row set, shift data or column set.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DecodeError {}

/// Behavior: `io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils#writeValue`
fn write_value(out: &mut Vec<u8>, command: u8, value: i64) {
    if value > i64::from(i32::MAX) || value < i64::from(i32::MIN) {
        out.push(command | LONG_VALUE);
        out.extend_from_slice(&value.to_le_bytes());
    } else if value > i64::from(i16::MAX) || value < i64::from(i16::MIN) {
        out.push(command | INT_VALUE);
        out.extend_from_slice(&(value as i32).to_le_bytes());
    } else if value > i64::from(i8::MAX) || value < i64::from(i8::MIN) {
        out.push(command | SHORT_VALUE);
        out.extend_from_slice(&(value as i16).to_le_bytes());
    } else {
        out.push(command | BYTE_VALUE);
        out.push(value as i8 as u8);
    }
}

/// Behavior: `ExternalizableRowSetUtils#writeShortsThenBytes`
fn write_shorts_then_bytes(out: &mut Vec<u8>, shorts: &[i16], byte_count: usize) {
    let (shorts, bytes) = shorts.split_at(shorts.len() - byte_count);
    match shorts {
        [] => {}
        [one] => write_value(out, OFFSET, i64::from(*one)),
        many => {
            write_value(out, SHORT_ARRAY, many.len() as i64);
            for value in many {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
    match bytes {
        [] => {}
        [one] => write_value(out, OFFSET, i64::from(*one)),
        many => {
            write_value(out, BYTE_ARRAY, many.len() as i64);
            out.extend(many.iter().map(|value| *value as i8 as u8));
        }
    }
}

/// Writes the differences collected so far: runs of four or more that fit a byte as byte
/// arrays, the rest as short arrays.
///
/// Behavior: `ExternalizableRowSetUtils#flushShorts`, `#shouldWriteBytes`
fn flush_shorts(out: &mut Vec<u8>, shorts: &mut Vec<i16>) {
    let mut written = 0;
    let mut trailing_bytes = 0;
    for (index, &next) in shorts.iter().enumerate() {
        if i8::try_from(next).is_ok() {
            trailing_bytes += 1;
            continue;
        }
        if trailing_bytes >= 4 {
            write_shorts_then_bytes(out, &shorts[written..index], trailing_bytes);
            written = index;
        }
        trailing_bytes = 0;
    }
    write_shorts_then_bytes(out, &shorts[written..], trailing_bytes);
    shorts.clear();
}

/// Behavior: `ExternalizableRowSetUtils#appendWithOffsetDelta`
fn append(out: &mut Vec<u8>, shorts: &mut Vec<i16>, offset: i64, value: i64, negate: bool) -> i64 {
    let delta = value - offset;
    let signed = if negate { -delta } else { delta };
    if delta >= i64::from(i16::MAX) {
        flush_shorts(out, shorts);
        write_value(out, OFFSET, signed);
    } else {
        shorts.push(signed as i16);
    }
    value
}

/// Appends the encoding of `rows` to `out`.
///
/// Behavior: `ExternalizableRowSetUtils#writeExternalCompressedDeltas`
pub fn write_row_set(out: &mut Vec<u8>, rows: &RowSet) {
    let mut offset = 0;
    let mut shorts = Vec::new();
    for range in rows.ranges() {
        offset = append(out, &mut shorts, offset, range.first(), false);
        if range.last() != range.first() {
            offset = append(out, &mut shorts, offset, range.last(), true);
        }
    }
    flush_shorts(out, &mut shorts);
    out.push(END);
}

/// The encoding of `rows`.
pub fn encode_row_set(rows: &RowSet) -> Vec<u8> {
    let mut out = Vec::new();
    write_row_set(&mut out, rows);
    out
}

/// Reads bytes off the front of an encoded value.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let Some((head, rest)) = self.0.split_first_chunk::<N>() else {
            return Err(DecodeError("row set ends in the middle of a value".into()));
        };
        self.0 = rest;
        Ok(*head)
    }

    /// Behavior: `ExternalizableRowSetUtils#readValue`
    fn value(&mut self, command: u8) -> Result<i64, DecodeError> {
        Ok(match command & VALUE_MASK {
            LONG_VALUE => i64::from_le_bytes(self.take()?),
            INT_VALUE => i64::from(i32::from_le_bytes(self.take()?)),
            SHORT_VALUE => i64::from(i16::from_le_bytes(self.take()?)),
            BYTE_VALUE => i64::from(self.take::<1>()?[0] as i8),
            _ => return Err(DecodeError(format!("Bad command: {command}"))),
        })
    }
}

/// Collects range boundaries into ranges.
#[derive(Default)]
struct Ranges {
    offset: i64,
    pending: Option<RowKey>,
    ranges: Vec<RowRange>,
}

impl Ranges {
    fn difference(&mut self, value: i64) -> Result<(), DecodeError> {
        let invalid = || DecodeError("row set has an invalid sequence of keys".into());
        let actual = self
            .offset
            .checked_add(value.checked_abs().ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        self.offset = actual;
        if value < 0 {
            // the end of the range the pending key starts
            let first = self.pending.take().ok_or_else(invalid)?;
            self.push(first, actual)
        } else {
            if let Some(key) = self.pending.replace(actual) {
                self.push(key, key)?;
            }
            Ok(())
        }
    }

    fn push(&mut self, first: RowKey, last: RowKey) -> Result<(), DecodeError> {
        let ordered = self.ranges.last().is_none_or(|prev| prev.last() < first);
        if first > last || !ordered {
            return Err(DecodeError(
                "row set has an invalid sequence of keys".into(),
            ));
        }
        self.ranges.push(RowRange::new(first, last));
        Ok(())
    }
}

/// Reads one row set off the front of `bytes`.
///
/// Java's reader accepts any byte sequence and builds whatever its builder makes of it;
/// here a sequence that is not ascending, or that ends early, is an error.
///
/// Behavior: `ExternalizableRowSetUtils#readExternalCompressedDelta`
fn read_row_set(reader: &mut Reader<'_>) -> Result<RowSet, DecodeError> {
    let mut ranges = Ranges::default();
    loop {
        let command = reader.take::<1>()?[0];
        match command & CMD_MASK {
            OFFSET => {
                let value = reader.value(command)?;
                ranges.difference(value)?;
            }
            SHORT_ARRAY => {
                for _ in 0..reader.value(command)? {
                    ranges.difference(i64::from(i16::from_le_bytes(reader.take()?)))?;
                }
            }
            BYTE_ARRAY => {
                for _ in 0..reader.value(command)? {
                    ranges.difference(i64::from(reader.take::<1>()?[0] as i8))?;
                }
            }
            END => break,
            _ => return Err(DecodeError(format!("Bad command: {command}"))),
        }
    }
    if let Some(key) = ranges.pending.take() {
        ranges.push(key, key)?;
    }
    Ok(RowSet::from_ranges(ranges.ranges))
}

/// The row set `bytes` encode. Bytes after the end marker are ignored, as in Java.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageProtoUtil#toRowSet`
pub fn decode_row_set(bytes: &[u8]) -> Result<RowSet, DecodeError> {
    read_row_set(&mut Reader(bytes))
}

/// The encoding of shift data: the row set of range starts, the one of range ends and the
/// one of the starts' destinations, one after the other.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl.RowSetShiftDataWriter`
pub fn encode_shift_data(shifted: &RowSetShiftData) -> Vec<u8> {
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut destinations = Vec::new();
    for shift in shifted.ranges() {
        let mut start = shift.first;
        if shift.delta < 0 && start < -shift.delta {
            start = -shift.delta;
        }
        starts.push(start);
        ends.push(shift.last);
        destinations.push(start + shift.delta);
    }
    let mut out = Vec::new();
    for keys in [starts, ends, destinations] {
        write_row_set(&mut out, &RowSet::from_keys(keys));
    }
    out
}

/// The shift data `bytes` encode.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl#extractIndexShiftData`
pub fn decode_shift_data(bytes: &[u8]) -> Result<RowSetShiftData, DecodeError> {
    let mut reader = Reader(bytes);
    let starts = read_row_set(&mut reader)?;
    let ends = read_row_set(&mut reader)?;
    let destinations = read_row_set(&mut reader)?;
    if starts.size() != ends.size() || starts.size() != destinations.size() {
        return Err(DecodeError("shift data row sets differ in size".into()));
    }
    let shifts = starts
        .iter()
        .zip(ends.iter())
        .zip(destinations.iter())
        .map(|((first, last), destination)| ShiftRange {
            first,
            last,
            delta: destination - first,
        })
        .collect();
    RowSetShiftData::new(shifts).map_err(|problem| DecodeError(problem.to_string()))
}

/// A set of column indices, as a client names the columns it wants. It may name columns
/// the table does not have; they have no data.
///
/// Port of the `java.util.BitSet` of columns.
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct ColumnSet(BTreeSet<usize>);

impl ColumnSet {
    /// The first `count` columns.
    pub fn all(count: usize) -> Self {
        Self((0..count).collect())
    }

    pub fn of(columns: impl IntoIterator<Item = usize>) -> Self {
        Self(columns.into_iter().collect())
    }

    pub fn contains(&self, column: usize) -> bool {
        self.0.contains(&column)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Behavior: `java.util.BitSet#cardinality`
    pub fn cardinality(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().copied()
    }

    pub fn insert(&mut self, column: usize) {
        self.0.insert(column);
    }

    /// Behavior: `java.util.BitSet#or`
    pub fn union_with(&mut self, other: &ColumnSet) {
        self.0.extend(other.0.iter().copied());
    }

    /// Behavior: `java.util.BitSet#and`
    pub fn intersect_with(&mut self, other: &ColumnSet) {
        self.0.retain(|column| other.0.contains(column));
    }

    /// The columns of `self` that are not in `other`.
    ///
    /// Behavior: `java.util.BitSet#andNot`
    pub fn minus(&self, other: &ColumnSet) -> ColumnSet {
        Self(self.0.difference(&other.0).copied().collect())
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Behavior: `java.util.BitSet#valueOf(java.nio.ByteBuffer)`
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut columns = BTreeSet::new();
        for (index, byte) in bytes.iter().enumerate() {
            for bit in (0..8).filter(|bit| byte & (1 << bit) != 0) {
                columns.insert(index * 8 + bit);
            }
        }
        Self(columns)
    }

    /// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl.BitSetWriter`
    pub fn to_bytes(&self) -> Vec<u8> {
        let bits = self.0.last().map_or(0, |last| last + 1);
        let mut bytes = vec![0u8; bits.div_ceil(8)];
        for &column in &self.0 {
            bytes[column / 8] |= 1 << (column % 8);
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(rows: &RowSet) -> Vec<u8> {
        let bytes = encode_row_set(rows);
        assert_eq!(&decode_row_set(&bytes).unwrap(), rows);
        bytes
    }

    #[test]
    fn small_row_sets_are_written_by_the_rules_of_the_java_writer() {
        // worked by hand from the Java writer; the fixture tests compare with Java's bytes
        assert_eq!(round_trip(&RowSet::empty()), [END]);
        // one key: an offset command with a byte value
        assert_eq!(round_trip(&RowSet::from_keys([0])), [0x0c, 0, END]);
        // a range: two differences that fit a byte, written as a byte array of two
        assert_eq!(
            round_trip(&RowSet::from_range(0, 4)),
            [0x1c, 2, 0, 0xfc, END]
        );
        assert_eq!(
            round_trip(&RowSet::from_range(0, 88)),
            [0x1c, 2, 0, 0xa8, END]
        );
    }

    #[test]
    fn differences_choose_the_narrowest_command() {
        // 200 does not fit a byte: one short, written as an offset with a short value
        assert_eq!(round_trip(&RowSet::from_keys([200])), [0x09, 200, 0, END]);
        // at or over Short.MAX_VALUE the difference is written at once, as an int
        assert_eq!(
            round_trip(&RowSet::from_keys([40_000])),
            [0x0a, 0x40, 0x9c, 0, 0, END]
        );
        // and over the int range as a long, negated for the end of a range
        let big = 1i64 << 40;
        let mut expected = vec![0x0c, 5, 0x0b];
        expected.extend_from_slice(&(-(big - 5)).to_le_bytes());
        expected.push(END);
        assert_eq!(round_trip(&RowSet::from_range(5, big)), expected);
    }

    #[test]
    fn shorts_and_bytes_are_grouped_as_java_groups_them() {
        // short, short, then four that fit bytes: a short array of 2 and a byte array of 4,
        // whose first is the negated end of the range 2000-2001
        let rows = RowSet::from_keys([1000, 2000, 2001, 2003, 2005, 2007]);
        assert_eq!(
            round_trip(&rows),
            [0x14, 2, 0xe8, 3, 0xe8, 3, 0x1c, 4, 0xff, 2, 2, 2, END]
        );
        // fewer than four byte-sized values between shorts stay in the short array
        let rows = RowSet::from_keys([1000, 1002, 1004, 3000]);
        assert_eq!(
            round_trip(&rows),
            [0x14, 4, 0xe8, 3, 2, 0, 2, 0, 0xcc, 7, END]
        );
    }

    #[test]
    fn random_row_sets_survive_a_round_trip() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |bound: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % bound
        };
        for _ in 0..500 {
            let mut key = 0i64;
            let mut ranges = Vec::new();
            for _ in 0..next(40) {
                let gap = match next(4) {
                    0 => next(3),
                    1 => next(300),
                    2 => next(70_000),
                    _ => next(1 << 36),
                } as i64;
                let first = key + gap + 1;
                let last = first + if next(2) == 0 { 0 } else { next(50_000) as i64 };
                ranges.push(RowRange::new(first, last));
                key = last + 1;
            }
            round_trip(&RowSet::from_ranges(ranges));
        }
    }

    #[test]
    fn malformed_row_sets_are_errors() {
        assert!(decode_row_set(&[]).is_err());
        assert!(decode_row_set(&[0x0c]).is_err());
        assert!(decode_row_set(&[0x38]).is_err());
        assert!(decode_row_set(&[0x08, 0]).is_err());
        // the end of a range with no start
        assert!(decode_row_set(&[0x0c, 0xfb, END]).is_err());
        // a short array that claims more values than there are
        assert!(decode_row_set(&[0x14, 9, 1, 0, END]).is_err());
    }

    #[test]
    fn shift_data_round_trips() {
        let shifted = RowSetShiftData::new(vec![
            ShiftRange {
                first: 4,
                last: 9,
                delta: -2,
            },
            ShiftRange {
                first: 20,
                last: 30,
                delta: 5,
            },
        ])
        .unwrap();
        assert_eq!(
            decode_shift_data(&encode_shift_data(&shifted)).unwrap(),
            shifted
        );
        assert_eq!(
            encode_shift_data(&RowSetShiftData::empty()),
            [END, END, END]
        );
    }

    #[test]
    fn a_shift_below_zero_starts_where_its_destination_is_zero() {
        let shift = |first| {
            RowSetShiftData::new(vec![ShiftRange {
                first,
                last: 9,
                delta: -3,
            }])
            .unwrap()
        };
        assert_eq!(
            decode_shift_data(&encode_shift_data(&shift(0))).unwrap(),
            shift(3)
        );
    }

    #[test]
    fn column_sets_are_little_endian_bit_sets() {
        let columns = ColumnSet::of([0, 2, 9]);
        assert_eq!(columns.to_bytes(), [0b101, 0b10]);
        assert_eq!(ColumnSet::from_bytes(&[0b101, 0b10, 0]), columns);
        assert_eq!(ColumnSet::default().to_bytes(), Vec::<u8>::new());
        assert_eq!(ColumnSet::all(3).to_bytes(), [0b111]);
    }
}
