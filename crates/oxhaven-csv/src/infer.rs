//! From the cells of a column to its type and values.
//!
//! A column whose type the caller named is read with that type's parser, and a cell it
//! does not take is an error. Otherwise the type is inferred, by the rules of Java's
//! reader:
//!
//! - An empty cell is null and says nothing about the type. A column of nulls only is a
//!   `String` column.
//! - The first cell with text decides the family. A number starts the numeric family; then
//!   `true` or `false` starts booleans, and a date-time starts instants.
//! - Numeric: `int` as far as the cells are `int`s, then `long` from the first cell that
//!   is not, then `double`. The column has the last type that was needed and the earlier
//!   values are converted to it. The null of a type is not one of its values: an `int`
//!   column cannot hold `-2147483648`, which makes it a `long` column.
//! - When a cell does not fit its family, and for every other column: `char` if every
//!   cell with text is a single character, else `String`.
//!
//! Each parser carries on from the cell the one before it stopped at, and the rows above
//! are read again only by the parser that reaches the end. That is why what a cell
//! "would have been" under another type never matters.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::schema::ColumnType;

use crate::cells::CellColumn;
use crate::tokens::{self, DateTime};
use crate::{not_ported, CsvError};

/// A type a column can be read as.
///
/// Behavior: the parsers of `io.deephaven.csv.parsers.Parsers` that
/// `deephaven.csv.read` can name
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parser {
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    /// A `float`: the cell read as a `double` and narrowed (`Parsers.FLOAT_FAST`).
    Float,
    Double,
    DateTime,
    Char,
    String,
}

impl Parser {
    /// The class of Java's parser, which its error message names.
    fn java_class(self) -> &'static str {
        match self {
            Parser::Boolean => "io.deephaven.csv.parsers.BooleanAsByteParser",
            Parser::Byte => "io.deephaven.csv.parsers.ByteParser",
            Parser::Short => "io.deephaven.csv.parsers.ShortParser",
            Parser::Int => "io.deephaven.csv.parsers.IntParser",
            Parser::Long => "io.deephaven.csv.parsers.LongParser",
            Parser::Float => "io.deephaven.csv.parsers.FloatFastParser",
            Parser::Double => "io.deephaven.csv.parsers.DoubleParser",
            Parser::DateTime => "io.deephaven.csv.parsers.DateTimeAsLongParser",
            Parser::Char => "io.deephaven.csv.parsers.CharParser",
            Parser::String => "io.deephaven.csv.parsers.StringParser",
        }
    }
}

/// The values of a column, or of a run of its rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Values {
    Boolean(Vec<Option<bool>>),
    Byte(Vec<Option<i8>>),
    Short(Vec<Option<i16>>),
    Int(Vec<Option<i32>>),
    Long(Vec<Option<i64>>),
    Float(Vec<Option<f32>>),
    Double(Vec<Option<f64>>),
    DateTime(Vec<Option<DateTime>>),
    Char(Vec<Option<u16>>),
    String(Vec<Option<String>>),
}

impl Values {
    pub(crate) fn column_type(&self) -> ColumnType {
        match self {
            Values::Boolean(_) => ColumnType::Boolean,
            Values::Byte(_) => ColumnType::Byte,
            Values::Short(_) => ColumnType::Short,
            Values::Int(_) => ColumnType::Int,
            Values::Long(_) => ColumnType::Long,
            Values::Float(_) => ColumnType::Float,
            Values::Double(_) => ColumnType::Double,
            Values::DateTime(_) => ColumnType::Instant,
            Values::Char(_) => ColumnType::Char,
            Values::String(_) => ColumnType::String,
        }
    }

    /// The values as the Arrow array of their column type.
    pub(crate) fn into_array(self) -> Result<ArrayRef, CsvError> {
        Ok(match self {
            Values::Boolean(values) => Arc::new(BooleanArray::from(values)),
            Values::Byte(values) => Arc::new(Int8Array::from(values)),
            Values::Short(values) => Arc::new(Int16Array::from(values)),
            Values::Int(values) => Arc::new(Int32Array::from(values)),
            Values::Long(values) => Arc::new(Int64Array::from(values)),
            Values::Float(values) => Arc::new(Float32Array::from(values)),
            Values::Double(values) => Arc::new(Float64Array::from(values)),
            Values::Char(values) => Arc::new(UInt16Array::from(values)),
            Values::String(values) => Arc::new(StringArray::from(values)),
            Values::DateTime(values) => {
                let mut nanos = Vec::with_capacity(values.len());
                for value in values {
                    nanos.push(match value {
                        None => None,
                        Some(DateTime::Nanos(value)) => Some(value),
                        Some(DateTime::Region(zone)) => {
                            return Err(not_ported(
                                "io.deephaven.csv.DeephavenTimeZoneParser#tryParse",
                                format_args!("a date-time in the time zone {zone}"),
                            ))
                        }
                    });
                }
                Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("UTC"))
            }
        })
    }

    /// `self` followed by `other`, which is of the same type.
    fn followed_by(self, other: Values) -> Values {
        macro_rules! join {
            ($($variant:ident),*) => {
                match (self, other) {
                    $((Values::$variant(mut head), Values::$variant(tail)) => {
                        head.extend(tail);
                        Values::$variant(head)
                    })*
                    _ => unreachable!("two runs of one parser have one type"),
                }
            };
        }
        join!(Boolean, Byte, Short, Int, Long, Float, Double, DateTime, Char, String)
    }
}

/// What the parsers of one column share.
struct Column<'a> {
    cells: &'a CellColumn,
    /// Whether every cell with text read so far is one byte long or one character: only
    /// then can the column still be a `char` column.
    ///
    /// Behavior: `io.deephaven.csv.parsers.Parser.GlobalContext#isNullOrWidthOneSoFar`
    width_one: bool,
}

impl Column<'_> {
    /// Reads rows `begin..end` as `T`, stopping at the first cell `read` does not take.
    /// Gives the values and the row it stopped at, which is `end` when it took them all.
    fn run_of<T>(
        &mut self,
        begin: usize,
        end: usize,
        mut read: impl FnMut(&[u8], &mut bool) -> Result<Option<T>, CsvError>,
    ) -> Result<(Vec<Option<T>>, usize), CsvError> {
        let mut values = Vec::new();
        let mut row = begin;
        while row < end {
            let cell = self.cells.get(row);
            if cell.is_empty() {
                values.push(None);
            } else {
                match read(cell, &mut self.width_one)? {
                    Some(value) => values.push(Some(value)),
                    None => break,
                }
            }
            row += 1;
        }
        Ok((values, row))
    }

    /// Reads rows `begin..end` with `parser`, as [`Column::run_of`].
    ///
    /// Behavior: `io.deephaven.csv.parsers.Parser#tryParse` of each parser, with the
    /// reserved values of `io.deephaven.csv.CsvTools#makeMySinkFactory`: the null of each
    /// primitive type is not a value a cell can have
    fn run(
        &mut self,
        parser: Parser,
        begin: usize,
        end: usize,
    ) -> Result<(Values, usize), CsvError> {
        // a number, a date-time or a string longer than one byte rules out `char`
        fn wide(cell: &[u8], width_one: &mut bool) {
            if cell.len() > 1 {
                *width_one = false;
            }
        }
        fn integer<T: TryFrom<i64> + PartialEq>(
            null: T,
        ) -> impl FnMut(&[u8], &mut bool) -> Result<Option<T>, CsvError> {
            move |cell, width_one| {
                let value = tokens::parse_long(cell).and_then(|value| T::try_from(value).ok());
                let value = value.filter(|value| *value != null);
                if value.is_some() {
                    wide(cell, width_one);
                }
                Ok(value)
            }
        }
        Ok(match parser {
            Parser::Byte => {
                let (values, stop) = self.run_of(begin, end, integer(i8::MIN))?;
                (Values::Byte(values), stop)
            }
            Parser::Short => {
                let (values, stop) = self.run_of(begin, end, integer(i16::MIN))?;
                (Values::Short(values), stop)
            }
            Parser::Int => {
                let (values, stop) = self.run_of(begin, end, integer(i32::MIN))?;
                (Values::Int(values), stop)
            }
            Parser::Long => {
                let (values, stop) = self.run_of(begin, end, integer(i64::MIN))?;
                (Values::Long(values), stop)
            }
            Parser::Double => {
                let (values, stop) = self.run_of(begin, end, |cell, width_one| {
                    let value = tokens::parse_double(cell)?.filter(|value| *value != -f64::MAX);
                    if value.is_some() {
                        wide(cell, width_one);
                    }
                    Ok(value)
                })?;
                (Values::Double(values), stop)
            }
            Parser::Float => {
                let (values, stop) = self.run_of(begin, end, |cell, width_one| {
                    // Behavior: `io.deephaven.csv.tokenization.RangeTests#isInRangeForFloat`
                    let in_range =
                        |value: &f64| !value.is_finite() || value.abs() <= f64::from(f32::MAX);
                    let value = tokens::parse_double(cell)?
                        .filter(in_range)
                        .filter(|value| *value != f64::from(-f32::MAX));
                    if value.is_some() {
                        wide(cell, width_one);
                    }
                    Ok(value.map(|value| value as f32))
                })?;
                (Values::Float(values), stop)
            }
            Parser::Boolean => {
                let (values, stop) = self.run_of(begin, end, |cell, width_one| {
                    let value = tokens::parse_boolean(cell);
                    if value.is_some() {
                        *width_one = false;
                    }
                    Ok(value)
                })?;
                (Values::Boolean(values), stop)
            }
            Parser::DateTime => {
                let (values, stop) = self.run_of(begin, end, |cell, width_one| {
                    let value = tokens::parse_date_time(cell)?;
                    let value = value.filter(|value| *value != DateTime::Nanos(i64::MIN));
                    if value.is_some() {
                        wide(cell, width_one);
                    }
                    Ok(value)
                })?;
                (Values::DateTime(values), stop)
            }
            Parser::Char => {
                if !self.width_one {
                    return Ok((Values::Char(Vec::new()), begin));
                }
                let (values, stop) = self.run_of(begin, end, |cell, width_one| {
                    let Some(value) = tokens::parse_char(cell) else {
                        *width_one = false;
                        return Ok(None);
                    };
                    Ok((value != u16::MAX).then_some(value))
                })?;
                (Values::Char(values), stop)
            }
            Parser::String => {
                let (values, stop) = self.run_of(begin, end, |cell, _| {
                    Ok(Some(String::from_utf8_lossy(cell).into_owned()))
                })?;
                (Values::String(values), stop)
            }
        })
    }

    /// The whole column read with `parser`, which is the last one there is to try.
    ///
    /// Behavior: `io.deephaven.csv.reading.ParseDenseStorageToColumn.OnePhaseParser#onePhaseParse`
    fn one_phase(&mut self, parser: Parser) -> Result<Values, CsvError> {
        let (values, stop) = self.run(parser, 0, self.cells.len())?;
        if stop < self.cells.len() {
            return Err(CsvError::Invalid(format!(
                "Parsing failed on input, with nothing left to fall back to. Parser {} \
                 successfully parsed {stop} items before failure.",
                parser.java_class()
            )));
        }
        Ok(values)
    }

    /// The numeric family, from the first cell with text at row `first`.
    ///
    /// Behavior: `io.deephaven.csv.reading.ParseDenseStorageToColumn#parseNumerics` and
    /// `#unifyNumericResults`
    fn numerics(&mut self, first: usize) -> Result<Values, CsvError> {
        let end = self.cells.len();
        let (ints, stop) = self.run_of_ints(first, end)?;
        if stop == end {
            let nulls = std::iter::repeat_n(None, first);
            return Ok(Values::Int(nulls.chain(ints).collect()));
        }
        let (longs, stop) = self.run_of_longs(stop, end)?;
        if stop == end {
            let nulls = std::iter::repeat_n(None, first);
            let ints = ints.into_iter().map(|v| v.map(i64::from));
            return Ok(Values::Long(nulls.chain(ints).chain(longs).collect()));
        }
        let (Values::Double(doubles), stop) = self.run(Parser::Double, stop, end)? else {
            unreachable!("the double parser gives doubles")
        };
        if stop == end {
            let nulls = std::iter::repeat_n(None, first);
            let ints = ints.into_iter().map(|v| v.map(f64::from));
            // Behavior: `io.deephaven.csv.reading.TypeConverter#copyLongToDouble`, a cast
            let longs = longs.into_iter().map(|v| v.map(|v| v as f64));
            let values = nulls.chain(ints).chain(longs).chain(doubles).collect();
            return Ok(Values::Double(values));
        }
        self.others(None, stop)
    }

    fn run_of_ints(
        &mut self,
        begin: usize,
        end: usize,
    ) -> Result<(Vec<Option<i32>>, usize), CsvError> {
        match self.run(Parser::Int, begin, end)? {
            (Values::Int(values), stop) => Ok((values, stop)),
            _ => unreachable!("the int parser gives ints"),
        }
    }

    fn run_of_longs(
        &mut self,
        begin: usize,
        end: usize,
    ) -> Result<(Vec<Option<i64>>, usize), CsvError> {
        match self.run(Parser::Long, begin, end)? {
            (Values::Long(values), stop) => Ok((values, stop)),
            _ => unreachable!("the long parser gives longs"),
        }
    }

    /// What is left when the column is not numeric: `first`, the parser the first cell
    /// with text suggested, if any, then `char`, each from the row the one before stopped
    /// at, starting with `from`; and `String` for the whole column when neither reaches
    /// the end.
    ///
    /// Behavior: `io.deephaven.csv.reading.ParseDenseStorageToColumn.AFewOtherParsers#parse`
    fn others(&mut self, first: Option<Parser>, from: usize) -> Result<Values, CsvError> {
        let end = self.cells.len();
        let mut begin = from;
        for parser in first.into_iter().chain([Parser::Char]) {
            let (tail, stop) = self.run(parser, begin, end)?;
            if stop < end {
                begin = stop;
                continue;
            }
            if begin == 0 {
                return Ok(tail);
            }
            // the rows before the parser took over: nulls, or cells that a parser tried
            // earlier took and that this one takes as well
            let (head, stop) = self.run(parser, 0, begin)?;
            if stop < begin {
                return Err(CsvError::Invalid(format!(
                    "Logic error: second parser phase failed on input. Parser was: {}",
                    parser.java_class()
                )));
            }
            return Ok(head.followed_by(tail));
        }
        self.one_phase(Parser::String)
    }
}

/// The type and values of a column from its cells: read with `parser`, or inferred.
///
/// Behavior: `io.deephaven.csv.reading.ParseDenseStorageToColumn#doit` with the parsers
/// `io.deephaven.csv.parsers.Parsers#DEFAULT` when none is named, and `Parsers#STRING` for
/// a column without a value
pub(crate) fn column_values(
    cells: &CellColumn,
    parser: Option<Parser>,
) -> Result<Values, CsvError> {
    let mut column = Column {
        cells,
        width_one: true,
    };
    if let Some(parser) = parser {
        return column.one_phase(parser);
    }
    let Some(first) = (0..cells.len()).find(|row| !cells.get(*row).is_empty()) else {
        return column.one_phase(Parser::String);
    };
    let cell = cells.get(first);
    if tokens::parse_double(cell)?.is_some() {
        return column.numerics(first);
    }
    let suggested = if tokens::parse_boolean(cell).is_some() {
        Some(Parser::Boolean)
    } else if tokens::parse_date_time(cell)?.is_some() {
        Some(Parser::DateTime)
    } else {
        None
    };
    column.others(suggested, first)
}
