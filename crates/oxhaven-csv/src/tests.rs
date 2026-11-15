//! The reader against what the Java reference read, and the writer against what it wrote.
//!
//! `tests/data/recorded.json` holds a few hundred CSV texts with the table Java 42.9 made
//! of each, or the fact that it refused; `tests/data/record.py` records it.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use serde_json::Value;

use crate::cells::CellColumn;
use crate::infer::{column_values, Values};
use crate::tokens::{parse_date_time, parse_double, parse_long, DateTime};
use crate::{read, read_bytes, write, ColumnSpec, CsvError, Parser, ReadOptions};

const RECORDED: &str = include_str!("../tests/data/recorded.json");

/// The reads Java makes a table of and this crate refuses, with the Java method the
/// refusal names.
const REFUSED: [(&str, &str); 2] = [
    (
        "datetime_regions",
        "io.deephaven.csv.DeephavenTimeZoneParser#tryParse",
    ),
    (
        "double_exponent_short",
        "ch.randelshofer.fastdoubleparser.JavaDoubleParser#parseDouble",
    ),
];

fn parser_of(dtype: &str) -> Parser {
    match dtype {
        "bool_" => Parser::Boolean,
        "byte" => Parser::Byte,
        "char" => Parser::Char,
        "short" => Parser::Short,
        "int32" => Parser::Int,
        "long" => Parser::Long,
        "float32" => Parser::Float,
        "double" => Parser::Double,
        "string" => Parser::String,
        "Instant" => Parser::DateTime,
        other => panic!("no parser for dtypes.{other}"),
    }
}

/// The options `deephaven.csv.read` makes of its keyword arguments.
fn options_of(args: &Value) -> ReadOptions {
    let mut options = ReadOptions::default();
    for (name, value) in args.as_object().unwrap() {
        let flag = || value.as_bool().unwrap();
        let count = || value.as_i64().unwrap();
        let character = || value.as_str().unwrap().chars().next().unwrap();
        match name.as_str() {
            "header" => {
                for pair in value.as_array().unwrap() {
                    options.columns.push(ColumnSpec {
                        name: pair[0].as_str().unwrap().to_string(),
                        parser: parser_of(pair[1].as_str().unwrap()),
                    });
                }
            }
            "headless" => options.has_header_row = !flag(),
            "header_row" => options.skip_header_rows = count(),
            "skip_rows" => options.skip_rows = count(),
            "num_rows" => options.num_rows = count(),
            "ignore_empty_lines" => options.ignore_empty_lines = flag(),
            "allow_missing_columns" => options.allow_missing_columns = flag(),
            "ignore_excess_columns" => options.ignore_excess_columns = flag(),
            "delimiter" => options.delimiter = character(),
            "quote" => options.quote = character(),
            "ignore_surrounding_spaces" => options.ignore_surrounding_spaces = flag(),
            "trim" => options.trim = flag(),
            other => panic!("no option {other}"),
        }
    }
    options
}

/// The name Java has for a column type, and the values of a column as the recording
/// spells them: integers, characters and instants as numbers, floating point numbers as
/// the bits of the `double`.
fn column_as_recorded(column_type: &ColumnType, values: &dyn Array) -> (&'static str, Vec<Value>) {
    macro_rules! numbers {
        ($arrow:ty, $text:expr) => {{
            let text = $text;
            let values = values.as_primitive::<$arrow>().iter();
            values
                .map(|v| v.map_or(Value::Null, |v| Value::String(text(v))))
                .collect()
        }};
    }
    let bits = |value: f64| (value.to_bits() as i64).to_string();
    match column_type {
        ColumnType::Boolean => {
            let text = |v: Option<bool>| v.map_or(Value::Null, |v| Value::String(v.to_string()));
            ("Boolean", values.as_boolean().iter().map(text).collect())
        }
        ColumnType::Byte => ("byte", numbers!(Int8Type, |v: i8| v.to_string())),
        ColumnType::Short => ("short", numbers!(Int16Type, |v: i16| v.to_string())),
        ColumnType::Int => ("int", numbers!(Int32Type, |v: i32| v.to_string())),
        ColumnType::Long => ("long", numbers!(Int64Type, |v: i64| v.to_string())),
        ColumnType::Char => ("char", numbers!(UInt16Type, |v: u16| v.to_string())),
        ColumnType::Float => ("float", numbers!(Float32Type, |v: f32| bits(f64::from(v)))),
        ColumnType::Double => ("double", numbers!(Float64Type, bits)),
        ColumnType::Instant => (
            "Instant",
            numbers!(TimestampNanosecondType, |v: i64| v.to_string()),
        ),
        ColumnType::String => {
            let text = |v: Option<&str>| v.map_or(Value::Null, |v| Value::String(v.to_string()));
            (
                "String",
                values.as_string::<i32>().iter().map(text).collect(),
            )
        }
        other => panic!("a CSV column of type {other:?}"),
    }
}

/// Every recorded case: the same column names, types and values where Java made a table,
/// and a refusal that is not "not ported" where Java refused.
#[test]
fn reads_what_java_reads() {
    let cases: Vec<Value> = serde_json::from_str(RECORDED).unwrap();
    assert!(cases.len() > 500, "{} cases", cases.len());
    let mut tables = 0;
    let mut failures = 0;
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let text = case["text"].as_str().unwrap();
        let result = read_bytes(text.as_bytes(), &options_of(&case["args"]));
        if let Some((_, java)) = REFUSED.iter().find(|(refused, _)| *refused == name) {
            assert!(case.get("columns").is_some(), "{name}: Java reads it");
            match result {
                Err(CsvError::NotPorted { java: named, .. }) => assert_eq!(named, *java),
                other => panic!("{name}: expected a refusal, got {:?}", other.map(|_| ())),
            }
            continue;
        }
        let Some(columns) = case.get("columns") else {
            failures += 1;
            match result {
                Err(CsvError::Invalid(_)) => {}
                other => panic!("{name}: Java refuses it, got {:?}", other.map(|_| ())),
            }
            continue;
        };
        tables += 1;
        let table = result.unwrap_or_else(|error| panic!("{name}: {error}"));
        let recorded: Vec<(&str, &str, &Vec<Value>)> = columns
            .as_array()
            .unwrap()
            .iter()
            .map(|column| {
                let name = column["name"].as_str().unwrap();
                let kind = column["type"].as_str().unwrap();
                (name, kind, column["values"].as_array().unwrap())
            })
            .collect();
        // Java's table has the row keys `0..size` without being marked flat
        assert!(!table.is_flat(), "{name}: flat");
        let rows = table.row_set();
        assert_eq!(rows.size(), case["rows"].as_u64().unwrap(), "{name}: rows");
        let definition = table.definition();
        assert_eq!(
            definition.columns().len(),
            recorded.len(),
            "{name}: columns"
        );
        for (index, column) in definition.columns().iter().enumerate() {
            let values = table.column(index).chunk(&rows).unwrap();
            let (kind, values) = column_as_recorded(&column.column_type, &values);
            let (recorded_name, recorded_kind, recorded_values) = recorded[index];
            assert_eq!(column.name, recorded_name, "{name}: column {index}");
            assert_eq!(kind, recorded_kind, "{name}: type of {}", column.name);
            assert_eq!(
                &values, recorded_values,
                "{name}: values of {}",
                column.name
            );
        }
    }
    // the recording holds both kinds, so neither comparison is skipped
    assert!(
        tables > 450 && failures > 40,
        "{tables} tables, {failures} failures"
    );
}

fn values_of(cells: &[&str], parser: Option<Parser>) -> Result<Values, CsvError> {
    column_values(&CellColumn::of(cells), parser)
}

#[test]
fn the_first_cell_with_text_chooses_the_family() {
    let ints = |values: &[Option<i32>]| Values::Int(values.to_vec());
    assert_eq!(
        values_of(&["", "5", ""], None),
        Ok(ints(&[None, Some(5), None]))
    );
    // a null of the type is not a value of it
    let widened = Values::Long(vec![Some(1), Some(-2_147_483_648)]);
    assert_eq!(values_of(&["1", "-2147483648"], None), Ok(widened));
    let doubles = Values::Double(vec![Some(1.0), Some(3e9), Some(1.5), None]);
    assert_eq!(
        values_of(&["1", "3000000000", "1.5", ""], None),
        Ok(doubles)
    );
    // single digits can still be characters, longer numbers cannot
    let chars = Values::Char(vec![Some(49), Some(120)]);
    assert_eq!(values_of(&["1", "x"], None), Ok(chars));
    let strings =
        |values: &[&str]| Values::String(values.iter().map(|v| Some(v.to_string())).collect());
    assert_eq!(values_of(&["12", "x"], None), Ok(strings(&["12", "x"])));
    assert_eq!(values_of(&["true", "x"], None), Ok(strings(&["true", "x"])));
    assert_eq!(values_of(&["x", "true"], None), Ok(strings(&["x", "true"])));
    assert_eq!(
        values_of(&["", ""], None),
        Ok(Values::String(vec![None, None]))
    );
    assert_eq!(values_of(&[], None), Ok(Values::String(Vec::new())));
    assert_eq!(
        values_of(&[], Some(Parser::Int)),
        Ok(Values::Int(Vec::new()))
    );
}

#[test]
fn a_named_type_takes_no_other_cell() {
    let error = values_of(&["1", "", "x"], Some(Parser::Int)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Parsing failed on input, with nothing left to fall back to. Parser \
         io.deephaven.csv.parsers.IntParser successfully parsed 2 items before failure."
    );
    assert!(values_of(&["-128"], Some(Parser::Byte)).is_err());
    assert_eq!(
        values_of(&["-127"], Some(Parser::Byte)),
        Ok(Values::Byte(vec![Some(-127)]))
    );
}

#[test]
fn numbers_are_read_as_java_reads_them() {
    assert_eq!(parse_long(b" +5\t"), Some(5));
    assert_eq!(parse_long(b"-9223372036854775808"), Some(i64::MIN));
    assert_eq!(parse_long(b"9223372036854775808"), None);
    // Java's overflow check lets this one through, wrapped
    assert_eq!(
        parse_long(b"25000000000000000000"),
        Some(6_553_255_926_290_448_384)
    );
    for not_a_long in ["", "-", "+", "1_0", "1.0", "0x10", "1 2", "--1"] {
        assert_eq!(parse_long(not_a_long.as_bytes()), None, "{not_a_long}");
    }

    for (text, value) in [
        ("1.", 1.0),
        (".5", 0.5),
        ("1.e5", 1e5),
        ("1e+05", 1e5),
        ("1d", 1.0),
        ("1.0F", 1.0),
        ("\n1.5 ", 1.5),
        ("0x1p3", 8.0),
        ("0X1.8P1", 3.0),
        ("0x1p-1074", 5e-324),
        ("0x1p-1075", 0.0),
        ("0x1.8p-1075", 5e-324),
        ("0x1p1024", f64::INFINITY),
        ("0x1.fffffffffffff8p1023", f64::INFINITY),
        ("0x1.00000000000008p0", 1.0),
        ("0x1.00000000000018p0", 1.000_000_000_000_000_4),
        ("-Infinity", f64::NEG_INFINITY),
        ("1e400", f64::INFINITY),
        ("1e-99999999999", 0.0),
    ] {
        assert_eq!(parse_double(text.as_bytes()), Ok(Some(value)), "{text}");
    }
    assert!(parse_double(b"-NaN").unwrap().unwrap().is_nan());

    // what Java's parser makes of a hexadecimal number that is not what the number is
    let zeros = "0".repeat(300);
    for (text, bits) in [
        // the digits after the first sixteen are dropped, not rounded
        (
            "0x1.0000000000000800000000000000000000001p0",
            4_607_182_418_800_017_408_u64,
        ),
        ("0x123456789abcdef81p0", 4_896_033_290_900_389_087),
        ("0xde0b6b3a763ffff8p0", 4_894_218_095_696_988_160),
        ("0xde0b6b3a764000008p0", 4_912_232_494_206_470_144),
        ("0x0.000000000000000000001p0", 4_228_880_050_100_895_744),
        ("0x1.00000000000008p-1023", 2_251_799_813_685_248),
        // an exponent stops growing at 1024
        (&format!("0x0.{zeros}1p10240"), 3_796_534_485_873_328_128),
        (&format!("0x1{zeros}p-10240"), 5_399_815_953_217_224_704),
        ("0x1p10240", f64::INFINITY.to_bits()),
        ("0x1p-10750", 0),
    ] {
        let value = parse_double(text.as_bytes()).unwrap().unwrap();
        assert_eq!(value.to_bits(), bits, "{text}");
    }
    // the same for a decimal number, which Java then reads by an algorithm this crate
    // does not have
    let zeros = "0".repeat(800);
    for text in [format!("0.{zeros}1e10240"), format!("1{zeros}e-10240")] {
        let error = parse_double(text.as_bytes()).unwrap_err();
        let java = "ch.randelshofer.fastdoubleparser.JavaDoubleParser#parseDouble";
        assert!(matches!(error, CsvError::NotPorted { java: named, .. } if named == java));
    }
    assert_eq!(parse_double(b"1e10240"), Ok(Some(f64::INFINITY)));
    assert_eq!(
        parse_double(format!("0.{zeros}1e1100").as_bytes()),
        Ok(Some(1e299))
    );
    for not_a_double in [
        "",
        ".",
        "+",
        "1e",
        "1e+",
        "nan",
        "inf",
        "infinity",
        "Infinityd",
        "0x10",
        "0x1p",
        "0x.p1",
        "1_0",
        "- 1",
        "1 e5",
        "1dd",
        "NaNd",
    ] {
        assert_eq!(
            parse_double(not_a_double.as_bytes()),
            Ok(None),
            "{not_a_double}"
        );
    }
}

#[test]
fn date_times_are_read_as_java_reads_them() {
    let nanos = |text: &str| match parse_date_time(text.as_bytes()) {
        Ok(Some(DateTime::Nanos(nanos))) => Some(nanos),
        Ok(Some(DateTime::Region(_))) => panic!("{text}: a region"),
        Ok(None) => None,
        Err(error) => panic!("{text}: {error}"),
    };
    let base = 1_704_164_645_000_000_000;
    assert_eq!(nanos("2024-01-02T03:04:05Z"), Some(base));
    assert_eq!(nanos("2024-01-02 03:04:05.5Z"), Some(base + 500_000_000));
    assert_eq!(
        nanos("2024-01-02T03:04:05,123456789Z"),
        Some(base + 123_456_789)
    );
    assert_eq!(nanos("20240102T030405Z"), Some(base));
    assert_eq!(
        nanos("2024-01-02T03:04:05+01:00"),
        Some(base - 3_600_000_000_000)
    );
    assert_eq!(
        nanos("2024-01-02T03:04:05-0130"),
        Some(base + 5_400_000_000_000)
    );
    assert_eq!(nanos("2024-01-02T03:04:05 utc"), Some(base));
    assert_eq!(
        nanos("2024-01-02T03:04:05 Etc/GMT+5"),
        Some(base + 18_000_000_000_000)
    );
    assert_eq!(nanos("2024-01-02T03Z"), Some(base - 245_000_000_000));
    for not_a_date_time in [
        "2024-01-02",
        "2024-01-02T03:04:05",
        "2024-01-02T03:04:05 NY",
        "2024-01-02T03:04:05 Z",
        "2024-01-02T03:04:05.1234567891Z",
        " 2024-01-02T03:04:05Z",
        "2024-01-02T03:04:05Zjunk",
    ] {
        assert_eq!(nanos(not_a_date_time), None, "{not_a_date_time}");
    }
    let region = parse_date_time(b"2024-01-02T03:04:05 et");
    assert_eq!(region, Ok(Some(DateTime::Region("ET".to_string()))));
    // a date that does not exist is an error, even before what makes it no date-time
    for wrong in [
        "2024-13-02T03:04:05Z",
        "2023-02-29T00:00Z",
        "2024-01-02T24:00Zjunk",
    ] {
        let error = parse_date_time(wrong.as_bytes()).unwrap_err().to_string();
        assert!(
            error.starts_with("java.time.DateTimeException: Invalid"),
            "{error}"
        );
    }
}

#[test]
fn what_java_reads_from_elsewhere_is_refused_by_name() {
    let refused = |path: &str| match read(path, &ReadOptions::default()) {
        Err(CsvError::NotPorted { java, .. }) => java,
        other => panic!("{path}: {:?}", other.map(|_| ())),
    };
    let url = "io.deephaven.csv.CsvTools#readCsv(URL, CsvSpecs)";
    assert_eq!(refused("https://example.com/a.csv"), url);
    assert_eq!(refused("file://host/a.csv"), url);
    assert_eq!(
        refused("/tmp/a.csv.gz"),
        "io.deephaven.engine.util.PathUtil#open"
    );
    assert_eq!(
        refused("file:/tmp/a.tar"),
        "io.deephaven.engine.util.PathUtil#open"
    );
    let not_utf8 = read_bytes(b"A\n\xff\n", &ReadOptions::default());
    assert!(matches!(not_utf8, Err(CsvError::NotPorted { .. })));
    let missing = read("/no/such/directory/a.csv", &ReadOptions::default());
    assert!(matches!(missing, Err(CsvError::Io(_))));
}

/// A file in the system's temporary directory that is removed when the test ends.
struct TempFile(std::path::PathBuf);

impl TempFile {
    fn new(name: &str) -> Self {
        let file = format!("oxhaven-csv-{}-{name}", std::process::id());
        TempFile(std::env::temp_dir().join(file))
    }

    fn path(&self) -> &str {
        self.0.to_str().unwrap()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// The text the reference wrote for a table of these values (42.9): every type, a null
/// of each, and cells that need quotes.
const WRITTEN: &str = "B,By,C,S,I,L,F,D,Str,T\n\
    true,1,a,1,1,1,1.5,1.5,plain,2024-01-02T03:04:05Z\n\
    false,-2,\",\",-2,-2,-2,1.0E10,1.0E-5,\"q\"\"uo,te\",2024-01-02T03:04:05.123456789Z\n\
    ,,,,,,,,,\n";

fn every_type() -> ReadOptions {
    let column = |name: &str, parser| ColumnSpec {
        name: name.to_string(),
        parser,
    };
    ReadOptions {
        columns: vec![
            column("B", Parser::Boolean),
            column("By", Parser::Byte),
            column("C", Parser::Char),
            column("S", Parser::Short),
            column("I", Parser::Int),
            column("L", Parser::Long),
            column("F", Parser::Float),
            column("D", Parser::Double),
            column("Str", Parser::String),
            column("T", Parser::DateTime),
        ],
        ..ReadOptions::default()
    }
}

#[test]
fn writes_what_java_writes_and_reads_it_back() {
    let source = TempFile::new("source.csv");
    std::fs::write(source.path(), WRITTEN).unwrap();
    let table = read(&format!("file:{}", source.path()), &every_type()).unwrap();
    let written = TempFile::new("written.csv");
    write(&*table, written.path(), &[], "UTC").unwrap();
    assert_eq!(std::fs::read_to_string(written.path()).unwrap(), WRITTEN);

    let some = ["I".to_string(), "Str".to_string()];
    write(&*table, written.path(), &some, "UTC").unwrap();
    let text = std::fs::read_to_string(written.path()).unwrap();
    assert_eq!(text, "I,Str\n1,plain\n-2,\"q\"\"uo,te\"\n,\n");

    // a column that is not there: the error of Java, and the file it left empty
    let error = write(&*table, written.path(), &["Nope".to_string()], "UTC").unwrap_err();
    assert_eq!(
        error.to_string(),
        "Unknown column names [Nope], available column names are [B, By, C, S, I, L, F, D, \
         Str, T]"
    );
    assert_eq!(std::fs::read_to_string(written.path()).unwrap(), "");
    let nowhere = write(&*table, "/no/such/directory/a.csv", &[], "UTC");
    assert!(matches!(nowhere, Err(CsvError::Io(_))));

    // Java writes an instant with the offset of the machine's zone: only UTC is here
    write(&*table, written.path(), &[], "Etc/UTC").unwrap();
    assert_eq!(std::fs::read_to_string(written.path()).unwrap(), WRITTEN);
    for zone in ["America/New_York", "Etc/GMT+5", "utc", ""] {
        let refused = write(&*table, written.path(), &[], zone).unwrap_err();
        let java = "io.deephaven.time.DateTimeUtils#timeZone";
        assert!(matches!(refused, CsvError::NotPorted { java: named, .. } if named == java));
        write(&*table, written.path(), &some, zone).unwrap();
    }
}

#[test]
fn writes_a_table_without_rows_or_without_columns() {
    let written = TempFile::new("empty.csv");
    let no_rows = read_bytes(b"A,B\n", &ReadOptions::default()).unwrap();
    write(&*no_rows, written.path(), &[], "UTC").unwrap();
    assert_eq!(std::fs::read_to_string(written.path()).unwrap(), "A,B\n");
    // rows and no column: Java writes the empty header line and nothing else
    let no_columns = read_bytes(b"\n\n\n", &ReadOptions::default()).unwrap();
    assert_eq!(no_columns.row_set().size(), 2);
    write(&*no_columns, written.path(), &[], "UTC").unwrap();
    assert_eq!(std::fs::read_to_string(written.path()).unwrap(), "\n");
}
