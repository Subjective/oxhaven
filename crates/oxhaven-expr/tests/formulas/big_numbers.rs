//! Formulas over `BigDecimal` and `BigInteger` values checked against what Java computed.
//!
//! `tests/data/java_big_numbers.txt` lists the formulas: the operators of
//! `QueryLanguageFunctionUtils` with a big number beside every other type, casts, the
//! methods of both classes, their static methods, fields and constructors, and the library
//! functions called with them. `tests/data/record_java_big_numbers.py` ran each one on
//! Deephaven 42.9, as a script of the console runs it, and wrote
//! `tests/data/java_big_numbers.json` with the `toString` text of every value: a client
//! is sent a `BigDecimal` without its scale, and the text has it.
//!
//! Every formula must do here what it did in Java, its values equal as text, scale
//! included, or be refused as unsupported. The refused ones are listed in
//! `tests/data/java_big_numbers_refused.txt`, which the test compares with the compiler;
//! rerun with `OXHAVEN_WRITE_REFUSED=1` to rewrite it.

use super::recorded::{cells, ours, Ours};
use super::*;
use arrow::array::BinaryArray;
use oxhaven_column::java_values::{
    big_decimal_cell, big_decimal_parts, big_decimal_text, big_integer_cell, big_integer_text,
};
use serde_json::Value;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn data(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(file)
}

/// The cell of the `BigDecimal` whose `toString` is `text`, as `new BigDecimal(text)` reads
/// it: digits with an optional point and exponent.
fn decimal_cell(text: &str) -> Vec<u8> {
    let (mantissa, exponent) = match text.split_once(['E', 'e']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().expect("an exponent")),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let unscaled: i128 = format!("{whole}{fraction}").parse().expect("digits");
    big_decimal_cell(
        fraction.len() as i32 - exponent,
        &big_integer_cell(unscaled),
    )
}

/// The column of the fixture's Java type `java` from the text of each cell.
fn fixture_column(java: &str, texts: &[Option<&str>]) -> Column {
    fn parsed<T: std::str::FromStr>(texts: &[Option<&str>]) -> Vec<Option<T>>
    where
        T::Err: std::fmt::Debug,
    {
        texts
            .iter()
            .map(|text| text.map(|text| text.parse().expect("a number")))
            .collect()
    }
    match java {
        "java.math.BigDecimal" | "java.math.BigInteger" => {
            let cells: Vec<Option<Vec<u8>>> = texts
                .iter()
                .map(|text| {
                    text.map(|text| match java {
                        "java.math.BigDecimal" => decimal_cell(text),
                        _ => big_integer_cell(text.parse().expect("an integer")),
                    })
                })
                .collect();
            let column_type = match java {
                "java.math.BigDecimal" => ColumnType::BigDecimal,
                _ => ColumnType::BigInteger,
            };
            (column_type, Arc::new(BinaryArray::from_iter(cells)))
        }
        "int" => (
            ColumnType::Int,
            Arc::new(Int32Array::from(parsed::<i32>(texts))),
        ),
        "long" => (
            ColumnType::Long,
            Arc::new(Int64Array::from(parsed::<i64>(texts))),
        ),
        "short" => (
            ColumnType::Short,
            Arc::new(Int16Array::from(parsed::<i16>(texts))),
        ),
        "byte" => (
            ColumnType::Byte,
            Arc::new(Int8Array::from(parsed::<i8>(texts))),
        ),
        "double" => (
            ColumnType::Double,
            Arc::new(Float64Array::from(parsed::<f64>(texts))),
        ),
        "float" => (
            ColumnType::Float,
            Arc::new(Float32Array::from(parsed::<f32>(texts))),
        ),
        // A `char` is recorded as its number.
        "char" => (
            ColumnType::Char,
            Arc::new(UInt16Array::from(parsed::<u16>(texts))),
        ),
        "java.lang.Boolean" => (
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(parsed::<bool>(texts))),
        ),
        "java.lang.String" => (
            ColumnType::String,
            Arc::new(StringArray::from(texts.to_vec())),
        ),
        other => panic!("no fixture column of type {other}"),
    }
}

/// The fixture's table, of every row or of the rows from `from` on.
fn fixture(document: &Value, from: usize) -> TestTable {
    let columns: Vec<(String, Column)> = cells(&document["fixture"])
        .iter()
        .map(|column| {
            let texts: Vec<Option<&str>> =
                cells(&column["values"]).iter().map(Value::as_str).collect();
            let (column_type, values) =
                fixture_column(column["java"].as_str().expect("a type"), &texts);
            let values = values.slice(from, values.len() - from);
            let name = column["name"].as_str().expect("a name").to_string();
            (name, (column_type, values))
        })
        .collect();
    TestTable::with_previous(
        columns
            .iter()
            .map(|(name, column)| (name.as_str(), column.clone(), column.clone())),
    )
}

/// The column type of Java's type name, `None` for one no column of this crate has.
fn column_type_of(java: &str) -> Option<ColumnType> {
    Some(match java {
        "java.math.BigDecimal" => ColumnType::BigDecimal,
        "java.math.BigInteger" => ColumnType::BigInteger,
        "int" => ColumnType::Int,
        "long" => ColumnType::Long,
        "short" => ColumnType::Short,
        "byte" => ColumnType::Byte,
        "char" => ColumnType::Char,
        "double" => ColumnType::Double,
        "float" => ColumnType::Float,
        "java.lang.Boolean" => ColumnType::Boolean,
        "java.lang.String" => ColumnType::String,
        _ => return None,
    })
}

/// Whether the cell at `row` of `values` is the value whose Java text is `expected`; a
/// floating point value is compared by its bits after reading the text, which names one.
fn agrees(column_type: &ColumnType, values: &ArrayRef, row: usize, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return values.is_null(row);
    };
    if values.is_null(row) {
        return false;
    }
    let float = |text: &str| match text {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        text => text.parse().expect("a number"),
    };
    match column_type {
        ColumnType::BigDecimal => {
            let cell = values.as_binary::<i32>().value(row);
            let (scale, unscaled) = big_decimal_parts(cell).expect("a BigDecimal cell");
            big_decimal_text(scale, unscaled) == expected
        }
        ColumnType::BigInteger => {
            big_integer_text(values.as_binary::<i32>().value(row)) == expected
        }
        ColumnType::Int => values.as_primitive::<Int32Type>().value(row).to_string() == expected,
        ColumnType::Long => values.as_primitive::<Int64Type>().value(row).to_string() == expected,
        ColumnType::Short => values.as_primitive::<Int16Type>().value(row).to_string() == expected,
        ColumnType::Byte => values.as_primitive::<Int8Type>().value(row).to_string() == expected,
        ColumnType::Char => values.as_primitive::<UInt16Type>().value(row).to_string() == expected,
        ColumnType::Double => {
            let value = values.as_primitive::<Float64Type>().value(row);
            value.to_bits() == float(expected).to_bits()
        }
        ColumnType::Float => {
            let value = values.as_primitive::<Float32Type>().value(row);
            value.to_bits() == (float(expected) as f32).to_bits()
        }
        ColumnType::Boolean => values.as_boolean().value(row).to_string() == expected,
        ColumnType::String => values.as_string::<i32>().value(row) == expected,
        other => panic!("no comparison of a {other:?} column"),
    }
}

/// How Oxhaven's answer for one recorded formula relates to Java's.
enum Outcome {
    Agrees,
    Refused(String),
    Differs(String),
}

fn compare(table: &TestTable, case: &Value) -> Outcome {
    let formula = case["formula"].as_str().expect("a formula");
    let ours = ours_with_script_compiler(table, formula);
    if let Ours::Rejected(FormulaError::Unsupported(construct)) = &ours {
        return Outcome::Refused(construct.clone());
    }
    if let Some(error) = case.get("error") {
        let cause = error["cause"].as_str().unwrap_or_default();
        return match (error["when"].as_str().expect("when"), ours) {
            ("compile", Ours::Rejected(_)) | ("evaluate", Ours::Failed(_)) => Outcome::Agrees,
            (_, Ours::Rejected(error)) => Outcome::Differs(format!(
                "Java fails evaluating ({cause}); Oxhaven rejects: {error}"
            )),
            (_, Ours::Failed(failure)) => Outcome::Differs(format!(
                "Java does not compile it ({cause}); Oxhaven fails evaluating: {}",
                failure.message
            )),
            (when, Ours::Column(_)) => Outcome::Differs(format!(
                "Java fails ({when}: {cause}); Oxhaven evaluates it"
            )),
        };
    }
    let (column_type, values) = match ours {
        Ours::Column(column) => column,
        Ours::Rejected(error) => return Outcome::Differs(format!("Oxhaven rejects: {error}")),
        Ours::Failed(failure) => {
            return Outcome::Differs(format!("Oxhaven fails evaluating: {}", failure.message))
        }
    };
    let java = case["result"]["java"].as_str().expect("a type");
    if column_type_of(java).as_ref() != Some(&column_type) {
        return Outcome::Differs(format!(
            "Java returns {java}; Oxhaven returns {column_type:?}"
        ));
    }
    let expected: Vec<Option<&str>> = cells(&case["result"]["values"])
        .iter()
        .map(Value::as_str)
        .collect();
    let differing: Vec<usize> = (0..expected.len())
        .filter(|row| !agrees(&column_type, &values, *row, expected[*row]))
        .collect();
    if differing.is_empty() {
        Outcome::Agrees
    } else {
        let ours: Vec<String> = match column_type {
            ColumnType::BigDecimal | ColumnType::BigInteger => (0..values.len())
                .map(|row| match values.is_null(row) {
                    true => "null".to_string(),
                    false => {
                        let cell = values.as_binary::<i32>().value(row);
                        match big_decimal_parts(cell) {
                            Some((scale, unscaled)) if column_type == ColumnType::BigDecimal => {
                                big_decimal_text(scale, unscaled)
                            }
                            _ => big_integer_text(cell),
                        }
                    }
                })
                .collect(),
            _ => render(&values),
        };
        Outcome::Differs(format!(
            "rows {differing:?}: Java returns {expected:?}, Oxhaven {ours:?}"
        ))
    }
}

/// [`ours`] with the compiler of the console, which the recording ran as.
fn ours_with_script_compiler(table: &TestTable, formula: &str) -> Ours {
    super::recorded::ours_with(&Compiler, table, formula, false)
}

#[test]
fn every_recorded_big_number_formula_does_what_java_did_or_is_refused_by_name() {
    let text = fs::read_to_string(data("java_big_numbers.json")).expect("the recording");
    let document: Value = serde_json::from_str(&text).expect("JSON");
    let from = document["nonnull_from"].as_u64().expect("a row") as usize;
    let all = fixture(&document, 0);
    let nonnull = fixture(&document, from);

    let (mut agreed, mut refused, mut differences) = (0, String::new(), Vec::new());
    let cases = cells(&document["cases"]);
    for case in cases {
        let rows = case["rows"].as_str().expect("rows");
        let table = if rows == "nonnull" { &nonnull } else { &all };
        let formula = case["formula"].as_str().expect("a formula");
        match compare(table, case) {
            Outcome::Agrees => agreed += 1,
            Outcome::Refused(construct) => {
                let java = match case.get("error").map(|error| &error["when"]) {
                    None => "java-accepts",
                    Some(when) if when == "compile" => "java-rejects",
                    Some(_) => "java-fails",
                };
                writeln!(refused, "{java}\t{rows}\t{formula}\t{construct}").expect("a string");
            }
            Outcome::Differs(how) => differences.push(format!("{rows} {formula}: {how}")),
        }
    }
    println!(
        "{} recorded formulas: {agreed} agree with Java, {} differ, {} refused",
        cases.len(),
        differences.len(),
        refused.lines().count()
    );
    assert!(
        differences.is_empty(),
        "{} formulas differ from Java:\n{}",
        differences.len(),
        differences.join("\n")
    );
    let path = data("java_big_numbers_refused.txt");
    if std::env::var_os("OXHAVEN_WRITE_REFUSED").is_some() {
        fs::write(&path, &refused).expect("a writable list");
    }
    let listed = fs::read_to_string(&path).expect("the list of refused formulas");
    assert!(
        refused == listed,
        "the refused formulas are not those of {}; rerun with OXHAVEN_WRITE_REFUSED=1 and \
         review the change",
        path.display()
    );
    assert!(agreed > 600, "only {agreed} formulas agree");
}

/// What the old refusal said of a big number column, and what each form gives now.
#[test]
fn a_big_number_column_is_a_value_like_any_other() {
    let document: Value =
        serde_json::from_str(&fs::read_to_string(data("java_big_numbers.json")).expect("file"))
            .expect("JSON");
    let table = fixture(&document, 0);
    let Ours::Column((column_type, values)) = ours(&table, "BD", false) else {
        panic!("BD is a value");
    };
    assert_eq!(column_type, ColumnType::BigDecimal);
    assert!(agrees(&column_type, &values, 1, Some("1.50")));
    // `min` takes no object: javac finds no overload.
    assert!(matches!(
        ours(&table, "min(BD, BD2)", false),
        Ours::Rejected(FormulaError::Type(_))
    ));
}
