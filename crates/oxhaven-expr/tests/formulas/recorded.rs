//! Formulas checked against what the Java reference server answered for them.
//!
//! `tests/data/java_formulas.txt` lists formulas of the operators and
//! `tests/data/java_functions.txt` calls of the function library and its constants.
//! `tests/data/record_java_formulas.py` ran each one on Deephaven 42.9 over one input table
//! and wrote the `.json` file of the same name: the result column, or whether Java failed
//! to compile the formula or failed while evaluating it.
//!
//! Every recorded formula must do here what it did in Java, or be refused as unsupported.
//! The refused ones are listed, with the construct each names, in the `_refused.txt` file
//! of the same name, and the test fails when that list and the compiler disagree, so the
//! accepted subset cannot change unnoticed. Run the test with `OXHAVEN_WRITE_REFUSED=1` to
//! rewrite the lists.
//!
//! Results are compared exactly, except those of the functions PARITY.md A1 lets differ by
//! a tolerance.

use super::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn data(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(file)
}

/// `where` lines that Java does not treat as formulas: `WhereFilterFactory` turns them into
/// match filters before the formula parser sees them, and so must the caller of the formula
/// compiler.
const MATCH_FILTERS: [&str; 4] = [
    "I in 1, 2, 3",
    "S in `a`, `b`",
    "S not in `a`",
    "S icase in `A`",
];

fn float_cell(cell: &Value) -> Option<f64> {
    match cell {
        Value::Null => None,
        Value::String(word) => Some(match word.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            "-0.0" => -0.0,
            other => panic!("unknown float word {other}"),
        }),
        number => Some(number.as_f64().expect("a number")),
    }
}

fn int_cell<T: TryFrom<i64>>(cell: &Value) -> Option<T> {
    (!cell.is_null()).then(|| {
        let value = cell.as_i64().expect("an integer");
        T::try_from(value)
            .ok()
            .expect("a value in the type's range")
    })
}

/// A column of the recording's type `kind` from JSON cells.
pub(super) fn column_of(kind: &str, cells: &[Value]) -> Column {
    if let Some(element) = kind
        .strip_prefix("list<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        return list_column_of(element, cells);
    }
    let cells = cells.iter();
    match kind {
        "bool" => (
            ColumnType::Boolean,
            Arc::new(cells.map(Value::as_bool).collect::<BooleanArray>()),
        ),
        "int8" => (
            ColumnType::Byte,
            Arc::new(cells.map(int_cell).collect::<Int8Array>()),
        ),
        "int16" => (
            ColumnType::Short,
            Arc::new(cells.map(int_cell).collect::<Int16Array>()),
        ),
        "uint16" => (
            ColumnType::Char,
            Arc::new(cells.map(int_cell).collect::<UInt16Array>()),
        ),
        "int32" => (
            ColumnType::Int,
            Arc::new(cells.map(int_cell).collect::<Int32Array>()),
        ),
        "int64" => (
            ColumnType::Long,
            Arc::new(cells.map(int_cell).collect::<Int64Array>()),
        ),
        "float32" => (
            ColumnType::Float,
            Arc::new(
                cells
                    .map(|cell| float_cell(cell).map(|v| v as f32))
                    .collect::<Float32Array>(),
            ),
        ),
        "float64" => (
            ColumnType::Double,
            Arc::new(cells.map(float_cell).collect::<Float64Array>()),
        ),
        "string" => (
            ColumnType::String,
            Arc::new(cells.map(Value::as_str).collect::<StringArray>()),
        ),
        "timestamp" => (
            ColumnType::Instant,
            Arc::new(
                cells
                    .map(int_cell)
                    .collect::<TimestampNanosecondArray>()
                    .with_data_type(ColumnType::Instant.arrow_type()),
            ),
        ),
        other => panic!("no column of type {other}"),
    }
}

/// A column of arrays of the recording's type `element`: a JSON list per cell, or null.
fn list_column_of(element: &str, cells: &[Value]) -> Column {
    let lengths: Vec<Option<usize>> = cells
        .iter()
        .map(|cell| cell.as_array().map(Vec::len))
        .collect();
    let elements: Vec<Value> = cells
        .iter()
        .filter_map(Value::as_array)
        .flatten()
        .cloned()
        .collect();
    let (element_type, elements) = column_of(element, &elements);
    let column_type = ColumnType::array(element_type);
    let DataType::List(field) = column_type.arrow_type() else {
        unreachable!("an array is a list")
    };
    let valid: Vec<bool> = lengths.iter().map(Option::is_some).collect();
    let offsets =
        arrow::buffer::OffsetBuffer::from_lengths(lengths.iter().map(|length| length.unwrap_or(0)));
    let nulls = valid
        .contains(&false)
        .then(|| arrow::buffer::NullBuffer::from(valid));
    (
        column_type,
        Arc::new(arrow::array::ListArray::new(
            field, offsets, elements, nulls,
        )),
    )
}

/// The column type of a recorded list column, which `column_of` reads as an array: Java's
/// type name says whether it is a vector.
pub(super) fn list_type(java_type: &str, column_type: ColumnType) -> ColumnType {
    match column_type {
        ColumnType::Array(element) if java_type.starts_with("io.deephaven.vector.") => {
            ColumnType::Vector(element)
        }
        other => other,
    }
}

/// The recording's type name for a Java column type, or `None` for a type Oxhaven has no
/// column type for.
pub(super) fn kind_of(java_type: &str) -> Option<&'static str> {
    Some(match java_type {
        "java.lang.Boolean" => "bool",
        "byte" => "int8",
        "short" => "int16",
        "char" => "uint16",
        "int" => "int32",
        "long" => "int64",
        "float" => "float32",
        "double" => "float64",
        "java.lang.String" => "string",
        "java.time.Instant" => "timestamp",
        // The type of a column array (`Col_`) of a primitive column.
        "io.deephaven.vector.ByteVector" => "list<int8>",
        "io.deephaven.vector.ShortVector" => "list<int16>",
        "io.deephaven.vector.CharVector" => "list<uint16>",
        "io.deephaven.vector.IntVector" => "list<int32>",
        "io.deephaven.vector.LongVector" => "list<int64>",
        "io.deephaven.vector.FloatVector" => "list<float32>",
        "io.deephaven.vector.DoubleVector" => "list<float64>",
        // The arrays an operator on a vector or array returns, and a cast names.
        "boolean[]" => "list<bool>",
        "byte[]" => "list<int8>",
        "short[]" => "list<int16>",
        "char[]" => "list<uint16>",
        "int[]" => "list<int32>",
        "long[]" => "list<int64>",
        "float[]" => "list<float32>",
        "double[]" => "list<float64>",
        "java.lang.String[]" => "list<string>",
        _ => return None,
    })
}

pub(super) fn cells(value: &Value) -> &[Value] {
    value.as_array().expect("a list")
}

/// The table every recorded formula ran on.
fn input(document: &Value) -> TestTable {
    let columns: Vec<(&str, Column)> = cells(&document["columns"])
        .iter()
        .map(|column| {
            let name = column["name"].as_str().expect("a name");
            let kind = column["type"].as_str().expect("a type");
            let (column_type, values) = column_of(kind, cells(&column["values"]));
            let java_type = column["java"].as_str().unwrap_or_default();
            (name, (list_type(java_type, column_type), values))
        })
        .collect();
    TestTable::with_previous(
        columns
            .into_iter()
            .map(|(name, column)| (name, column.clone(), column)),
    )
}

/// How Oxhaven's answer for one formula relates to Java's.
enum Outcome {
    Agrees,
    /// Oxhaven refuses the formula as unsupported, naming this construct.
    Refused(String),
    Differs(String),
}

/// What Oxhaven does with a formula: an error from parsing or type checking, a failure of
/// the evaluation, or the result column.
pub(super) enum Ours {
    Rejected(FormulaError),
    Failed(TableFailure),
    Column(Column),
}

///
/// The recordings were made with table operations of a client, so the compiler is the one
/// those run with: Java's server refuses a client some calls that it computes in a script.
pub(super) fn ours(table: &TestTable, formula: &str, filter: bool) -> Ours {
    let compiled = oxhaven_expr::RequestCompiler
        .parse(formula)
        .and_then(|expr| oxhaven_expr::RequestCompiler.compile(&expr, &table.definition));
    let compiled = match compiled {
        Ok(compiled) => compiled,
        Err(error) => return Ours::Rejected(error),
    };
    // A filter must be boolean: `ConditionFilter` refuses any other formula with "Invalid
    // condition filter expression type: boolean required", and so must the operator that
    // asks for the formula.
    if filter && compiled.result_type() != &ColumnType::Boolean {
        return Ours::Rejected(FormulaError::Type(
            "a filter formula must be boolean".to_string(),
        ));
    }
    // An evaluation that reaches behavior that is not ported ends in `unported!`.
    let evaluated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        compiled.evaluate(table, &table.rows, false)
    }));
    let evaluated = match evaluated {
        Ok(evaluated) => evaluated,
        Err(payload) => {
            let message = match payload.downcast::<String>() {
                Ok(message) => *message,
                Err(payload) => payload.downcast::<&str>().map_or_else(
                    |_| "a panic without a message".to_string(),
                    |m| m.to_string(),
                ),
            };
            assert!(message.starts_with("unported: "), "{formula}: {message}");
            return Ours::Rejected(FormulaError::Unsupported(message));
        }
    };
    match evaluated {
        Ok(values) => {
            assert_eq!(
                values.data_type(),
                &compiled.result_type().arrow_type(),
                "declared type of {formula}"
            );
            Ours::Column((compiled.result_type().clone(), values))
        }
        Err(failure) => Ours::Failed(failure),
    }
}

fn compare(table: &TestTable, case: &Value) -> Outcome {
    let formula = case["formula"].as_str().expect("a formula");
    let filter = case["op"] == "where";
    let ours = ours(table, formula, filter);
    match &ours {
        Ours::Rejected(FormulaError::Unsupported(construct)) => {
            return Outcome::Refused(construct.clone())
        }
        // A Java string may hold half of a surrogate pair, which the client then receives
        // as `?`. An Arrow string cannot hold it, and the evaluation says so.
        Ours::Failed(failure) if failure.message.contains("unpaired UTF-16 surrogate") => {
            return Outcome::Refused(failure.message.clone())
        }
        _ => {}
    }

    if let Some(error) = case.get("error") {
        let cause = error["cause"].as_str().unwrap_or_default();
        let mut when = error["when"].as_str().expect("when");
        // The recorder takes a failure for one of compilation when its message does not
        // name the formula's evaluation. An `Instant` out of range is such a failure,
        // although it arises from the values.
        if cause.contains("DateTimeOverflowException") {
            when = "evaluate";
        }
        return match (when, ours) {
            ("compile", Ours::Rejected(_)) | ("evaluate", Ours::Failed(_)) => Outcome::Agrees,
            // Java's `where` throws for a null `Boolean`; the formula only reports it.
            ("evaluate", Ours::Column((ColumnType::Boolean, values)))
                if filter && values.null_count() > 0 =>
            {
                Outcome::Agrees
            }
            ("compile", Ours::Failed(failure)) => Outcome::Differs(format!(
                "Java does not compile it ({cause}); Oxhaven fails evaluating: {}",
                failure.message
            )),
            ("compile", Ours::Column(_)) => Outcome::Differs(format!(
                "Java does not compile it ({cause}); Oxhaven evaluates it"
            )),
            ("evaluate", Ours::Rejected(error)) => Outcome::Differs(format!(
                "Java fails evaluating ({cause}); Oxhaven rejects: {error}"
            )),
            ("evaluate", Ours::Column(_)) => Outcome::Differs(format!(
                "Java fails evaluating ({cause}); Oxhaven evaluates it"
            )),
            (when, _) => panic!("unknown failure time {when}"),
        };
    }

    let (column_type, values) = match ours {
        Ours::Column(column) => column,
        Ours::Rejected(error) => return Outcome::Differs(format!("Oxhaven rejects: {error}")),
        Ours::Failed(failure) => {
            return Outcome::Differs(format!("Oxhaven fails evaluating: {}", failure.message))
        }
    };

    if filter {
        if column_type != ColumnType::Boolean || values.null_count() > 0 {
            return Outcome::Differs(format!(
                "Oxhaven returns {column_type:?} {:?}",
                render(&values)
            ));
        }
        let kept: Vec<i64> = values
            .as_boolean()
            .iter()
            .enumerate()
            .filter_map(|(row, keep)| (keep == Some(true)).then_some(row as i64))
            .collect();
        let expected: Vec<i64> = cells(&case["kept"])
            .iter()
            .map(|row| row.as_i64().expect("a row"))
            .collect();
        return if kept == expected {
            Outcome::Agrees
        } else {
            Outcome::Differs(format!("Java keeps rows {expected:?}, Oxhaven {kept:?}"))
        };
    }

    let result = &case["result"];
    let java_type = result["java"].as_str().expect("a Java type");
    // A list column is recorded with the type of its elements, which Java's type name
    // lacks for a vector or array of objects.
    let list = result["kind"]
        .as_str()
        .filter(|kind| kind.starts_with("list<"));
    let expected = match list.or_else(|| kind_of(java_type)) {
        Some(kind) if !result["values"].is_null() => {
            let (column_type, values) = column_of(kind, cells(&result["values"]));
            (list_type(java_type, column_type), values)
        }
        _ => {
            return Outcome::Differs(format!(
                "Java returns a {java_type} column; Oxhaven returns {column_type:?}"
            ))
        }
    };
    if column_type != expected.0 {
        return Outcome::Differs(format!(
            "Java returns {java_type}; Oxhaven returns {column_type:?}"
        ));
    }
    // The clock and the random numbers have no value to compare, as in the oracle's
    // harness (`oracle/harness/formulas/tags.py`, `nondeterministic`).
    let changes = ["now(", "nowSystem(", "random"]
        .iter()
        .any(|call| formula.contains(call));
    if changes || super::tagged::same_values(formula, &expected.1, &values) {
        Outcome::Agrees
    } else {
        let (actual, expected) = (render(&values), render(&expected.1));
        Outcome::Differs(format!("Java returns {expected:?}, Oxhaven {actual:?}"))
    }
}

#[test]
fn every_recorded_formula_does_what_java_did_or_is_refused_by_name() {
    check_recording("java_formulas", 1000);
}

#[test]
fn every_recorded_function_call_does_what_java_did_or_is_refused_by_name() {
    check_recording("java_functions", 3000);
}

#[test]
fn every_formula_recorded_later_does_what_java_did_or_is_refused_by_name() {
    check_recording("java_additions", 20);
}

/// `tests/data/java_vectors.txt`, recorded by `tests/data/record_java_vectors.py`: the
/// functions and methods of vectors and arrays, over every element type, with empty and
/// null cells, null elements, NaN, infinities and values that overflow.
#[test]
fn every_recorded_vector_formula_does_what_java_did_or_is_refused_by_name() {
    check_recording("java_vectors", 500);
}

/// Compares the recording `name` with the compiler; at least `least` formulas must agree.
fn check_recording(name: &str, least: usize) {
    let text = fs::read_to_string(data(&format!("{name}.json"))).expect("the recording");
    let document: Value = serde_json::from_str(&text).expect("JSON");
    let table = input(&document);

    let mut agreed = 0;
    let mut refused = String::new();
    let mut refusals: BTreeMap<&str, usize> = BTreeMap::new();
    let mut differences = Vec::new();
    let cases = cells(&document["cases"]);
    for case in cases {
        let op = case["op"].as_str().expect("an op");
        let formula = case["formula"].as_str().expect("a formula");
        if op == "where" && MATCH_FILTERS.contains(&formula) {
            assert!(
                matches!(Compiler.parse(formula), Err(FormulaError::Syntax(_))),
                "{formula} is not a formula"
            );
            continue;
        }
        match compare(&table, case) {
            Outcome::Agrees => agreed += 1,
            Outcome::Refused(construct) => {
                let java = match case.get("error").map(|error| &error["when"]) {
                    None => "java-accepts",
                    Some(when) if when == "compile" => "java-rejects",
                    Some(_) => "java-fails",
                };
                *refusals.entry(java).or_default() += 1;
                writeln!(refused, "{java}\t{op}\t{formula}\t{construct}").expect("a string");
            }
            Outcome::Differs(how) => differences.push(format!("{op} {formula}: {how}")),
        }
    }

    println!(
        "{} recorded formulas: {agreed} agree with Java, {} differ, {} match filters skipped, \
         refused as unsupported: {refusals:?}",
        cases.len(),
        differences.len(),
        MATCH_FILTERS.len(),
    );
    assert!(
        differences.is_empty(),
        "{} formulas differ from Java:\n{}",
        differences.len(),
        differences.join("\n")
    );

    let path = data(&format!("{name}_refused.txt"));
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
    // Guards against a recording that silently lists nothing.
    assert!(agreed > least, "only {agreed} formulas agree");
}
