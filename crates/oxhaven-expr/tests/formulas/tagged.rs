//! Every case of the formula corpus, `oracle/corpus/formulas`, run through the compiler and
//! the evaluator and compared with the outcome recorded from the Java reference.
//!
//! `oracle/README.md`, "Formula corpus", describes the files. A case does here what it did in
//! Java, or it is refused as unsupported. The refused cases are listed, each with the
//! construct it names, in `tests/data/corpus_refused.txt`, and the test fails when that list
//! and the compiler disagree. Run the test with `OXHAVEN_WRITE_REFUSED=1` to rewrite the
//! list.
//!
//! Two kinds of case are not formulas and are not run: a `where` case that Java turns into a
//! match or range filter, which is the where operator's to implement (PARITY.md, "Filters";
//! [`is_filter_syntax`] decides), and a case whose setup fails in Java, which records nothing
//! to compare with.

use super::recorded::{cells, column_of, kind_of, ours, Ours};
use super::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle/corpus/formulas")
}

fn refused_list() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/corpus_refused.txt")
}

/// Why a case stops before its formula is compared.
enum Stop {
    /// Something the compiler or this test does not support, by name.
    Refused(String),
    /// The setup does not do here what it did in Java.
    Differs(String),
}

/// A table of `size` rows with these columns.
fn table(columns: Vec<(String, Column)>, size: usize) -> TestTable {
    let definition = TableDefinition::new(
        columns
            .iter()
            .map(|(name, column)| ColumnDefinition::new(name.as_str(), column.0.clone()))
            .collect(),
    )
    .expect("distinct column names");
    let columns = columns
        .into_iter()
        .map(|(_, (column_type, values))| {
            assert_eq!(values.len(), size, "columns have one value per row");
            Arc::new(TestColumn {
                column_type,
                current: values.clone(),
                previous: values,
            }) as Arc<dyn ColumnSource>
        })
        .collect();
    TestTable {
        definition,
        columns,
        rows: Arc::new(RowSet::flat(size as u64)),
    }
}

/// The columns of a table, by name, as they are read through the column sources.
fn columns_of(table: &TestTable) -> Vec<(String, Column)> {
    table
        .definition
        .columns()
        .iter()
        .zip(&table.columns)
        .map(|(definition, source)| {
            let values = source.chunk(&table.rows).expect("a readable column");
            (
                definition.name.to_string(),
                (source.column_type().clone(), values),
            )
        })
        .collect()
}

/// The input table of a fixture.
fn fixture(fixture: &Value) -> Result<TestTable, Stop> {
    if let Some(size) = fixture.get("empty") {
        return Ok(table(Vec::new(), size.as_u64().expect("a size") as usize));
    }
    let rows = cells(&fixture["rows"]);
    let mut columns = Vec::new();
    for (index, column) in cells(&fixture["columns"]).iter().enumerate() {
        let kind = match column["type"].as_str().expect("a type") {
            "char" => "uint16",
            array if array.ends_with("[]") => {
                return Err(Stop::Refused(format!("fixture column of type {array}")))
            }
            other => other,
        };
        let values: Vec<Value> = rows.iter().map(|row| row[index].clone()).collect();
        columns.push((
            column["name"].as_str().expect("a name").to_string(),
            column_of(kind, &values),
        ));
    }
    Ok(table(columns, rows.len()))
}

/// Splits `Name = expression` of `update`, `select` and `view`; a bare `Name` is the column
/// itself.
fn assignment(text: &str) -> (&str, &str) {
    match text.split_once('=') {
        Some((name, expression)) if !expression.starts_with('=') => {
            (name.trim(), expression.trim())
        }
        _ => (text.trim(), text.trim()),
    }
}

/// Applies the operations a case lists before its formula.
fn set_up(mut current: TestTable, setup: &[Value]) -> Result<TestTable, Stop> {
    for operation in setup {
        let op = operation[0].as_str().expect("an operation");
        if op == "where" {
            return Err(Stop::Refused("setup: where".to_string()));
        }
        let size = current.rows.size() as usize;
        let mut selected: Vec<(String, Column)> = Vec::new();
        for text in cells(&operation[1]) {
            let (name, expression) = assignment(text.as_str().expect("a string"));
            let column = match ours(&current, expression, false) {
                Ours::Column(column) => column,
                Ours::Rejected(FormulaError::Unsupported(construct)) => {
                    return Err(Stop::Refused(format!("setup: {construct}")))
                }
                Ours::Rejected(error) => {
                    return Err(Stop::Differs(format!(
                        "setup {expression} is rejected: {error}"
                    )))
                }
                Ours::Failed(failure) => {
                    return Err(Stop::Differs(format!(
                        "setup {expression} fails: {}",
                        failure.message
                    )))
                }
            };
            selected.push((name.to_string(), column.clone()));
            // A later formula of the same operation reads this column.
            let mut columns = columns_of(&current);
            match columns.iter_mut().find(|(existing, _)| existing == name) {
                Some(existing) => existing.1 = column,
                None => columns.push((name.to_string(), column)),
            }
            current = table(columns, size);
        }
        if op != "update" {
            // `select` and `view` keep only what they list.
            let mut last: Vec<(String, Column)> = Vec::new();
            for (name, column) in selected {
                last.retain(|(existing, _)| *existing != name);
                last.push((name, column));
            }
            current = table(last, size);
        }
    }
    Ok(current)
}

/// The A1 tolerance of PARITY.md: for finite cells of a formula that calls a function which
/// is not correctly rounded.
fn within(expected: f64, actual: f64, relative: f64, absolute: f64) -> bool {
    if expected.to_bits() == actual.to_bits() || (expected.is_nan() && actual.is_nan()) {
        return true;
    }
    if !expected.is_finite() || !actual.is_finite() {
        return false;
    }
    (expected - actual).abs() <= (relative * expected.abs().max(actual.abs())).max(absolute)
}

/// `rules.INEXACT_FUNCTIONS` of the harness: the functions whose results PARITY.md A1 lets
/// differ by the tolerance.
const INEXACT: [&str; 16] = [
    "exp", "expm1", "log", "log10", "log1p", "pow", "sin", "cos", "tan", "asin", "acos", "atan",
    "atan2", "sinh", "cosh", "tanh",
];

fn calls_inexact(formula: &str) -> bool {
    INEXACT.iter().any(|name| {
        formula.match_indices(name).any(|(at, _)| {
            let before = formula[..at].chars().next_back();
            let after = formula[at + name.len()..].trim_start();
            !before.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '.')
                && after.starts_with('(')
        })
    })
}

pub(super) fn same_values(formula: &str, expected: &ArrayRef, actual: &ArrayRef) -> bool {
    if render(expected) == render(actual) {
        return true;
    }
    if !calls_inexact(formula) || expected.len() != actual.len() {
        return false;
    }
    let pairs = |relative: f64, absolute: f64, cell: &dyn Fn(&ArrayRef, usize) -> Option<f64>| {
        (0..expected.len()).all(|row| match (cell(expected, row), cell(actual, row)) {
            (None, None) => true,
            (Some(e), Some(a)) => within(e, a, relative, absolute),
            _ => false,
        })
    };
    match expected.data_type() {
        DataType::Float64 => pairs(1e-9, 1e-12, &|array, row| {
            let values = array.as_primitive::<Float64Type>();
            values.is_valid(row).then(|| values.value(row))
        }),
        DataType::Float32 => pairs(1e-5, 1e-6, &|array, row| {
            let values = array.as_primitive::<Float32Type>();
            values.is_valid(row).then(|| f64::from(values.value(row)))
        }),
        _ => false,
    }
}

enum Outcome {
    Agrees,
    Refused(String),
    Differs(String),
}

/// Compares what the formula of `case` does on `table` with the recorded outcome `java`.
fn compare(table: &TestTable, case: &Value, java: &Value) -> Outcome {
    let formula = case["formula"].as_str().expect("a formula");
    let filter = case["kind"] == "where";
    let ours = ours(table, formula, filter);
    match &ours {
        Ours::Rejected(FormulaError::Unsupported(construct)) => {
            return Outcome::Refused(construct.clone())
        }
        // Java sends half of a surrogate pair as `?`; an Arrow string cannot hold it.
        Ours::Failed(failure) if failure.message.contains("unpaired UTF-16 surrogate") => {
            return Outcome::Refused(failure.message.clone())
        }
        _ => {}
    }

    if let Some(error) = java.get("error") {
        // Only the status is compared (PARITY.md A5), and every failure of a formula is
        // INVALID_ARGUMENT, at compilation or at evaluation.
        let cause = error["cause"].as_str().unwrap_or_default();
        return match ours {
            Ours::Rejected(_) | Ours::Failed(_) => Outcome::Agrees,
            // Java's `where` throws for a null `Boolean`; the formula only reports it.
            Ours::Column((ColumnType::Boolean, values)) if filter && values.null_count() > 0 => {
                Outcome::Agrees
            }
            Ours::Column(_) => {
                Outcome::Differs(format!("Java fails ({cause}); Oxhaven evaluates it"))
            }
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
        if values.null_count() > 0 {
            return Outcome::Differs(format!("Oxhaven returns {:?}", render(&values)));
        }
        if java["nondeterministic"] == true {
            return Outcome::Agrees;
        }
        let kept: Vec<i64> = values
            .as_boolean()
            .iter()
            .enumerate()
            .filter_map(|(row, keep)| (keep == Some(true)).then_some(row as i64))
            .collect();
        let expected: Vec<i64> = cells(&java["rows"])
            .iter()
            .map(|row| row.as_i64().expect("a row"))
            .collect();
        return if kept == expected {
            Outcome::Agrees
        } else {
            Outcome::Differs(format!("Java keeps rows {expected:?}, Oxhaven {kept:?}"))
        };
    }

    let java_type = java["type"].as_str().unwrap_or("no type");
    let Some(kind) = kind_of(java_type) else {
        return Outcome::Differs(format!(
            "Java returns a {java_type} column; Oxhaven returns {column_type:?}"
        ));
    };
    if java["nondeterministic"] == true {
        let expected = column_of(kind, &[]).0;
        return if expected == column_type {
            Outcome::Agrees
        } else {
            Outcome::Differs(format!(
                "Java returns {java_type}; Oxhaven returns {column_type:?}"
            ))
        };
    }
    let expected = column_of(kind, cells(&java["values"]));
    if column_type != expected.0 {
        return Outcome::Differs(format!(
            "Java returns {java_type}; Oxhaven returns {column_type:?}"
        ));
    }
    if same_values(formula, &expected.1, &values) {
        Outcome::Agrees
    } else {
        Outcome::Differs(format!(
            "Java returns {:?}, Oxhaven {:?}",
            render(&expected.1),
            render(&values)
        ))
    }
}

/// One side of a comparison in a filter string: an identifier or a literal, and the rest of
/// the text.
///
/// Behavior: `io.deephaven.api.expression.SelectFactoryConstants#ID_PTRN` and
/// `#LITERAL_PTRN`.
fn filter_operand(text: &str) -> Option<(bool, &str, &str)> {
    let end_of =
        |accept: &dyn Fn(char) -> bool| text.find(|c: char| !accept(c)).unwrap_or(text.len());
    let first = text.chars().next()?;
    if first.is_ascii_alphabetic() || first == '_' || first == '$' {
        let end = end_of(&|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        return Some((true, &text[..end], &text[end..]));
    }
    if let Some(quote) = ['\'', '"', '`'].into_iter().find(|quote| *quote == first) {
        let end = 1 + text[1..].find(quote)? + 1;
        return Some((false, &text[..end], &text[end..]));
    }
    // `-?\d+`, `[+-]?\d*\.\d+f?` or `[+-]?\d+\.\d*f?`.
    let sign = usize::from(first == '-' || first == '+');
    let digits = |from: usize| {
        from + text[from..]
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len() - from)
    };
    let whole = digits(sign);
    let mut end = whole;
    if text[whole..].starts_with('.') {
        end = digits(whole + 1);
        if end == whole + 1 && whole == sign {
            return None;
        }
        if text[end..].starts_with('f') {
            end += 1;
        }
    } else if whole == sign || first == '+' {
        return None;
    }
    Some((false, &text[..end], &text[end..]))
}

/// Whether Java reads a `where` string as a match or range filter and not as a formula.
///
/// Behavior: `io.deephaven.engine.table.impl.select.WhereFilterFactory`, the static
/// initializer and `#getWhereFilterOneSideColumn`: one identifier or literal, a comparison
/// operator (`=` included), and one identifier or literal, of which at least one is an
/// identifier that is not a row variable; or `Column [icase] [not] in ...`, or `includes`.
///
/// When the other side is an identifier too, Java still makes a match or range filter, and
/// that filter falls back on the formula when the identifier is not a value of the column's
/// type. `null`, `NaN`, `Infinity`, `true` and `false` are such values. Any other identifier
/// is a column or a constant, for which the formula decides, so the case is run as a formula.
fn is_filter_syntax(text: &str) -> bool {
    let Some((left_is_name, left, rest)) = filter_operand(text.trim()) else {
        return false;
    };
    let rest = rest.trim_start();
    if left_is_name {
        let mut words = rest.split_whitespace().map(str::to_ascii_lowercase);
        let mut word = words.next();
        if word.as_deref() == Some("icase") {
            word = words.next();
        }
        if word.as_deref() == Some("not") {
            word = words.next();
        }
        if matches!(word.as_deref(), Some("in" | "includes")) && words.next().is_some() {
            return true;
        }
    }
    let Some(operator) = ["==", "!=", "<=", ">=", "=", "<", ">"]
        .into_iter()
        .find(|operator| rest.starts_with(operator))
    else {
        return false;
    };
    let Some((right_is_name, right, rest)) = filter_operand(rest[operator.len()..].trim_start())
    else {
        return false;
    };
    if !rest.trim().is_empty() || (!left_is_name && !right_is_name) {
        return false;
    }
    let is_value = |name: &str| {
        ["null", "nan", "infinity", "true", "false"].contains(&name.to_lowercase().as_str())
    };
    // The recorded outcomes show that a value word is taken as the value on the left too:
    // `null < B1` keeps the rows `B1 > null` keeps, though as a formula it does not compile.
    let (column, value_is_name, value) = if left_is_name && !(right_is_name && is_value(left)) {
        (left, right_is_name, right)
    } else {
        (right, left_is_name, left)
    };
    if matches!(column, "i" | "ii" | "k") {
        return false;
    }
    !value_is_name || is_value(value)
}

#[test]
fn every_corpus_case_does_what_java_did_or_is_refused_by_name() {
    let text = fs::read_to_string(corpus().join("fixtures.json")).expect("the fixtures");
    let fixtures: Value = serde_json::from_str(&text).expect("JSON");
    let fixtures = &fixtures["fixtures"];

    let mut groups: Vec<PathBuf> = fs::read_dir(corpus().join("cases"))
        .expect("the cases")
        .map(|entry| entry.expect("a file").path())
        .collect();
    groups.sort();

    let mut total = 0;
    let mut agreed = 0;
    let mut filters = 0;
    let mut setup_failed = 0;
    let mut refused = String::new();
    let mut refusals: BTreeMap<String, usize> = BTreeMap::new();
    let mut differences = Vec::new();
    for group in groups {
        let text = fs::read_to_string(&group).expect("a group of cases");
        for line in text.lines() {
            let case: Value = serde_json::from_str(line).expect("a case");
            let id = case["id"].as_str().expect("an id");
            let java = case
                .get("java")
                .unwrap_or_else(|| panic!("{id} is not recorded"));
            total += 1;
            if case["kind"] == "where"
                && is_filter_syntax(case["formula"].as_str().expect("a formula"))
            {
                filters += 1;
                continue;
            }
            if java["error"]["stage"] == "setup" {
                setup_failed += 1;
                continue;
            }
            let name = case["fixture"].as_str().expect("a fixture");
            let setup = case.get("setup").map(cells).unwrap_or_default();
            let outcome = match fixture(&fixtures[name]).and_then(|table| set_up(table, setup)) {
                Ok(table) => compare(&table, &case, java),
                Err(Stop::Refused(construct)) => Outcome::Refused(construct),
                Err(Stop::Differs(how)) => Outcome::Differs(how),
            };
            match outcome {
                Outcome::Agrees => agreed += 1,
                Outcome::Refused(construct) => {
                    let java = if java.get("error").is_some() {
                        "java-fails"
                    } else {
                        "java-accepts"
                    };
                    // The count is by the first word, which names the kind of construct.
                    let word = construct.split(' ').next().unwrap_or_default();
                    *refusals.entry(format!("{java}: {word}")).or_default() += 1;
                    writeln!(refused, "{id}\t{java}\t{construct}").expect("a string");
                }
                Outcome::Differs(how) => {
                    differences.push(format!("{id} {}: {how}", case["formula"]));
                }
            }
        }
    }

    println!(
        "{total} corpus cases: {agreed} agree with Java, {} differ, {} refused as unsupported, \
         {filters} match and range filters not run, {setup_failed} with a setup that fails in \
         Java not run",
        differences.len(),
        refused.lines().count(),
    );
    println!("refused, by Java's outcome and the kind of construct: {refusals:#?}");
    assert!(
        differences.is_empty(),
        "{} cases differ from Java:\n{}",
        differences.len(),
        differences.join("\n")
    );

    let path = refused_list();
    if std::env::var_os("OXHAVEN_WRITE_REFUSED").is_some() {
        fs::write(&path, &refused).expect("a writable list");
    }
    let listed = fs::read_to_string(&path).expect("the list of refused cases");
    assert!(
        refused == listed,
        "the refused cases are not those of {}; rerun with OXHAVEN_WRITE_REFUSED=1 and review \
         the change",
        path.display()
    );
    // Guards against a corpus that silently lists nothing.
    assert!(agreed > 2000, "only {agreed} cases agree");
}
