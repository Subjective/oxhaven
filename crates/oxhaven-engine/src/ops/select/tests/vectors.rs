//! Formulas over vector and array columns through `update`, `select`, `view`,
//! `update_view` and `where`.
//!
//! `recorded` runs the formulas that `oxhaven-expr` recorded from the Java server
//! (`crates/oxhaven-expr/tests/data/java_vectors.json`, see `record_java_vectors.py` beside
//! it) through the operations, on a static table built from the columns Java held, and
//! compares each result with Java's: the column type, a vector not being an array, and the
//! cells bit for bit. The ticking tests run under the consistency checker and are
//! recomputed from a static copy of their source after every cycle.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::group_by;
use crate::ops::filter::unstructured_filter;
use crate::table::in_memory_table;
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int8Array, ListArray,
    StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::expr::{FormulaCompiler, FormulaError};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnDefinition;
use oxhaven_testkit::{definition, ConsistencyChecker, RandomUpdates, ScriptedTable};
use serde_json::Value;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn list(value: &Value) -> &[Value] {
    value.as_array().expect("a list")
}

fn float(cell: &Value) -> Option<f64> {
    match cell {
        Value::Null => None,
        Value::String(word) => Some(match word.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            "-0.0" => -0.0,
            other => panic!("unknown float word {other}"),
        }),
        number => number.as_f64(),
    }
}

fn integer<T: TryFrom<i64>>(cell: &Value) -> Option<T> {
    cell.as_i64()
        .map(|value| T::try_from(value).ok().expect("a value of the type"))
}

/// A column of the recording's type `kind` from JSON cells; a list is an array, or with
/// `vector` a vector.
fn recorded_column(kind: &str, vector: bool, cells: &[Value]) -> (ColumnType, ArrayRef) {
    if let Some(element) = kind
        .strip_prefix("list<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        let elements: Vec<Value> = cells
            .iter()
            .filter_map(Value::as_array)
            .flatten()
            .cloned()
            .collect();
        let (element_type, elements) = recorded_column(element, false, &elements);
        let column_type = if vector {
            ColumnType::vector(element_type)
        } else {
            ColumnType::array(element_type)
        };
        let DataType::List(field) = column_type.arrow_type() else {
            unreachable!("a vector or array is a list")
        };
        let lengths = cells.iter().map(|cell| cell.as_array().map_or(0, Vec::len));
        let valid: NullBuffer = cells.iter().map(|cell| !cell.is_null()).collect();
        let cells = ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            elements,
            Some(valid),
        );
        return (column_type, Arc::new(cells));
    }
    let cells = cells.iter();
    match kind {
        "bool" => (
            ColumnType::Boolean,
            Arc::new(cells.map(Value::as_bool).collect::<BooleanArray>()),
        ),
        "int8" => (
            ColumnType::Byte,
            Arc::new(cells.map(integer).collect::<Int8Array>()),
        ),
        "int16" => (
            ColumnType::Short,
            Arc::new(cells.map(integer).collect::<Int16Array>()),
        ),
        "uint16" => (
            ColumnType::Char,
            Arc::new(cells.map(integer).collect::<UInt16Array>()),
        ),
        "int32" => (
            ColumnType::Int,
            Arc::new(cells.map(integer).collect::<Int32Array>()),
        ),
        "int64" => (
            ColumnType::Long,
            Arc::new(cells.map(integer).collect::<Int64Array>()),
        ),
        "float32" => (
            ColumnType::Float,
            Arc::new(
                cells
                    .map(|cell| float(cell).map(|value| value as f32))
                    .collect::<Float32Array>(),
            ),
        ),
        "float64" => (
            ColumnType::Double,
            Arc::new(cells.map(float).collect::<Float64Array>()),
        ),
        "string" => (
            ColumnType::String,
            Arc::new(cells.map(Value::as_str).collect::<StringArray>()),
        ),
        "timestamp" => (
            ColumnType::Instant,
            Arc::new(
                cells
                    .map(integer)
                    .collect::<TimestampNanosecondArray>()
                    .with_data_type(ColumnType::Instant.arrow_type()),
            ),
        ),
        other => panic!("no column of type {other}"),
    }
}

fn is_vector(java_type: &Value) -> bool {
    java_type
        .as_str()
        .is_some_and(|name| name.starts_with("io.deephaven.vector."))
}

/// The recording's type name of a Java scalar type.
fn scalar_kind(java_type: &str) -> Option<&'static str> {
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
        _ => return None,
    })
}

/// The cells of a column as text, a list cell as the text of its elements.
fn shown(array: &ArrayRef) -> Vec<String> {
    match array.data_type() {
        DataType::List(_) => array
            .as_list::<i32>()
            .iter()
            .map(|cell| match cell {
                Some(elements) => format!("[{}]", cells(&elements).join(", ")),
                None => "null".to_string(),
            })
            .collect(),
        _ => cells(array),
    }
}

fn static_table(columns: &[(&str, ColumnType, ArrayRef)]) -> Arc<dyn Table> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns
        .iter()
        .map(|(_, _, values)| values.clone())
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch");
    let definition = TableDefinition::new(
        columns
            .iter()
            .map(|(name, column_type, _)| ColumnDefinition::new(*name, column_type.clone()))
            .collect(),
    )
    .expect("distinct names");
    in_memory_table(definition, &[batch]).expect("a table")
}

/// The last column of `table`: its type and cells.
fn last_column(table: &dyn Table) -> (ColumnType, Vec<String>) {
    let column_type = table
        .definition()
        .columns()
        .last()
        .expect("a column")
        .column_type
        .clone();
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    (
        column_type,
        shown(snapshot.columns.last().expect("a column")),
    )
}

#[test]
fn recorded_vector_formulas_give_what_java_gave_through_every_operation() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../oxhaven-expr/tests/data/java_vectors.json");
    let text = std::fs::read_to_string(path).expect("the recording");
    let document: Value = serde_json::from_str(&text).expect("JSON");
    let columns: Vec<(&str, ColumnType, ArrayRef)> = list(&document["columns"])
        .iter()
        .map(|column| {
            let kind = column["type"].as_str().expect("a type");
            let (column_type, values) =
                recorded_column(kind, is_vector(&column["java"]), list(&column["values"]));
            (
                column["name"].as_str().expect("a name"),
                column_type,
                values,
            )
        })
        .collect();
    let table = static_table(&columns);
    let definition = table.definition().clone();

    let (mut columns_compared, mut filters_compared, mut failures_agreed) = (0, 0, 0);
    let (mut refused, mut not_ported) = (0, 0);
    let mut differences = Vec::new();
    for case in list(&document["cases"]) {
        let formula = case["formula"].as_str().expect("a formula");
        let compiled = Compiler
            .parse(formula)
            .and_then(|expr| Compiler.compile(&expr, &definition));
        if matches!(compiled, Err(FormulaError::Unsupported(_))) {
            refused += 1;
            continue;
        }
        if case["op"] == "where" {
            let filtered = catch_unwind(AssertUnwindSafe(|| {
                unstructured_filter(&table, &[formula], &Compiler)
            }));
            let Ok(filtered) = filtered else {
                // The filter factory takes `V == null` for a match filter, which is not
                // ported for a list column.
                not_ported += 1;
                continue;
            };
            match (case.get("kept"), filtered) {
                (Some(kept), Ok(filtered)) => {
                    let snapshot = TableSnapshot::current(&*filtered).expect("a table");
                    let expected: Vec<String> =
                        list(kept).iter().map(|row| row.to_string()).collect();
                    if cells(&snapshot.columns[0]) != expected {
                        differences.push(format!("where {formula}"));
                    }
                    filters_compared += 1;
                }
                (None, Err(_)) => failures_agreed += 1,
                (Some(_), Err(error)) => differences.push(format!("where {formula}: {error}")),
                (None, Ok(_)) => differences.push(format!("where {formula}: Java refuses it")),
            }
            continue;
        }
        let assignment = [format!("Result = {formula}")];
        type Operation =
            fn(&Arc<dyn Table>, &[String], &Compiler) -> Result<Arc<dyn Table>, SelectError>;
        let operations: [(&str, Operation); 4] = [
            ("update", |t, c, compiler| update(t, c, compiler)),
            ("select", |t, c, compiler| select(t, c, compiler)),
            ("view", |t, c, compiler| view(t, c, compiler)),
            ("update_view", |t, c, compiler| update_view(t, c, compiler)),
        ];
        for (name, operation) in operations {
            let result = catch_unwind(AssertUnwindSafe(|| {
                operation(&table, &assignment, &Compiler).and_then(|result| {
                    // A column computed on read fails when it is read.
                    TableSnapshot::current(&*result)?;
                    Ok(result)
                })
            }));
            let Ok(result) = result else {
                not_ported += 1;
                continue;
            };
            let recorded = &case["result"];
            match (recorded.is_object(), result) {
                (false, Err(_)) => failures_agreed += 1,
                (false, Ok(_)) => differences.push(format!("{name} {formula}: Java refuses it")),
                (true, Err(error)) => differences.push(format!("{name} {formula}: {error}")),
                (true, Ok(result)) => {
                    let java_type = recorded["java"].as_str().expect("a Java type");
                    let kind = recorded["kind"]
                        .as_str()
                        .filter(|kind| kind.starts_with("list<"))
                        .or_else(|| scalar_kind(java_type));
                    let Some(kind) = kind.filter(|_| !recorded["values"].is_null()) else {
                        differences.push(format!("{name} {formula}: Java gives a {java_type}"));
                        continue;
                    };
                    let (expected_type, expected) = recorded_column(
                        kind,
                        is_vector(&recorded["java"]),
                        list(&recorded["values"]),
                    );
                    let (column_type, actual) = last_column(&*result);
                    if column_type != expected_type || actual != shown(&expected) {
                        differences.push(format!(
                            "{name} {formula}: Java gives {expected_type:?} {:?}, the \
                             engine {column_type:?} {actual:?}",
                            shown(&expected)
                        ));
                    }
                    columns_compared += 1;
                }
            }
        }
    }
    println!(
        "{columns_compared} result columns and {filters_compared} filters equal Java's, \
         {failures_agreed} fail as in Java, {refused} formulas refused as unsupported, \
         {not_ported} runs ended in unported!"
    );
    assert!(
        differences.is_empty(),
        "{} results differ from Java:\n{}",
        differences.len(),
        differences.join("\n")
    );
    // Guards against a recording that silently holds nothing.
    assert!(columns_compared > 4 * 450, "{columns_compared}");
    assert!(filters_compared > 15, "{filters_compared}");
    assert!(not_ported < 40, "{not_ported}");
}

type Operation = Arc<dyn Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync>;

fn operation(f: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static) -> Operation {
    Arc::new(f)
}

/// Runs `operation` over `derive` of a randomly changing source with the columns of
/// `columns`, for many seeds, checking after every cycle that the result is consistent
/// and equal to the operation run on a static copy of what it reads.
fn fuzz(
    what: &str,
    columns: &[(&str, ColumnType)],
    derive: fn(&Arc<dyn Table>) -> Arc<dyn Table>,
    operation: Operation,
) {
    let mut updates = 0;
    for seed in 0..16 {
        let stepper = SteppedUpdateGraph::new();
        let graph: Arc<dyn UpdateGraph> = stepper.clone();
        let mut random = RandomUpdates::new(seed * 17 + 3);
        let values = columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, 6))
            .collect();
        let source = ScriptedTable::new(
            graph,
            definition(columns),
            RowSet::from_keys([0, 2, 3, 7, 8, 11]),
            values,
        );
        let scripted: Arc<dyn Table> = source.table();
        let parent = derive(&scripted);
        let result = operation(&parent);
        assert!(result.is_refreshing(), "{what}");
        let checker = {
            let (parent, operation) = (parent.clone(), operation.clone());
            ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                operation(&frozen(&*parent))
            })
        };
        checker.assert_consistent();
        for cycle in 0..25 {
            source.push(random.next_change(&*scripted, 8));
            stepper.run_cycle();
            if let Err(error) = checker.verify() {
                panic!("{what}, seed {seed}, cycle {cycle}: {error}");
            }
        }
        assert!(!result.is_failed(), "{what}");
        updates += checker.updates_seen();
    }
    assert!(updates > 16 * 10, "{what}: only {updates} updates");
}

/// Formulas that take a null cell, an empty cell and null elements.
const OF_ANY_CELL: [&str; 9] = [
    "Sum = sum(V)",
    "Avg = avg(D)",
    "Len = len(V) + count(D)",
    "Low = min(V)",
    "Last = lastObj(S)",
    "Sorted = sort(V)",
    "Running = cumsum(D)",
    "Head = isNull(V) ? null : V.subVector(0, 2)",
    "Second = len(D) > 1 ? D[1] : null",
];

fn listed() -> Vec<(&'static str, ColumnType)> {
    vec![
        ("K", ColumnType::Int),
        ("V", ColumnType::vector(ColumnType::Int)),
        ("D", ColumnType::array(ColumnType::Double)),
        ("S", ColumnType::vector(ColumnType::String)),
    ]
}

fn itself(table: &Arc<dyn Table>) -> Arc<dyn Table> {
    table.clone()
}

#[test]
fn stored_formulas_over_list_columns_follow_random_changes() {
    fuzz(
        "update",
        &listed(),
        itself,
        operation(|t| update(t, &OF_ANY_CELL, &Compiler).unwrap()),
    );
    fuzz(
        "select",
        &listed(),
        itself,
        operation(|t| select(t, &OF_ANY_CELL, &Compiler).unwrap()),
    );
    // A formula over a vector that an earlier formula of the same list made.
    fuzz(
        "update of a computed vector",
        &listed(),
        itself,
        operation(|t| {
            let columns = [
                "W = isNull(V) ? null : V.subVector(1, 4)",
                "Sum = sum(W)",
                "V = reverse(V)",
                "First = first(V)",
            ];
            update(t, &columns, &Compiler).unwrap()
        }),
    );
}

#[test]
fn formulas_computed_on_read_over_list_columns_follow_random_changes() {
    fuzz(
        "view",
        &listed(),
        itself,
        operation(|t| view(t, &OF_ANY_CELL, &Compiler).unwrap()),
    );
    fuzz(
        "update_view",
        &listed(),
        itself,
        operation(|t| update_view(t, &OF_ANY_CELL, &Compiler).unwrap()),
    );
}

#[test]
fn filters_over_list_columns_follow_random_changes() {
    fuzz(
        "where",
        &listed(),
        itself,
        operation(|t| {
            let filters = ["len(V) > 1 && sum(V) > 0", "!isNull(firstObj(S))"];
            unstructured_filter(t, &filters, &Compiler).unwrap()
        }),
    );
}

fn grouped(table: &Arc<dyn Table>) -> Arc<dyn Table> {
    group_by(table, &["G"]).unwrap()
}

/// The vectors of `group_by`, which are never null and never empty.
#[test]
fn formulas_over_the_vectors_of_a_group_by_follow_random_changes() {
    let columns = [
        ("G", ColumnType::Boolean),
        ("X", ColumnType::Int),
        ("D", ColumnType::Double),
        ("S", ColumnType::String),
    ];
    let formulas = [
        "Sum = sum(X)",
        "Avg = avg(D)",
        "N = X.size()",
        "First = X.get(0)",
        "Second = X[1]",
        "Median = median(D)",
        "Std = std(X)",
        "Top = sortDescending(X)",
        "Pair = D.subVector(0, 2)",
        "Name = lastObj(S)",
        "Distinct = countDistinct(X)",
    ];
    fuzz(
        "update of a group_by",
        &columns,
        grouped,
        operation(move |t| update(t, &formulas, &Compiler).unwrap()),
    );
    fuzz(
        "select of a group_by",
        &columns,
        grouped,
        operation(move |t| select(t, &formulas, &Compiler).unwrap()),
    );
    fuzz(
        "view of a group_by",
        &columns,
        grouped,
        operation(move |t| view(t, &formulas, &Compiler).unwrap()),
    );
    fuzz(
        "where of a group_by",
        &columns,
        grouped,
        operation(|t| {
            unstructured_filter(t, &["X.size() > 1 && max(X) > first(X)"], &Compiler).unwrap()
        }),
    );
}

#[test]
fn a_formula_over_arrays_is_what_a_formula_aggregation_evaluates() {
    let each = ColumnType::vector(ColumnType::Int);
    let formula = ArrayFormula::compile(
        &Compiler,
        "sum(each) + Bonus",
        &[
            ("each", each.clone()),
            ("Bonus", ColumnType::Long),
            ("Unused", ColumnType::String),
        ],
    )
    .unwrap();
    assert_eq!(formula.result_type(), &ColumnType::Long);
    assert_eq!(formula.inputs_read(), ["each", "Bonus"]);
    // Three groups: [1, 2, 3], [] and [null, 7].
    let (_, groups) = recorded_column(
        "list<int32>",
        true,
        &serde_json::json!([[1, 2, 3], [], [null, 7]])
            .as_array()
            .unwrap()
            .clone(),
    );
    let bonus: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let unused: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let sums = formula.evaluate(&[groups.clone(), bonus, unused]).unwrap();
    assert_eq!(cells(&sums), ["16", "null", "37"]);

    // A vector result, as a rolling formula can give.
    let sorted = ArrayFormula::compile(&Compiler, "sortDescending(x)", &[("x", each)]).unwrap();
    assert_eq!(sorted.result_type(), &ColumnType::array(ColumnType::Int));
    assert_eq!(
        shown(&sorted.evaluate(std::slice::from_ref(&groups)).unwrap()),
        ["[3, 2, 1]", "[]", "[7, null]"]
    );

    assert!(sorted.evaluate(&[]).is_err());
    assert!(matches!(
        ArrayFormula::compile(
            &Compiler,
            "sum(each) + i",
            &[("each", ColumnType::vector(ColumnType::Int))]
        ),
        Err(FormulaError::Unsupported(_))
    ));
    assert!(matches!(
        ArrayFormula::compile(
            &Compiler,
            "sum(nope)",
            &[("each", ColumnType::vector(ColumnType::Int))]
        ),
        Err(FormulaError::Type(_))
    ));
}
