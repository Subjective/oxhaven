//! Functions an embedder supplies (`Compiler::with_functions`): when a call is one of
//! them, what it is told about its arguments, and what it is given to compute with.
//!
//! The order of lookup is `QueryLanguageParser#getMethod`'s: the static imports, then a
//! variable of the query scope that can be called. The cases are those observed for a
//! Python function on Java 42.9 (`docs/PYTHON_UDF.md`).

use super::*;
use oxhaven_contracts::expr::Literal;
use oxhaven_expr::{ExternalArgument, ExternalFunction, ExternalFunctions};
use std::sync::Mutex;

/// What a function was asked: its name and one description per argument.
type Asked = Vec<(String, Vec<String>)>;

/// Functions for the tests: `twice(long-like) -> long`, `tag(anything...) -> String`,
/// `sqrt(String) -> String`, `abs(anything) -> String`, `short_one() -> long` (which
/// returns too few values) and `refuse()`, which exists and cannot be called.
#[derive(Default)]
struct Functions {
    asked: Mutex<Asked>,
    /// The number of rows of every call that was made.
    calls: Arc<Mutex<Vec<usize>>>,
}

struct Function {
    name: String,
    result: ColumnType,
    calls: Arc<Mutex<Vec<usize>>>,
}

impl ExternalFunction for Function {
    fn result_type(&self) -> &ColumnType {
        &self.result
    }

    fn call(&self, arguments: &[ArrayRef], rows: usize) -> Result<ArrayRef, TableFailure> {
        self.calls.lock().unwrap().push(rows);
        Ok(match self.name.as_str() {
            "twice" => {
                let values = arrow::compute::cast(&arguments[0], &DataType::Int64).unwrap();
                let values = values.as_primitive::<Int64Type>();
                Arc::new(
                    values
                        .iter()
                        .map(|v| v.map(|v| 2 * v))
                        .collect::<Int64Array>(),
                )
            }
            "short_one" => Arc::new(Int64Array::from(vec![1; rows.saturating_sub(1)])),
            _ => {
                let texts = (0..rows).map(|row| {
                    let cells: Vec<String> = arguments
                        .iter()
                        .map(|argument| match argument.data_type() {
                            DataType::Null => "null".to_string(),
                            _ => render(&argument.slice(row, 1)).join(""),
                        })
                        .collect();
                    Some(format!("{}({})", self.name, cells.join(",")))
                });
                Arc::new(texts.collect::<StringArray>())
            }
        })
    }
}

impl ExternalFunctions for Functions {
    fn resolve(
        &self,
        name: &str,
        arguments: &[ExternalArgument<'_>],
    ) -> Option<Result<Arc<dyn ExternalFunction>, FormulaError>> {
        let described = arguments
            .iter()
            .map(|argument| match (argument.column_type, argument.literal) {
                (None, _) => "null".to_string(),
                (Some(column_type), Some(literal)) => format!("{column_type:?} {literal:?}"),
                (Some(column_type), None) => format!("{column_type:?}"),
            })
            .collect();
        self.asked
            .lock()
            .unwrap()
            .push((name.to_string(), described));
        let result = match name {
            "twice" | "short_one" => ColumnType::Long,
            "tag" | "sqrt" | "abs" => ColumnType::String,
            "refuse" => return Some(Err(FormulaError::Unsupported("refuse()".to_string()))),
            _ => return None,
        };
        Some(Ok(Arc::new(Function {
            name: name.to_string(),
            result,
            calls: self.calls.clone(),
        })))
    }
}

struct Fixture {
    table: TestTable,
    functions: Arc<Functions>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            table: TestTable::new([
                ("X", ints([Some(1), None, Some(3), Some(4)])),
                ("T", strings([Some("a"), Some("b"), None, Some("d")])),
                ("twice_column", ints([Some(0); 4])),
            ]),
            functions: Arc::new(Functions::default()),
        }
    }

    fn compile(
        &self,
        formula: &str,
    ) -> Result<Arc<dyn oxhaven_contracts::expr::Formula>, FormulaError> {
        let compiler = Compiler::with_functions(self.functions.clone());
        compiler
            .parse(formula)
            .and_then(|expr| compiler.compile(&expr, &self.table.definition))
    }

    fn error(&self, formula: &str) -> FormulaError {
        match self.compile(formula) {
            Ok(_) => panic!("{formula} compiled"),
            Err(error) => error,
        }
    }

    fn evaluate(&self, formula: &str) -> Result<Column, TableFailure> {
        let compiled = self
            .compile(formula)
            .unwrap_or_else(|e| panic!("{formula} did not compile: {e}"));
        let values = compiled.evaluate(&self.table, &self.table.rows, false)?;
        Ok((compiled.result_type().clone(), values))
    }

    fn assert(&self, formula: &str, expected: Column) {
        let actual = self
            .evaluate(formula)
            .unwrap_or_else(|e| panic!("{formula} failed: {e}"));
        assert_eq!(actual.0, expected.0, "result type of {formula}");
        assert_eq!(
            render(&actual.1),
            render(&expected.1),
            "values of {formula}"
        );
    }

    fn asked(&self) -> Asked {
        std::mem::take(&mut self.functions.asked.lock().unwrap())
    }

    fn calls(&self) -> Vec<usize> {
        std::mem::take(&mut self.functions.calls.lock().unwrap())
    }
}

#[test]
fn a_call_the_library_cannot_take_is_the_embedders() {
    let fixture = Fixture::new();
    fixture.assert("twice(X)", longs([Some(2), None, Some(6), Some(8)]));
    assert_eq!(
        fixture.asked(),
        [("twice".to_string(), vec!["Int".to_string()])]
    );
    // the call is a value like any other
    fixture.assert("twice(X) + 1", longs([Some(3), None, Some(7), Some(9)]));
    fixture.assert(
        "twice(twice(X + 1))",
        longs([Some(8), None, Some(16), Some(20)]),
    );
    // a comparison with null is false
    fixture.assert("twice(X) > 4", flags([false, false, true, true]));
    let formula = fixture.compile("twice(X) + ii").unwrap();
    assert_eq!(formula.columns(), ["X"]);
    assert!(formula.uses_row_variables());
}

#[test]
fn the_library_comes_first_when_its_function_fits() {
    let fixture = Fixture::new();
    // `abs(int)` is the library's, and the embedder is not asked
    fixture.assert("abs(X - 2)", ints([Some(1), None, Some(1), Some(2)]));
    assert_eq!(fixture.asked(), []);
    // the library has `sqrt`, and none that takes a `String`
    fixture.assert(
        "sqrt(T)",
        strings([
            Some("sqrt(\"a\")"),
            Some("sqrt(\"b\")"),
            Some("sqrt(null)"),
            Some("sqrt(\"d\")"),
        ]),
    );
    assert_eq!(
        fixture.asked(),
        [("sqrt".to_string(), vec!["String".to_string()])]
    );
}

#[test]
fn a_name_nobody_has_and_a_column_of_the_name_are_no_function() {
    let fixture = Fixture::new();
    let unknown = fixture.error("nosuch(X)");
    assert_eq!(
        unknown,
        FormulaError::Type("cannot find function nosuch".to_string())
    );
    assert_eq!(fixture.asked().len(), 1);
    // without the embedder's functions the name is unknown in the same words
    assert_eq!(
        fixture.table.compile_error("twice(X)"),
        FormulaError::Type("cannot find function twice".to_string())
    );
    // a column hides a function of its name
    let hidden = fixture.error("twice_column(X)");
    assert!(matches!(hidden, FormulaError::Type(_)), "{hidden:?}");
    assert_eq!(fixture.asked(), []);
    // the embedder's own refusal is the answer
    assert_eq!(
        fixture.error("refuse()"),
        FormulaError::Unsupported("refuse()".to_string())
    );
}

#[test]
fn the_arguments_are_described_and_given_in_order() {
    let fixture = Fixture::new();
    fixture.assert(
        "tag(X, null, `s`, 1.5, 'c', X + 1L)",
        strings([
            Some("tag(1,null,\"s\",1.5,99,2)"),
            Some("tag(null,null,\"s\",1.5,99,null)"),
            Some("tag(3,null,\"s\",1.5,99,4)"),
            Some("tag(4,null,\"s\",1.5,99,5)"),
        ]),
    );
    let literal = |literal: Literal| format!("{literal:?}");
    assert_eq!(
        fixture.asked(),
        [(
            "tag".to_string(),
            vec![
                "Int".to_string(),
                "null".to_string(),
                format!("String {}", literal(Literal::String("s".to_string()))),
                format!("Double {}", literal(Literal::Double(1.5))),
                format!("Char {}", literal(Literal::Char(99))),
                "Long".to_string(),
            ]
        )]
    );
    // no arguments: one call for all the rows
    fixture.assert("tag()", strings([Some("tag()"); 4]));
    assert_eq!(fixture.calls(), [4, 4]);
}

/// `?:`, `&&` and `||` evaluate an operand only for the rows that need it, so the function
/// is not called for the others.
#[test]
fn a_function_is_called_only_for_the_rows_that_reach_it() {
    let fixture = Fixture::new();
    fixture.assert(
        "isNull(X) ? -1L : twice(X)",
        longs([Some(2), Some(-1), Some(6), Some(8)]),
    );
    assert_eq!(fixture.calls(), [3]);
    fixture.assert(
        "!isNull(X) && twice(X) > 4",
        flags([false, false, true, true]),
    );
    assert_eq!(fixture.calls(), [3]);
}

#[test]
fn a_result_of_the_wrong_length_fails_the_evaluation() {
    let fixture = Fixture::new();
    let failure = fixture.evaluate("short_one()").unwrap_err();
    assert!(
        failure.message.contains("returned 3 values"),
        "{}",
        failure.message
    );
}
