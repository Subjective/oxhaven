//! Tests of the select family. `statics` covers results on static tables and refused
//! requests, `ticking` every incremental case under the consistency checker, `corpus` the results
//! recorded from the Java server, and `renames` the renaming and moving of columns.

mod corpus;
mod renames;
mod statics;
mod ticking;
mod vectors;

use super::*;
use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use arrow::datatypes::{DataType, Float32Type, Float64Type, TimestampNanosecondType};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::expr::{ColumnAssignment, Expr, Formula, RowVariable};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{ReferenceTable, TableSnapshot};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Each element as text: null is `null`, every NaN is `NaN`, and other floats carry their
/// bits, so that two arrays render equal exactly when a client would see the same values.
fn cells(array: &ArrayRef) -> Vec<String> {
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                return "null".to_string();
            }
            match array.data_type() {
                DataType::Float64 => {
                    let value = array.as_primitive::<Float64Type>().value(i);
                    if value.is_nan() {
                        "NaN".to_string()
                    } else {
                        format!("{value:?}#{:x}", value.to_bits())
                    }
                }
                DataType::Float32 => {
                    let value = array.as_primitive::<Float32Type>().value(i);
                    if value.is_nan() {
                        "NaN".to_string()
                    } else {
                        format!("{value:?}#{:x}", value.to_bits())
                    }
                }
                // Nanoseconds: printing the time needs a time zone database.
                DataType::Timestamp(..) => array
                    .as_primitive::<TimestampNanosecondType>()
                    .value(i)
                    .to_string(),
                _ => array_value_to_string(array, i).expect("a printable value"),
            }
        })
        .collect()
}

/// The current values of every column, as text, in row order.
fn rendered(table: &dyn Table) -> Vec<Vec<String>> {
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    snapshot.columns.iter().map(cells).collect()
}

/// The current values of the column named `name`, as plain numbers or strings.
fn values(table: &dyn Table, name: &str) -> Vec<String> {
    let index = table
        .definition()
        .index_of(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    let rows = table.row_set();
    let array = table.column(index).chunk(&rows).expect("a readable column");
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                "null".to_string()
            } else {
                array_value_to_string(&array, i).expect("a printable value")
            }
        })
        .collect()
}

fn names(table: &dyn Table) -> Vec<String> {
    let columns = table.definition().columns();
    columns.iter().map(|c| c.name.clone()).collect()
}

fn keys(table: &dyn Table) -> Vec<i64> {
    table.row_set().iter().collect()
}

/// A static table with the rows and values `table` has now, at the same row keys.
fn frozen(table: &dyn Table) -> Arc<dyn Table> {
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    ReferenceTable::new_static(table.definition().clone(), snapshot.rows, snapshot.columns)
}

/// The real compiler, counting the rows every formula it compiled was evaluated for.
struct Counting {
    rows: Arc<AtomicUsize>,
}

impl Counting {
    fn new() -> Self {
        Self {
            rows: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Rows evaluated since the last call.
    fn take(&self) -> usize {
        self.rows.swap(0, Ordering::SeqCst)
    }
}

struct Counted {
    formula: Arc<dyn Formula>,
    rows: Arc<AtomicUsize>,
}

impl Formula for Counted {
    fn result_type(&self) -> &ColumnType {
        self.formula.result_type()
    }
    fn columns(&self) -> &[String] {
        self.formula.columns()
    }
    fn column_arrays(&self) -> &[String] {
        self.formula.column_arrays()
    }
    fn uses_row_variables(&self) -> bool {
        self.formula.uses_row_variables()
    }
    fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure> {
        self.rows.fetch_add(rows.size() as usize, Ordering::SeqCst);
        self.formula.evaluate(table, rows, use_prev)
    }
}

impl FormulaCompiler for Counting {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        Compiler.parse(text)
    }
    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        Compiler.parse_assignment(text)
    }
    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        Ok(Arc::new(Counted {
            formula: Compiler.compile(expr, definition)?,
            rows: self.rows.clone(),
        }))
    }
}

/// A compiler for what the real one refuses so far: an expression that names a row
/// variable or a column array compiles to a formula that returns, as a long, the row key
/// for `k`, the row position for `i` and `ii`, and zero otherwise. Everything else goes to
/// the real compiler.
struct WithRowVariables;

struct RowFormula {
    columns: Vec<String>,
    arrays: Vec<String>,
    variable: Option<RowVariable>,
}

fn walk(expr: &Expr, f: &mut dyn FnMut(&Expr)) {
    f(expr);
    match expr {
        Expr::Binary { left, right, .. } => {
            walk(left, f);
            walk(right, f);
        }
        Expr::Unary { operand, .. } => walk(operand, f),
        Expr::ColumnArrayAccess { position, .. } => walk(position, f),
        _ => {}
    }
}

impl Formula for RowFormula {
    fn result_type(&self) -> &ColumnType {
        &ColumnType::Long
    }
    fn columns(&self) -> &[String] {
        &self.columns
    }
    fn column_arrays(&self) -> &[String] {
        &self.arrays
    }
    fn uses_row_variables(&self) -> bool {
        self.variable.is_some()
    }
    fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure> {
        let all = if use_prev {
            table.prev_row_set()
        } else {
            table.row_set()
        };
        let values = rows.iter().map(|key| match self.variable {
            Some(RowVariable::K) => key,
            Some(RowVariable::I | RowVariable::Ii) => {
                all.find(key).map_or(-1, |position| position as i64)
            }
            None => 0,
        });
        Ok(Arc::new(Int64Array::from_iter_values(values)))
    }
}

impl FormulaCompiler for WithRowVariables {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        Compiler.parse(text)
    }
    /// `Name = Column_[i - 1]`, which the real parser has no syntax for yet, is read as an
    /// access to `Column` at another row.
    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        if let Some((name, access)) = text.split_once('=') {
            if let Some((column, _)) = access.trim().split_once("_[") {
                return Ok(ColumnAssignment {
                    name: name.trim().to_string(),
                    expr: Expr::ColumnArrayAccess {
                        column: column.to_string(),
                        position: Box::new(Expr::RowVariable(RowVariable::I)),
                    },
                });
            }
        }
        Compiler.parse_assignment(text)
    }
    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        let mut formula = RowFormula {
            columns: Vec::new(),
            arrays: Vec::new(),
            variable: None,
        };
        walk(expr, &mut |node| match node {
            // `Column_` is the whole column as a vector.
            Expr::Column(name) if definition.index_of(name).is_none() => {
                formula.arrays.push(name.trim_end_matches('_').to_string())
            }
            Expr::Column(name) => formula.columns.push(name.clone()),
            Expr::ColumnVector(name) | Expr::ColumnArrayAccess { column: name, .. } => {
                formula.arrays.push(name.clone())
            }
            Expr::RowVariable(variable) => formula.variable = Some(*variable),
            _ => {}
        });
        if formula.variable.is_none() && formula.arrays.is_empty() {
            return Compiler.compile(expr, definition);
        }
        Ok(Arc::new(formula))
    }
}
