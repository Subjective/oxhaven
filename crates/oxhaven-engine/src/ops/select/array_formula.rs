//! A formula evaluated over columns that are given as arrays and belong to no table.
//!
//! This is the entry point for the operators that hand a formula the values of a group or
//! of a window as one vector: formula aggregation (`AggSpecFormula`, `Aggregation.Formula`)
//! and the rolling formula of `update_by`. Such an operator compiles its formula once
//! against the names and types it gives the formula's inputs, a vector type for each input
//! that holds a group or a window, and then evaluates it over one `ListArray` per input
//! with one entry per group or per row: the same chunk a vector column of a table gives.
//!
//! `update`, `select`, `view` and `where` need none of this: a formula over a vector
//! column of a table is evaluated like any other.

use super::sources::FormulaInput;
use arrow::array::ArrayRef;
use oxhaven_column::FlatSource;
use oxhaven_contracts::expr::{Expr, Formula, FormulaCompiler, FormulaError};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::Arc;

/// A formula compiled against named inputs, evaluated over arrays of their values.
pub struct ArrayFormula {
    formula: Arc<dyn Formula>,
    definition: TableDefinition,
}

impl ArrayFormula {
    /// Compiles `expression` for inputs with the names and types of `inputs`, in the order
    /// [`evaluate`](Self::evaluate) takes their values.
    ///
    /// An input that holds the values of a group or a window has the type
    /// `ColumnType::vector(element)`, which is the type Java gives it: `each` in
    /// `sum(each)` is an `IntVector` for an `int` column.
    ///
    /// A formula that uses `i`, `ii` or `k` is refused: the inputs have no rows of a table.
    pub fn compile(
        compiler: &dyn FormulaCompiler,
        expression: &str,
        inputs: &[(&str, ColumnType)],
    ) -> Result<Self, FormulaError> {
        Self::compile_parsed(compiler, &compiler.parse(expression)?, inputs)
    }

    /// [`compile`](Self::compile) of an expression already parsed.
    pub fn compile_parsed(
        compiler: &dyn FormulaCompiler,
        expression: &Expr,
        inputs: &[(&str, ColumnType)],
    ) -> Result<Self, FormulaError> {
        let columns = inputs
            .iter()
            .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
            .collect();
        let definition =
            TableDefinition::new(columns).map_err(|error| FormulaError::Type(error.to_string()))?;
        let formula = compiler.compile(expression, &definition)?;
        if formula.uses_row_variables() || !formula.column_arrays().is_empty() {
            return Err(FormulaError::Unsupported(
                "i, ii, k or a column array in a formula over the values of groups or windows"
                    .to_string(),
            ));
        }
        Ok(Self {
            formula,
            definition,
        })
    }

    pub fn result_type(&self) -> &ColumnType {
        self.formula.result_type()
    }

    /// The names of the inputs the formula reads.
    pub fn inputs_read(&self) -> &[String] {
        self.formula.columns()
    }

    /// The formula's value for each position of `values`, which holds one array per input
    /// of [`compile`](Self::compile), all of one length and each of the Arrow type of its
    /// input's column type. A vector input is a `ListArray` with one entry per group or
    /// window, as `ColumnType::list_element` describes a chunk of a vector column.
    pub fn evaluate(&self, values: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
        let definitions = self.definition.columns();
        if values.len() != definitions.len() {
            return Err(TableFailure {
                message: format!(
                    "a formula of {} inputs was given {} arrays",
                    definitions.len(),
                    values.len()
                ),
            });
        }
        let length = values.first().map_or(0, |values| values.len());
        if values.iter().any(|values| values.len() != length) {
            return Err(TableFailure {
                message: "the inputs of a formula have different lengths".to_string(),
            });
        }
        let sources = definitions
            .iter()
            .zip(values)
            .map(|(definition, values)| {
                FlatSource::new(definition.column_type.clone(), values.clone())
                    .map(|source| Arc::new(source) as Arc<dyn ColumnSource>)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = RowSet::flat(length as u64);
        let input = FormulaInput::fixed(rows.clone(), self.definition.clone(), sources);
        self.formula.evaluate(&input, &rows, false)
    }
}
