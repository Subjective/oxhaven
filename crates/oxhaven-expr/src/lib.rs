//! Formula parser, type checker, and vectorized evaluator.
//!
//! [`Compiler`] implements the `FormulaCompiler` contract. Text is parsed to the contract's
//! expression tree ([`parse`]), the tree is type-checked against a table definition
//! ([`types`]), and the typed tree is evaluated over Arrow arrays ([`eval`]).

mod eval;
mod functions;
mod parse;
mod types;

use oxhaven_contracts::expr::{ColumnAssignment, Expr, Formula, FormulaCompiler, FormulaError};
use oxhaven_contracts::schema::TableDefinition;
use std::sync::Arc;

/// The formula language.
#[derive(Clone, Copy, Default, Debug)]
pub struct Compiler;

impl FormulaCompiler for Compiler {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        parse::parse_expression(text)
    }

    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        parse::parse_assignment(text)
    }

    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        let checked = types::check(expr, definition)?;
        Ok(Arc::new(eval::CompiledFormula::new(checked)))
    }
}
