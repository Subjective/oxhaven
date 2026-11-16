//! Formula parser, type checker, and vectorized evaluator.
//!
//! [`Compiler`] implements the `FormulaCompiler` contract. Text is parsed to the contract's
//! expression tree ([`parse`]), the tree is type-checked against a table definition
//! ([`types`]), and the typed tree is evaluated over Arrow arrays ([`eval`]).

mod eval;
mod external;
mod functions;
mod parse;
mod types;

use oxhaven_contracts::expr::{ColumnAssignment, Expr, Formula, FormulaCompiler, FormulaError};
use oxhaven_contracts::schema::TableDefinition;
use std::sync::Arc;

pub use external::{ExternalArgument, ExternalFunction, ExternalFunctions};

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
        let checked = types::check(expr, definition, None)?;
        Ok(Arc::new(eval::CompiledFormula::new(checked)))
    }
}

/// The formula language as a table operation of a client may use it: [`Compiler`], without
/// the calls Java's server refuses to run for a client.
///
/// Java computes `Math.sin(X)` in a script of the console and refuses it in an operation
/// sent over gRPC, where every formula first passes the server's
/// `ColumnExpressionValidator`. With the lists the server ships
/// (`ColumnExpressionValidator.allowedMethods.*` in `dh-defaults.prop`) a formula may call
/// the default imports and any method of the boxed classes and of `String`, and no method
/// of `java.lang.Math` or `StrictMath`.
///
/// Behavior: `io.deephaven.server.table.validation.ParsingColumnExpressionValidator#validateInvocations`,
/// whose message the error carries.
#[derive(Clone, Copy, Default, Debug)]
pub struct RequestCompiler;

impl FormulaCompiler for RequestCompiler {
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
        let checked = types::check(expr, definition, None)?;
        if let Some(refusal) = &checked.refused_to_clients {
            return Err(FormulaError::Type(format!(
                "java.lang.IllegalStateException: {refusal}"
            )));
        }
        Ok(Arc::new(eval::CompiledFormula::new(checked)))
    }
}

impl Compiler {
    /// The formula language with the functions of `functions` beside its own: a call the
    /// function library cannot take is one of theirs, if they have the name.
    pub fn with_functions(functions: Arc<dyn ExternalFunctions>) -> ExtendedCompiler {
        ExtendedCompiler { functions }
    }
}

/// The formula language and an embedder's functions; see [`Compiler::with_functions`].
#[derive(Clone)]
pub struct ExtendedCompiler {
    functions: Arc<dyn ExternalFunctions>,
}

impl FormulaCompiler for ExtendedCompiler {
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
        let checked = types::check(expr, definition, Some(self.functions.as_ref()))?;
        Ok(Arc::new(eval::CompiledFormula::new(checked)))
    }
}
