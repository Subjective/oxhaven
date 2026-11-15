//! Functions a formula may call that are not the language's own: the embedder supplies
//! them, and the compiler asks for one when the function library cannot take a call.
//!
//! No Java counterpart as a type. Java's parser looks a call up among the static imports
//! and then among the variables of the query scope that can be called
//! (`QueryLanguageParser#getMethod`); this is the second half of that, with the variables
//! left to whoever holds them. `docs/PYTHON_UDF.md` has the design.

use arrow::array::ArrayRef;
use oxhaven_contracts::expr::{FormulaError, Literal};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::fmt;
use std::sync::Arc;

/// One argument of a call, as the compiler knows it when it asks for the function.
#[derive(Clone, Copy, Debug)]
pub struct ExternalArgument<'a> {
    /// The type of the argument; `None` for the `null` literal.
    pub column_type: Option<&'a ColumnType>,
    /// The literal, when the argument is nothing but one.
    pub literal: Option<&'a Literal>,
}

/// A function of the embedder, bound to the argument types of one call.
pub trait ExternalFunction: Send + Sync {
    /// The type of the column the call gives.
    fn result_type(&self) -> &ColumnType;

    /// Calls the function for `rows` rows, in order. Each argument is an array of `rows`
    /// values of the type the call was resolved with, null where the value is null; the
    /// `null` literal is an array of Arrow's null type. The result has `rows` values of
    /// [`ExternalFunction::result_type`]'s Arrow type.
    fn call(&self, arguments: &[ArrayRef], rows: usize) -> Result<ArrayRef, TableFailure>;
}

/// The functions of the embedder, by name.
pub trait ExternalFunctions: Send + Sync {
    /// The function `name` for a call with `arguments`; `None` when there is no function
    /// of that name, and an error when there is one that cannot take the call. Asked only
    /// for a name that is no column of the table and that the function library has no
    /// function for which takes the arguments.
    fn resolve(
        &self,
        name: &str,
        arguments: &[ExternalArgument<'_>],
    ) -> Option<Result<Arc<dyn ExternalFunction>, FormulaError>>;
}

/// A resolved function as a node of the typed tree: equal only to itself.
#[derive(Clone)]
pub(crate) struct Bound(pub(crate) Arc<dyn ExternalFunction>);

impl PartialEq for Bound {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for Bound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "external function -> {:?}", self.0.result_type())
    }
}
