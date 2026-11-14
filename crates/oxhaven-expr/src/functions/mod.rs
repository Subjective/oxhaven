//! The functions a formula can call by name.

/// A function of the formula library.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Function {
    /// Behavior: `io.deephaven.function.Basic#isNull`, for every argument type: true exactly
    /// when the argument is null.
    IsNull,
}

/// The function registered under `name`.
pub(crate) fn lookup(name: &str) -> Option<Function> {
    match name {
        "isNull" => Some(Function::IsNull),
        _ => None,
    }
}
