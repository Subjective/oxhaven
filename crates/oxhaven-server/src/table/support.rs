//! What operation handlers share: the context they run in, request helpers, name and column
//! checks, formula compilation, and the mapping from engine errors to the status codes the
//! Java server answers with.

use crate::session::ExportError;
use oxhaven_contracts::expr::{ColumnAssignment, Formula, FormulaCompiler, FormulaError};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_engine::ops::filter::FilterError;
use oxhaven_engine::ops::merge::MergeError;
use oxhaven_engine::ops::select::SelectError;
use oxhaven_engine::ops::sort::SortError;
use oxhaven_engine::table::TimeSource;
use oxhaven_proto::grpc::TableReference;
use std::sync::Arc;
use tonic::{Code, Status};

/// What every operation runs with.
#[derive(Clone)]
pub struct OpContext {
    /// The server's update graph: the graph of every refreshing table an operation creates.
    pub graph: Arc<dyn UpdateGraph>,
    /// The time, for tables that read it.
    pub clock: Arc<dyn TimeSource>,
    /// The formula language.
    pub compiler: Arc<dyn FormulaCompiler>,
}

/// The status of a failure that is not the client's to read: the text goes to the log under
/// a new id and the client is told the id. This is what the Java server answers when a
/// request handler throws anything but a status. Inside `create`, return the error instead
/// (`?` on a `TableFailure`, or `ExportError::Internal`): the export does the same.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
pub fn internal_status(message: impl std::fmt::Display) -> Status {
    let error_id = uuid::Uuid::new_v4();
    tracing::error!("Internal Error '{error_id}': {message}");
    Status::invalid_argument(format!("Details Logged w/ID '{error_id}'"))
}

/// The table reference of a request field, or the reference that names nothing if the
/// field is unset, which the service rejects as Java does.
pub fn source(reference: &Option<TableReference>) -> TableReference {
    reference.clone().unwrap_or_default()
}

/// A formula that was not accepted is, like any exception out of the Java engine, an
/// internal failure: the client is told `INVALID_ARGUMENT` and where the details were
/// logged.
impl From<FormulaError> for ExportError {
    fn from(error: FormulaError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// What an engine operator refuses is, in Java, an exception out of the engine, and the
/// Java server answers every such exception the same way: `INVALID_ARGUMENT` with an error
/// id, the text in the log. This is the one place that decides it, so a handler writes `?`
/// on an operator's result. A refusal that Java answers with a status of its own (a sort
/// column that does not exist, a negative row count) is checked by the handler before the
/// operator is called.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
macro_rules! engine_errors_are_internal {
    ($($error:ty),+) => {
        $(
            impl From<$error> for ExportError {
                fn from(error: $error) -> Self {
                    ExportError::Internal(error.to_string())
                }
            }
        )+
    };
}

engine_errors_are_internal!(SelectError, SortError, MergeError, FilterError);

/// The status of a request that reached code which has not been ported. `panic` is what a
/// handler panicked with; the result is `None` unless it is the message of an `unported!`
/// call, which names the Java behavior that is missing.
///
/// Java has the behavior, so this status has no Java counterpart. It exists so that a
/// request the engine cannot serve yet is refused by name and the server keeps running.
pub(super) fn unported_status(panic: &(dyn std::any::Any + Send)) -> Option<Status> {
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())?;
    let missing = message.strip_prefix("unported: ")?;
    Some(Status::unimplemented(format!("not ported: {missing}")))
}

/// A request for something that has not been ported, refused before the engine is asked.
/// `java` names the Java behavior that is missing.
pub fn not_ported(java: &str, what: impl std::fmt::Display) -> Status {
    Status::unimplemented(format!("not ported: {java}: {what}"))
}

/// Parses and compiles a formula expression, such as a filter, against `definition`.
pub fn compile(
    context: &OpContext,
    text: &str,
    definition: &TableDefinition,
) -> Result<Arc<dyn Formula>, ExportError> {
    let expr = context.compiler.parse(text)?;
    Ok(context.compiler.compile(&expr, definition)?)
}

/// Parses a column assignment, such as `Mid = (Bid + Ask) / 2`. Compile its expression
/// with `context.compiler.compile` against the definition it is evaluated in, which for
/// `update` grows with each assignment.
pub fn parse_assignment(context: &OpContext, text: &str) -> Result<ColumnAssignment, ExportError> {
    Ok(context.compiler.parse_assignment(text)?)
}

/// The names in `names` that are not columns of `definition`, in the order given.
pub fn missing_columns<'a>(
    definition: &TableDefinition,
    names: impl IntoIterator<Item = &'a String>,
) -> Vec<&'a str> {
    names
        .into_iter()
        .filter(|name| definition.index_of(name).is_none())
        .map(String::as_str)
        .collect()
}

/// Fails with `FAILED_PRECONDITION` and `column(s) not found: A, B` unless every name is a
/// column of `definition`.
///
/// Use it where the Java handler makes this check itself. Where Java leaves a missing
/// column to the engine, the engine's error is the one to return (`INVALID_ARGUMENT`).
///
/// Behavior: `io.deephaven.server.table.ops.SortTableGrpcImpl#create`,
/// `io.deephaven.server.table.ops.SelectDistinctGrpcImpl#create`
pub fn require_columns<'a>(
    definition: &TableDefinition,
    names: impl IntoIterator<Item = &'a String>,
) -> Result<(), Status> {
    let missing = missing_columns(definition, names);
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "column(s) not found: {}",
            missing.join(", ")
        )))
    }
}

/// Java's reserved words and literals: `javax.lang.model.SourceVersion#isKeyword` for the
/// latest source version.
const JAVA_KEYWORDS: &[&str] = &[
    "_",
    "abstract",
    "assert",
    "boolean",
    "break",
    "byte",
    "case",
    "catch",
    "char",
    "class",
    "const",
    "continue",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "extends",
    "false",
    "final",
    "finally",
    "float",
    "for",
    "goto",
    "if",
    "implements",
    "import",
    "instanceof",
    "int",
    "interface",
    "long",
    "native",
    "new",
    "null",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "short",
    "static",
    "strictfp",
    "super",
    "switch",
    "synchronized",
    "this",
    "throw",
    "throws",
    "transient",
    "true",
    "try",
    "void",
    "volatile",
    "while",
];

/// The names the query language keeps for itself.
const QUERY_RESERVED: &[&str] = &["in", "not", "i", "ii", "k"];

/// Checks that `name` can name a column: a Java identifier that is not a reserved word of
/// Java or of the query language. The error is the message of Java's
/// `InvalidNameException`; the caller chooses the status, since Java handlers differ.
///
/// For names outside ASCII, "letter" and "digit" are Unicode's alphabetic and numeric
/// properties, which is close to but not exactly `Character#isJavaIdentifierPart`.
///
/// Behavior: `io.deephaven.api.util.NameValidator#validateColumnName`
pub fn validate_column_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Invalid column name: name can not be null or empty".to_string());
    }
    if QUERY_RESERVED.contains(&name) || JAVA_KEYWORDS.contains(&name) {
        return Err(format!(
            "Invalid column name \"{name}\": \"{name}\" is a reserved keyword"
        ));
    }
    let mut chars = name.chars();
    let starts = chars
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_' || c == '$');
    if !starts || !chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$') {
        return Err(format!("Invalid column name \"{name}\""));
    }
    Ok(())
}

/// A status code as gRPC spells it, which is how it appears in Java's exception messages.
pub(super) fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxhaven_contracts::schema::{ColumnDefinition, ColumnType};

    #[test]
    fn column_names_are_java_identifiers_that_are_not_reserved() {
        for good in ["A", "a1", "_x", "$y", "Price_2", "In", "K", "Übung"] {
            assert_eq!(validate_column_name(good), Ok(()), "{good}");
        }
        assert_eq!(
            validate_column_name(""),
            Err("Invalid column name: name can not be null or empty".to_string())
        );
        for reserved in ["i", "ii", "k", "in", "not", "class", "true", "null", "_"] {
            assert_eq!(
                validate_column_name(reserved),
                Err(format!(
                    "Invalid column name \"{reserved}\": \"{reserved}\" is a reserved keyword"
                ))
            );
        }
        for bad in ["1a", "a b", "a-b", "a.b", " a", "a+"] {
            assert_eq!(
                validate_column_name(bad),
                Err(format!("Invalid column name \"{bad}\""))
            );
        }
    }

    #[test]
    fn missing_columns_are_failed_precondition_and_listed_in_request_order() {
        let definition = TableDefinition::new(vec![
            ColumnDefinition::new("A", ColumnType::Int),
            ColumnDefinition::new("B", ColumnType::Int),
        ])
        .unwrap();
        let names = ["Z".to_string(), "B".to_string(), "Y".to_string()];
        assert_eq!(missing_columns(&definition, &names), vec!["Z", "Y"]);
        let status = require_columns(&definition, &names).unwrap_err();
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), "column(s) not found: Z, Y");
        assert!(require_columns(&definition, &names[1..2]).is_ok());
    }
}
