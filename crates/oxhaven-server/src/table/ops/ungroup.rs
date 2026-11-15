//! `Ungroup`: a table with array columns replaced by their elements, a row for every
//! element.

use super::prelude::*;
use oxhaven_engine::ops::ungroup::{ungroup, UngroupError};
use oxhaven_proto::grpc::UngroupRequest;

/// Whatever `ungroup` refuses is an exception out of the engine in Java, which has no
/// check of its own: a column that does not exist or is not an array, and arrays of one
/// row that differ in size without `null_fill`, are all `INVALID_ARGUMENT` with an error
/// id.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<UngroupError> for ExportError {
    fn from(error: UngroupError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// Reads every string as a column name, as it is: the first that cannot name a column is
/// the error, with the message of Java's exception.
///
/// Behavior: `io.deephaven.api.ColumnName#of`
fn column_names(names: &[String]) -> Result<(), String> {
    names.iter().try_for_each(|name| validate_column_name(name))
}

/// Behavior: `io.deephaven.server.table.ops.UngroupGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = UngroupRequest;

    fn result_id(request: &UngroupRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &UngroupRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Java reads all the names before it asks the engine, so a string that cannot name a
    /// column is the error even after a name that the table does not have. Like the
    /// refusals of the engine it is an exception in Java, not a status, so the client
    /// sees the same answer for both and the log tells them apart. Without names, every
    /// array column is ungrouped.
    ///
    /// Behavior: `UngroupGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &UngroupRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let names = &request.columns_to_ungroup;
        column_names(names).map_err(ExportError::Internal)?;
        Ok(ungroup(&sources[0], request.null_fill, names)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn the_first_string_that_cannot_name_a_column_is_the_error() {
        assert_eq!(column_names(&[]), Ok(()));
        // Whether the table has the column is the engine's to say.
        assert_eq!(column_names(&names(&["A", "Nope", "A"])), Ok(()));
        let refused = |columns: &[&str]| column_names(&names(columns)).unwrap_err();
        assert_eq!(refused(&["A", "1 bad", ""]), "Invalid column name \"1 bad\"");
        assert_eq!(
            refused(&["", "1 bad"]),
            "Invalid column name: name can not be null or empty"
        );
        // A name is not trimmed.
        assert_eq!(refused(&[" A "]), "Invalid column name \" A \"");
        assert_eq!(
            refused(&["int"]),
            "Invalid column name \"int\": \"int\" is a reserved keyword"
        );
    }
}
