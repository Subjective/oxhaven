//! `SelectDistinct`: the distinct combinations of values of some columns, in the order in
//! which they first appear. `WhereIn`: the rows of a table whose key is, or with `inverted`
//! is not, among the keys of another table.

use super::prelude::*;
use oxhaven_engine::ops::distinct::{
    select_distinct, where_in, where_not_in, DistinctError, WhereInError,
};
use oxhaven_engine::ops::join::JoinMatch;
use oxhaven_proto::grpc::{SelectDistinctRequest, WhereInRequest};

/// The handler checks the columns itself, so the engine's `NoSuchColumn` is not expected;
/// it gets the handler's status all the same. Anything else is an exception out of the
/// engine.
///
/// Behavior: `io.deephaven.server.table.ops.SelectDistinctGrpcImpl#create`
impl From<DistinctError> for ExportError {
    fn from(error: DistinctError) -> Self {
        match error {
            DistinctError::NoSuchColumn(_) => {
                ExportError::Status(Status::failed_precondition(error.to_string()))
            }
            DistinctError::Table(_) => ExportError::Internal(error.to_string()),
        }
    }
}

/// Behavior: `io.deephaven.server.table.ops.SelectDistinctGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = SelectDistinctRequest;

    fn result_id(request: &SelectDistinctRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &SelectDistinctRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Every name must be a column of the source: Java refuses expressions such as `X = I`
    /// here, with the names that are not columns, before the engine is asked.
    ///
    /// Behavior: `SelectDistinctGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &SelectDistinctRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let parent = &sources[0];
        require_columns(parent.definition(), &request.column_names)?;
        Ok(select_distinct(parent, &request.column_names)?)
    }
}

/// What Java's `whereIn` refuses is an exception out of the engine: the recorded results
/// have `INVALID_ARGUMENT` for the missing columns and for the key types.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<WhereInError> for ExportError {
    fn from(error: WhereInError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// A pair of match columns as the request gives it: `Name`, `Left=Right` or `Left==Right`,
/// split at the first `=`, each side trimmed and checked as a column name. A string with
/// nothing after its `=` or `==`, or with a side that cannot name a column, is an
/// exception in Java, not a status.
///
/// Behavior: `io.deephaven.api.JoinMatch#parse`, `io.deephaven.api.ColumnName#parse`
fn join_match(text: &str) -> Result<JoinMatch, String> {
    let Some((left, right)) = text.split_once('=') else {
        let name = java_trim(text);
        validate_column_name(name)?;
        return Ok(JoinMatch::same(name));
    };
    let right = right.strip_prefix('=').unwrap_or(right);
    if right.is_empty() {
        return Err(format!(
            "Unable to parse match '{text}', expected form '<left>==<right>' or `<left>=<right>`"
        ));
    }
    let (left, right) = (java_trim(left), java_trim(right));
    validate_column_name(left)?;
    validate_column_name(right)?;
    Ok(JoinMatch::new(left, right))
}

/// Behavior: `io.deephaven.server.table.ops.WhereInGrpcImpl`
pub struct WhereIn;

impl TableOp for WhereIn {
    type Request = WhereInRequest;

    fn result_id(request: &WhereInRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    /// The left table, then the table of keys.
    ///
    /// Behavior: `WhereInGrpcImpl#refs`
    fn sources(request: &WhereInRequest) -> Vec<TableReference> {
        vec![source(&request.left_id), source(&request.right_id)]
    }

    /// Both tables and at least one pair of match columns. Java also refuses a request
    /// with unknown fields; prost drops them, so that check cannot be made here.
    ///
    /// Behavior: `WhereInGrpcImpl#validateRequest`,
    /// `io.deephaven.server.grpc.GrpcErrorHelper#checkRepeatedFieldNonEmpty`
    fn validate(request: &WhereInRequest) -> Result<(), Status> {
        let Some(left) = &request.left_id else {
            return Err(missing_field("WhereInRequest", "left_id", 2));
        };
        let Some(right) = &request.right_id else {
            return Err(missing_field("WhereInRequest", "right_id", 3));
        };
        if request.columns_to_match.is_empty() {
            return Err(Status::invalid_argument(
                "io.deephaven.proto.backplane.grpc.WhereInRequest must have at least one \
                 columns_to_match (5)",
            ));
        }
        validate_reference(left)?;
        validate_reference(right)
    }

    /// The match columns are read here, not in `validate`: a string that is no match is
    /// an exception out of `create` in Java, answered like one out of the engine.
    ///
    /// Behavior: `WhereInGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &WhereInRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let on = request
            .columns_to_match
            .iter()
            .map(|text| join_match(text))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExportError::Internal)?;
        let (left, right) = (&sources[0], &sources[1]);
        Ok(if request.inverted {
            where_not_in(left, right, &on)?
        } else {
            where_in(left, right, &on)?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(left: &str, right: &str) -> Result<JoinMatch, String> {
        Ok(JoinMatch::new(left, right))
    }

    #[test]
    fn a_match_is_a_name_or_two_names_around_one_or_two_equals_signs() {
        assert_eq!(join_match("A"), pair("A", "A"));
        assert_eq!(join_match(" A\t"), pair("A", "A"));
        assert_eq!(join_match("A=B"), pair("A", "B"));
        assert_eq!(join_match("A==B"), pair("A", "B"));
        assert_eq!(join_match(" A = B "), pair("A", "B"));
        assert_eq!(join_match("A == B"), pair("A", "B"));
        for refused in ["A=", "A==", "A= ", "=B", "", " ", "A===B", "A=B=C", "A B", "1A", "A=1B"] {
            assert!(join_match(refused).is_err(), "{refused:?}");
        }
        let message = join_match("A==").unwrap_err();
        assert!(message.starts_with("Unable to parse match 'A=='"), "{message}");
    }
}
