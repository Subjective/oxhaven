//! `NaturalJoin`, `ExactJoin` and `LeftJoin`: the left table's rows, each with columns of
//! the right row that has the same key.
//!
//! The three share Java's request checks: every match and every addition must be `Name` or
//! `Left=Right` (also `Left==Right`). The reference answers `LeftJoin` with `UNIMPLEMENTED`
//! once the request has passed those checks; the engine has no left join to call.

use super::prelude::*;
use oxhaven_engine::ops::join::{
    exact_join, natural_join, JoinAddition, JoinError, JoinMatch, NaturalJoinType,
};
use oxhaven_proto::grpc::natural_join_tables_request::JoinType;
use oxhaven_proto::grpc::{ExactJoinTablesRequest, LeftJoinTablesRequest, NaturalJoinTablesRequest};

/// What Java's join refuses is an exception out of the engine: the recorded results have
/// `INVALID_ARGUMENT` for every refusal, the missing columns, the clashing names, the key
/// types and the duplicate or missing right rows among them.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<JoinError> for ExportError {
    fn from(error: JoinError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// Java's `\s`: the characters the patterns of `MatchPairFactory` skip.
fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r')
}

/// The length of the column name at the start of `text` by the pattern `ID_PTRN`, an ASCII
/// letter, `_` or `$` followed by those or digits; zero if there is none.
///
/// Behavior: `io.deephaven.api.expression.SelectFactoryConstants#ID_PTRN`
fn name_length(text: &str) -> usize {
    let mut bytes = text.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' || b == b'$' => {}
        _ => return 0,
    }
    1 + bytes
        .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$')
        .count()
}

/// The names of `Name` or `Left=Right` (or `Left==Right`) with space around each part, or
/// `None` if `text` is neither. Java's `Matcher#matches` must consume the whole string, so
/// the `\Z` that ends the patterns does not let a final line terminator through, though
/// `\Z` alone would match before one (checked on OpenJDK 21: `"A\u2028"` is refused).
///
/// Behavior: the two patterns registered by
/// `io.deephaven.engine.table.impl.select.MatchPairFactory`, with `START_PTRN` and `END_PTRN`
/// of `io.deephaven.api.expression.SelectFactoryConstants`
fn match_names(text: &str) -> Option<(&str, &str)> {
    let text = text.trim_matches(is_space);
    let left = name_length(text);
    if left == 0 {
        return None;
    }
    let (left, rest) = text.split_at(left);
    if rest.is_empty() {
        return Some((left, left));
    }
    let rest = rest.trim_start_matches(is_space).strip_prefix('=')?;
    let rest = rest.strip_prefix('=').unwrap_or(rest);
    let rest = rest.trim_start_matches(is_space);
    let right = name_length(rest);
    (right > 0 && right == rest.len()).then_some((left, rest))
}

/// Reads a match or an addition as Java does, into the left (or new) name and the right
/// (or existing) name. A string that fits a pattern with a name that cannot name a column,
/// such as `in`, is refused with another message than one that fits no pattern; the status
/// is `INVALID_ARGUMENT` either way.
///
/// Behavior: `io.deephaven.engine.table.impl.select.MatchPairFactory#getExpression`,
/// `io.deephaven.api.expression.ExpressionParser#parse`,
/// `io.deephaven.engine.table.impl.MatchPair#MatchPair`, and the `catch` of
/// `io.deephaven.server.table.ops.JoinTablesGrpcImpl#validateRequest`
fn match_pair(text: &str) -> Result<(String, String), Status> {
    let refused = |reason: String| Status::invalid_argument(format!("{reason}: {text}"));
    let Some((left, right)) = match_names(text) else {
        return Err(refused(format!("Unable to parse expression: \"{text}\"")));
    };
    if validate_column_name(left).is_err() || validate_column_name(right).is_err() {
        return Err(refused(
            "Failed to get expression for all matched patterns".to_string(),
        ));
    }
    Ok((left.to_string(), right.to_string()))
}

/// The matches and additions of a request, read as Java reads them.
///
/// Behavior: `JoinTablesGrpcImpl#validateRequest` and the same reading in
/// `JoinTablesGrpcImpl#create`
fn parse(
    columns_to_match: &[String],
    columns_to_add: &[String],
) -> Result<(Vec<JoinMatch>, Vec<JoinAddition>), Status> {
    let on = columns_to_match
        .iter()
        .map(|text| match_pair(text).map(|(left, right)| JoinMatch::new(left, right)))
        .collect::<Result<Vec<_>, _>>()?;
    let add = columns_to_add
        .iter()
        .map(|text| match_pair(text).map(|(name, source)| JoinAddition::renamed(name, source)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((on, add))
}

/// What the three joins have in common: a result ticket, two sources and the column lists.
macro_rules! join_request {
    ($request:ty) => {
        type Request = $request;

        fn result_id(request: &$request) -> Option<&Ticket> {
            request.result_id.as_ref()
        }

        fn sources(request: &$request) -> Vec<TableReference> {
            vec![source(&request.left_id), source(&request.right_id)]
        }

        /// Behavior: `JoinTablesGrpcImpl#validateRequest`
        fn validate(request: &$request) -> Result<(), Status> {
            parse(&request.columns_to_match, &request.columns_to_add).map(|_| ())
        }
    };
}

/// Behavior: `io.deephaven.server.table.ops.JoinTablesGrpcImpl.NaturalJoinTablesGrpcImpl`
pub struct NaturalJoin;

impl TableOp for NaturalJoin {
    join_request!(NaturalJoinTablesRequest);

    /// The join type is read after the columns; a value the protocol does not define is an
    /// exception in Java, not a status.
    ///
    /// Behavior: `NaturalJoinTablesGrpcImpl#doJoin`, `NaturalJoinTablesGrpcImpl#adapt`
    fn create(
        _context: &OpContext,
        request: &NaturalJoinTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let (on, add) = parse(&request.columns_to_match, &request.columns_to_add)?;
        let join_type = match JoinType::try_from(request.join_type) {
            Ok(JoinType::NotSpecified | JoinType::ErrorOnDuplicate) => {
                NaturalJoinType::ErrorOnDuplicate
            }
            Ok(JoinType::FirstMatch) => NaturalJoinType::FirstMatch,
            Ok(JoinType::LastMatch) => NaturalJoinType::LastMatch,
            Ok(JoinType::ExactlyOneMatch) => NaturalJoinType::ExactlyOneMatch,
            Err(_) => {
                return Err(ExportError::Internal(
                    "Unsupported join type: UNRECOGNIZED".to_string(),
                ))
            }
        };
        Ok(natural_join(&sources[0], &sources[1], &on, &add, join_type)?)
    }
}

/// Behavior: `io.deephaven.server.table.ops.JoinTablesGrpcImpl.ExactJoinTablesGrpcImpl`
pub struct ExactJoin;

impl TableOp for ExactJoin {
    join_request!(ExactJoinTablesRequest);

    /// Behavior: `ExactJoinTablesGrpcImpl#doJoin`
    fn create(
        _context: &OpContext,
        request: &ExactJoinTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let (on, add) = parse(&request.columns_to_match, &request.columns_to_add)?;
        Ok(exact_join(&sources[0], &sources[1], &on, &add)?)
    }
}

/// Behavior: `io.deephaven.server.table.ops.JoinTablesGrpcImpl.LeftJoinTablesGrpcImpl`
pub struct LeftJoin;

impl TableOp for LeftJoin {
    join_request!(LeftJoinTablesRequest);

    /// Java reads the columns and waits for both sources, then refuses the join.
    ///
    /// Behavior: `JoinTablesGrpcImpl#create`, `LeftJoinTablesGrpcImpl#doJoin`
    fn create(
        _context: &OpContext,
        request: &LeftJoinTablesRequest,
        _sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        parse(&request.columns_to_match, &request.columns_to_add)?;
        Err(Status::unimplemented("LeftJoinTables is currently unimplemented").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn names(text: &str) -> Option<(String, String)> {
        match_pair(text).ok()
    }

    fn pair(left: &str, right: &str) -> Option<(String, String)> {
        Some((left.to_string(), right.to_string()))
    }

    #[test]
    fn a_match_is_a_name_or_two_names_joined_by_one_or_two_equals_signs() {
        assert_eq!(names("A"), pair("A", "A"));
        assert_eq!(names(" \tA \r\n"), pair("A", "A"));
        assert_eq!(names("A=B"), pair("A", "B"));
        assert_eq!(names("A==B"), pair("A", "B"));
        assert_eq!(names(" A \t= B "), pair("A", "B"));
        assert_eq!(names("A ==\nB"), pair("A", "B"));
        assert_eq!(names("$x_1=_y$2"), pair("$x_1", "_y$2"));
        assert_eq!(names("A\u{0B}"), pair("A", "A"));
        // what Java's patterns refuse, run on OpenJDK 21
        for refused in [
            "", " ", "=", "A=", "=B", "A===B", "A = = B", "A B", "A=B=C", "1A", "A=1B", "A-B",
            "Übung", "A\u{2028}", "A=B \u{85}", "A\u{2028}\u{2028}", "A\u{2028} ", "A\u{a0}",
            "A+B", "A=B C", "A\n\u{2028}", "A\u{1c}",
        ] {
            assert_eq!(names(refused), None, "{refused:?}");
        }
    }

    #[test]
    fn a_name_that_cannot_name_a_column_is_refused_with_another_message() {
        let unparsable = match_pair("A B").unwrap_err();
        assert_eq!(unparsable.code(), Code::InvalidArgument);
        assert_eq!(
            unparsable.message(),
            "Unable to parse expression: \"A B\": A B"
        );
        for reserved in ["in", "A=i", "class==B", "true"] {
            let status = match_pair(reserved).unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument);
            assert_eq!(
                status.message(),
                format!("Failed to get expression for all matched patterns: {reserved}")
            );
        }
    }
}
