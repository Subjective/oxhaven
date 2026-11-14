//! `Aj`, `Raj` and the deprecated `AsOfJoin`: the left table's rows, each with columns of
//! the right row whose stamp is closest to its own on one side.
//!
//! `Aj` and `Raj` share a request and a handler in Java and differ in the rules they take:
//! `>=` and `>` for `Aj`, `<=` and `<` for `Raj`. Their columns are read by the parsers of
//! `io.deephaven.api`, which are not those of the other joins: a match is split at its
//! first `=` and each side is trimmed and checked as a column name, so the messages, and
//! in places the strings accepted, differ from `NaturalJoin`'s.
//!
//! `AsOfJoin` is one of the joins of `JoinTablesGrpcImpl`: its columns are read as
//! `NaturalJoin`'s are, the last match names the stamp columns, and the rule is a field of
//! the request, which Java reads the other way around.

use super::join::{is_space, name_length, parse as match_pairs};
use super::prelude::*;
use oxhaven_engine::ops::asof::{
    as_of_join, AsOfJoinError, AsOfMatch, AsOfRule, JoinAddition, JoinMatch,
};
use oxhaven_proto::grpc::as_of_join_tables_request::MatchRule;
use oxhaven_proto::grpc::{AjRajTablesRequest, AsOfJoinTablesRequest};

/// What Java's as-of join refuses is an exception out of the engine: the recorded results
/// have `INVALID_ARGUMENT` for every refusal.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<AsOfJoinError> for ExportError {
    fn from(error: AsOfJoinError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// `text` trimmed, if it can name a column; otherwise the message of Java's
/// `InvalidNameException`, which has the trimmed text.
///
/// Behavior: `io.deephaven.api.ColumnName#parse`
fn column_name(text: &str) -> Result<&str, String> {
    let name = java_trim(text);
    validate_column_name(name)?;
    Ok(name)
}

/// An exact match, `Name`, `Left=Right` or `Left==Right`. The text is split at its first
/// `=`, so `A===B` is refused for the name `=B`.
///
/// Behavior: `io.deephaven.api.JoinMatch#parse`
fn join_match(text: &str) -> Result<JoinMatch, String> {
    let Some((left, right)) = text.split_once('=') else {
        return column_name(text).map(JoinMatch::same);
    };
    let right = right.strip_prefix('=').unwrap_or(right);
    if right.is_empty() {
        return Err(format!(
            "Unable to parse match '{text}', expected form '<left>==<right>' or `<left>=<right>`"
        ));
    }
    Ok(JoinMatch::new(column_name(left)?, column_name(right)?))
}

/// An added column, `Name` or `Name=Source`, split at its first `=`. `A==B` is refused for
/// the name `=B`.
///
/// Behavior: `io.deephaven.api.JoinAddition#parse`
fn join_addition(text: &str) -> Result<JoinAddition, String> {
    let Some((name, source)) = text.split_once('=') else {
        return column_name(text).map(JoinAddition::same);
    };
    Ok(JoinAddition::renamed(
        column_name(name)?,
        column_name(source)?,
    ))
}

/// The pattern of an as-of match as Java prints it in its refusal.
///
/// Behavior: `io.deephaven.api.AsOfJoinMatch#EXPRESSION_PATTERN`
const AS_OF_PATTERN: &str =
    r"\A\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*(>=?|<=?)\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\Z";

/// The parts of `Left >= Right`, with any of the four rules and space around each part, or
/// `None` if `text` is not of that form. As with the other joins, Java's `Matcher#matches`
/// must consume the whole string.
///
/// Behavior: `io.deephaven.api.AsOfJoinMatch#EXPRESSION_PATTERN`,
/// `io.deephaven.api.AsOfJoinRule#parse`
fn as_of_parts(text: &str) -> Option<(&str, AsOfRule, &str)> {
    let text = text.trim_matches(is_space);
    let (left, rest) = text.split_at(name_length(text));
    if left.is_empty() {
        return None;
    }
    let rest = rest.trim_start_matches(is_space);
    let (rule, rest) = if let Some(rest) = rest.strip_prefix(">=") {
        (AsOfRule::GreaterThanEqual, rest)
    } else if let Some(rest) = rest.strip_prefix("<=") {
        (AsOfRule::LessThanEqual, rest)
    } else if let Some(rest) = rest.strip_prefix('>') {
        (AsOfRule::GreaterThan, rest)
    } else {
        (AsOfRule::LessThan, rest.strip_prefix('<')?)
    };
    let right = rest.trim_start_matches(is_space);
    (!right.is_empty() && name_length(right) == right.len()).then_some((left, rule, right))
}

/// An as-of match, `Left >= Right` with any of the four rules. A name that fits the
/// pattern and cannot name a column, such as `in`, is refused with the message of the
/// name.
///
/// Behavior: `io.deephaven.api.AsOfJoinMatch#parse`
fn as_of_match(text: &str) -> Result<AsOfMatch, String> {
    let Some((left, rule, right)) = as_of_parts(text) else {
        return Err(format!(
            "Input expression {text} does not match expected pattern {AS_OF_PATTERN}"
        ));
    };
    validate_column_name(left)?;
    validate_column_name(right)?;
    Ok(AsOfMatch::new(left, rule, right))
}

/// The name of a rule in Java's messages.
///
/// Behavior: `io.deephaven.api.AsOfJoinRule`
fn rule_name(rule: AsOfRule) -> &'static str {
    match rule {
        AsOfRule::LessThanEqual => "LESS_THAN_EQUAL",
        AsOfRule::LessThan => "LESS_THAN",
        AsOfRule::GreaterThanEqual => "GREATER_THAN_EQUAL",
        AsOfRule::GreaterThan => "GREATER_THAN",
    }
}

/// The two operations of `AjRajTablesRequest`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Direction {
    Aj,
    Raj,
}

/// The as-of match of `Aj` or `Raj`: a column name alone, trimmed as Java's `String#trim`
/// trims, is that column in both tables under the operation's inclusive rule; anything
/// else is read as an as-of match and must have a rule of the operation.
///
/// Behavior: `io.deephaven.api.AsOfJoinMatch#parseForAj`, `#parseForRaj`
fn as_of_match_for(text: &str, direction: Direction) -> Result<AsOfMatch, String> {
    let (inclusive, operation) = match direction {
        Direction::Aj => (AsOfRule::GreaterThanEqual, "aj"),
        Direction::Raj => (AsOfRule::LessThanEqual, "raj"),
    };
    if let Ok(name) = column_name(text) {
        return Ok(AsOfMatch::new(name, inclusive, name));
    }
    let parsed = as_of_match(text)?;
    if parsed.rule.is_aj() != (direction == Direction::Aj) {
        return Err(format!(
            "Unsupported operation for {operation}: {}",
            rule_name(parsed.rule)
        ));
    }
    Ok(parsed)
}

/// The columns of an `Aj` or `Raj` request, read in Java's order: the exact matches, the
/// as-of match, the additions. The first that cannot be read is the error.
///
/// Behavior: the `try` of `io.deephaven.server.table.ops.AjRajGrpcImpl#validateRequest`,
/// and the same reading in `AjRajGrpcImpl#create`
fn aj_raj_columns(
    request: &AjRajTablesRequest,
    direction: Direction,
) -> Result<(Vec<JoinMatch>, AsOfMatch, Vec<JoinAddition>), Status> {
    let read = || -> Result<_, String> {
        let exact = request
            .exact_match_columns
            .iter()
            .map(|text| join_match(text))
            .collect::<Result<Vec<_>, _>>()?;
        let as_of = as_of_match_for(&request.as_of_column, direction)?;
        let add = request
            .columns_to_add
            .iter()
            .map(|text| join_addition(text))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((exact, as_of, add))
    };
    read().map_err(Status::invalid_argument)
}

/// Java checks that the request has both tables and an as-of column, then the shape of
/// the two references, then the columns. It also refuses unknown fields, between the
/// first and the second of these; prost drops them, so that check cannot be made here.
///
/// Behavior: `AjRajGrpcImpl#validateRequest`
fn aj_raj_validate(request: &AjRajTablesRequest, direction: Direction) -> Result<(), Status> {
    let Some(left) = &request.left_id else {
        return Err(missing_field("AjRajTablesRequest", "left_id", 2));
    };
    let Some(right) = &request.right_id else {
        return Err(missing_field("AjRajTablesRequest", "right_id", 3));
    };
    if request.as_of_column.is_empty() {
        return Err(missing_field("AjRajTablesRequest", "as_of_column", 5));
    }
    validate_reference(left)?;
    validate_reference(right)?;
    aj_raj_columns(request, direction).map(|_| ())
}

/// Behavior: `AjRajGrpcImpl#create`
fn aj_raj_create(
    request: &AjRajTablesRequest,
    direction: Direction,
    sources: &[Arc<dyn Table>],
) -> Result<Arc<dyn Table>, ExportError> {
    let (exact, as_of, add) = aj_raj_columns(request, direction)?;
    Ok(as_of_join(&sources[0], &sources[1], &exact, &as_of, &add)?)
}

/// `Aj` and `Raj`: one request, one handler, two sets of rules.
macro_rules! aj_raj {
    ($(#[$doc:meta])* $handler:ident => $direction:expr) => {
        $(#[$doc])*
        pub struct $handler;

        impl TableOp for $handler {
            type Request = AjRajTablesRequest;

            fn result_id(request: &AjRajTablesRequest) -> Option<&Ticket> {
                request.result_id.as_ref()
            }

            /// Behavior: `AjRajGrpcImpl#refs`
            fn sources(request: &AjRajTablesRequest) -> Vec<TableReference> {
                vec![source(&request.left_id), source(&request.right_id)]
            }

            fn validate(request: &AjRajTablesRequest) -> Result<(), Status> {
                aj_raj_validate(request, $direction)
            }

            fn create(
                _context: &OpContext,
                request: &AjRajTablesRequest,
                sources: &[Arc<dyn Table>],
            ) -> Result<Arc<dyn Table>, ExportError> {
                aj_raj_create(request, $direction, sources)
            }
        }
    };
}

aj_raj! {
    /// Behavior: `io.deephaven.server.table.ops.AjRajGrpcImpl.AjGrpcImpl`
    Aj => Direction::Aj
}

aj_raj! {
    /// Behavior: `io.deephaven.server.table.ops.AjRajGrpcImpl.RajGrpcImpl`
    Raj => Direction::Raj
}

/// The deprecated operation, which Java still serves.
///
/// Behavior: `io.deephaven.server.table.ops.JoinTablesGrpcImpl.AsOfJoinTablesGrpcImpl`
pub struct AsOfJoin;

impl TableOp for AsOfJoin {
    type Request = AsOfJoinTablesRequest;

    fn result_id(request: &AsOfJoinTablesRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &AsOfJoinTablesRequest) -> Vec<TableReference> {
        vec![source(&request.left_id), source(&request.right_id)]
    }

    /// The columns, then the rule: a number the protocol does not define is refused.
    ///
    /// Behavior: `JoinTablesGrpcImpl#validateRequest`,
    /// `AsOfJoinTablesGrpcImpl#validateRequest`
    fn validate(request: &AsOfJoinTablesRequest) -> Result<(), Status> {
        match_pairs(&request.columns_to_match, &request.columns_to_add)?;
        if MatchRule::try_from(request.as_of_match_rule).is_err() {
            return Err(Status::invalid_argument("Unrecognized as-of match rule"));
        }
        Ok(())
    }

    /// The last match names the stamp columns; a request without matches is, in Java, an
    /// `ArrayIndexOutOfBoundsException`. The rule of the request is the rule of the join
    /// with its sides exchanged: `LESS_THAN_EQUAL` asks for `left >= right`. Java says of
    /// this that it "looks wrong" and is kept for the clients that rely on it.
    ///
    /// Behavior: `JoinTablesGrpcImpl#create`, `AsOfJoinTablesGrpcImpl#doJoin`,
    /// `AsOfJoinTablesGrpcImpl#adapt`
    fn create(
        _context: &OpContext,
        request: &AsOfJoinTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let (mut exact, add) = match_pairs(&request.columns_to_match, &request.columns_to_add)?;
        let Some(stamps) = exact.pop() else {
            return Err(ExportError::Internal(
                "Index -1 out of bounds for length 0".to_string(),
            ));
        };
        let rule = match MatchRule::try_from(request.as_of_match_rule) {
            Ok(MatchRule::LessThanEqual) => AsOfRule::GreaterThanEqual,
            Ok(MatchRule::LessThan) => AsOfRule::GreaterThan,
            Ok(MatchRule::GreaterThanEqual) => AsOfRule::LessThanEqual,
            Ok(MatchRule::GreaterThan) => AsOfRule::LessThan,
            Err(_) => {
                return Err(ExportError::Internal(
                    "Unsupported join type: UNRECOGNIZED".to_string(),
                ))
            }
        };
        let as_of = AsOfMatch::new(stamps.left, rule, stamps.right);
        Ok(as_of_join(&sources[0], &sources[1], &exact, &as_of, &add)?)
    }
}

// Every expectation below is what Java 42.9 answered for the string in an `AjTables` or
// `RajTables` request: as its as-of column, as an exact match or as an addition.
#[cfg(test)]
mod tests {
    use super::*;

    type Match = Result<(String, &'static str, String), String>;
    type Pair = Result<(String, String), String>;

    fn aj(text: &str) -> Match {
        as_of_match_for(text, Direction::Aj).map(|m| (m.left, m.rule.operator(), m.right))
    }

    fn raj(text: &str) -> Match {
        as_of_match_for(text, Direction::Raj).map(|m| (m.left, m.rule.operator(), m.right))
    }

    fn matched(left: &str, rule: &'static str, right: &str) -> Match {
        Ok((left.to_string(), rule, right.to_string()))
    }

    fn no_match(text: &str) -> Match {
        Err(format!(
            "Input expression {text} does not match expected pattern \
             \\A\\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*(>=?|<=?)\\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\Z"
        ))
    }

    fn reserved<T>(name: &str) -> Result<T, String> {
        Err(format!(
            "Invalid column name \"{name}\": \"{name}\" is a reserved keyword"
        ))
    }

    #[test]
    fn an_as_of_match_is_a_name_or_two_names_around_one_of_four_rules() {
        assert_eq!(aj("T"), matched("T", ">=", "T"));
        assert_eq!(raj("T"), matched("T", "<=", "T"));
        assert_eq!(aj(" T "), matched("T", ">=", "T"));
        assert_eq!(aj("\tT\n"), matched("T", ">=", "T"));
        // a name alone is trimmed as `String#trim` trims; the parts of a match are not
        assert_eq!(raj("\u{1}T\u{2}"), matched("T", "<=", "T"));
        assert_eq!(aj("\u{1}T>=T2"), no_match("\u{1}T>=T2"));
        assert_eq!(aj("T>=T2\u{1}"), no_match("T>=T2\u{1}"));
        // and a name alone need not fit the pattern of a match
        assert_eq!(aj("\u{dc}bung"), matched("\u{dc}bung", ">=", "\u{dc}bung"));
        assert_eq!(aj("\u{dc}bung>=T2"), no_match("\u{dc}bung>=T2"));

        assert_eq!(aj("T>=T2"), matched("T", ">=", "T2"));
        assert_eq!(aj("T>T2"), matched("T", ">", "T2"));
        assert_eq!(raj("T<=T2"), matched("T", "<=", "T2"));
        assert_eq!(raj("T<T2"), matched("T", "<", "T2"));
        assert_eq!(aj(" T >= T2 "), matched("T", ">=", "T2"));
        assert_eq!(aj("T\t>\nT2"), matched("T", ">", "T2"));
        assert_eq!(aj("T\u{0B}>=\u{0C}T2"), matched("T", ">=", "T2"));
        assert_eq!(aj("T>=T2\r\n"), matched("T", ">=", "T2"));
        assert_eq!(aj("T$>=_T"), matched("T$", ">=", "_T"));
        assert_eq!(aj("T>=T"), matched("T", ">=", "T"));
        assert_eq!(aj("t>=t2"), matched("t", ">=", "t2"));

        for refused in [
            "T>=",
            ">=T2",
            "T=T2",
            "T==T2",
            "T>==T2",
            "T=>T2",
            "T=<T2",
            "T T2",
            "T\u{2028}",
            "T>=T2\u{2028}",
            "T>=T2\u{85}",
            " ",
            "\u{a0}T",
            "T<>T2",
            "T>>T2",
            "T> =T2",
            "T>=T2>=T",
            "1T>=T2",
            "T>=2T",
            "T\u{1c}>=T2",
            "T,T2",
        ] {
            assert_eq!(aj(refused), no_match(refused), "{refused:?}");
            assert_eq!(raj(refused), no_match(refused), "{refused:?}");
        }
    }

    #[test]
    fn a_reserved_name_is_refused_as_no_match_alone_and_by_name_in_a_match() {
        // alone it is not a column name, so it is read as a match, which it is not
        for alone in ["in", "class"] {
            assert_eq!(aj(alone), no_match(alone));
            assert_eq!(raj(alone), no_match(alone));
        }
        assert_eq!(aj("in>=T2"), reserved("in"));
        assert_eq!(aj("T>=in"), reserved("in"));
        // the left name first, and the names before the rule
        assert_eq!(aj("i<k"), reserved("i"));
        assert_eq!(raj("in>=T2"), reserved("in"));
    }

    #[test]
    fn each_operation_refuses_the_rules_of_the_other_by_javas_name() {
        let unsupported =
            |operation: &str, rule: &str| Err(format!("Unsupported operation for {operation}: {rule}"));
        assert_eq!(aj("T<=T2"), unsupported("aj", "LESS_THAN_EQUAL"));
        assert_eq!(aj("T<T2"), unsupported("aj", "LESS_THAN"));
        assert_eq!(raj("T>=T2"), unsupported("raj", "GREATER_THAN_EQUAL"));
        assert_eq!(raj("T\t>\nT2"), unsupported("raj", "GREATER_THAN"));
        assert_eq!(raj(" T >= T2 "), unsupported("raj", "GREATER_THAN_EQUAL"));
    }

    fn exact(text: &str) -> Pair {
        join_match(text).map(|m| (m.left, m.right))
    }

    fn addition(text: &str) -> Pair {
        join_addition(text).map(|a| (a.name, a.source))
    }

    fn pair(left: &str, right: &str) -> Pair {
        Ok((left.to_string(), right.to_string()))
    }

    fn invalid(name: &str) -> Pair {
        Err(format!("Invalid column name \"{name}\""))
    }

    fn no_name() -> Pair {
        Err("Invalid column name: name can not be null or empty".to_string())
    }

    #[test]
    fn an_exact_match_is_split_at_its_first_equals_sign() {
        assert_eq!(exact("K"), pair("K", "K"));
        assert_eq!(exact(" K "), pair("K", "K"));
        assert_eq!(exact("\tK\n"), pair("K", "K"));
        assert_eq!(exact("\u{1}K"), pair("K", "K"));
        assert_eq!(exact("K=K2"), pair("K", "K2"));
        assert_eq!(exact("K==K2"), pair("K", "K2"));
        assert_eq!(exact("K = K2"), pair("K", "K2"));
        assert_eq!(exact("K == K2"), pair("K", "K2"));

        for text in ["K=", "K==", "=", "=="] {
            assert_eq!(
                exact(text),
                Err(format!(
                    "Unable to parse match '{text}', expected form '<left>==<right>' or \
                     `<left>=<right>`"
                ))
            );
        }
        for text in ["=K", "==K", "", " ", "K=\u{1}", "K= "] {
            assert_eq!(exact(text), no_name(), "{text:?}");
        }
        assert_eq!(exact("K===K2"), invalid("=K2"));
        assert_eq!(exact("K= =K2"), invalid("=K2"));
        assert_eq!(exact("K=K2=K"), invalid("K2=K"));
        assert_eq!(exact("K K"), invalid("K K"));
        assert_eq!(exact("1K"), invalid("1K"));
        assert_eq!(exact("K,K2"), invalid("K,K2"));
        assert_eq!(exact("K\u{2028}"), invalid("K\u{2028}"));
        assert_eq!(exact("K==\u{2028}K2"), invalid("\u{2028}K2"));
        for text in ["in", "K=in", "in=K"] {
            assert_eq!(exact(text), reserved("in"), "{text:?}");
        }
    }

    #[test]
    fn an_addition_has_one_equals_sign_at_most() {
        assert_eq!(addition("V"), pair("V", "V"));
        assert_eq!(addition(" V "), pair("V", "V"));
        assert_eq!(addition("\u{1}V"), pair("V", "V"));
        assert_eq!(addition("X=V"), pair("X", "V"));
        assert_eq!(addition("X = V"), pair("X", "V"));
        assert_eq!(addition("X==V"), invalid("=V"));
        assert_eq!(addition("X=V=W"), invalid("V=W"));
        assert_eq!(addition("V\u{2028}"), invalid("V\u{2028}"));
        assert_eq!(addition("1X=V"), invalid("1X"));
        assert_eq!(addition("X=1V"), invalid("1V"));
        for text in ["X=", "=V", "", " ", "="] {
            assert_eq!(addition(text), no_name(), "{text:?}");
        }
        for text in ["in", "X=in"] {
            assert_eq!(addition(text), reserved("in"), "{text:?}");
        }
    }
}
