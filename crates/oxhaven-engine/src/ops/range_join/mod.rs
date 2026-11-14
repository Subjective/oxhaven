//! Range join. Task 4.5.
//!
//! [`range_join`] gives each left row the right rows of its bucket whose range value lies
//! between the row's two bounds, as one array per aggregated right column.
//!
//! The pinned reference restricts the operation, and so does this module, with the same
//! refusals:
//!
//! - both tables must be static;
//! - the only aggregation is `group`;
//! - the right table must be ascending in its range column within every bucket some left
//!   row has, once the rows with a null or NaN range value are set aside.
//!
//! | File | Holds |
//! | --- | --- |
//! | `mod.rs` | The arguments, the refusals, the public function |
//! | `search.rs` | The range of each left row of a bucket, as positions in the bucket's right rows |
//! | `ranges.rs` | Buckets by exact match, range values by type, the ranges of every left row |
//! | `grouped.rs` | The result table: the left columns and the vector columns over the ranges |
//!
//! # The result
//!
//! Every result column of a `group` aggregation is a vector column
//! ([`ColumnType::Vector`](oxhaven_contracts::schema::ColumnType::Vector) of the right
//! column's type). It holds no values of its own: a cell is read from the right column
//! through the range of its left row, as Java's `RangeAggregateColumnSource` reads it.

mod grouped;
mod ranges;
mod search;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod tests;

use crate::ops::agg::Pair;
use crate::ops::join::JoinMatch;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use regex::Regex;
use std::fmt;
use std::sync::{Arc, OnceLock};

/// Which right rows a left row's start bound lets into its range.
///
/// Behavior: io.deephaven.api.RangeStartRule
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RangeStartRule {
    /// Rows whose range value is greater than the start.
    LessThan,
    /// Rows whose range value is at least the start.
    LessThanOrEqual,
    /// As `LessThanOrEqual`, and when no right row has the start as its range value, also
    /// the last row before the start.
    LessThanOrEqualAllowPreceding,
}

/// Which right rows a left row's end bound lets into its range.
///
/// Behavior: io.deephaven.api.RangeEndRule
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RangeEndRule {
    /// Rows whose range value is less than the end.
    GreaterThan,
    /// Rows whose range value is at most the end.
    GreaterThanOrEqual,
    /// As `GreaterThanOrEqual`, and when no right row has the end as its range value, also
    /// the first row after the end.
    GreaterThanOrEqualAllowFollowing,
}

/// The range match of a range join: two columns of the left table bound the values of one
/// column of the right table.
///
/// Behavior: io.deephaven.api.RangeJoinMatch
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RangeJoinMatch {
    left_start: String,
    start_rule: RangeStartRule,
    right_range: String,
    end_rule: RangeEndRule,
    left_end: String,
}

impl RangeJoinMatch {
    /// Refused when the start and the end are the same column.
    ///
    /// Behavior: io.deephaven.api.RangeJoinMatch#of and `#checkLeftColumnsDifferent`
    pub fn new(
        left_start: impl Into<String>,
        start_rule: RangeStartRule,
        right_range: impl Into<String>,
        end_rule: RangeEndRule,
        left_end: impl Into<String>,
    ) -> Result<Self, RangeJoinError> {
        let (left_start, left_end) = (left_start.into(), left_end.into());
        if left_start == left_end {
            return Err(RangeJoinError::InvalidMatch(format!(
                "RangeJoinMatch must have distinct ColumnNames for leftStartColumn() and \
                 leftEndColumn() in order to properly define a range, {left_start} specified for \
                 both fields"
            )));
        }
        Ok(Self {
            left_start,
            start_rule,
            right_range: right_range.into(),
            end_rule,
            left_end,
        })
    }

    /// The match written as text: `Lo <= P <= Hi`, with `<` for a strict bound, `<-` in
    /// front to allow the preceding row and `->` behind to allow the following one, each
    /// only next to `<=`.
    ///
    /// Not checked here: that each name is one Java accepts as a column name
    /// (io.deephaven.api.ColumnName#of refuses reserved words); [`Self::parse_checked`]
    /// takes that check.
    ///
    /// Behavior: io.deephaven.api.RangeJoinMatch#parse
    pub fn parse(input: &str) -> Result<Self, RangeJoinError> {
        Self::parse_checked(input, |_| Ok(()))
    }

    /// As [`Self::parse`], with `check_name` asked of the three column names in the order
    /// left start, right range, left end, once the text fits the pattern and before
    /// anything else about it is refused. Its error is the text of the refusal.
    ///
    /// Behavior: io.deephaven.api.RangeJoinMatch#parse, where `check_name` is
    /// io.deephaven.api.ColumnName#of
    pub fn parse_checked(
        input: &str,
        check_name: impl Fn(&str) -> Result<(), String>,
    ) -> Result<Self, RangeJoinError> {
        const ID: &str = r"[a-zA-Z_$][a-zA-Z0-9_$]*";
        static PATTERN: OnceLock<Regex> = OnceLock::new();
        let pattern = PATTERN.get_or_init(|| {
            let space = r"(?-u:\s)*";
            let text = format!(
                r"\A{space}(<-)?{space}({ID}){space}(<=?){space}({ID}){space}(<=?){space}({ID}){space}(->)?{space}\z"
            );
            Regex::new(&text).expect("a pattern")
        });
        let Some(found) = pattern.captures(input) else {
            // The pattern as Java writes it, which the text above is in this crate's syntax.
            let java =
                format!(r"\A\s*(<-)?\s*({ID})\s*(<=?)\s*({ID})\s*(<=?)\s*({ID})\s*(->)?\s*\Z");
            return Err(RangeJoinError::InvalidMatch(format!(
                "Input expression {input} does not match expected pattern {java}"
            )));
        };
        for name in [2, 4, 6] {
            check_name(&found[name]).map_err(RangeJoinError::InvalidMatch)?;
        }
        let allow_preceding = found.get(1).is_some();
        let start_allows_equal = found[3].len() == 2;
        let end_allows_equal = found[5].len() == 2;
        let allow_following = found.get(7).is_some();
        if allow_preceding && !start_allows_equal {
            return Err(RangeJoinError::InvalidMatch(format!(
                "Error parsing expression {input}: <- operator for \"allow preceding\" may only \
                 be combined with <= start match expression"
            )));
        }
        let start_rule = if allow_preceding {
            RangeStartRule::LessThanOrEqualAllowPreceding
        } else if start_allows_equal {
            RangeStartRule::LessThanOrEqual
        } else {
            RangeStartRule::LessThan
        };
        if allow_following && !end_allows_equal {
            return Err(RangeJoinError::InvalidMatch(format!(
                "Error parsing expression {input}: -> operator for \"allow following\" may only \
                 be combined with <= end match expression"
            )));
        }
        let end_rule = if allow_following {
            RangeEndRule::GreaterThanOrEqualAllowFollowing
        } else if end_allows_equal {
            RangeEndRule::GreaterThanOrEqual
        } else {
            RangeEndRule::GreaterThan
        };
        Self::new(&found[2], start_rule, &found[4], end_rule, &found[6])
    }

    /// The column of the left table that bounds the start of the range.
    pub fn left_start(&self) -> &str {
        &self.left_start
    }

    pub fn start_rule(&self) -> RangeStartRule {
        self.start_rule
    }

    /// The column of the right table whose values the bounds select.
    pub fn right_range(&self) -> &str {
        &self.right_range
    }

    pub fn end_rule(&self) -> RangeEndRule {
        self.end_rule
    }

    /// The column of the left table that bounds the end of the range.
    pub fn left_end(&self) -> &str {
        &self.left_end
    }
}

/// Behavior: io.deephaven.api.Strings#of(RangeJoinMatch)
impl fmt::Display for RangeJoinMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start_rule == RangeStartRule::LessThanOrEqualAllowPreceding {
            f.write_str("<- ")?;
        }
        let start = match self.start_rule {
            RangeStartRule::LessThan => "<",
            _ => "<=",
        };
        let end = match self.end_rule {
            RangeEndRule::GreaterThan => "<",
            _ => "<=",
        };
        let (left_start, right_range, left_end) =
            (&self.left_start, &self.right_range, &self.left_end);
        write!(f, "{left_start} {start} {right_range} {end} {left_end}")?;
        if self.end_rule == RangeEndRule::GreaterThanOrEqualAllowFollowing {
            f.write_str(" ->")?;
        }
        Ok(())
    }
}

/// One aggregation of a range join over the right rows of each left row's range.
///
/// Behavior: io.deephaven.api.agg.Aggregation as
/// io.deephaven.engine.table.impl.rangejoin.SupportedRangeJoinAggregations sorts it
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RangeAggregation {
    /// `group`: for each pair, the values of the right column `input` over the range, in
    /// the right table's order, as the array column `output`.
    Group(Vec<Pair>),
    /// Any other aggregation, all of which the reference refuses. The text describes it in
    /// the refusal, as io.deephaven.api.agg.AggregationDescriptions would.
    Other(String),
}

impl RangeAggregation {
    pub fn group(pairs: impl IntoIterator<Item = Pair>) -> Self {
        RangeAggregation::Group(pairs.into_iter().collect())
    }
}

/// Why a range join was refused. Each variant names the Java exception the reference
/// throws. The recorded Java results have `INVALID_ARGUMENT` for every one of them.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RangeJoinError {
    /// Java's `IllegalArgumentException` for a range match that does not parse or whose
    /// start and end are one column.
    InvalidMatch(String),
    /// Java's `UnsupportedOperationException` "rangeJoin only supports static (not
    /// refreshing) inputs at this time".
    Refreshing { left: bool, right: bool },
    /// Java's `IllegalArgumentException` "Invalid exact matches": columns a table does
    /// not have and pairs of differing types, one entry each.
    InvalidExactMatches(Vec<String>),
    /// Java's `IllegalArgumentException` "Invalid range match": columns a table does not
    /// have and bounds whose type is not the range column's, one entry each.
    InvalidRangeMatch(Vec<String>),
    /// Java's `IllegalArgumentException` "Invalid range value type": the range columns
    /// are vectors or arrays, which Java can not order.
    InvalidRangeValueType(ColumnType),
    /// Java's `UnsupportedOperationException` "rangeJoin only supports the "group"
    /// aggregation at this time", with the description of each aggregation refused.
    UnsupportedAggregations(Vec<String>),
    /// Java's `OutOfOrderException` "Encountered out of order data in right table at row
    /// key": the row key of the first right row of a bucket whose range value is less
    /// than that of the row before it.
    OutOfOrder(RowKey),
    /// Java's `NoSuchColumnException`: a `group` input that is not a column of the right
    /// table.
    NoSuchColumn {
        missing: String,
        present: Vec<String>,
    },
    /// A table could not be read.
    Table(TableFailure),
}

impl fmt::Display for RangeJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn side(refreshing: bool) -> &'static str {
            if refreshing {
                "refreshing"
            } else {
                "static"
            }
        }
        match self {
            RangeJoinError::InvalidMatch(message) => f.write_str(message),
            RangeJoinError::Refreshing { left, right } => write!(
                f,
                "rangeJoin only supports static (not refreshing) inputs at this time: left \
                 table is {}, right table is {}",
                side(*left),
                side(*right)
            ),
            RangeJoinError::InvalidExactMatches(issues) => {
                write!(f, "Invalid exact matches: {}", issues.join(", "))
            }
            RangeJoinError::InvalidRangeMatch(issues) => {
                write!(f, "Invalid range match: {}", issues.join(", "))
            }
            RangeJoinError::InvalidRangeValueType(column_type) => write!(
                f,
                "Invalid range value type {column_type:?}, must be primitive or comparable"
            ),
            RangeJoinError::UnsupportedAggregations(aggregations) => write!(
                f,
                "rangeJoin only supports the \"group\" aggregation at this time - unsupported \
                 aggregations were requested: [{}]",
                aggregations.join(", ")
            ),
            RangeJoinError::OutOfOrder(row_key) => write!(
                f,
                "Encountered out of order data in right table at row key {row_key}"
            ),
            RangeJoinError::NoSuchColumn { missing, present } => write!(
                f,
                "Unknown column names [{missing}], available column names are [{}]",
                present.join(", ")
            ),
            RangeJoinError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for RangeJoinError {}

impl From<TableFailure> for RangeJoinError {
    fn from(failure: TableFailure) -> Self {
        RangeJoinError::Table(failure)
    }
}

/// The rows of `left`, in its order and at its row keys, each with the right rows of its
/// range aggregated.
///
/// A left row's bucket is the right rows whose `exact` columns equal its own, as the
/// natural join compares keys; with no `exact` every right row is in it. Right rows whose
/// range value is null, or NaN, are in no bucket. The row's range is the rows of its
/// bucket whose range value lies between its two bounds as the rules of `range` say. A
/// null bound is open. A row whose start is beyond its end, or equal to it unless both
/// rules allow equality, or one of whose bounds is NaN, has no range: its aggregated
/// cells are null, where those of an empty range are empty arrays.
///
/// The result has the left table's columns, and for each pair of each `group` aggregation
/// a column of vectors named by its output. An output named like a column that is already
/// there replaces it where it stands. The result is static and carries no attribute of
/// the left table.
///
/// Refusals are checked in Java's order: refreshing tables, the exact matches, the range
/// match, the type of the range columns, the aggregations, then the order of the right
/// table, then the inputs of the aggregations.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#rangeJoin and
/// io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation
pub fn range_join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    range: &RangeJoinMatch,
    aggregations: &[RangeAggregation],
) -> Result<Arc<dyn Table>, RangeJoinError> {
    grouped::join(left, right, exact, range, aggregations)
}
