//! Cross join, `join` in Deephaven: every left row with every right row that has the same
//! values in the match columns. Task 4.3.
//!
//! The result has one row per matching pair, ordered by left row and, within a left row, by
//! right row. Its row keys put the left row key in the high bits and, in the low bits, the
//! right row key (no match columns) or the position of the right row among the right rows
//! with the same key; see `layout`. The number of low bits grows when a right group or the
//! right table's last row key needs more, which moves every row. Java's row keys are
//! reproduced, although `PARITY.md` A4 does not require it, because the bits decide when a
//! join is refused for running out of row key space, and that is visible.
//!
//! Java's left outer join (`CrossJoinHelper#leftOuterJoin`) shares this code but has no API
//! in the pinned reference's gRPC protocol or `Table`; it is not ported.

mod keyed;
mod layout;
mod region;
mod zero;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod random;
#[cfg(test)]
mod tests;

use crate::ops::join::natural::Plan;
use crate::ops::join::{JoinAddition, JoinError, JoinMatch, NaturalJoinType};
use crate::table::{MergedOperator, QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use keyed::Keyed;
use layout::{min_bits, Layout, LeftRows, RightRows};
use oxhaven_column::{Clock, RedirectedSource, RowRedirection};
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::Arc;
use zero::ZeroKey;

/// The number of bits reserved for the right part of a result row key when a request
/// names none.
///
/// Behavior: io.deephaven.engine.table.impl.CrossJoinHelper#DEFAULT_NUM_RIGHT_BITS_TO_RESERVE,
/// the configuration default `CrossJoinHelper.numRightBitsToReserve=10`
pub const DEFAULT_RESERVE_BITS: i32 = 10;

/// Why a cross join was refused, or why its result failed in a cycle.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CrossJoinError {
    /// A refusal shared with the natural join: unknown or conflicting columns, match
    /// columns of different types, tables of different update graphs, a failed table.
    Join(JoinError),
    /// Java's `OutOfKeySpaceException`: a result row key would need more than 63 bits.
    OutOfKeySpace(String),
    /// Java's `AssertionFailure` from `CrossJoinShiftState#setNumShiftBits`: the number of
    /// bits for the right part of a row key must be from 1 to 62.
    ReserveBits(i64),
}

impl fmt::Display for CrossJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrossJoinError::Join(error) => error.fmt(f),
            CrossJoinError::OutOfKeySpace(message) => f.write_str(message),
            CrossJoinError::ReserveBits(bits) => write!(
                f,
                "Assertion failed: asserted newNumShiftBits < 63 and > 0, instead newNumShiftBits == {bits}"
            ),
        }
    }
}

impl std::error::Error for CrossJoinError {}

impl From<TableFailure> for CrossJoinError {
    fn from(failure: TableFailure) -> Self {
        CrossJoinError::Join(JoinError::Table(failure))
    }
}

/// Behavior: io.deephaven.engine.table.impl.CrossJoinShiftState#setNumShiftBits
fn check_reserve(bits: impl Into<i64>) -> Result<u32, CrossJoinError> {
    let bits = bits.into();
    if bits > 0 && bits < 63 {
        Ok(bits as u32)
    } else {
        Err(CrossJoinError::ReserveBits(bits))
    }
}

/// The incremental half of either kind of cross join.
enum Operator {
    Keyed(Keyed),
    Zero(ZeroKey),
}

impl MergedOperator for Operator {
    /// `upstream[0]` is the left table's change and `upstream[1]` the right table's.
    fn on_update(
        &self,
        _result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        let (left, right) = (upstream[0].as_deref(), upstream[1].as_deref());
        match self {
            Operator::Keyed(keyed) => keyed.on_update(left, right),
            Operator::Zero(zero) => zero.on_update(left, right),
        }
    }
}

/// Every pair of a row of `left` and a row of `right` whose `on` columns are equal: for each
/// left row in left order, its matching right rows in right order. With no `on`, every
/// pair. The result has the left table's columns and the added columns `add` of the right
/// table; with no `add`, every right column whose name is not the left name of a match.
///
/// Keys are equal as in [`natural_join`](crate::ops::join::natural_join): null equals null,
/// every NaN equals every NaN, and `-0.0` equals `0.0`.
///
/// `reserve_bits` is the number of bits of row key space a refreshing join starts with for
/// the right rows of one left row; it must be from 1 to 62 where it is used. A join that
/// would need row keys of more than 63 bits is refused, and a refreshing one fails when a
/// cycle takes it there.
///
/// The result is refreshing when either table is. It is not flat and has none of the
/// add-only, append-only and blink attributes.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#join and
/// io.deephaven.engine.table.impl.CrossJoinHelper#join
pub fn cross_join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    on: &[JoinMatch],
    add: &[JoinAddition],
    reserve_bits: i32,
) -> Result<Arc<dyn Table>, CrossJoinError> {
    let plan = Plan::new(
        &**left,
        &**right,
        on,
        add,
        NaturalJoinType::ErrorOnDuplicate,
    )
    .map_err(CrossJoinError::Join)?;
    if left.is_refreshing() && right.is_refreshing() {
        match (left.update_graph(), right.update_graph()) {
            (Some(a), Some(b)) if Arc::ptr_eq(&a, &b) => {}
            _ => return Err(CrossJoinError::Join(JoinError::UpdateGraphConflict)),
        }
    }

    let parents = [left.clone(), right.clone()];
    let mut refused = None;
    let result = QueryTable::derive_merged(&parents, Vec::new(), || {
        start(plan, left, right, reserve_bits).map_err(|error| {
            let failure = TableFailure {
                message: error.to_string(),
            };
            refused = Some(error);
            failure
        })
    });
    match result {
        Ok(table) => Ok(table),
        Err(failure) => Err(refused.unwrap_or(CrossJoinError::Join(JoinError::Table(failure)))),
    }
}

/// Reads both tables and returns the result's initial state with the operator.
///
/// Behavior: io.deephaven.engine.table.impl.CrossJoinHelper#internalJoin,
/// #zeroKeyColumnsJoin and #makeResult
fn start(
    plan: Plan,
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    reserve: i32,
) -> Result<(TableSpec, Operator), CrossJoinError> {
    let both_static = !left.is_refreshing() && !right.is_refreshing();
    let clock: Option<Arc<dyn Clock>> = [left, right]
        .into_iter()
        .filter(|table| table.is_refreshing())
        .find_map(|table| table.update_graph())
        .map(|graph| Arc::new(graph) as Arc<dyn Clock>);
    let keyed = !plan.key_types.is_empty();
    let left_count = left.definition().columns().len();
    let added = left_count..left_count + plan.additions.len();
    let left_columns = ModifiedColumnTransformer::identity(left_count);
    let right_columns = ModifiedColumnTransformer::new(
        plan.additions
            .iter()
            .zip(added)
            .map(|((_, source), result)| (*source, [result])),
    );

    let (layout, rows, operator) = if keyed {
        let layout = Layout::new(1, true, clock);
        let built = Keyed::build(
            left,
            right,
            plan.left_keys,
            plan.right_keys,
            reserve,
            &layout,
            left_columns,
            right_columns,
        )?;
        (layout, built.rows, Operator::Keyed(built.operator))
    } else {
        // Two static tables need only the bits of the right table's last row key.
        let reserve = if both_static { 1 } else { reserve };
        let (left_rows, right_rows) = (left.row_set(), right.row_set());
        let right_last = right_rows.last_row_key().unwrap_or(-1);
        zero::check_key_space(
            left_rows.last_row_key().unwrap_or(-1),
            right_last,
            reserve.max(0) as u32,
        )
        .map_err(CrossJoinError::OutOfKeySpace)?;
        let bits = check_reserve(i64::from(reserve).max(i64::from(min_bits(right_last))))?;
        let layout = Layout::new(bits, false, clock);
        let rows = zero::rows(&left_rows, &right_rows, bits);
        let operator = ZeroKey {
            left: left.clone(),
            right: right.clone(),
            layout: layout.clone(),
            left_columns,
            right_columns,
        };
        (layout, rows, Operator::Zero(operator))
    };

    let mut definition = left.definition().columns().to_vec();
    let to_left: Arc<dyn RowRedirection> = Arc::new(LeftRows(layout.clone()));
    let to_right: Arc<dyn RowRedirection> = Arc::new(RightRows(layout));
    let mut columns: Vec<Arc<dyn ColumnSource>> = (0..left_count)
        .map(|index| {
            Arc::new(RedirectedSource::new(to_left.clone(), left.column(index)))
                as Arc<dyn ColumnSource>
        })
        .collect();
    for (name, source) in &plan.additions {
        let column_type = right.definition().columns()[*source].column_type.clone();
        definition.push(ColumnDefinition::new(name.as_str(), column_type));
        columns.push(Arc::new(RedirectedSource::new(
            to_right.clone(),
            right.column(*source),
        )));
    }
    let definition = match TableDefinition::new(definition) {
        Ok(definition) => definition,
        Err(e) => panic!("the added columns passed the name checks: {e}"),
    };
    Ok((TableSpec::new(definition, rows, columns), operator))
}
