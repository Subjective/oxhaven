//! A built-in application with the two tables of the web IDE acceptance test, for a server
//! that has no script session to create tables with (`oxhaven-server --demo-tables`).
//!
//! The tables are the ones `oracle/ide/app.d/ide_tables.py` builds on the Java reference and
//! `oracle/ide/expected.json` describes, as fields of an application with the same id and
//! name as `oracle/ide/app.d/ide_app.app`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use oxhaven_column::FlatSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::ops::sort::tail;
use oxhaven_engine::table::{time_table, QueryTable, TableSpec, TimeTableOptions};

use super::fields::{Application, Applications};
use crate::session::ExportValue;
use crate::table::support::OpContext;

pub const APPLICATION_ID: &str = "ide_app";
pub const APPLICATION_NAME: &str = "IDE acceptance tables";
pub const STATIC_FIELD: &str = "ide_static";
pub const TICKING_FIELD: &str = "ide_ticking";

/// The time between two rows of the table behind `ide_ticking`: `PT0.5S`.
const TICK_NANOS: i64 = 500_000_000;

/// `new_table([int_col("Id", ..), string_col("Sym", ..), double_col("Price", ..)])`: five
/// rows that never change.
///
/// Behavior: `io.deephaven.engine.util.TableTools#newTable(ColumnHolder...)`
pub fn static_table() -> Result<Arc<dyn Table>, String> {
    let columns: [(&str, ColumnType, ArrayRef); 3] = [
        (
            "Id",
            ColumnType::Int,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        ),
        (
            "Sym",
            ColumnType::String,
            Arc::new(StringArray::from(vec![
                "AAPL", "MSFT", "GOOG", "AMZN", "OXHV",
            ])),
        ),
        (
            "Price",
            ColumnType::Double,
            Arc::new(Float64Array::from(vec![
                101.5, 202.25, 303.125, 404.0, 505.75,
            ])),
        ),
    ];
    let mut definitions = Vec::with_capacity(columns.len());
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(columns.len());
    for (name, column_type, values) in columns {
        definitions.push(ColumnDefinition::new(name, column_type.clone()));
        sources.push(Arc::new(
            FlatSource::new(column_type, values).map_err(|e| e.to_string())?,
        ));
    }
    let definition = TableDefinition::new(definitions).map_err(|e| e.to_string())?;
    // not marked flat: Java's `newTable` does not mark its result either
    Ok(QueryTable::new_static(TableSpec::new(
        definition,
        RowSet::flat(5),
        sources,
    )))
}

/// The row key of each row, or its square: `ii` and `ii * ii` of a table whose rows are at
/// keys `0..size` and never move.
struct RowNumbers {
    column_type: ColumnType,
    squared: bool,
}

impl ColumnSource for RowNumbers {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let values: Int64Array = rows
            .iter()
            // Java's long multiplication wraps
            .map(|key| {
                if self.squared {
                    key.wrapping_mul(key)
                } else {
                    key
                }
            })
            .collect::<Vec<i64>>()
            .into();
        Ok(Arc::new(values))
    }

    /// A row's values never change.
    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }
}

/// `ticks.update(["N = ii", "Sq = ii * ii"])` for the time table `ticks`, which is flat and
/// only ever appends rows, so that the position of a row is its key and the two columns can
/// be computed on read.
///
/// This stands in for the engine's `update`, whose formula language does not have the row
/// variable `ii` yet ("unsupported in formulas: row variable Ii"). It is not a general
/// operator: a parent that removes, modifies or moves rows fails the result.
fn count_rows(ticks: &Arc<dyn Table>) -> Result<Arc<dyn Table>, TableFailure> {
    let failure = |message: &str| TableFailure {
        message: message.to_string(),
    };
    if !(ticks.is_flat() && ticks.attributes().append_only) {
        return Err(failure(
            "the demo row counter needs a flat append-only table",
        ));
    }
    let counted = QueryTable::derive(ticks, || {
        let mut columns = ticks.definition().columns().to_vec();
        columns.push(ColumnDefinition::new("N", ColumnType::Long));
        columns.push(ColumnDefinition::new("Sq", ColumnType::Long));
        let definition = TableDefinition::new(columns).map_err(|e| failure(&e.to_string()))?;
        let mut sources: Vec<Arc<dyn ColumnSource>> = (0..ticks.definition().columns().len())
            .map(|index| ticks.column(index))
            .collect();
        for squared in [false, true] {
            sources.push(Arc::new(RowNumbers {
                column_type: ColumnType::Long,
                squared,
            }));
        }
        let spec = TableSpec::new(definition, (*ticks.row_set()).clone(), sources)
            .with_attributes(ticks.attributes())
            .flat();
        let operator = move |_result: &QueryTable, upstream: &TableUpdate| {
            let appended_only = upstream.removed.is_empty()
                && upstream.modified.is_empty()
                && upstream.shifted == RowSetShiftData::empty();
            if !appended_only {
                return Err(failure(
                    "the demo row counter saw a change that is not an append",
                ));
            }
            Ok(upstream.clone())
        };
        Ok((spec, operator))
    })?;
    Ok(counted)
}

/// `time_table("PT0.5S").update(["N = ii", "Sq = ii * ii"]).tail(3)`: the last three rows of
/// a table that gains a row every half second of `context.clock`, in the cycles of
/// `context.graph`.
///
/// The time table and `tail` are the engine's. The `update` between them is
/// [`count_rows`], a stand-in written for this table.
pub fn ticking_table(context: &OpContext) -> Result<Arc<dyn Table>, String> {
    let ticks: Arc<dyn Table> = time_table(
        context.graph.clone(),
        context.clock.clone(),
        TimeTableOptions {
            start_nanos: None,
            period_nanos: TICK_NANOS,
            blink: false,
        },
    )
    .map_err(|e| e.to_string())?;
    let counted = count_rows(&ticks).map_err(|e| e.to_string())?;
    tail(&counted, 3).map_err(|e| e.to_string())
}

/// Creates the application `ide_app` with the fields `ide_static` and `ide_ticking`.
pub fn install(
    applications: &Arc<Applications>,
    context: &OpContext,
) -> Result<Application, String> {
    let application = applications
        .create(APPLICATION_ID, APPLICATION_NAME)
        .map_err(|e| e.to_string())?;
    application
        .set_field(STATIC_FIELD, ExportValue::Table(static_table()?))
        .map_err(|e| e.to_string())?;
    application
        .set_field(TICKING_FIELD, ExportValue::Table(ticking_table(context)?))
        .map_err(|e| e.to_string())?;
    Ok(application)
}
