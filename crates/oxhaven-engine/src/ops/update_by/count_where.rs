//! Count-where: each row's result is the number of rows, up to it in its bucket or in its
//! window, that pass a list of filters.
//!
//! The filters are those of `where`, checked against the parent table, and run in the
//! order given, each on the rows the ones before it accepted. Java runs them on chunks of
//! the rows an update computes (for a window, the rows in the windows of those rows) and
//! keeps one flag per row; so do the functions here, on the rows themselves.

use super::rolling::Slide;
use super::{failure, invalid, UpdateByError};
use crate::ops::filter::RowFilters;
use arrow::array::{ArrayRef, Int64Array};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use std::sync::Arc;

/// The filters of one count-where operator.
pub(super) struct CountWhere {
    filters: RowFilters,
    /// Whether Java runs the filters over a table made of the chunk at hand, which it
    /// cannot make of no rows.
    chunk_table: bool,
    /// Whether a filter is a match that became a formula, which Java cannot run here.
    lost_formula: bool,
}

/// The rows of a bucket Java computes at once, by default.
///
/// Behavior: io.deephaven.api.updateby.UpdateByControl#chunkCapacityDefault
const CHUNK: usize = 4096;

/// The filters of a count-where over `parent`.
///
/// Java refuses a filter written as a formula that uses `i`, `ii` or `k`, and fails to
/// make its filter of one that reads a column as an array. A comparison that only became a
/// formula because its column or value did not fit a match or a range filter (`I > ii`,
/// `L < L2`) passes both tests: Java keeps the formula inside the filter it tried first,
/// and runs it, and every filter after it, over a table made of the chunk at hand. That
/// gives `where`'s answers, unless the filter it tried first is a match (`DS == DS`): the
/// copy Java makes of that filter for the chunk's table has neither values nor the
/// formula, and fails on the first rows it is given. Three more exceptions are left
/// `unported!`:
///
/// - the row variables of such a formula are positions in that chunk;
/// - when a filter reads an `Instant` or a `Boolean` column, and none of the filters
///   before the first such formula does, making the operator's context throws a
///   `NullPointerException` on a worker thread and the Java server shuts down;
/// - without filters Java counts the flags a pooled chunk was left with.
///
/// # Panics
/// With `unported!` for those three, and for a filter of a vector or array column.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor#makeCountWhereOperator
/// and io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.CountFilter#createCountFilters
pub(super) fn filters(
    parent: &dyn Table,
    texts: &[String],
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<CountWhere, UpdateByError> {
    let Some(compiler) = compiler else {
        return Err(invalid(
            "a count-where needs a formula compiler: use update_by_with",
        ));
    };
    let definition = parent.definition();
    let filters = RowFilters::parse(texts, definition, compiler).map_err(invalid)?;
    let reinterpreted = |shape: &crate::ops::filter::FilterShape| {
        shape.columns.iter().any(|&column| {
            matches!(
                definition.columns()[column].column_type,
                ColumnType::Instant | ColumnType::Boolean
            )
        })
    };
    // Java looks at the filters in order, and at none after the first formula it keeps
    // inside another filter.
    let shapes = filters.shapes();
    let kept_inside = shapes
        .iter()
        .position(|shape| shape.formula && !shape.written_as_formula);
    for shape in &shapes[..kept_inside.unwrap_or(shapes.len())] {
        if shape.formula && shape.row_variables {
            return Err(invalid(
                "UpdateBy CountWhere operator does not support refreshing filters",
            ));
        }
        if shape.formula && shape.arrays {
            return Err(invalid(
                "Error creating condition filter in UpdateBy CountWhere Operator",
            ));
        }
    }
    if let Some(first) = kept_inside {
        if shapes[first..]
            .iter()
            .any(|shape| shape.row_variables || shape.arrays)
        {
            unported!(
                "io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.Context#applyFilters",
                "a count-where filter with i, ii, k or a column array that Java runs over \
                 a table of the chunk at hand"
            );
        }
        // Behavior: `originalChunksRequired` and `chunkSourceTableRequired` of
        // UpdateByOperatorFactory.OperationVisitor#makeCountWhereOperator, and the
        // constructor of CountWhereOperator.Context, which stores into
        // `originalFillContexts` whenever a column is reinterpreted.
        if shapes.iter().any(reinterpreted) && !shapes[..first].iter().any(reinterpreted) {
            unported!(
                "io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.Context#Context",
                "a count-where whose filters read an Instant or Boolean column, the first \
                 of them after a comparison that became a formula: the Java server shuts down"
            );
        }
    }
    if texts.is_empty() {
        unported!(
            "io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.Context#applyFilters",
            "a count-where without filters, which in Java counts the contents of an unset chunk"
        );
    }
    if let Some(list) = filters
        .columns()
        .iter()
        .map(|&column| &definition.columns()[column].column_type)
        .find(|column_type| column_type.list_element().is_some())
    {
        unported!(
            "io.deephaven.engine.table.impl.select.WhereFilter#filter",
            "a count-where filter of a column of type {list:?}"
        );
    }
    let chunk_table = kept_inside.is_some() || shapes.iter().any(reinterpreted);
    let lost_formula = shapes.iter().any(|shape| shape.written_as_match);
    Ok(CountWhere {
        filters,
        chunk_table,
        lost_formula,
    })
}

impl CountWhere {
    /// The parent columns the filters read.
    pub(super) fn columns(&self) -> &[usize] {
        self.filters.columns()
    }

    /// For each of `rows`, in order, whether every filter accepts it. A filter that fails
    /// on a row, as a formula that evaluates to null does, fails the operation.
    ///
    /// Behavior: CountWhereOperator.Context#applyFilters
    ///
    /// Behavior of the failure: io.deephaven.engine.table.impl.select.MatchFilter#copy and
    /// #filter, for the copy that CountWhereOperator.Context#Context makes of a match
    /// filter whose `init` handed over to a formula
    fn passing(&self, table: &dyn Table, rows: &RowSet) -> Result<Vec<bool>, TableFailure> {
        if self.lost_formula {
            return Err(failure(
                "Cannot read the array length because \"keys\" is null",
            ));
        }
        let accepted = self.filters.accepted(table, rows, false)?;
        let mut accepted = accepted.iter().peekable();
        Ok(rows
            .iter()
            .map(|row| accepted.next_if_eq(&row).is_some())
            .collect())
    }

    /// The running count of the rows of `rows` that pass, after each row, continuing from
    /// `seed`: the result of the row before the first one, or 0 at the start of a bucket.
    /// The result is never null.
    ///
    /// Behavior: CountWhereOperator.Context#accumulateCumulative, #push and #reset, seeded by
    /// io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator#initializeCumulative
    pub(super) fn cumulative(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        seed: i64,
    ) -> Result<ArrayRef, TableFailure> {
        let mut count = seed;
        let counts = self.passing(table, rows)?.into_iter().map(|passed| {
            count += i64::from(passed);
            count
        });
        Ok(Arc::new(Int64Array::from_iter_values(counts)))
    }

    /// Fails where Java cannot make a table of a chunk. `entering` holds, for each row
    /// computed, in order, how many rows enter the window to reach it; 0 for a row
    /// without a timestamp.
    ///
    /// Java computes the rows of a bucket in chunks, each with the rows that enter the
    /// window in it. When it runs the filters over a table of the chunk at hand (for a
    /// filter of an `Instant` or `Boolean` column, and for a comparison that became a
    /// formula), a chunk that no row enters fails its assertion: the rows computed have
    /// no timestamp, or their windows hold no row, or held all their rows before.
    ///
    /// Behavior: CountWhereOperator.Context#assignInputChunksToFilters and
    /// io.deephaven.engine.table.impl.sources.chunkcolumnsource.LongChunkColumnSource#addChunk;
    /// the chunks are those of
    /// io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingBase#processWindowBucketOperatorSet
    pub(super) fn check_chunks(&self, entering: &[usize]) -> Result<(), TableFailure> {
        let empty = |chunk: &[usize]| chunk.iter().all(|entering| *entering == 0);
        if self.chunk_table && entering.chunks(CHUNK).any(empty) {
            return Err(failure(
                "Assertion failed: asserted chunk.size() > 0, instead chunk.size() == 0.",
            ));
        }
        Ok(())
    }

    /// The number of rows in the window that pass, for each slide of the window over the
    /// rows `influencers`. A window that holds no row counts 0; one whose two sides add up
    /// to no rows at all slides its head ahead of its tail, and fails as in Java.
    ///
    /// Behavior: CountWhereOperator.Context#accumulateRolling, #push and #pop
    pub(super) fn rolling(
        &self,
        table: &dyn Table,
        influencers: &RowSet,
        slides: &[Slide],
    ) -> Result<ArrayRef, TableFailure> {
        let passing = self.passing(table, influencers)?;
        let (mut head, mut tail, mut count) = (0, 0, 0i64);
        let mut counts = Vec::with_capacity(slides.len());
        for slide in slides {
            if slide.pop > tail - head {
                return Err(failure(format!(
                    "Assertion failed: asserted buffer.size() >= {}, instead buffer.size() == {}.",
                    slide.pop,
                    tail - head
                )));
            }
            count -= passing[head..head + slide.pop]
                .iter()
                .filter(|p| **p)
                .count() as i64;
            head += slide.pop;
            count += passing[tail..tail + slide.push]
                .iter()
                .filter(|p| **p)
                .count() as i64;
            tail += slide.push;
            counts.push(count);
        }
        Ok(Arc::new(Int64Array::from(counts)))
    }
}
