//! The column sources this family adds: formulas computed on read, formulas computed on read
//! and remembered by input value, and constants; and the table a formula is evaluated against.

use super::failure;
use crate::table::{forget_reads_through, register_reads_through};
use arrow::array::{new_empty_array, ArrayRef, UInt32Array};
use arrow::row::{RowConverter, SortField};
use oxhaven_column::RowRedirection;
use oxhaven_contracts::expr::Formula;
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{
    ColumnSource, Table, TableAttributes, TableFailure, UpdateListener,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

// The sources of this module that compute their values on read are registered as sources
// that do not hold their values, so that `select` copies them: see `crate::table::holds_values`.

/// Whose rows a formula sees when it asks for row positions and keys.
#[derive(Clone)]
enum Rows {
    /// The rows of the table the operation was applied to, current or previous.
    Of(Arc<dyn Table>),
    /// A fixed set, for a static result that addresses its rows differently from its parent.
    Fixed(Arc<RowSet>),
}

/// The table a formula of one operation is evaluated against: the columns it could name at
/// its place in the column list, over the parent's rows.
///
/// Behavior: the `columnsOfInterest` and `rowSet` that
/// io.deephaven.engine.table.impl.select.SelectColumn#initInputs gives a column.
#[derive(Clone)]
pub(super) struct FormulaInput {
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
    rows: Rows,
}

impl FormulaInput {
    pub(super) fn over(
        parent: &Arc<dyn Table>,
        definition: TableDefinition,
        columns: Vec<Arc<dyn ColumnSource>>,
    ) -> Self {
        Self {
            definition,
            columns,
            rows: Rows::Of(parent.clone()),
        }
    }

    pub(super) fn fixed(
        rows: RowSet,
        definition: TableDefinition,
        columns: Vec<Arc<dyn ColumnSource>>,
    ) -> Self {
        Self {
            definition,
            columns,
            rows: Rows::Fixed(Arc::new(rows)),
        }
    }
}

impl Dependency for FormulaInput {
    fn satisfied(&self, step: u64) -> bool {
        match &self.rows {
            Rows::Of(parent) => parent.satisfied(step),
            Rows::Fixed(_) => true,
        }
    }
}

impl Table for FormulaInput {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn row_set(&self) -> Arc<RowSet> {
        match &self.rows {
            Rows::Of(parent) => parent.row_set(),
            Rows::Fixed(rows) => rows.clone(),
        }
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        match &self.rows {
            Rows::Of(parent) => parent.prev_row_set(),
            Rows::Fixed(rows) => rows.clone(),
        }
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        self.columns[index].clone()
    }

    fn is_refreshing(&self) -> bool {
        match &self.rows {
            Rows::Of(parent) => parent.is_refreshing(),
            Rows::Fixed(_) => false,
        }
    }

    fn attributes(&self) -> TableAttributes {
        TableAttributes::default()
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        match &self.rows {
            Rows::Of(parent) => parent.update_graph(),
            Rows::Fixed(_) => None,
        }
    }

    /// Nothing listens to a formula's input: the operation listens to the parent.
    fn add_listener(&self, _listener: &Arc<dyn UpdateListener>) {}
}

/// A column whose values are computed from a formula each time they are read, and stored
/// nowhere. A formula that fails makes the read fail, and only for the rows asked for.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ViewColumnSource, as made by
/// io.deephaven.engine.table.impl.select.AbstractFormulaColumn#getDataView
pub(super) struct FormulaSource {
    formula: Arc<dyn Formula>,
    input: FormulaInput,
}

impl FormulaSource {
    pub(super) fn shared(formula: Arc<dyn Formula>, input: FormulaInput) -> Arc<dyn ColumnSource> {
        register_reads_through(Self { formula, input })
    }
}

impl Drop for FormulaSource {
    fn drop(&mut self) {
        forget_reads_through(self);
    }
}

impl ColumnSource for FormulaSource {
    fn column_type(&self) -> &ColumnType {
        self.formula.result_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.formula.evaluate(&self.input, rows, false)
    }

    /// Behavior: io.deephaven.engine.table.impl.sources.ViewColumnSource#getPrev: the formula
    /// over the previous values of its inputs and the previous rows.
    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.formula.evaluate(&self.input, rows, true)
    }
}

/// What a [`LazySource`] has computed so far: one result per distinct tuple of input values.
struct Remembered {
    /// Encodes a tuple of input values as bytes that are equal exactly when the values are.
    /// `None` for a formula that reads no column, whose only tuple is the empty one.
    encoder: Option<RowConverter>,
    /// Where the result for each encoded tuple is: an array of `results` and an index in it.
    places: BTreeMap<Vec<u8>, (usize, usize)>,
    results: Vec<ArrayRef>,
}

/// A column computed from a formula on read, like [`FormulaSource`], that remembers every
/// result by the values of the formula's input columns and never computes one twice. Results
/// are remembered for as long as the column lives, and by previous-value reads too.
///
/// Behavior: io.deephaven.engine.table.impl.select.AbstractFormulaColumn#getLazyView and the
/// `__lazyResultCache` of the class that
/// io.deephaven.engine.table.impl.select.DhFormulaColumn#generateClassBody writes. Java keys
/// its cache by the boxed input values, so every NaN is one key; here NaNs with different
/// bit patterns are different keys, which costs a recomputation and changes no result.
pub(super) struct LazySource {
    formula: Arc<dyn Formula>,
    input: FormulaInput,
    /// The input's columns that the formula reads at the current row.
    reads: Vec<Arc<dyn ColumnSource>>,
    remembered: Mutex<Remembered>,
}

impl LazySource {
    pub(super) fn shared(
        formula: Arc<dyn Formula>,
        input: FormulaInput,
    ) -> Result<Arc<dyn ColumnSource>, TableFailure> {
        let reads = formula
            .columns()
            .iter()
            .map(|name| {
                input
                    .definition
                    .index_of(name)
                    .map(|index| input.columns[index].clone())
                    .ok_or_else(|| failure(format!("the formula reads a missing column {name}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoder = if reads.is_empty() {
            None
        } else {
            let fields = reads
                .iter()
                .map(|source| SortField::new(source.column_type().arrow_type()))
                .collect();
            Some(RowConverter::new(fields).map_err(|e| failure(e.to_string()))?)
        };
        Ok(register_reads_through(Self {
            formula,
            input,
            reads,
            remembered: Mutex::new(Remembered {
                encoder,
                places: BTreeMap::new(),
                results: Vec::new(),
            }),
        }))
    }

    fn read(&self, rows: &RowSet, use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let size = rows.size() as usize;
        if size == 0 {
            return Ok(new_empty_array(&self.column_type().arrow_type()));
        }
        let inputs = self
            .reads
            .iter()
            .map(|source| {
                if use_prev {
                    source.prev_chunk(rows)
                } else {
                    source.chunk(rows)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut remembered = self
            .remembered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let keys: Vec<Vec<u8>> = match &mut remembered.encoder {
            None => vec![Vec::new(); size],
            Some(encoder) => {
                let encoded = encoder
                    .convert_columns(&inputs)
                    .map_err(|e| failure(e.to_string()))?;
                encoded.iter().map(|row| row.as_ref().to_vec()).collect()
            }
        };

        // The first row of each tuple not seen before, in row order.
        let mut unseen: BTreeMap<&[u8], usize> = BTreeMap::new();
        let mut first_rows = Vec::new();
        for (position, key) in keys.iter().enumerate() {
            if !remembered.places.contains_key(key) && !unseen.contains_key(key.as_slice()) {
                unseen.insert(key, first_rows.len());
                first_rows.push(position as RowKey);
            }
        }
        if !first_rows.is_empty() {
            let positions = RowSet::from_keys(first_rows.iter().copied());
            let compute = rows.sub_set_for_positions(&positions, false);
            let computed = self.formula.evaluate(&self.input, &compute, use_prev)?;
            if computed.len() != first_rows.len() {
                return Err(failure(format!(
                    "the formula returned {} values for {} rows",
                    computed.len(),
                    first_rows.len()
                )));
            }
            let batch = remembered.results.len();
            remembered.results.push(computed);
            for (key, index) in unseen {
                remembered.places.insert(key.to_vec(), (batch, index));
            }
        }

        let places: Vec<(usize, usize)> = keys.iter().map(|key| remembered.places[key]).collect();
        let results: Vec<&dyn arrow::array::Array> = remembered
            .results
            .iter()
            .map(|result| result.as_ref())
            .collect();
        arrow::compute::interleave(&results, &places).map_err(|e| failure(e.to_string()))
    }
}

impl Drop for LazySource {
    fn drop(&mut self) {
        forget_reads_through(self);
    }
}

impl ColumnSource for LazySource {
    fn column_type(&self) -> &ColumnType {
        self.formula.result_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, true)
    }
}

/// A column with the same value in every row, now and on the previous cycle.
///
/// Behavior: io.deephaven.engine.table.impl.sources.SingleValueColumnSource as filled by
/// io.deephaven.engine.table.impl.select.analyzers.ConstantColumnLayer
pub(super) struct ConstantSource {
    column_type: ColumnType,
    /// One element.
    value: ArrayRef,
}

impl ConstantSource {
    /// Evaluates `formula`, which reads no column and no row variable, once.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.analyzers.ConstantColumnLayer#initialize,
    /// which reads the formula at row key 0 whatever rows the table has.
    pub(super) fn evaluate(
        formula: &Arc<dyn Formula>,
        definition: TableDefinition,
        columns: Vec<Arc<dyn ColumnSource>>,
    ) -> Result<Arc<dyn ColumnSource>, TableFailure> {
        let row = RowSet::from_keys([0]);
        let input = FormulaInput::fixed(row.clone(), definition, columns);
        let value = formula.evaluate(&input, &row, false)?;
        if value.len() != 1 {
            return Err(failure(format!(
                "the formula returned {} values for one row",
                value.len()
            )));
        }
        Ok(Arc::new(Self {
            column_type: formula.result_type().clone(),
            value,
        }))
    }

    fn repeat(&self, count: usize) -> Result<ArrayRef, TableFailure> {
        let indices = UInt32Array::from(vec![0u32; count]);
        arrow::compute::take(self.value.as_ref(), &indices, None)
            .map_err(|e| failure(e.to_string()))
    }
}

impl ColumnSource for ConstantSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.repeat(rows.size() as usize)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.repeat(rows.size() as usize)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], _use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let indices: UInt32Array = keys
            .iter()
            .map(|&key| (key != NULL_ROW_KEY).then_some(0u32))
            .collect();
        arrow::compute::take(self.value.as_ref(), &indices, None)
            .map_err(|e| failure(e.to_string()))
    }
}

/// Maps position `p` to the `p`th key of a fixed row set: how a flat static result reads the
/// columns of a parent that is not flat.
///
/// Behavior: io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection over a static
/// table's row set
pub(super) struct ByPosition(pub(super) Arc<RowSet>);

impl RowRedirection for ByPosition {
    fn redirect(&self, keys: &[RowKey], _use_prev: bool) -> Vec<RowKey> {
        keys.iter()
            .map(|&key| {
                u64::try_from(key)
                    .ok()
                    .and_then(|position| self.0.get(position))
                    .unwrap_or(NULL_ROW_KEY)
            })
            .collect()
    }
}
