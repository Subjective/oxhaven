//! The result columns of an aggregation: one value per group, at row key = slot.

use super::values::failure;
use crate::table::{shared_reads_through, tracked_source};
use arrow::array::{new_null_array, Array, ArrayRef, UInt32Array};
use oxhaven_column::{Clock, WritableSource};
use oxhaven_contracts::graph::{ClockPhase, ClockValue, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

/// A value's place in storage: the batch it was written in and its index there.
type Place = (u32, u32);

/// Where an unwritten row key reads from: the single null of batch 0.
const NULL_PLACE: Place = (0, 0);

/// A writable column whose row keys are the dense slots `0..n` of an aggregation, with
/// previous values for the current cycle.
///
/// It behaves as [`oxhaven_column::WritableSource`] does for the writes an aggregation
/// makes (values written by row key; a key never written reads as null; while the graph
/// is updating, a write keeps the value the key held when the cycle began, and previous
/// reads answer with it until the clock moves on), but finds a key's value by indexing a
/// vector instead of searching a tree. Values are kept as the Arrow arrays they were
/// written from, so nothing here depends on the column type.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource, the array
/// sources Java's aggregation operators keep their results in, with
/// io.deephaven.engine.table.impl.sources.IntegerArraySource#startTrackingPrevValues for
/// previous values
pub(crate) struct SlotSource {
    column_type: ColumnType,
    clock: Option<Arc<dyn Clock>>,
    state: RwLock<State>,
}

struct State {
    /// Written arrays. Batch 0 is a single null.
    batches: Vec<ArrayRef>,
    /// Total length of `batches`, excluding batch 0.
    stored: u64,
    /// The place of each row key's value; keys past the end read as null.
    places: Vec<Place>,
    /// What each key changed in step `prev_step` held before it, first write wins.
    prev: HashMap<RowKey, Place>,
    prev_step: u64,
}

impl SlotSource {
    /// An empty column. With `clock`, writes made while it is updating keep previous values.
    pub fn new(column_type: ColumnType, clock: Option<Arc<dyn Clock>>) -> SlotSource {
        let null = new_null_array(&column_type.arrow_type(), 1);
        SlotSource {
            column_type,
            clock,
            state: RwLock::new(State {
                batches: vec![null],
                stored: 0,
                places: Vec::new(),
                prev: HashMap::new(),
                prev_step: 0,
            }),
        }
    }

    fn updating_step(&self) -> Option<u64> {
        match self.clock.as_ref()?.now() {
            ClockValue {
                step,
                phase: ClockPhase::Updating,
            } => Some(step),
            _ => None,
        }
    }

    /// Writes `values` at `keys`, in the order given; when a key repeats, its last value
    /// wins.
    pub fn fill_by_keys(&self, keys: &[RowKey], values: &ArrayRef) -> Result<(), TableFailure> {
        let expected = self.column_type.arrow_type();
        if values.data_type() != &expected {
            return Err(failure(format!(
                "column of type {:?} needs Arrow type {expected}, got {}",
                self.column_type,
                values.data_type()
            )));
        }
        if values.len() != keys.len() {
            return Err(failure(format!(
                "{} values for {} row keys",
                values.len(),
                keys.len()
            )));
        }
        if let Some(bad) = keys
            .iter()
            .find(|&&k| !(0..i64::from(u32::MAX)).contains(&k))
        {
            return Err(failure(format!("cannot write at row key {bad}")));
        }
        if keys.is_empty() {
            return Ok(());
        }
        let step = self.updating_step();
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if step != Some(state.prev_step) {
            state.prev.clear();
        }
        if let Some(step) = step {
            state.prev_step = step;
        }
        state.compact_if_wasteful();
        let Ok(batch) = u32::try_from(state.batches.len()) else {
            return Err(failure("aggregation result column is full"));
        };
        state.batches.push(values.clone());
        state.stored += values.len() as u64;
        let end = keys.iter().max().map_or(0, |&k| k as usize + 1);
        if state.places.len() < end {
            state.places.resize(end, NULL_PLACE);
        }
        let State { places, prev, .. } = &mut *state;
        for (index, &key) in keys.iter().enumerate() {
            let old = std::mem::replace(&mut places[key as usize], (batch, index as u32));
            if step.is_some() {
                prev.entry(key).or_insert(old);
            }
        }
        Ok(())
    }

    fn gather(
        &self,
        keys: impl Iterator<Item = RowKey>,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure> {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let overlay =
            use_prev && !state.prev.is_empty() && self.updating_step() == Some(state.prev_step);
        let places: Vec<Place> = keys
            .map(|key| {
                if overlay {
                    if let Some(&place) = state.prev.get(&key) {
                        return place;
                    }
                }
                usize::try_from(key)
                    .ok()
                    .and_then(|key| state.places.get(key).copied())
                    .unwrap_or(NULL_PLACE)
            })
            .collect();
        gather(&state.batches, &places)
    }
}

impl State {
    /// Rewrites storage as one array when most stored values are dead or are spread over
    /// many small arrays. Skipped while previous values are recorded, because those still
    /// point into the old arrays.
    fn compact_if_wasteful(&mut self) {
        if !self.prev.is_empty() {
            return;
        }
        let live = self.places.len() as u64;
        let batches = self.batches.len() as u64 - 1;
        let mostly_dead = self.stored > 2 * live + 1024;
        let fragmented = batches > 64 && batches * 64 > live;
        if !(mostly_dead || fragmented) {
            return;
        }
        let Ok(compacted) = gather(&self.batches, &self.places) else {
            return;
        };
        // Unwritten keys become nulls of the new batch, which reads the same.
        self.batches.truncate(1);
        self.stored = live;
        self.places = (0..live as u32).map(|index| (1, index)).collect();
        self.batches.push(compacted);
    }
}

/// The values at `places`, in order.
fn gather(batches: &[ArrayRef], places: &[Place]) -> Result<ArrayRef, TableFailure> {
    let to_failure = |error: arrow::error::ArrowError| failure(error.to_string());
    // The common case of a result written in one piece: one `take` from that batch.
    if let Some(&(batch, _)) = places.first() {
        if batch != 0 && places.iter().all(|&(b, _)| b == batch) {
            let indices: UInt32Array = places.iter().map(|&(_, index)| index).collect();
            return arrow::compute::take(&batches[batch as usize], &indices, None)
                .map_err(to_failure);
        }
    }
    let mut dense: HashMap<u32, usize> = HashMap::new();
    let mut used: Vec<&dyn Array> = Vec::new();
    let indices: Vec<(usize, usize)> = places
        .iter()
        .map(|&(batch, index)| {
            let position = *dense.entry(batch).or_insert_with(|| {
                used.push(batches[batch as usize].as_ref());
                used.len() - 1
            });
            (position, index as usize)
        })
        .collect();
    if used.is_empty() {
        used.push(batches[0].as_ref());
    }
    arrow::compute::interleave(&used, &indices).map_err(to_failure)
}

impl ColumnSource for SlotSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.gather(rows.iter(), false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.gather(rows.iter(), true)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        self.gather(keys.iter().copied(), use_prev)
    }
}

/// A result column of an aggregation: a [`SlotSource`], or for a column of tables a
/// [`WritableSource`], which keeps alive the tables its cells name.
#[derive(Clone)]
pub(crate) enum ResultColumn {
    Slots(Arc<SlotSource>),
    Tables(Arc<WritableSource>),
}

impl ResultColumn {
    /// An empty column; with `graph`, it keeps previous values.
    pub fn new(column_type: ColumnType, graph: Option<&Arc<dyn UpdateGraph>>) -> ResultColumn {
        if matches!(column_type, ColumnType::Table(_)) {
            return ResultColumn::Tables(match graph {
                Some(graph) => tracked_source(graph, column_type),
                None => Arc::new(WritableSource::new(column_type)),
            });
        }
        let clock = graph.map(|graph| Arc::new(graph.clone()) as Arc<dyn Clock>);
        ResultColumn::Slots(Arc::new(SlotSource::new(column_type, clock)))
    }

    pub fn column_type(&self) -> &ColumnType {
        match self {
            ResultColumn::Slots(source) => source.column_type(),
            ResultColumn::Tables(source) => source.column_type(),
        }
    }

    pub fn fill_by_keys(&self, keys: &[RowKey], values: &ArrayRef) -> Result<(), TableFailure> {
        match self {
            ResultColumn::Slots(source) => source.fill_by_keys(keys, values),
            ResultColumn::Tables(source) => source.fill_by_keys(keys, values),
        }
    }

    /// The column, to read.
    pub fn as_source(&self) -> &dyn ColumnSource {
        match self {
            ResultColumn::Slots(source) => &**source,
            ResultColumn::Tables(source) => &**source,
        }
    }

    /// The column, as a result shows a column that holds its values.
    pub fn source(&self) -> Arc<dyn ColumnSource> {
        match self {
            ResultColumn::Slots(source) => source.clone(),
            ResultColumn::Tables(source) => source.clone(),
        }
    }

    /// The column, as a result shows a column that Java reads from somewhere else.
    pub fn shared(&self) -> Arc<dyn ColumnSource> {
        match self {
            ResultColumn::Slots(source) => shared_reads_through(source),
            ResultColumn::Tables(source) => shared_reads_through(source),
        }
    }
}

#[cfg(test)]
#[path = "slot_source_tests.rs"]
mod tests;
