//! `merge` of a partitioned table: a union whose parts come and go with the rows of the
//! underlying table.
//!
//! The key space is laid out as in the plain `merge` (`ops/merge/union.rs`): each
//! constituent has a slot, sized in whole allocation units, in which its rows keep their
//! own row keys. Slots are in the order of the underlying table's rows. A slot grows when
//! its constituent outgrows it and never shrinks; a slot appears where a row of the
//! underlying table is added and disappears with it, and the slots after it move.

use super::{failure, tables_of, PartitionedError, PartitionedTable};
use crate::ops::merge::{append_shift_data, key_space_for, next_slot, shared_attributes};
use crate::table::{mark, reads_through, Marks, QueryTable, TableSpec};
use arrow::array::{new_empty_array, Array, ArrayRef, AsArray};
use arrow::datatypes::Int64Type;
use oxhaven_column::FlatSource;
use oxhaven_contracts::graph::{locked, ClockPhase, Dependency, Notification, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_rowset::{RowSetBuilderRandom, ShiftBuilder};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak};

/// No step: steps start at 1.
const NEVER: u64 = 0;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where each slot starts and what is read in it.
#[derive(Default)]
struct Layout {
    /// `firsts[i]` is the first row key of slot `i`; the last element is the end of the
    /// allocated key space.
    firsts: Vec<RowKey>,
    /// For each slot, the constituent's column of each result column.
    columns: Vec<Vec<Arc<dyn ColumnSource>>>,
}

struct Layouts {
    current: Arc<Layout>,
    /// The layout before the change made on step `changed_step`.
    previous: Arc<Layout>,
    changed_step: u64,
}

/// The layout of one merge, shared by its columns and its listener.
struct Shared {
    graph: Arc<dyn UpdateGraph>,
    layouts: RwLock<Layouts>,
}

impl Shared {
    fn layout(&self, use_prev: bool) -> Arc<Layout> {
        let layouts = self.layouts.read().unwrap_or_else(PoisonError::into_inner);
        let clock = self.graph.clock();
        let changed_this_cycle =
            clock.phase == ClockPhase::Updating && clock.step == layouts.changed_step;
        if use_prev && changed_this_cycle {
            layouts.previous.clone()
        } else {
            layouts.current.clone()
        }
    }

    fn replace(&self, layout: Layout) {
        let mut layouts = self.layouts.write().unwrap_or_else(PoisonError::into_inner);
        layouts.previous = std::mem::replace(&mut layouts.current, Arc::new(layout));
        layouts.changed_step = self.graph.clock().step;
    }
}

/// One column of the result.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UnionColumnSource
struct UnionColumn {
    column_type: ColumnType,
    index: usize,
    shared: Arc<Shared>,
}

impl UnionColumn {
    fn read(&self, rows: &RowSet, use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let layout = self.shared.layout(use_prev);
        let mut chunks: Vec<ArrayRef> = Vec::new();
        let mut slot = 0;
        let mut inner: Vec<RowRange> = Vec::new();
        let mut flush = |slot: usize, inner: &mut Vec<RowRange>| -> Result<(), TableFailure> {
            if inner.is_empty() {
                return Ok(());
            }
            let rows = RowSet::from_ranges(inner.drain(..));
            let source = &layout.columns[slot][self.index];
            chunks.push(if use_prev {
                source.prev_chunk(&rows)?
            } else {
                source.chunk(&rows)?
            });
            Ok(())
        };
        for range in rows.ranges() {
            let mut first = range.first();
            while first <= range.last() {
                if layout.firsts.len() < 2 || first >= layout.firsts[layout.firsts.len() - 1] {
                    return Err(failure("merge: a row key outside every constituent"));
                }
                if layout.firsts[slot + 1] <= first {
                    flush(slot, &mut inner)?;
                    while layout.firsts[slot + 1] <= first {
                        slot += 1;
                    }
                }
                let base = layout.firsts[slot];
                let last = range.last().min(layout.firsts[slot + 1] - 1);
                inner.push(RowRange::new(first - base, last - base));
                first = last + 1;
            }
        }
        flush(slot, &mut inner)?;
        if chunks.is_empty() {
            return Ok(new_empty_array(&self.column_type.arrow_type()));
        }
        let parts: Vec<&dyn Array> = chunks.iter().map(|chunk| chunk.as_ref()).collect();
        arrow::compute::concat(&parts).map_err(|error| failure(error.to_string()))
    }
}

impl ColumnSource for UnionColumn {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, true)
    }
}

/// Keeps one parent's update for the current step.
///
/// Behavior: io.deephaven.engine.table.impl.ListenerRecorder
struct Recorder {
    graph: Arc<dyn UpdateGraph>,
    parent: Arc<dyn Table>,
    union: Weak<Union>,
    recorded: Mutex<(u64, Option<Arc<TableUpdate>>)>,
}

impl Recorder {
    fn subscribe(
        union: &Weak<Union>,
        graph: &Arc<dyn UpdateGraph>,
        parent: Arc<dyn Table>,
    ) -> Arc<Recorder> {
        let recorder = Arc::new(Recorder {
            graph: graph.clone(),
            parent,
            union: union.clone(),
            recorded: Mutex::new((NEVER, None)),
        });
        let listener: Arc<dyn UpdateListener> = recorder.clone();
        recorder.parent.add_listener(&listener);
        recorder
    }

    /// The update recorded on `step`, if any.
    fn recorded(&self, step: u64) -> Option<Arc<TableUpdate>> {
        let recorded = lock(&self.recorded);
        recorded.1.clone().filter(|_| recorded.0 == step)
    }

    fn record(&self, update: Option<Arc<TableUpdate>>, failure: Option<&TableFailure>) {
        *lock(&self.recorded) = (self.graph.clock().step, update);
        if let Some(union) = self.union.upgrade() {
            union.parent_notified(failure);
        }
    }
}

impl Dependency for Recorder {
    fn satisfied(&self, step: u64) -> bool {
        self.parent.satisfied(step)
    }
}

impl UpdateListener for Recorder {
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        self.record(Some(Arc::new(update.clone())), None);
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        self.record(None, Some(failure));
    }
}

/// One constituent shown by the merge.
struct Part {
    handle: i64,
    table: Arc<dyn Table>,
    /// The row of the underlying table whose cell it is.
    row_key: RowKey,
    /// The constituent's column of each result column, and its index in the constituent.
    columns: Vec<(usize, Arc<dyn ColumnSource>)>,
    /// `None` for a static constituent.
    recorder: Option<Arc<Recorder>>,
}

impl Part {
    /// Behavior: the definition check of
    /// io.deephaven.engine.table.impl.sources.UnionSourceManager for each constituent
    fn new(
        definition: &TableDefinition,
        row_key: RowKey,
        handle: i64,
        table: Arc<dyn Table>,
    ) -> Result<Part, TableFailure> {
        let columns = definition
            .columns()
            .iter()
            .map(|column| {
                let own = table.definition();
                match own.index_of(&column.name) {
                    Some(index) if own.columns()[index].column_type == column.column_type => {
                        Ok((index, table.column(index)))
                    }
                    _ => Err(failure(format!(
                        "merge: a constituent does not have column {} of the constituent \
                         definition",
                        column.name
                    ))),
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(Part {
            handle,
            table,
            row_key,
            columns,
            recorder: None,
        })
    }

    fn sources(&self) -> Vec<Arc<dyn ColumnSource>> {
        self.columns
            .iter()
            .map(|(_, source)| source.clone())
            .collect()
    }
}

struct State {
    parts: Vec<Part>,
    enqueued_step: u64,
    completed_step: u64,
    failed: bool,
    upstream_failure: Option<TableFailure>,
    /// The constituents of the rows the underlying table added or replaced on a step,
    /// read once the underlying table had finished that step.
    incoming: (u64, Result<Vec<Part>, TableFailure>),
}

/// The listener of a merge: one recorder on the underlying table and one on each
/// constituent that is shown. It runs once per step in which any of them changed, after
/// all of them, and the constituents that the step brought in, have finished.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UnionSourceManager with its
/// `ConstituentChangesListener` and `MergedUnionListener`, and
/// io.deephaven.engine.table.impl.partitioned.ConstituentDependency for the wait on the
/// constituents that a step brings in
struct Union {
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    underlying: Arc<Recorder>,
    /// The constituent column of the underlying table and its index.
    cells: (usize, Arc<dyn ColumnSource>),
    shared: Arc<Shared>,
    result: OnceLock<Weak<QueryTable>>,
    state: Mutex<State>,
}

fn place(builder: &mut RowSetBuilderRandom, rows: &RowSet, offset: RowKey) {
    builder.add_ranges(
        rows.ranges()
            .map(|range| RowRange::new(range.first() + offset, range.last() + offset)),
    );
}

impl Union {
    fn parts_at(&self, rows: &RowSet) -> Result<Vec<Part>, TableFailure> {
        let tables = self.cells.1.chunk(rows)?;
        let handles = tables.as_primitive::<Int64Type>().clone();
        tables_of(&tables)?
            .into_iter()
            .zip(rows.iter())
            .zip(handles.iter())
            .map(|((table, row_key), handle)| match (table, handle) {
                (Some(table), Some(handle)) => Part::new(&self.definition, row_key, handle, table),
                _ => Err(failure("merge: a partitioned table has a null constituent")),
            })
            .collect()
    }

    /// The rows of the underlying table whose constituent is new on this step: the added
    /// rows, and the modified rows when the constituent column was modified.
    fn changed_cells(&self, update: &TableUpdate) -> RowSet {
        if update.modified_columns.contains(self.cells.0) {
            update.added.union(&update.modified)
        } else {
            update.added.clone()
        }
    }

    /// Reads, once per step, the constituents that the underlying table's update brings
    /// in. The underlying table must have finished the step.
    fn read_incoming(&self, state: &mut State, step: u64) {
        if state.incoming.0 == step {
            return;
        }
        let incoming = match self.underlying.recorded(step) {
            Some(update) => self.parts_at(&self.changed_cells(&update)),
            None => Ok(Vec::new()),
        };
        state.incoming = (step, incoming);
    }

    fn parent_notified(self: &Arc<Self>, failure: Option<&TableFailure>) {
        let step = self.graph.clock().step;
        {
            let mut state = lock(&self.state);
            if state.failed {
                return;
            }
            if state.upstream_failure.is_none() {
                state.upstream_failure = failure.cloned();
            }
            if state.enqueued_step == step {
                return;
            }
            state.enqueued_step = step;
        }
        self.graph
            .add_notification(Box::new(UnionNotification(self.clone())));
    }

    fn can_execute(&self, step: u64) -> bool {
        if !self.underlying.satisfied(step) {
            return false;
        }
        let mut state = lock(&self.state);
        if !state.parts.iter().all(|part| part.table.satisfied(step)) {
            return false;
        }
        self.read_incoming(&mut state, step);
        match &state.incoming.1 {
            Ok(parts) => parts.iter().all(|part| part.table.satisfied(step)),
            // The failure is reported when the notification runs.
            Err(_) => true,
        }
    }

    fn run(self: &Arc<Self>) {
        let step = self.graph.clock().step;
        let mut state = lock(&self.state);
        let outcome = match state.upstream_failure.take() {
            Some(failure) => Err(failure),
            None => self.process(&mut state, step),
        };
        state.completed_step = step;
        if outcome.is_err() {
            state.failed = true;
        }
        drop(state);
        let Some(result) = self.result.get().and_then(Weak::upgrade) else {
            return;
        };
        match outcome {
            Ok(update) => result.notify(update),
            Err(failure) => result.fail(failure),
        }
    }

    /// Behavior: UnionSourceManager.ChangeProcessingContext#processChanges
    fn process(
        self: &Arc<Self>,
        state: &mut State,
        step: u64,
    ) -> Result<TableUpdate, TableFailure> {
        self.read_incoming(state, step);
        let incoming = std::mem::replace(&mut state.incoming.1, Ok(Vec::new()))?;
        let underlying = self.underlying.recorded(step);
        // The handle each row with a new cell holds now.
        let replacing: HashMap<RowKey, i64> = incoming
            .iter()
            .map(|part| (part.row_key, part.handle))
            .collect();
        let prev = self.shared.layout(false);

        let mut added = RowSetBuilderRandom::new();
        let mut removed = RowSetBuilderRandom::new();
        let mut modified = RowSetBuilderRandom::new();
        let mut shifts = ShiftBuilder::new();
        let mut modified_columns = ModifiedColumnSet::empty();
        let mut firsts: Vec<RowKey> = vec![0];
        let mut parts: Vec<Part> = Vec::with_capacity(state.parts.len() + incoming.len());
        let weak = Arc::downgrade(self);
        let mut incoming = incoming.into_iter().peekable();
        let admit = |mut part: Part,
                     firsts: &mut Vec<RowKey>,
                     parts: &mut Vec<Part>,
                     added: &mut RowSetBuilderRandom|
         -> Result<(), TableFailure> {
            check_listenable(&part.table)?;
            let first = firsts[firsts.len() - 1];
            let rows = part.table.row_set();
            firsts.push(next_slot(first, key_space_for(rows.last_row_key())?)?);
            place(added, &rows, first);
            if part.table.is_refreshing() {
                part.recorder = Some(Recorder::subscribe(&weak, &self.graph, part.table.clone()));
            }
            parts.push(part);
            Ok(())
        };

        for (slot, mut part) in std::mem::take(&mut state.parts).into_iter().enumerate() {
            let prev_first = prev.firsts[slot];
            let prev_allocation = prev.firsts[slot + 1] - prev_first;
            let prev_last = prev.firsts[slot + 1] - 1;
            // Where the row of this constituent is now, if it is still its row.
            let row_key = match &underlying {
                Some(update) if update.removed.contains(part.row_key) => None,
                Some(update) => Some(update.shifted.apply_to_key(part.row_key)),
                None => Some(part.row_key),
            };
            let kept = row_key.filter(|row_key| {
                replacing
                    .get(row_key)
                    .is_none_or(|&handle| handle == part.handle)
            });
            let Some(row_key) = kept else {
                place(&mut removed, &part.table.prev_row_set(), prev_first);
                continue;
            };
            while incoming.peek().is_some_and(|new| new.row_key < row_key) {
                let new = incoming.next().expect("peeked");
                admit(new, &mut firsts, &mut parts, &mut added)?;
            }
            // The cell was modified to the table it already held.
            if incoming.peek().is_some_and(|new| new.row_key == row_key) {
                incoming.next();
            }
            part.row_key = row_key;
            let curr_first = firsts[firsts.len() - 1];
            let slot_delta = curr_first - prev_first;
            let changes = part
                .recorder
                .as_ref()
                .and_then(|recorder| recorder.recorded(step));
            match changes.filter(|changes| !changes.is_empty()) {
                None => {
                    firsts.push(next_slot(curr_first, prev_allocation)?);
                    if !part.table.row_set().is_empty() {
                        shifts.shift_range(prev_first, prev_last, slot_delta);
                    }
                }
                Some(changes) => {
                    let needed = key_space_for(part.table.row_set().last_row_key())?;
                    let allocation = needed.max(prev_allocation);
                    firsts.push(next_slot(curr_first, allocation)?);
                    place(&mut added, &changes.added, curr_first);
                    place(&mut removed, &changes.removed, prev_first);
                    place(&mut modified, &changes.modified, curr_first);
                    modified_columns = modified_columns.union(&ModifiedColumnSet::of(
                        part.columns
                            .iter()
                            .enumerate()
                            .filter(|(_, (own, _))| changes.modified_columns.contains(*own))
                            .map(|(result, _)| result),
                    ));
                    if part.table.prev_row_set().size() != changes.removed.size() {
                        if changes.shifted.is_empty() {
                            shifts.shift_range(prev_first, prev_last, slot_delta);
                        } else {
                            append_shift_data(
                                &mut shifts,
                                &changes.shifted,
                                prev_first,
                                prev_allocation,
                                curr_first,
                                allocation,
                            );
                        }
                    }
                }
            }
            parts.push(part);
        }
        for new in incoming {
            admit(new, &mut firsts, &mut parts, &mut added)?;
        }

        let shifted = shifts.build().map_err(|error| failure(error.to_string()))?;
        self.shared.replace(Layout {
            firsts,
            columns: parts.iter().map(Part::sources).collect(),
        });
        state.parts = parts;
        Ok(TableUpdate {
            added: added.build(),
            removed: removed.build(),
            modified: modified.build(),
            shifted,
            modified_columns,
        })
    }
}

impl Dependency for Union {
    fn satisfied(&self, step: u64) -> bool {
        let (completed_step, enqueued_step) = {
            let state = lock(&self.state);
            (state.completed_step, state.enqueued_step)
        };
        if completed_step == step {
            true
        } else if enqueued_step == step {
            false
        } else {
            self.can_execute(step)
        }
    }
}

struct UnionNotification(Arc<Union>);

impl Notification for UnionNotification {
    fn can_execute(&self, step: u64) -> bool {
        self.0.can_execute(step)
    }

    fn run(self: Box<Self>) {
        self.0.run();
    }
}

fn mark_merged(result: &Arc<QueryTable>) {
    // Behavior: PartitionedTableImpl#merge sets `Table.MERGED_TABLE_ATTRIBUTE`
    mark(
        result,
        Marks {
            merged: true,
            ..Marks::default()
        },
    );
}

/// A static table of `definition` without rows.
pub(super) fn empty(definition: TableDefinition) -> Result<Arc<QueryTable>, TableFailure> {
    let columns = definition
        .columns()
        .iter()
        .map(|column| {
            let empty = new_empty_array(&column.column_type.arrow_type());
            Ok(
                Arc::new(FlatSource::new(column.column_type.clone(), empty)?)
                    as Arc<dyn ColumnSource>,
            )
        })
        .collect::<Result<_, TableFailure>>()?;
    Ok(QueryTable::new_static(TableSpec::new(
        definition,
        RowSet::empty(),
        columns,
    )))
}

/// A constituent that has failed cannot be listened to: the merge fails, when it is made
/// or in the cycle that shows the constituent.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#addUpdateListener, as
/// UnionSourceManager calls it for each constituent
fn check_listenable(constituent: &Arc<dyn Table>) -> Result<(), TableFailure> {
    if constituent.is_failed() {
        return Err(failure(
            "io.deephaven.engine.exceptions.TableAlreadyFailedException: Can not listen to \
             failed table QueryTable",
        ));
    }
    Ok(())
}

/// Behavior: io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#merge
pub(super) fn merge(partitioned: &PartitionedTable) -> Result<Arc<dyn Table>, PartitionedError> {
    let underlying = partitioned.table().clone();
    let definition = partitioned.constituent_definition().clone();
    let graph = match underlying.update_graph() {
        Some(graph) if underlying.is_refreshing() => graph,
        _ => {
            let constituents = partitioned.constituents()?;
            if !constituents.is_empty() {
                return crate::ops::merge::merge(&constituents)
                    .map_err(|error| PartitionedError::IllegalArgument(error.to_string()));
            }
            let result = empty(definition)?;
            mark_merged(&result);
            return Ok(result);
        }
    };
    let cells = (
        underlying
            .definition()
            .index_of(partitioned.constituent_column())
            .expect("checked when the partitioned table was made"),
        partitioned.column(),
    );
    let result = locked(&*graph, || -> Result<Arc<QueryTable>, TableFailure> {
        if underlying.is_failed() {
            return Err(failure("the parent table has failed"));
        }
        let shared = Arc::new(Shared {
            graph: graph.clone(),
            layouts: RwLock::new(Layouts {
                current: Arc::default(),
                previous: Arc::default(),
                changed_step: NEVER,
            }),
        });
        let union = Arc::new_cyclic(|union: &Weak<Union>| Union {
            graph: graph.clone(),
            definition: definition.clone(),
            underlying: Recorder::subscribe(union, &graph, underlying.clone()),
            cells,
            shared: shared.clone(),
            result: OnceLock::new(),
            state: Mutex::new(State {
                parts: Vec::new(),
                enqueued_step: NEVER,
                completed_step: NEVER,
                failed: false,
                upstream_failure: None,
                incoming: (NEVER, Ok(Vec::new())),
            }),
        });
        let weak = Arc::downgrade(&union);
        let mut parts = union.parts_at(&underlying.row_set())?;
        let mut firsts = vec![0];
        let mut rows = RowSetBuilderRandom::new();
        for part in &mut parts {
            check_listenable(&part.table)?;
            let first = firsts[firsts.len() - 1];
            let own = part.table.row_set();
            firsts.push(next_slot(first, key_space_for(own.last_row_key())?)?);
            place(&mut rows, &own, first);
            if part.table.is_refreshing() {
                part.recorder = Some(Recorder::subscribe(&weak, &graph, part.table.clone()));
            }
        }
        *shared
            .layouts
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Layouts {
            current: Arc::new(Layout {
                firsts,
                columns: parts.iter().map(Part::sources).collect(),
            }),
            previous: Arc::default(),
            changed_step: NEVER,
        };
        let constituents: Vec<Arc<dyn Table>> =
            parts.iter().map(|part| part.table.clone()).collect();
        lock(&union.state).parts = parts;
        let columns = definition
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                reads_through(UnionColumn {
                    column_type: column.column_type.clone(),
                    index,
                    shared: shared.clone(),
                })
            })
            .collect();
        let mut spec = TableSpec::new(definition.clone(), rows.build(), columns);
        if !partitioned.constituent_changes_permitted() {
            // Behavior: PartitionedTableImpl#merge, which gives the result the attributes
            // its constituents share when no constituent can come or go
            spec = spec.with_attributes(shared_attributes(&constituents));
        }
        let result = QueryTable::dependent(graph.clone(), spec, union.clone());
        let _ = union.result.set(Arc::downgrade(&result));
        Ok(result)
    })?;
    mark_merged(&result);
    Ok(result)
}
