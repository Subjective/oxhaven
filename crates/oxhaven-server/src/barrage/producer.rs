//! The producer of a table's Barrage messages: it records the table's updates, and sends
//! each subscriber the snapshots it needs and the changes within its viewport and columns.
//!
//! Port of `io.deephaven.server.barrage.BarrageMessageProducer`. The life of a
//! subscription is Java's:
//!
//! 1. A new or changed subscription waits in the pending list with the viewport and
//!    columns the client asked for.
//! 2. The propagation job accepts it: the request becomes the target, and the subscription
//!    is "growing" until snapshots have covered the target.
//! 3. Each run of the job reads one snapshot for every growing subscription, under the
//!    update graph's lock, and at that moment the snapshot's viewport and columns become
//!    the ones the update listener records for.
//! 4. Updates recorded up to the snapshot are sent first, for the old viewport; then the
//!    snapshot; then the updates recorded after it, for the new viewport.
//!
//! What differs from Java, all of it invisible in the state a client ends up with:
//!
//! - A snapshot is read under the shared lock, where Java first tries without the lock.
//! - The size of a growing snapshot does not depend on how long the last one took: the
//!   first is Java's minimum number of cells, later ones its maximum.
//! - Blink tables are refused, see [`Producer::add_subscription`].

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::datatypes::Schema;
use oxhaven_contracts::graph::{locked, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_engine::graph::{ParentListener, UpdateHandler};
use oxhaven_engine::update::compose;
use oxhaven_proto::flight::FlightData;
use tonic::Status;

use super::codec::ColumnSet;
use super::writer::{subscription_messages, BarrageMessage, ModColumn, SubView, WireOptions};
use crate::flight::{schema_message, wire_schema};
use crate::table::support::internal_status;

/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#MIN_SNAPSHOT_CELL_COUNT`
pub(super) const MIN_SNAPSHOT_CELL_COUNT: u64 = 1 << 13;

/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#MAX_SNAPSHOT_CELL_COUNT`
pub(super) const MAX_SNAPSHOT_CELL_COUNT: u64 = 1 << 24;

/// Something to run off the calling thread.
pub type Job = Box<dyn FnOnce() + Send>;

/// Runs the propagation job, now or later, and tells the time.
///
/// Port of the part of `io.deephaven.server.util.Scheduler` the producer uses.
pub trait Scheduler: Send + Sync {
    /// Milliseconds on a clock that `run_at` times refer to.
    fn now_ms(&self) -> i64;
    fn run_immediately(&self, job: Job);
    fn run_at(&self, time_ms: i64, job: Job);
}

/// Where a subscription's messages go.
///
/// Port of the `StreamObserver<BarrageMessageWriter.MessageView>` of a subscription.
pub trait Listener: Send + Sync {
    fn on_next(&self, messages: Vec<FlightData>);
    /// Ends the stream with an error. Nothing sent afterwards arrives.
    fn on_error(&self, status: Status);
    /// Ends the stream.
    fn on_completed(&self);
}

/// Names a subscription to its producer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SubscriptionId(u64);

/// See the module documentation for the life of a subscription.
///
/// Port of `BarrageMessageProducer.Subscription`.
struct Subscription {
    options: WireOptions,
    listener: Arc<dyn Listener>,

    /// The viewport in effect; `None` for the whole table.
    viewport: Option<RowSet>,
    subscribed_columns: ColumnSet,
    reverse_viewport: bool,

    is_active: bool,
    pending_delete: bool,
    has_pending_update: bool,
    pending_initial_snapshot: bool,

    /// What the client asked for and the job has not accepted yet.
    pending_viewport: Option<RowSet>,
    pending_reverse_viewport: bool,
    pending_columns: Option<ColumnSet>,

    /// What the next snapshot makes the viewport and columns in effect; after the
    /// snapshot was read, what they were before it.
    snapshot_viewport: Option<RowSet>,
    snapshot_columns: Option<ColumnSet>,
    snapshot_reverse_viewport: bool,

    /// Where a changed subscription is growing to.
    target_viewport: Option<RowSet>,
    target_columns: ColumnSet,
    target_reverse_viewport: bool,

    is_growing_viewport: bool,
    /// Positions the client still needs, and the ones the current snapshot brings.
    growing_remaining_viewport: Option<RowSet>,
    growing_incremental_viewport: Option<RowSet>,
    is_first_snapshot: bool,
}

impl Subscription {
    fn is_viewport(&self) -> bool {
        self.viewport.is_some()
    }

    /// Behavior: `BarrageMessageProducer.Subscription#isFullSubscription`
    fn is_full_subscription(&self) -> bool {
        !self.is_viewport()
            || (self.has_pending_update && self.pending_viewport.is_none())
            || (self.is_growing_viewport && self.target_viewport.is_none())
    }

    /// Swaps the viewport and columns in effect with the snapshot's.
    ///
    /// Behavior: `BarrageMessageProducer#flipSnapshotStateForSubscriptions`
    fn flip_snapshot_state(&mut self) {
        std::mem::swap(&mut self.viewport, &mut self.snapshot_viewport);
        std::mem::swap(
            &mut self.reverse_viewport,
            &mut self.snapshot_reverse_viewport,
        );
        let columns = std::mem::take(&mut self.subscribed_columns);
        self.subscribed_columns = self.snapshot_columns.replace(columns).unwrap_or_default();
    }
}

/// One update of the table, with the data of the rows subscribers can see.
///
/// Port of `BarrageMessageProducer.Delta`.
struct Delta {
    step: i64,
    update: TableUpdate,
    recorded_adds: RowSet,
    recorded_mods: RowSet,
    subscribed_columns: ColumnSet,
    modified_columns: ColumnSet,
    /// Per column, the values of `recorded_adds` if the column was recorded.
    add_data: Vec<Option<ArrayRef>>,
    /// Per column, the values of `recorded_mods` if the column was modified and recorded.
    mod_data: Vec<Option<ArrayRef>>,
}

#[derive(Default)]
struct State {
    last_update_time: i64,
    last_scheduled_update_time: i64,
    /// The table's row set as of the last message sent.
    propagation_row_set: RowSet,
    parent_table_size: u64,
    pending_error: Option<TableFailure>,
    pending_deltas: Vec<Delta>,

    /// The union of the viewports in effect, which the listener records data for.
    active_viewport: Option<RowSet>,
    active_reverse_viewport: Option<RowSet>,
    post_snapshot_viewport: Option<RowSet>,
    post_snapshot_reverse_viewport: Option<RowSet>,
    active_columns: ColumnSet,
    post_snapshot_columns: ColumnSet,

    num_full_subscriptions: usize,
    num_growing_subscriptions: usize,
    subscriptions: BTreeMap<SubscriptionId, Subscription>,
    pending: Vec<SubscriptionId>,
    active: Vec<SubscriptionId>,
    next_id: u64,

    /// Whether a snapshot with rows has been read; see the module documentation.
    snapshot_seen: bool,
    /// The producer failed to record an update and records no more.
    stopped: bool,
}

/// What the job needs of a snapshot plan after the state lock is released.
struct SnapshotPlan {
    growing: Vec<SubscriptionId>,
    columns: ColumnSet,
    positions: RowSet,
    reverse_positions: RowSet,
}

/// See the module documentation.
pub struct Producer {
    table: Arc<dyn Table>,
    /// The graph of a refreshing table.
    graph: Option<Arc<dyn UpdateGraph>>,
    scheduler: Arc<dyn Scheduler>,
    update_interval_ms: i64,
    /// The schema every subscriber is sent, and per column whether its field is nullable.
    schema: Schema,
    nullable: Vec<bool>,
    state: Mutex<State>,
    /// Held by the thread that runs the propagation job.
    run_lock: Mutex<()>,
    needs_run: AtomicBool,
    listener: OnceLock<Arc<dyn UpdateListener>>,
    this: Weak<Producer>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn failure(problem: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: problem.to_string(),
    }
}

/// Removes from `rows` the keys it shares with `other` and returns them.
///
/// Behavior: `io.deephaven.engine.rowset.WritableRowSet#extract`
fn extract(rows: &mut RowSet, other: &RowSet) -> RowSet {
    let common = rows.intersect(other);
    rows.remove(&common);
    common
}

/// The key a row had before `shifted`, for a key that is the first (`is_end` false) or
/// last (`is_end` true) of a range of the current row set. A key no row moved to maps to
/// the nearest key outside the range that was moved over it.
///
/// Behavior: `io.deephaven.engine.table.impl.util.ShiftInversionHelper#mapToPrevKeyspace`
fn map_to_prev_keyspace(shifted: &RowSetShiftData, key: RowKey, is_end: bool) -> RowKey {
    let shifts = shifted.ranges();
    let index = shifts.partition_point(|shift| shift.last + shift.delta < key);
    if let Some(shift) = shifts.get(index) {
        if shift.first + shift.delta <= key {
            return key - shift.delta;
        }
        if shift.delta > 0 && shift.first <= key {
            return shift.first - RowKey::from(is_end);
        }
    }
    if let Some(shift) = index.checked_sub(1).map(|before| &shifts[before]) {
        if shift.delta < 0 && key <= shift.last {
            return shift.last + RowKey::from(!is_end);
        }
    }
    key
}

/// Reads rows of `table` as a snapshot message: every row with `positions` of `None`,
/// otherwise the rows at the forward and reverse positions. Only `columns` have data.
/// The caller holds the update graph's lock if the table is refreshing.
///
/// Behavior: `io.deephaven.engine.table.impl.remote.ConstructSnapshot#constructBackplaneSnapshotInPositionSpace`,
/// `ConstructSnapshot#snapshotAllTable`
pub(super) fn construct_snapshot(
    table: &Arc<dyn Table>,
    columns: Option<&ColumnSet>,
    positions: Option<(&RowSet, &RowSet)>,
) -> Result<BarrageMessage, TableFailure> {
    let rows_added = (*table.row_set()).clone();
    let rows_included = match positions {
        None => rows_added.clone(),
        Some((forward, reverse)) => rows_added
            .sub_set_for_positions(forward, false)
            .union(&rows_added.sub_set_for_positions(reverse, true)),
    };
    let definition = table.definition();
    let mut add_columns = Vec::with_capacity(definition.columns().len());
    let mut mod_columns = Vec::with_capacity(definition.columns().len());
    for (index, column) in definition.columns().iter().enumerate() {
        let wanted = columns.is_none_or(|columns| columns.contains(index));
        let empty = new_empty_array(&column.column_type.arrow_type());
        add_columns.push(if wanted && !rows_included.is_empty() {
            table.column(index).chunk(&rows_included)?
        } else {
            empty.clone()
        });
        mod_columns.push(ModColumn {
            rows_modified: RowSet::empty(),
            data: empty,
        });
    }
    // the step the snapshot is consistent with; a static table has none
    let step = match table.update_graph() {
        Some(graph) if table.is_refreshing() => graph.clock().step as i64,
        _ => -1,
    };
    Ok(BarrageMessage {
        first_seq: step,
        last_seq: step,
        is_snapshot: true,
        table_size: rows_added.size() as i64,
        rows_added,
        rows_removed: RowSet::empty(),
        shifted: RowSetShiftData::empty(),
        rows_included,
        add_columns,
        mod_columns,
    })
}

/// Per column of `table`, whether its field is nullable in `schema`.
pub(super) fn nullable_columns(table: &Arc<dyn Table>, schema: &Schema) -> Vec<bool> {
    table
        .definition()
        .columns()
        .iter()
        .map(|column| {
            schema
                .field_with_name(&column.name)
                .map_or(true, |field| field.is_nullable())
        })
        .collect()
}

impl Producer {
    /// Makes the producer of `table` and, if the table is refreshing, starts listening to
    /// it. Updates are sent no more often than every `update_interval_ms`.
    ///
    /// Behavior: `BarrageMessageProducer.Operation#initialize`
    pub fn new(
        table: Arc<dyn Table>,
        scheduler: Arc<dyn Scheduler>,
        update_interval_ms: i64,
    ) -> Arc<Self> {
        let graph = match table.update_graph() {
            Some(graph) if table.is_refreshing() => Some(graph),
            _ => None,
        };
        let schema = wire_schema(&table);
        let nullable = nullable_columns(&table, &schema);
        let producer = Arc::new_cyclic(|this| Producer {
            table,
            graph,
            scheduler,
            update_interval_ms,
            schema,
            nullable,
            state: Mutex::new(State::default()),
            run_lock: Mutex::new(()),
            needs_run: AtomicBool::new(false),
            listener: OnceLock::new(),
            this: this.clone(),
        });
        let subscribe = || {
            lock(&producer.state).parent_table_size = producer.table.row_set().size();
            if producer.graph.is_some() {
                let handler: Arc<dyn UpdateHandler> = producer.clone();
                let listener =
                    ParentListener::subscribe(producer.table.clone(), Arc::downgrade(&handler));
                let _ = producer.listener.set(listener);
            }
        };
        match &producer.graph {
            Some(graph) => locked(graph.as_ref(), subscribe),
            None => subscribe(),
        }
        producer
    }

    pub fn table(&self) -> &Arc<dyn Table> {
        &self.table
    }

    pub fn update_interval_ms(&self) -> i64 {
        self.update_interval_ms
    }

    fn column_count(&self) -> usize {
        self.table.definition().columns().len()
    }

    fn job(&self) -> Job {
        let this = self.this.clone();
        Box::new(move || {
            if let Some(producer) = this.upgrade() {
                producer.run();
            }
        })
    }

    /// Behavior: `BarrageMessageProducer.UpdatePropagationJob#scheduleImmediately`
    fn schedule_immediately(&self) {
        let newly_needed = !self.needs_run.swap(true, Ordering::SeqCst);
        if newly_needed && self.run_lock.try_lock().is_ok() {
            self.scheduler.run_immediately(self.job());
        }
    }

    /// Schedules the job after an update: at once if the last run is at least the update
    /// interval ago, otherwise when the interval has passed, once.
    ///
    /// Behavior: `BarrageMessageProducer#schedulePropagation`
    fn schedule_propagation(&self, state: &mut State) {
        let now = self.scheduler.now_ms();
        if state.last_scheduled_update_time != 0
            && state.last_scheduled_update_time > state.last_update_time
        {
            // an already scheduled run is coming up
        } else if now - state.last_update_time < self.update_interval_ms {
            let next = state.last_update_time + self.update_interval_ms;
            state.last_scheduled_update_time = next;
            self.scheduler.run_at(next, self.job());
        } else {
            self.schedule_immediately();
        }
    }

    /// Adds a subscription, whose first snapshot is sent by the next run of the job.
    /// `columns` of `None` is every column, `viewport` of `None` the whole table.
    ///
    /// Blink tables are not ported: the subscriber is told `UNIMPLEMENTED`, where Java
    /// sends an empty snapshot and then each cycle's rows.
    ///
    /// Behavior: `BarrageMessageProducer#addSubscription`
    pub fn add_subscription(
        &self,
        listener: Arc<dyn Listener>,
        options: WireOptions,
        columns: Option<ColumnSet>,
        viewport: Option<RowSet>,
        reverse_viewport: bool,
    ) -> Option<SubscriptionId> {
        if self.table.attributes().blink {
            listener.on_error(Status::unimplemented(
                "not ported yet: io.deephaven.server.barrage.BarrageMessageProducer (blink tables)",
            ));
            return None;
        }
        let mut state = lock(&self.state);
        let id = SubscriptionId(state.next_id);
        state.next_id += 1;
        let subscription = Subscription {
            options,
            listener,
            viewport: Some(RowSet::empty()),
            subscribed_columns: ColumnSet::default(),
            reverse_viewport,
            is_active: false,
            pending_delete: false,
            has_pending_update: true,
            pending_initial_snapshot: true,
            pending_viewport: viewport,
            pending_reverse_viewport: reverse_viewport,
            pending_columns: Some(columns.unwrap_or_else(|| ColumnSet::all(self.column_count()))),
            snapshot_viewport: None,
            snapshot_columns: None,
            snapshot_reverse_viewport: false,
            target_viewport: None,
            target_columns: ColumnSet::default(),
            target_reverse_viewport: false,
            is_growing_viewport: false,
            growing_remaining_viewport: None,
            growing_incremental_viewport: None,
            is_first_snapshot: false,
        };
        state.subscriptions.insert(id, subscription);
        state.pending.push(id);
        drop(state);
        // the first snapshot should go out as soon as possible
        self.schedule_immediately();
        Some(id)
    }

    /// Marks a subscription as changed and schedules the job. False if there is no such
    /// subscription.
    ///
    /// Behavior: `BarrageMessageProducer#findAndUpdateSubscription`
    fn find_and_update(&self, id: SubscriptionId, update: impl FnOnce(&mut Subscription)) -> bool {
        let mut state = lock(&self.state);
        let Some(subscription) = state.subscriptions.get_mut(&id) else {
            return false;
        };
        update(subscription);
        if !subscription.has_pending_update {
            subscription.has_pending_update = true;
            state.pending.push(id);
        }
        drop(state);
        self.schedule_immediately();
        true
    }

    /// Changes the viewport and columns of a subscription. A subscription to the whole
    /// table cannot be changed: it is ended with an error.
    ///
    /// Behavior: `BarrageMessageProducer#updateSubscription`
    pub fn update_subscription(
        &self,
        id: SubscriptionId,
        viewport: Option<RowSet>,
        columns: Option<ColumnSet>,
        reverse_viewport: bool,
    ) -> bool {
        let all = ColumnSet::all(self.column_count());
        self.find_and_update(id, |subscription| {
            if subscription.is_full_subscription() {
                subscription.listener.on_error(Status::invalid_argument(
                    "cannot change from full subscription to viewport or vice versa",
                ));
                subscription.pending_delete = true;
                return;
            }
            subscription.pending_viewport = viewport;
            subscription.pending_reverse_viewport = reverse_viewport;
            subscription.pending_columns = Some(columns.unwrap_or(all));
        })
    }

    /// Ends a subscription: the next run of the job forgets it and completes its stream.
    ///
    /// Behavior: `BarrageMessageProducer#removeSubscription`
    pub fn remove_subscription(&self, id: SubscriptionId) {
        self.find_and_update(id, |subscription| subscription.pending_delete = true);
    }

    /// Records what subscribers need of one update of the table.
    ///
    /// Behavior: `BarrageMessageProducer#enqueueUpdate`
    fn enqueue_update(
        &self,
        state: &mut State,
        upstream: &TableUpdate,
    ) -> Result<(), TableFailure> {
        let row_set = self.table.row_set();
        let has_viewport =
            state.active_viewport.is_some() || state.active_reverse_viewport.is_some();

        let (mut adds_to_record, mods_to_record) = if state.num_full_subscriptions > 0 {
            (upstream.added.clone(), upstream.modified.clone())
        } else if has_viewport {
            // the rows the viewports cover, forward and reverse together
            let mut delta_viewport = RowSet::empty();
            if let Some(viewport) = &state.active_viewport {
                delta_viewport.insert(&row_set.sub_set_for_positions(viewport, false));
            }
            if let Some(viewport) = &state.active_reverse_viewport {
                delta_viewport.insert(&row_set.sub_set_for_positions(viewport, true));
            }
            (
                delta_viewport.intersect(&upstream.added),
                delta_viewport.intersect(&upstream.modified),
            )
        } else {
            // New viewport subscriptions are waiting for their snapshots: there is no
            // data to record, but the update itself is needed to follow the row set.
            (RowSet::empty(), RowSet::empty())
        };

        // Viewports are positions: added and removed rows move other rows into view.
        // Without previous rows every row is new and already among the adds.
        let prev_row_set = self.table.prev_row_set();
        if has_viewport
            && !(upstream.added.is_empty() && upstream.removed.is_empty())
            && !row_set.is_empty()
            && !prev_row_set.is_empty()
        {
            let mut scoped = Vec::new();
            for id in &state.active {
                let subscription = &state.subscriptions[id];
                let Some(viewport) = &subscription.viewport else {
                    continue;
                };
                if subscription.pending_delete {
                    continue;
                }
                for positions in viewport.ranges() {
                    scoped_rows(
                        &mut scoped,
                        &upstream.shifted,
                        &row_set,
                        &prev_row_set,
                        *positions,
                        subscription.reverse_viewport,
                    );
                }
            }
            // The ranges are in the previous key space, and may name keys between rows
            // and rows the update removes. Java shifts those keys too and then keeps the
            // ones that are rows; only rows that stay can be shifted here.
            let scoped = RowSet::from_ranges(scoped)
                .intersect(&prev_row_set)
                .minus(&upstream.removed);
            let scoped = upstream.shifted.apply(&scoped).map_err(failure)?;
            adds_to_record.insert(&scoped.intersect(&row_set));
        }

        let modified_columns = if upstream.modified.is_empty() {
            ColumnSet::default()
        } else {
            match &upstream.modified_columns {
                ModifiedColumnSet::All => state.active_columns.clone(),
                ModifiedColumnSet::Columns(columns) => {
                    let mut modified = ColumnSet::of(columns.iter().copied());
                    modified.intersect_with(&state.active_columns);
                    modified
                }
            }
        };

        let read = |columns: &ColumnSet, rows: &RowSet| {
            let count = self.column_count();
            let mut data = vec![None; count];
            if !rows.is_empty() {
                for column in columns.iter().filter(|column| *column < count) {
                    data[column] = Some(self.table.column(column).chunk(rows)?);
                }
            }
            Ok::<_, TableFailure>(data)
        };
        let add_data = read(&state.active_columns, &adds_to_record)?;
        let mod_data = read(&modified_columns, &mods_to_record)?;
        let step = self
            .graph
            .as_ref()
            .map_or(-1, |graph| graph.clock().step as i64);
        state.pending_deltas.push(Delta {
            step,
            update: upstream.clone(),
            recorded_adds: adds_to_record,
            recorded_mods: mods_to_record,
            subscribed_columns: state.active_columns.clone(),
            modified_columns,
            add_data,
            mod_data,
        });
        Ok(())
    }

    /// Runs the propagation job, unless another thread is running it: that thread then
    /// runs it again.
    ///
    /// Behavior: `BarrageMessageProducer.UpdatePropagationJob#run`
    pub fn run(&self) {
        self.needs_run.store(true, Ordering::SeqCst);
        loop {
            let Ok(running) = self.run_lock.try_lock() else {
                return;
            };
            if self.needs_run.swap(false, Ordering::SeqCst) {
                if let Err(status) = self.propagate() {
                    let mut state = lock(&self.state);
                    for subscription in state.subscriptions.values() {
                        subscription.listener.on_error(status.clone());
                    }
                    state.subscriptions.clear();
                    state.active.clear();
                    state.pending.clear();
                }
            }
            drop(running);
            if !self.needs_run.load(Ordering::SeqCst) {
                return;
            }
        }
    }

    /// Accepts the pending subscription changes, and plans the snapshot the growing
    /// subscriptions need.
    ///
    /// Behavior: the first half of `BarrageMessageProducer#updateSubscriptionsSnapshotAndPropagate`
    fn accept_changes(
        &self,
        state: &mut State,
        first_subscription: &mut bool,
        deleted: &mut Vec<Subscription>,
    ) -> Option<SnapshotPlan> {
        let mut pending_changes = false;
        let updated = std::mem::take(&mut state.pending);
        if !updated.is_empty() {
            // forget the deleted subscriptions
            let mut index = 0;
            while index < state.active.len() {
                let id = state.active[index];
                if !state.subscriptions[&id].pending_delete {
                    index += 1;
                    continue;
                }
                state.active.swap_remove(index);
                if let Some(subscription) = state.subscriptions.remove(&id) {
                    if !subscription.is_viewport() {
                        state.num_full_subscriptions -= 1;
                    }
                    if subscription.is_growing_viewport {
                        state.num_growing_subscriptions -= 1;
                    }
                    deleted.push(subscription);
                }
            }

            // The viewports of the subscriptions without pending changes; the snapshot
            // adds the others.
            state.build_post_snapshot_viewports(true);

            for id in updated {
                let Some(subscription) = state.subscriptions.get_mut(&id) else {
                    continue;
                };
                if subscription.pending_delete {
                    // It never became active. Java forgets it without completing its
                    // stream; the stream is completed here.
                    if let Some(subscription) = state.subscriptions.remove(&id) {
                        deleted.push(subscription);
                    }
                    continue;
                }
                pending_changes = true;
                if !subscription.is_growing_viewport {
                    subscription.is_growing_viewport = true;
                    state.num_growing_subscriptions += 1;
                }
                subscription.has_pending_update = false;
                if !subscription.is_active {
                    *first_subscription |= state.active.is_empty();
                    subscription.is_active = true;
                    state.active.push(id);
                }
                subscription.target_viewport = subscription.pending_viewport.take();
                subscription.target_columns =
                    subscription.pending_columns.take().unwrap_or_default();
                subscription.target_reverse_viewport = subscription.pending_reverse_viewport;
                subscription.is_first_snapshot = true;
                subscription.growing_remaining_viewport = Some(
                    subscription
                        .target_viewport
                        .clone()
                        .unwrap_or_else(|| RowSet::flat(i64::MAX as u64)),
                );
            }
        }
        if !deleted.is_empty() && !pending_changes {
            // only removals: the viewports recorded for can shrink at once
            state.promote_snapshot_to_active();
        }
        if state.num_growing_subscriptions == 0 {
            return None;
        }

        if !pending_changes {
            state.post_snapshot_viewport = Some(state.active_viewport.clone().unwrap_or_default());
            state.post_snapshot_reverse_viewport =
                Some(state.active_reverse_viewport.clone().unwrap_or_default());
            state.post_snapshot_columns = state.active_columns.clone();
        }

        // viewports are served before subscriptions to the whole table
        let mut columns = ColumnSet::default();
        let mut growing = VecDeque::new();
        for id in &state.active {
            let subscription = &state.subscriptions[id];
            if subscription.is_growing_viewport {
                columns.union_with(&subscription.target_columns);
                if subscription.target_viewport.is_none() {
                    growing.push_back(*id);
                } else {
                    growing.push_front(*id);
                }
            }
        }
        let growing = Vec::from(growing);

        // The snapshot is limited in size, to keep the update graph responsive.
        let cell_count = if state.snapshot_seen {
            MAX_SNAPSHOT_CELL_COUNT
        } else {
            MIN_SNAPSHOT_CELL_COUNT
        };
        let mut rows_remaining = cell_count / (columns.cardinality().max(1) as u64);

        let mut viewport_rows = RowSet::empty();
        let mut reverse_viewport_rows = RowSet::empty();
        let mut positions = RowSet::empty();
        let mut reverse_positions = RowSet::empty();
        for id in &growing {
            let Some(subscription) = state.subscriptions.get_mut(id) else {
                continue;
            };
            let scoping = if subscription.target_reverse_viewport {
                &mut reverse_viewport_rows
            } else {
                &mut viewport_rows
            };
            let mut remaining = subscription
                .growing_remaining_viewport
                .take()
                .unwrap_or_default();

            // What the client has stays valid unless the direction changed or a column
            // was added.
            let added_columns = subscription
                .target_columns
                .minus(&subscription.subscribed_columns);
            let viewport_valid = subscription.reverse_viewport
                == subscription.target_reverse_viewport
                && added_columns.is_empty();
            let mut snapshot_viewport = match &subscription.viewport {
                Some(viewport) if viewport_valid => {
                    if subscription.is_first_snapshot {
                        // rows of the old viewport that the new one keeps are not sent
                        let kept = extract(&mut remaining, viewport);
                        scoping.insert(&kept);
                        kept
                    } else {
                        viewport.clone()
                    }
                }
                _ => RowSet::empty(),
            };
            subscription.is_first_snapshot = false;

            let current = if subscription.target_reverse_viewport {
                &mut reverse_positions
            } else {
                &mut positions
            };
            // rows this snapshot reads anyway, for a subscription served before
            let mut incremental = extract(&mut remaining, current);
            if rows_remaining > 0 {
                let mut additional = remaining.clone();
                match additional.get(rows_remaining) {
                    Some(key) => {
                        additional.retain(&RowSet::flat(key as u64));
                        remaining.remove(&RowSet::flat(key as u64));
                    }
                    None => remaining = RowSet::empty(),
                }
                incremental.insert(&additional);
                current.insert(&additional);
                scoping.insert(&additional);
                rows_remaining -= additional.size();
            }
            snapshot_viewport.insert(&incremental);

            subscription.growing_remaining_viewport = Some(remaining);
            subscription.growing_incremental_viewport = Some(incremental);
            subscription.snapshot_viewport = Some(snapshot_viewport);
            subscription.snapshot_columns = Some(subscription.target_columns.clone());
            subscription.snapshot_reverse_viewport = subscription.target_reverse_viewport;
        }

        state
            .post_snapshot_viewport
            .get_or_insert_default()
            .insert(&viewport_rows);
        state
            .post_snapshot_reverse_viewport
            .get_or_insert_default()
            .insert(&reverse_viewport_rows);
        state.post_snapshot_columns.union_with(&columns);
        Some(SnapshotPlan {
            growing,
            columns,
            positions,
            reverse_positions,
        })
    }

    /// Reads the planned snapshot. With the graph's lock still held, so that no update
    /// comes between, the snapshot's viewports and columns become the ones in effect.
    ///
    /// Behavior: `BarrageMessageProducer#getSnapshot`,
    /// `BarrageMessageProducer.SnapshotControl#snapshotCompletedConsistently`
    fn snapshot(&self, plan: &SnapshotPlan) -> Result<BarrageMessage, TableFailure> {
        let read = || {
            let snapshot = construct_snapshot(
                &self.table,
                Some(&plan.columns),
                Some((&plan.positions, &plan.reverse_positions)),
            )?;
            let mut state = lock(&self.state);
            state.parent_table_size = snapshot.rows_added.size();
            for id in &plan.growing {
                if let Some(subscription) = state.subscriptions.get_mut(id) {
                    subscription.flip_snapshot_state();
                }
            }
            state.finalize_snapshot(&plan.growing);
            state.promote_snapshot_to_active();
            Ok(snapshot)
        };
        match &self.graph {
            Some(graph) => locked(graph.as_ref(), read),
            None => read(),
        }
    }

    /// One run of the propagation job.
    ///
    /// Behavior: `BarrageMessageProducer#updateSubscriptionsSnapshotAndPropagate`
    fn propagate(&self) -> Result<(), Status> {
        let mut first_subscription = false;
        let mut deleted = Vec::new();
        let plan = {
            let mut state = lock(&self.state);
            state.last_update_time = self.scheduler.now_ms();
            self.accept_changes(&mut state, &mut first_subscription, &mut deleted)
        };
        let complete_deleted = |deleted: Vec<Subscription>| {
            for subscription in deleted {
                subscription.listener.on_completed();
            }
        };

        let snapshot = match &plan {
            Some(plan) => match self.snapshot(plan) {
                Ok(snapshot) => Some(snapshot),
                Err(failure) => {
                    complete_deleted(deleted);
                    return Err(internal_status(failure));
                }
            },
            None => None,
        };
        let growing = plan.map(|plan| plan.growing).unwrap_or_default();

        let mut state = lock(&self.state);
        if snapshot
            .as_ref()
            .is_some_and(|snapshot| !snapshot.rows_included.is_empty())
        {
            state.snapshot_seen = true;
        }
        if growing.is_empty() && state.pending_deltas.is_empty() && state.pending_error.is_none() {
            // Java returns here without completing the streams of deleted subscriptions.
            drop(state);
            complete_deleted(deleted);
            return Ok(());
        }

        // The updates up to the snapshot's step go out before it.
        let max_step = snapshot
            .as_ref()
            .map_or(i64::MAX, |snapshot| snapshot.first_seq);
        let split = state
            .pending_deltas
            .iter()
            .rposition(|delta| delta.step <= max_step)
            .map_or(0, |index| index + 1);
        let deltas = std::mem::take(&mut state.pending_deltas);

        let mut pre_snapshot = None;
        if !first_subscription && split > 0 {
            let prev = state.propagation_row_set.clone();
            let message = self
                .aggregate_updates(&mut state, &deltas[..split])
                .map_err(internal_status)?;
            pre_snapshot = Some((message, prev, state.propagation_row_set.clone()));
        }
        if first_subscription {
            // the propagation row set is only kept up while there are subscribers
            if let Some(snapshot) = &snapshot {
                state.propagation_row_set = snapshot.rows_added.clone();
            }
        }
        let mut post_snapshot = None;
        if split < deltas.len() {
            let prev = state.propagation_row_set.clone();
            let message = self
                .aggregate_updates(&mut state, &deltas[split..])
                .map_err(internal_status)?;
            post_snapshot = Some((message, prev, state.propagation_row_set.clone()));
        }

        if let Some((message, prev, current)) = &pre_snapshot {
            self.propagate_to_subscribers(&mut state, message, prev, current);
        }
        if let Some(snapshot) = &snapshot {
            for id in &growing {
                self.propagate_snapshot(&mut state, *id, snapshot);
            }
        }
        if let Some((message, prev, current)) = &post_snapshot {
            self.propagate_to_subscribers(&mut state, message, prev, current);
        }
        complete_deleted(deleted);

        // an error ends every subscription: no more updates are coming
        if let Some(failure) = &state.pending_error {
            let status = internal_status(failure);
            for id in std::mem::take(&mut state.active) {
                if let Some(subscription) = state.subscriptions.remove(&id) {
                    subscription.listener.on_error(status.clone());
                }
            }
            state.num_full_subscriptions = 0;
            state.num_growing_subscriptions = 0;
        }

        let more = state.num_growing_subscriptions > 0;
        state.last_update_time = self.scheduler.now_ms();
        drop(state);
        if more {
            self.schedule_immediately();
        }
        Ok(())
    }

    /// Sends `message` to every subscription that has had its first snapshot.
    ///
    /// Behavior: `BarrageMessageProducer#propagateToSubscribers`
    fn propagate_to_subscribers(
        &self,
        state: &mut State,
        message: &BarrageMessage,
        prev_row_set: &RowSet,
        row_set: &RowSet,
    ) {
        let mut failed = Vec::new();
        for id in &state.active {
            let subscription = &state.subscriptions[id];
            if subscription.pending_initial_snapshot || subscription.pending_delete {
                continue;
            }
            // Before its snapshot is sent, a changed subscription still has the viewport
            // and columns of before the change.
            let is_pre_snapshot = subscription.snapshot_viewport.is_some();
            let (viewport, columns, reversed) = if is_pre_snapshot {
                (
                    subscription.snapshot_viewport.as_ref(),
                    subscription.snapshot_columns.as_ref(),
                    subscription.snapshot_reverse_viewport,
                )
            } else {
                (
                    subscription.viewport.as_ref(),
                    Some(&subscription.subscribed_columns),
                    subscription.reverse_viewport,
                )
            };
            let keys = |rows: &RowSet| viewport.map(|v| rows.sub_set_for_positions(v, reversed));
            let client_view_prev = keys(prev_row_set);
            let client_view = keys(row_set);
            let messages = subscription_messages(
                message,
                &self.nullable,
                &SubView {
                    options: subscription.options,
                    is_initial_snapshot: false,
                    is_full_subscription: subscription.is_full_subscription(),
                    viewport,
                    reverse_viewport: subscription.reverse_viewport,
                    keyspace_viewport_prev: client_view_prev.as_ref(),
                    keyspace_viewport: client_view.as_ref(),
                    subscribed_columns: columns,
                },
            );
            match messages {
                Ok(messages) => subscription.listener.on_next(messages),
                Err(problem) => {
                    subscription.listener.on_error(internal_status(problem));
                    failed.push(*id);
                }
            }
        }
        self.mark_deleted(state, &failed);
    }

    /// Marks subscriptions whose stream failed as deleted, for the next run to forget.
    fn mark_deleted(&self, state: &mut State, failed: &[SubscriptionId]) {
        for id in failed {
            if let Some(subscription) = state.subscriptions.get_mut(id) {
                subscription.pending_delete = true;
                if !subscription.has_pending_update {
                    subscription.has_pending_update = true;
                    state.pending.push(*id);
                }
            }
        }
        if !failed.is_empty() {
            self.schedule_immediately();
        }
    }

    /// Sends a growing subscription its part of `snapshot`, after the schema if this is
    /// its first.
    ///
    /// Behavior: `BarrageMessageProducer#propagateSnapshotForSubscription`
    fn propagate_snapshot(&self, state: &mut State, id: SubscriptionId, snapshot: &BarrageMessage) {
        let Some(subscription) = state.subscriptions.get_mut(&id) else {
            return;
        };
        if subscription.pending_delete {
            return;
        }
        // By now the snapshot's viewport and columns are in effect, and `snapshot_*` hold
        // what was in effect before.
        let mut needs_snapshot =
            subscription.pending_initial_snapshot || subscription.snapshot_viewport.is_some();
        if subscription.snapshot_columns.take().is_some() {
            needs_snapshot = true;
        }
        let mut failed = false;
        if needs_snapshot {
            let full_subscription = subscription.is_full_subscription();
            let empty = RowSet::empty();
            // only the rows of the snapshot this subscription asked for
            let positions = if full_subscription {
                subscription.growing_incremental_viewport.as_ref()
            } else {
                subscription.viewport.as_ref()
            };
            let keyspace_viewport = snapshot
                .rows_added
                .sub_set_for_positions(positions.unwrap_or(&empty), subscription.reverse_viewport);
            let keyspace_viewport_prev = (!full_subscription).then(|| {
                snapshot.rows_added.sub_set_for_positions(
                    subscription.snapshot_viewport.as_ref().unwrap_or(&empty),
                    subscription.snapshot_reverse_viewport,
                )
            });
            if subscription.pending_initial_snapshot {
                subscription
                    .listener
                    .on_next(vec![schema_message(&self.schema)]);
            }
            // without rows the message still tells the viewport and columns in effect
            let messages = subscription_messages(
                snapshot,
                &self.nullable,
                &SubView {
                    options: subscription.options,
                    is_initial_snapshot: subscription.pending_initial_snapshot,
                    is_full_subscription: full_subscription,
                    viewport: subscription.viewport.as_ref(),
                    reverse_viewport: subscription.reverse_viewport,
                    keyspace_viewport_prev: keyspace_viewport_prev.as_ref(),
                    keyspace_viewport: Some(&keyspace_viewport),
                    subscribed_columns: Some(&subscription.subscribed_columns),
                },
            );
            match messages {
                Ok(messages) => subscription.listener.on_next(messages),
                Err(problem) => {
                    subscription.listener.on_error(internal_status(problem));
                    failed = true;
                }
            }
        }
        subscription.snapshot_viewport = None;
        subscription.growing_incremental_viewport = None;
        subscription.pending_initial_snapshot = false;
        if failed {
            self.mark_deleted(state, &[id]);
        }
    }

    /// Makes one message of consecutive recorded updates, and brings the propagation row
    /// set up to its end.
    ///
    /// Behavior: `BarrageMessageProducer#aggregateUpdatesInRange`
    fn aggregate_updates(
        &self,
        state: &mut State,
        deltas: &[Delta],
    ) -> Result<BarrageMessage, TableFailure> {
        let types: Vec<_> = self
            .table
            .definition()
            .columns()
            .iter()
            .map(|column| column.column_type.arrow_type())
            .collect();
        let (first, last) = match (deltas.first(), deltas.last()) {
            (Some(first), Some(last)) => (first, last),
            _ => return Err(failure("no updates to aggregate")),
        };

        let update;
        let rows_included;
        let mut add_columns = Vec::with_capacity(types.len());
        let mut mod_columns = Vec::with_capacity(types.len());
        if let [delta] = deltas {
            update = delta.update.clone();
            rows_included = delta.recorded_adds.clone();
            for (index, data_type) in types.iter().enumerate() {
                let empty = || new_empty_array(data_type);
                add_columns.push(match &delta.add_data[index] {
                    Some(data) if delta.subscribed_columns.contains(index) => data.clone(),
                    _ => empty(),
                });
                mod_columns.push(if delta.modified_columns.contains(index) {
                    ModColumn {
                        rows_modified: delta.recorded_mods.clone(),
                        data: delta.mod_data[index].clone().unwrap_or_else(empty),
                    }
                } else {
                    ModColumn {
                        rows_modified: RowSet::empty(),
                        data: empty(),
                    }
                });
            }
        } else {
            let mut coalesced = first.update.clone();
            for delta in &deltas[1..] {
                coalesced = compose(&state.propagation_row_set, &coalesced, &delta.update)
                    .map_err(failure)?;
            }

            // The included rows of the whole range, and the columns they have data for.
            let mut add_column_set = ColumnSet::default();
            let mut mod_column_set = ColumnSet::default();
            let mut local_added = RowSet::empty();
            for delta in deltas {
                local_added.remove(&delta.update.removed);
                local_added = delta.update.shifted.apply(&local_added).map_err(failure)?;
                if local_added.is_empty() {
                    add_column_set.clear();
                }
                if !delta.recorded_adds.is_empty() {
                    if add_column_set.is_empty() {
                        add_column_set.union_with(&delta.subscribed_columns);
                    } else if add_column_set != delta.subscribed_columns {
                        return Err(failure(
                            "updates recorded for different columns cannot be aggregated",
                        ));
                    }
                    local_added.insert(&delta.recorded_adds);
                }
                if !delta.recorded_mods.is_empty() {
                    mod_column_set.union_with(&delta.modified_columns);
                }
            }
            match &coalesced.modified_columns {
                ModifiedColumnSet::All => mod_column_set.union_with(&ColumnSet::all(types.len())),
                ModifiedColumnSet::Columns(columns) => {
                    mod_column_set.union_with(&ColumnSet::of(columns.iter().copied()));
                }
            }

            // Columns modified by the same updates share where their values come from.
            let mut infos: HashMap<Vec<bool>, ColumnInfo> = HashMap::new();
            for (index, data_type) in types.iter().enumerate() {
                let wanted_adds = add_column_set.contains(index);
                let wanted_mods = mod_column_set.contains(index);
                let empty = new_empty_array(data_type);
                if !wanted_adds && !wanted_mods {
                    add_columns.push(empty.clone());
                    mod_columns.push(ModColumn {
                        rows_modified: RowSet::empty(),
                        data: empty,
                    });
                    continue;
                }
                let modifies: Vec<bool> = deltas
                    .iter()
                    .map(|delta| delta.modified_columns.contains(index))
                    .collect();
                let info = match infos.entry(modifies) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let info =
                            ColumnInfo::new(deltas, entry.key(), &local_added, &coalesced.added)?;
                        entry.insert(info)
                    }
                };
                let sources: Vec<ArrayRef> = deltas
                    .iter()
                    .flat_map(|delta| [&delta.add_data[index], &delta.mod_data[index]])
                    .map(|data| data.clone().unwrap_or_else(|| empty.clone()))
                    .collect();
                let gather = |mapping: &[(usize, usize)]| {
                    if mapping.is_empty() {
                        return Ok(empty.clone());
                    }
                    let sources: Vec<&dyn Array> = sources.iter().map(AsRef::as_ref).collect();
                    arrow::compute::interleave(&sources, mapping).map_err(failure)
                };
                add_columns.push(if wanted_adds {
                    gather(&info.added_mapping)?
                } else {
                    empty.clone()
                });
                mod_columns.push(if wanted_mods {
                    ModColumn {
                        rows_modified: info.recorded_mods.clone(),
                        data: gather(&info.modified_mapping)?,
                    }
                } else {
                    ModColumn {
                        rows_modified: RowSet::empty(),
                        data: empty.clone(),
                    }
                });
            }
            update = coalesced;
            rows_included = local_added;
        }

        state.propagation_row_set.remove(&update.removed);
        state.propagation_row_set = update
            .shifted
            .apply(&state.propagation_row_set)
            .map_err(failure)?;
        state.propagation_row_set.insert(&update.added);
        Ok(BarrageMessage {
            first_seq: first.step,
            last_seq: last.step,
            is_snapshot: false,
            rows_added: update.added,
            rows_removed: update.removed,
            shifted: update.shifted,
            rows_included,
            add_columns,
            mod_columns,
            table_size: state.propagation_row_set.size() as i64,
        })
    }

    /// The state a test may look at: how many subscriptions the producer knows.
    #[cfg(test)]
    pub(super) fn subscription_count(&self) -> usize {
        lock(&self.state).subscriptions.len()
    }
}

/// For the columns that the same updates of a range modify: which of their rows the
/// aggregated message reports as modified, and where the value of each included and each
/// modified row was recorded, as (source, position) with source `2 * update` for the
/// update's added data and `2 * update + 1` for its modified data.
struct ColumnInfo {
    recorded_mods: RowSet,
    added_mapping: Vec<(usize, usize)>,
    modified_mapping: Vec<(usize, usize)>,
}

impl ColumnInfo {
    /// The latest recorded value of each row wins: the updates are walked backwards, the
    /// rows still without a value following the shifts back.
    ///
    /// Behavior: the `getColumnInfo` function of `BarrageMessageProducer#aggregateUpdatesInRange`
    fn new(
        deltas: &[Delta],
        modifies: &[bool],
        local_added: &RowSet,
        coalesced_added: &RowSet,
    ) -> Result<Self, TableFailure> {
        let mut recorded_mods = RowSet::empty();
        for (delta, &modifies) in deltas.iter().zip(modifies) {
            recorded_mods.remove(&delta.update.removed);
            recorded_mods = delta
                .update
                .shifted
                .apply(&recorded_mods)
                .map_err(failure)?;
            if modifies {
                recorded_mods.insert(&delta.recorded_mods);
            }
        }
        recorded_mods.remove(coalesced_added);

        let mut added = Filling::new(local_added);
        let mut modified = Filling::new(&recorded_mods);
        for (index, (delta, &modifies)) in deltas.iter().zip(modifies).enumerate().rev() {
            if added.remaining.is_empty() && modified.remaining.is_empty() {
                break;
            }
            added.fill(&delta.recorded_adds, 2 * index)?;
            // a modified row may have been recorded as it moved into view
            modified.fill(&delta.recorded_adds, 2 * index)?;
            if modifies {
                // a modified row may be sent whole
                added.fill(&delta.recorded_mods, 2 * index + 1)?;
                modified.fill(&delta.recorded_mods, 2 * index + 1)?;
            }
            let unshift = delta.update.shifted.inverse();
            added.remaining = unshift.apply(&added.remaining).map_err(failure)?;
            modified.remaining = unshift.apply(&modified.remaining).map_err(failure)?;
        }
        Ok(Self {
            recorded_mods,
            added_mapping: added.finish()?,
            modified_mapping: modified.finish()?,
        })
    }
}

/// The rows of an aggregated message whose values are being located.
struct Filling {
    /// The rows without a value yet, in the key space of the update being looked at.
    remaining: RowSet,
    /// Their positions in the message's data.
    unfilled: RowSet,
    mapping: Vec<Option<(usize, usize)>>,
}

impl Filling {
    fn new(rows: &RowSet) -> Self {
        Self {
            remaining: rows.clone(),
            unfilled: RowSet::flat(rows.size()),
            mapping: vec![None; rows.size() as usize],
        }
    }

    /// Takes the values of the remaining rows that `recorded` has, from `source`.
    fn fill(&mut self, recorded: &RowSet, source: usize) -> Result<(), TableFailure> {
        let found = self.remaining.intersect(recorded);
        if found.is_empty() {
            return Ok(());
        }
        let source_positions = recorded.invert(&found).map_err(failure)?;
        let destinations = self.remaining.invert(&found).map_err(failure)?;
        let rows_to_fill = self.unfilled.sub_set_for_positions(&destinations, false);
        self.remaining.remove(&found);
        self.unfilled.remove(&rows_to_fill);
        for (destination, position) in rows_to_fill.iter().zip(source_positions.iter()) {
            self.mapping[destination as usize] = Some((source, position as usize));
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<(usize, usize)>, TableFailure> {
        self.mapping
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| failure("an aggregated update has a row with no recorded value"))
    }
}

/// Adds to `scoped`, in the previous key space, the rows that an update moved into the
/// viewport range `positions`: the keys the range covers now that it did not cover
/// before.
///
/// Behavior: the loop over viewport ranges in `BarrageMessageProducer#enqueueUpdate`
fn scoped_rows(
    scoped: &mut Vec<RowRange>,
    shifted: &RowSetShiftData,
    row_set: &RowSet,
    prev_row_set: &RowSet,
    positions: RowRange,
    reverse: bool,
) {
    let (pos_start, pos_end) = (positions.first(), positions.last());
    let size = row_set.size() as i64;
    let prev_size = prev_row_set.size() as i64;
    let (local_start, local_end) = if reverse {
        // positions count from the last row
        let last = size - 1;
        if last - pos_start < 0 {
            return;
        }
        ((last - pos_end).max(0), last - pos_start)
    } else {
        (pos_start, pos_end)
    };
    // both row sets are known not to be empty
    let key_at = |rows: &RowSet, position: i64, size: i64| {
        rows.get(position.min(size - 1) as u64).unwrap_or(0)
    };
    let curr_key_start = map_to_prev_keyspace(shifted, key_at(row_set, local_start, size), false);
    let curr_key_end = map_to_prev_keyspace(shifted, key_at(row_set, local_end, size), true);
    if curr_key_end < curr_key_start {
        // the viewport holds no row that existed before
        return;
    }
    let (prev_start, prev_end) = if reverse {
        let last = prev_size - 1;
        ((last - pos_end).max(0), last - pos_start)
    } else {
        (local_start, local_end)
    };
    // the keys the viewport covered before; past either end of the previous row set it
    // covered nothing
    let prev_key_start = if prev_start >= prev_size {
        prev_row_set.last_row_key().map_or(0, |last| last + 1)
    } else {
        key_at(prev_row_set, prev_start, prev_size)
    };
    let prev_key_end = if prev_end < 0 {
        -1
    } else {
        key_at(prev_row_set, prev_end, prev_size)
    };
    if curr_key_start < prev_key_start {
        scoped.push(RowRange::new(
            curr_key_start,
            (prev_key_start - 1).min(curr_key_end),
        ));
    }
    if curr_key_end > prev_key_end {
        scoped.push(RowRange::new(
            (prev_key_end + 1).max(curr_key_start),
            curr_key_end,
        ));
    }
}

impl State {
    /// The union of the viewports and columns of the active subscriptions, leaving out
    /// the ones with pending changes if asked: the snapshot adds those.
    ///
    /// Behavior: `BarrageMessageProducer#buildPostSnapshotViewports`
    fn build_post_snapshot_viewports(&mut self, ignore_pending: bool) {
        let mut viewport = RowSet::empty();
        let mut reverse_viewport = RowSet::empty();
        self.post_snapshot_columns.clear();
        for id in &self.active {
            let subscription = &self.subscriptions[id];
            if ignore_pending && subscription.has_pending_update {
                continue;
            }
            self.post_snapshot_columns
                .union_with(&subscription.subscribed_columns);
            if let Some(rows) = &subscription.viewport {
                if subscription.reverse_viewport {
                    reverse_viewport.insert(rows);
                } else {
                    viewport.insert(rows);
                }
            }
        }
        self.post_snapshot_viewport = Some(viewport);
        self.post_snapshot_reverse_viewport = Some(reverse_viewport);
    }

    /// Behavior: `BarrageMessageProducer#promoteSnapshotToActive`
    fn promote_snapshot_to_active(&mut self) {
        let non_empty = |rows: Option<RowSet>| rows.filter(|rows| !rows.is_empty());
        self.active_viewport = non_empty(self.post_snapshot_viewport.take());
        self.active_reverse_viewport = non_empty(self.post_snapshot_reverse_viewport.take());
        self.active_columns = std::mem::take(&mut self.post_snapshot_columns);
    }

    /// Ends the growth of the subscriptions whose target the snapshot reached, or whose
    /// remaining positions are past the end of the table.
    ///
    /// Behavior: `BarrageMessageProducer#finalizeSnapshotForSubscriptions`
    fn finalize_snapshot(&mut self, growing: &[SubscriptionId]) {
        let mut rebuild = false;
        for id in growing {
            let Some(subscription) = self.subscriptions.get_mut(id) else {
                continue;
            };
            let complete = subscription
                .growing_remaining_viewport
                .as_ref()
                .and_then(RowSet::first_row_key)
                .is_none_or(|first| first as u64 >= self.parent_table_size);
            if complete {
                subscription.is_growing_viewport = false;
                self.num_growing_subscriptions -= 1;
                subscription.viewport = subscription.target_viewport.take();
                if subscription.viewport.is_none() {
                    self.num_full_subscriptions += 1;
                }
                subscription.growing_remaining_viewport = None;
                rebuild = true;
            }
        }
        if rebuild {
            // subscriptions with pending changes are not left out here
            self.build_post_snapshot_viewports(false);
        }
    }
}

impl UpdateHandler for Producer {
    /// Behavior: `BarrageMessageProducer.DeltaListener#onUpdate`
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        let mut state = lock(&self.state);
        if state.stopped {
            return Ok(());
        }
        if !state.active.is_empty() {
            if let Err(failure) = self.enqueue_update(&mut state, update) {
                // the producer fails, not the table
                state.stopped = true;
                state.pending_error = Some(failure);
            }
            self.schedule_propagation(&mut state);
        }
        state.parent_table_size = self.table.row_set().size();
        Ok(())
    }

    /// Behavior: `BarrageMessageProducer.DeltaListener#onFailureInternal`
    fn on_failure(&self, failure: &TableFailure) {
        let mut state = lock(&self.state);
        state.pending_error = Some(failure.clone());
        self.schedule_propagation(&mut state);
    }
}
