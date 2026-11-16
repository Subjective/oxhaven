//! The subscription to a view of a hierarchical table: every message is a snapshot of the
//! rows of the viewport, sent when the viewport changes and, no more often than the
//! update interval, when the source or the key table has changed.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use arrow::array::new_empty_array;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::graph::{ParentListener, UpdateHandler};
use oxhaven_proto::flight::FlightData;
use tonic::Status;

use super::{snapshot_schema, ViewOf};
use crate::barrage::codec::ColumnSet;
use crate::barrage::producer::{Listener, Scheduler};
use crate::barrage::writer::{subscription_messages, BarrageMessage, SubView, WireOptions};
use crate::flight::schema_message;
use crate::session::ExportError;
use crate::table::support::{internal_status, unported_status};
use crate::wire_columns::WireColumn;

/// The most rows of a viewport.
///
/// Behavior: `io.deephaven.chunk.util.pools.ChunkPoolConstants#LARGEST_POOLED_CHUNK_CAPACITY`
const LARGEST_VIEWPORT: u64 = 1 << 16;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Active,
    Failed,
    Done,
}

/// What Java guards by its scheduling lock.
struct Scheduling {
    state: State,
    snapshot_pending: bool,
    scheduled_time_ms: i64,
    last_snapshot_time_ms: i64,
    upstream_data_changed: bool,
    upstream_failure: Option<TableFailure>,
    pending_columns: Option<ColumnSet>,
    pending_rows: Option<RowSet>,
}

/// What Java guards by its snapshot lock.
struct Current {
    columns: ColumnSet,
    rows: RowSet,
    prev_keyspace_viewport: RowSet,
}

/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableViewSubscription`
pub struct ViewSubscription {
    scheduler: Arc<dyn Scheduler>,
    view: Arc<ViewOf>,
    listener: Arc<dyn Listener>,
    options: WireOptions,
    interval_ms: i64,
    /// Per column of a snapshot, how it is sent.
    wire: Vec<WireColumn>,
    nullable: Vec<bool>,
    scheduling: Mutex<Scheduling>,
    current: Mutex<Current>,
    /// The listeners of the source and of the key table, which they hold weakly.
    upstream: OnceLock<Vec<Arc<dyn UpdateListener>>>,
    this: Weak<ViewSubscription>,
}

impl ViewSubscription {
    /// Subscribes `listener` to `view`, sends it the schema, and asks for its first
    /// viewport as [`ViewSubscription::set_viewport`] does.
    ///
    /// Behavior: the constructor, and
    /// `io.deephaven.server.hierarchicaltable.HierarchicalTableViewExchangeMarshaller#subscribe`
    pub fn subscribe(
        scheduler: Arc<dyn Scheduler>,
        view: Arc<ViewOf>,
        listener: Arc<dyn Listener>,
        options: WireOptions,
        interval_ms: i64,
    ) -> Arc<ViewSubscription> {
        let schema = snapshot_schema(&view.table);
        let hierarchy = view.view.hierarchy().clone();
        let definition = hierarchy.columns();
        let wire: Vec<WireColumn> = definition
            .columns()
            .iter()
            .map(|column| WireColumn::described(&column.column_type))
            .collect();
        let nullable = schema.fields().iter().map(|f| f.is_nullable()).collect();
        let subscription = Arc::new_cyclic(|this| ViewSubscription {
            scheduler,
            view: view.clone(),
            listener,
            options,
            interval_ms,
            current: Mutex::new(Current {
                columns: ColumnSet::all(wire.len()),
                rows: RowSet::empty(),
                prev_keyspace_viewport: RowSet::empty(),
            }),
            wire,
            nullable,
            scheduling: Mutex::new(Scheduling {
                state: State::Active,
                snapshot_pending: false,
                scheduled_time_ms: i64::MAX,
                last_snapshot_time_ms: 0,
                upstream_data_changed: false,
                upstream_failure: None,
                pending_columns: None,
                pending_rows: None,
            }),
            upstream: OnceLock::new(),
            this: this.clone(),
        });
        let refreshing: Vec<Arc<dyn Table>> = [Some(hierarchy.source()), view.view.key_table()]
            .into_iter()
            .flatten()
            .filter(|table| table.is_refreshing())
            .cloned()
            .collect();
        if let Some(graph) = refreshing.iter().find_map(|table| table.update_graph()) {
            let handler: Arc<dyn UpdateHandler> = subscription.clone();
            let listeners = locked(graph.as_ref(), || {
                refreshing
                    .iter()
                    .map(|table| ParentListener::subscribe(table.clone(), Arc::downgrade(&handler)))
                    .collect()
            });
            let _ = subscription.upstream.set(listeners);
        }
        subscription.listener.on_next(vec![schema_message(&schema)]);
        subscription
    }

    /// Ends the subscription and the stream.
    ///
    /// Behavior: `HierarchicalTableViewSubscription#completed`
    pub fn completed(&self) {
        lock(&self.scheduling).state = State::Done;
        self.listener.on_completed();
    }

    /// Changes the columns and rows the subscriber is sent; `None` keeps what it has.
    ///
    /// Behavior: `HierarchicalTableViewSubscription#setViewport`
    pub fn set_viewport(
        &self,
        columns: Option<ColumnSet>,
        rows: Option<RowSet>,
        reverse_viewport: bool,
    ) -> Result<(), Status> {
        if lock(&self.scheduling).state != State::Active {
            return Ok(());
        }
        if let Some(columns) = &columns {
            let length = columns.iter().max().map_or(0, |last| last + 1);
            if length > self.wire.len() {
                return Err(Status::invalid_argument(format!(
                    "Requested columns out of range: length={length}, available length={}",
                    self.wire.len()
                )));
            }
        }
        if let Some(rows) = &rows {
            if rows.ranges().len() > 1 {
                return Err(Status::invalid_argument(
                    "HierarchicalTableView subscriptions only support contiguous viewports",
                ));
            }
            if rows.size() > LARGEST_VIEWPORT {
                return Err(Status::invalid_argument(format!(
                    "HierarchicalTableView subscriptions only support viewport size up to \
                     {LARGEST_VIEWPORT} rows, requested {} rows",
                    rows.size()
                )));
            }
        }
        if reverse_viewport {
            return Err(Status::invalid_argument(
                "HierarchicalTableView subscriptions do not support reverse viewports",
            ));
        }
        let now = self.scheduler.now_ms();
        let mut scheduling = lock(&self.scheduling);
        if scheduling.state != State::Active {
            return Ok(());
        }
        let changed = columns.is_some() || rows.is_some();
        if columns.is_some() {
            scheduling.pending_columns = columns;
        }
        if rows.is_some() {
            scheduling.pending_rows = rows;
        }
        if changed {
            self.schedule_immediately(&mut scheduling, now);
        }
        Ok(())
    }

    fn job(&self) -> Box<dyn FnOnce() + Send> {
        let this = self.this.clone();
        Box::new(move || {
            if let Some(this) = this.upgrade() {
                this.process();
            }
        })
    }

    /// Behavior: `HierarchicalTableViewSubscription#scheduleImmediately`
    fn schedule_immediately(&self, scheduling: &mut Scheduling, now: i64) {
        if !scheduling.snapshot_pending || now < scheduling.scheduled_time_ms {
            scheduling.snapshot_pending = true;
            scheduling.scheduled_time_ms = now;
            self.scheduler.run_immediately(self.job());
        }
    }

    /// Behavior: `HierarchicalTableViewSubscription#scheduleAtInterval`
    fn schedule_at_interval(&self, scheduling: &mut Scheduling, now: i64) {
        let target = scheduling.last_snapshot_time_ms + self.interval_ms;
        if target < now {
            self.schedule_immediately(scheduling, now);
        } else if !scheduling.snapshot_pending || target < scheduling.scheduled_time_ms {
            scheduling.snapshot_pending = true;
            scheduling.scheduled_time_ms = target;
            self.scheduler.run_at(target, self.job());
        }
    }

    /// Sends a snapshot if one is due, or the failure of the source or the key table.
    ///
    /// Behavior: `HierarchicalTableViewSubscription#process`
    fn process(&self) {
        if lock(&self.scheduling).state == State::Done {
            return;
        }
        let mut current = lock(&self.current);
        let failure = {
            let mut scheduling = lock(&self.scheduling);
            if !scheduling.snapshot_pending {
                return;
            }
            scheduling.snapshot_pending = false;
            match scheduling.state {
                State::Done => return,
                State::Failed => {
                    scheduling.state = State::Done;
                    scheduling.upstream_data_changed = false;
                    scheduling.pending_columns = None;
                    scheduling.pending_rows = None;
                    Some(scheduling.upstream_failure.take())
                }
                State::Active => {
                    let mut send = scheduling.upstream_data_changed;
                    scheduling.upstream_data_changed = false;
                    if let Some(columns) = scheduling.pending_columns.take() {
                        current.columns = columns;
                        send = true;
                    }
                    if let Some(rows) = scheduling.pending_rows.take() {
                        current.rows = rows;
                        send = true;
                    }
                    if !send {
                        return;
                    }
                    scheduling.last_snapshot_time_ms = self.scheduler.now_ms();
                    None
                }
            }
        };
        if let Some(failure) = failure {
            let message = failure.map_or_else(String::new, |failure| failure.message);
            self.listener.on_error(internal_status(message));
            return;
        }
        // A panic here would end the job without a word to the client.
        let sent = catch_unwind(AssertUnwindSafe(|| self.snapshot(&mut current))).unwrap_or_else(
            |panic| {
                Err(match unported_status(panic.as_ref()) {
                    Some(status) => status,
                    None => internal_status("a snapshot of a hierarchical table panicked"),
                })
            },
        );
        match sent {
            Ok(messages) => self.listener.on_next(messages),
            Err(status) => {
                self.listener.on_error(status);
                lock(&self.scheduling).state = State::Done;
            }
        }
    }

    /// Behavior: `HierarchicalTableViewSubscription#buildAndSendSnapshot`
    fn snapshot(&self, current: &mut Current) -> Result<Vec<FlightData>, Status> {
        let status = |error: ExportError| match error {
            ExportError::Status(status) => status,
            ExportError::Internal(message) => internal_status(message),
        };
        let columns: Vec<usize> = current.columns.iter().collect();
        let first = current.rows.first_row_key().unwrap_or(0) as u64;
        let snapshot = self
            .view
            .view
            .snapshot(Some(&columns), first, current.rows.size())
            .map_err(|error| status(error.into()))?;
        let size = snapshot.size;
        let rows_included = match (current.rows.first_row_key(), current.rows.last_row_key()) {
            (Some(first), Some(last)) if (first as u64) < size => {
                RowSet::from_range(first, last.min(size as i64 - 1))
            }
            _ => RowSet::empty(),
        };
        let mut data = snapshot.columns.into_iter();
        let mut add_columns = Vec::with_capacity(self.wire.len());
        for (index, wire) in self.wire.iter().enumerate() {
            add_columns.push(match current.columns.contains(index) {
                true if !rows_included.is_empty() => {
                    let stored = data.next().ok_or_else(|| {
                        internal_status("a snapshot without one of the columns asked for")
                    })?;
                    wire.cells(stored)
                        .map_err(|failure| internal_status(failure.message))?
                }
                _ => new_empty_array(&wire.arrow_type()),
            });
        }
        let message = BarrageMessage {
            first_seq: -1,
            last_seq: -1,
            is_snapshot: true,
            rows_added: RowSet::flat(size),
            rows_removed: RowSet::empty(),
            shifted: RowSetShiftData::empty(),
            rows_included,
            add_columns,
            mod_columns: Vec::new(),
            table_size: size as i64,
        };
        let messages = subscription_messages(
            &message,
            &self.nullable,
            &SubView {
                options: self.options,
                is_initial_snapshot: false,
                is_full_subscription: false,
                viewport: Some(&current.rows),
                reverse_viewport: false,
                keyspace_viewport_prev: Some(&current.prev_keyspace_viewport),
                keyspace_viewport: Some(&message.rows_included),
                subscribed_columns: Some(&current.columns),
            },
        )
        .map_err(|problem| internal_status(problem.0))?;
        current.prev_keyspace_viewport = message.rows_included;
        Ok(messages)
    }
}

impl UpdateHandler for ViewSubscription {
    /// Behavior: `HierarchicalTableViewSubscription.ChangeListener#onUpdate`
    fn on_update(&self, _update: &TableUpdate) -> Result<(), TableFailure> {
        let now = self.scheduler.now_ms();
        let mut scheduling = lock(&self.scheduling);
        if scheduling.state == State::Active {
            scheduling.upstream_data_changed = true;
            self.schedule_at_interval(&mut scheduling, now);
        }
        Ok(())
    }

    /// Behavior: `HierarchicalTableViewSubscription.ChangeListener#onFailureInternal`
    fn on_failure(&self, failure: &TableFailure) {
        let now = self.scheduler.now_ms();
        let mut scheduling = lock(&self.scheduling);
        if scheduling.state == State::Active {
            scheduling.upstream_failure = Some(failure.clone());
            scheduling.state = State::Failed;
            self.schedule_immediately(&mut scheduling, now);
        }
    }
}
