//! The local copy of a remote table: the messages of a subscription applied to column
//! sources, and the refreshing table that shows them.
//!
//! The network side hands every message to an [`Inbox`]. The table's update source takes
//! what has arrived at the start of each cycle of the local update graph, applies the
//! messages in order, and tells the table's listeners the one change they add up to.
//!
//! Java keeps the cells in dense storage behind a row redirection, so that a shift moves
//! a mapping and not a value. Here the cells are kept by row key in sparse sources, and a
//! shift moves them; what a reader of the table sees is the same.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use arrow::array::Array;
use oxhaven_column::{Clock, WritableSource};
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_engine::table::{QueryTable, RootSource, TableSpec};
use oxhaven_engine::update::compose;

use super::message::RemoteMessage;
use super::schema::RemoteSchema;
use crate::barrage::codec::ColumnSet;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn failure(problem: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: problem.to_string(),
    }
}

/// How the rows of the remote table are laid out locally.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Layout {
    /// The rows have the row keys the server gives them.
    ///
    /// Behavior: `io.deephaven.extensions.barrage.table.BarrageRedirectedTable` of a full
    /// subscription
    Keyed,
    /// A blink table: the rows that arrived since the last cycle, at `0..count`.
    ///
    /// Behavior: `io.deephaven.extensions.barrage.table.BarrageBlinkTable`
    Blink,
}

/// The cells and rows of a remote table as of the last message applied.
pub(super) struct Replica {
    layout: Layout,
    columns: Vec<Arc<WritableSource>>,
    /// The rows the table has. For a blink table, the rows shown since the last cycle.
    rows: RowSet,
    /// What the last snapshot said the server is honouring.
    server_viewport: Option<RowSet>,
    server_reverse_viewport: bool,
    server_columns: Option<ColumnSet>,
}

impl Replica {
    /// An empty copy of a table of `schema`.
    ///
    /// Behavior: `io.deephaven.extensions.barrage.table.BarrageTable#make`
    pub(super) fn new(schema: &RemoteSchema) -> Self {
        Replica {
            layout: if schema.is_blink() {
                Layout::Blink
            } else {
                Layout::Keyed
            },
            columns: schema
                .definition
                .columns()
                .iter()
                .map(|column| Arc::new(WritableSource::new(column.column_type.clone())))
                .collect(),
            rows: RowSet::empty(),
            server_viewport: None,
            server_reverse_viewport: false,
            server_columns: None,
        }
    }

    pub(super) fn rows(&self) -> &RowSet {
        &self.rows
    }

    /// Behavior: `BarrageTable#isSubscribedColumn`
    fn is_subscribed(&self, column: usize) -> bool {
        self.server_columns
            .as_ref()
            .is_none_or(|columns| columns.contains(column))
    }

    /// Behavior: `BarrageTable#updateServerViewport`
    fn update_server_viewport(&mut self, message: &RemoteMessage) {
        self.server_viewport = message.effective_viewport.clone();
        self.server_columns = message
            .effective_columns
            .clone()
            .filter(|columns| columns.cardinality() != self.columns.len());
        self.server_reverse_viewport = message.effective_reverse_viewport;
    }

    /// The rows the server has sent cells for: all of them, or those at the positions of
    /// the viewport it is growing.
    fn populated(&self) -> Option<RowSet> {
        self.server_viewport.as_ref().map(|viewport| {
            self.rows
                .sub_set_for_positions(viewport, self.server_reverse_viewport)
        })
    }

    /// Writes the cells of the included rows of `message` at `destination`.
    fn write_included(
        &self,
        message: &RemoteMessage,
        destination: &RowSet,
    ) -> Result<(), TableFailure> {
        for (index, column) in self.columns.iter().enumerate() {
            if !self.is_subscribed(index) {
                continue;
            }
            let Some(cells) = message.add_columns.get(index) else {
                return Err(failure(format!(
                    "a Barrage message has no data for column {index}"
                )));
            };
            if cells.len() as u64 != destination.size() {
                return Err(failure(format!(
                    "a Barrage message has {} cells of column {index} for {} rows",
                    cells.len(),
                    destination.size()
                )));
            }
            column.fill(destination, cells)?;
        }
        Ok(())
    }

    /// Applies one message of a full subscription and returns the change it makes to the
    /// table.
    ///
    /// The order is the order of the message: removed rows go, the rows that are left
    /// shift, added rows arrive, then the cells of the included rows and of each column's
    /// modified rows are written. While the server is still growing towards the whole
    /// table, only the rows inside its viewport have cells, and rows that fall out of the
    /// viewport lose theirs.
    ///
    /// Behavior: `BarrageRedirectedTable#processUpdate`
    fn apply_keyed(&mut self, message: &RemoteMessage) -> Result<TableUpdate, TableFailure> {
        if message.is_snapshot {
            self.update_server_viewport(message);
        }
        let mut populated = self.populated();

        // removes
        if !message.rows_removed.is_subset_of(&self.rows) {
            return Err(failure(format!(
                "a Barrage message removes rows the table does not have: {:?}",
                message.rows_removed.minus(&self.rows)
            )));
        }
        self.rows.remove(&message.rows_removed);
        if let Some(populated) = &mut populated {
            populated.remove(&message.rows_removed);
        }
        for column in &self.columns {
            column.remove(&message.rows_removed);
        }

        // shifts
        if !message.shifted.is_empty() {
            for column in &self.columns {
                column.apply_shifts(&message.shifted, &self.rows)?;
            }
            self.rows = message.shifted.apply(&self.rows).map_err(failure)?;
            if let Some(rows) = &populated {
                populated = Some(message.shifted.apply(rows).map_err(failure)?);
            }
        }

        // A snapshot of a subscription that is still growing may name rows the table has.
        if !message.is_snapshot && message.rows_added.overlaps(&self.rows) {
            return Err(failure(format!(
                "a Barrage message adds rows the table already has: {:?}",
                message.rows_added.intersect(&self.rows)
            )));
        }
        self.rows.insert(&message.rows_added);

        if !message.rows_included.is_empty() {
            if !message.rows_included.is_subset_of(&self.rows) {
                return Err(failure(format!(
                    "a Barrage message has data for rows the table does not have: {:?}",
                    message.rows_included.minus(&self.rows)
                )));
            }
            self.write_included(message, &message.rows_included)?;
        }

        let mut modified = RowSet::empty();
        let mut modified_columns = Vec::new();
        for (index, column) in message.mod_columns.iter().enumerate() {
            if column.rows_modified.is_empty() {
                continue;
            }
            if !column.rows_modified.is_subset_of(&self.rows) {
                return Err(failure(format!(
                    "a Barrage message modifies rows of column {index} the table does not \
                     have: {:?}",
                    column.rows_modified.minus(&self.rows)
                )));
            }
            let Some(source) = self.columns.get(index) else {
                return Err(failure(format!(
                    "a Barrage message modifies column {index}, which the table does not have"
                )));
            };
            source.fill(&column.rows_modified, &column.data)?;
            modified.insert(&column.rows_modified);
            modified_columns.push(index);
        }

        // cells outside the viewport are not kept up to date, so they are not kept
        if let Some(mut stale) = populated {
            if let Some(now) = self.populated() {
                stale.remove(&now);
            }
            for column in &self.columns {
                column.remove(&stale);
            }
        }

        Ok(TableUpdate {
            added: message.rows_added.clone(),
            removed: message.rows_removed.clone(),
            modified,
            shifted: message.shifted.clone(),
            modified_columns: ModifiedColumnSet::of(modified_columns),
        })
    }

    /// Appends the included rows of one message of a blink table after the rows that
    /// arrived since the last cycle.
    ///
    /// Behavior: `BarrageBlinkTable#processUpdate`
    fn append_blink(&mut self, message: &RemoteMessage) -> Result<(), TableFailure> {
        if message.is_snapshot {
            self.update_server_viewport(message);
        }
        if !message.shifted.is_empty() {
            return Err(failure("Barrage blink table cannot shift rows"));
        }
        if message
            .mod_columns
            .iter()
            .any(|column| !column.rows_modified.is_empty())
        {
            return Err(failure("Barrage blink table cannot modify rows"));
        }
        let count = message.rows_included.size();
        if count == 0 {
            return Ok(());
        }
        let first = self.rows.size() as RowKey;
        let destination = RowSet::from_range(first, first + count as RowKey - 1);
        self.write_included(message, &destination)?;
        self.rows.insert(&destination);
        Ok(())
    }

    /// Applies `messages`, in order, and returns the one change they make, or `None` if
    /// they change nothing.
    ///
    /// For a blink table the rows shown since the last call go and the included rows of
    /// the messages take their place.
    ///
    /// Behavior: `BarrageRedirectedTable#applyUpdates` (with
    /// `io.deephaven.engine.table.impl.util.UpdateCoalescer` for several messages),
    /// `BarrageBlinkTable#applyUpdates`
    pub(super) fn apply(
        &mut self,
        messages: &[RemoteMessage],
    ) -> Result<Option<TableUpdate>, TableFailure> {
        match self.layout {
            Layout::Keyed => {
                let before = self.rows.clone();
                let mut total: Option<TableUpdate> = None;
                for message in messages {
                    let update = self.apply_keyed(message)?;
                    if update.is_empty() {
                        continue;
                    }
                    total = Some(match total {
                        None => update,
                        Some(first) => compose(&before, &first, &update).map_err(failure)?,
                    });
                }
                Ok(total)
            }
            Layout::Blink => {
                let removed = std::mem::replace(&mut self.rows, RowSet::empty());
                for message in messages {
                    self.append_blink(message)?;
                }
                if self.rows.size() < removed.size() {
                    let gone = removed.minus(&self.rows);
                    for column in &self.columns {
                        column.remove(&gone);
                    }
                }
                if removed.is_empty() && self.rows.is_empty() {
                    return Ok(None);
                }
                Ok(Some(TableUpdate {
                    added: self.rows.clone(),
                    removed,
                    ..TableUpdate::empty()
                }))
            }
        }
    }

    /// Whether the table shows rows that the next cycle must take away.
    fn has_blink_rows(&self) -> bool {
        self.layout == Layout::Blink && !self.rows.is_empty()
    }

    /// Starts keeping previous values, for a table that is about to refresh.
    ///
    /// Behavior: `BarrageTable#maybeEnablePrevTracking`
    fn start_tracking(&self, graph: &Arc<dyn UpdateGraph>) {
        for column in &self.columns {
            let clock: Arc<dyn Clock> = Arc::new(graph.clone());
            column.start_tracking_prev(clock);
        }
    }

    fn sources(&self) -> Vec<Arc<dyn ColumnSource>> {
        self.columns
            .iter()
            .map(|column| column.clone() as Arc<dyn ColumnSource>)
            .collect()
    }
}

/// Asks the local update graph for a cycle soon.
///
/// Behavior: `BarrageTable#doWakeup` (`UpdateSourceRegistrar#requestRefresh`)
pub type Wake = Arc<dyn Fn() + Send + Sync>;

/// What has arrived for a table since its update source last ran.
#[derive(Default)]
struct Arrived {
    messages: Vec<RemoteMessage>,
    /// The subscription ended; the table fails on its next cycle.
    error: Option<TableFailure>,
    /// The table is gone or has failed: nothing more is wanted.
    closed: bool,
}

/// Where the network side of a subscription leaves messages for the table.
pub(super) struct Inbox {
    arrived: Mutex<Arrived>,
    wake: Option<Wake>,
    /// Told when the table is gone or has failed.
    released: tokio::sync::Notify,
}

impl Inbox {
    pub(super) fn new(wake: Option<Wake>) -> Arc<Self> {
        Arc::new(Inbox {
            arrived: Mutex::new(Arrived::default()),
            wake,
            released: tokio::sync::Notify::new(),
        })
    }

    fn wake(&self) {
        if let Some(wake) = &self.wake {
            wake();
        }
    }

    /// Hands over a message. Returns whether the table still wants messages.
    ///
    /// Behavior: `BarrageTable#handleBarrageMessage`
    pub(super) fn deliver(&self, message: RemoteMessage) -> bool {
        {
            let mut arrived = lock(&self.arrived);
            if arrived.closed || arrived.error.is_some() {
                return false;
            }
            arrived.messages.push(message);
        }
        self.wake();
        true
    }

    /// Fails the table on its next cycle.
    ///
    /// Behavior: `BarrageTable#handleBarrageError`
    pub(super) fn fail(&self, error: TableFailure) {
        {
            let mut arrived = lock(&self.arrived);
            if arrived.closed || arrived.error.is_some() {
                return;
            }
            arrived.error = Some(error);
        }
        self.wake();
    }

    fn close(&self) {
        let mut arrived = lock(&self.arrived);
        arrived.closed = true;
        arrived.messages.clear();
        drop(arrived);
        self.released.notify_one();
    }

    /// Completes when the table is gone or has failed.
    pub(super) async fn released(&self) {
        loop {
            // registered before the check, so a close in between is not missed
            let notified = self.released.notified();
            if lock(&self.arrived).closed {
                return;
            }
            notified.await;
        }
    }
}

/// The update source of a subscribed table.
///
/// Behavior: `BarrageTable.SourceRefresher`, `BarrageTable#realRefresh`
struct Refresher {
    table: Weak<QueryTable>,
    replica: Mutex<Replica>,
    inbox: Arc<Inbox>,
}

impl Drop for Refresher {
    /// The table is gone: the subscription has nobody to serve.
    ///
    /// Behavior: `BarrageTable#destroy`, and the `destroy` of
    /// `io.deephaven.client.impl.BarrageSubscriptionImpl`, which the table keeps alive
    fn drop(&mut self) {
        self.inbox.close();
    }
}

impl UpdateSource for Refresher {
    fn refresh(&self) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        let (messages, error) = {
            let mut arrived = lock(&self.inbox.arrived);
            if arrived.closed {
                return;
            }
            (std::mem::take(&mut arrived.messages), arrived.error.take())
        };
        if table.is_failed() {
            return self.inbox.close();
        }
        // An error is delivered before anything that arrived with it.
        if let Some(error) = error {
            table.fail(error);
            return self.inbox.close();
        }
        let mut replica = lock(&self.replica);
        if messages.is_empty() && !replica.has_blink_rows() {
            return;
        }
        match replica.apply(&messages) {
            Ok(Some(update)) => {
                let again = replica.has_blink_rows();
                drop(replica);
                table.notify(update);
                if again {
                    // the rows of a blink table go on the next cycle
                    self.inbox.wake();
                }
            }
            Ok(None) => {}
            Err(problem) => {
                drop(replica);
                table.fail(problem);
                self.inbox.close();
            }
        }
    }
}

/// The refreshing table of a subscription whose server has acknowledged it: a root of
/// `graph` that starts with the rows `replica` holds and changes as messages arrive in
/// `inbox`.
///
/// The caller holds the shared lock of `graph`, so no cycle runs between the moment the
/// rows are read and the moment the table is a source of the graph.
///
/// Behavior: `BarrageTable#make`, `BarrageTable#addSourceToRegistrar`, and the end of
/// `BarrageRedirectedTable#applyUpdates` for the moment a table becomes complete
pub(super) fn live_table(
    graph: Arc<dyn UpdateGraph>,
    schema: &RemoteSchema,
    replica: Replica,
    inbox: Arc<Inbox>,
) -> Arc<QueryTable> {
    replica.start_tracking(&graph);
    let mut spec = TableSpec::new(
        schema.definition.clone(),
        replica.rows().clone(),
        replica.sources(),
    )
    .with_attributes(schema.table_attributes());
    // Behavior: the `setFlat` calls of BarrageBlinkTable and BarrageRedirectedTable
    if replica.layout == Layout::Blink || schema.is_flat() {
        spec = spec.flat();
    }
    let wake_for_blink = replica.has_blink_rows();
    let table = QueryTable::root(graph, spec, |table| RootSource {
        source: Arc::new(Refresher {
            table,
            replica: Mutex::new(replica),
            inbox: inbox.clone(),
        }),
        input: None,
    });
    if wake_for_blink {
        inbox.wake();
    }
    table
}

#[cfg(test)]
mod tests;
