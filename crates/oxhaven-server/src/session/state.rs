//! One client session: its token, its export table, its export listeners and the things to
//! close when it ends.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use oxhaven_proto::grpc::ExportNotification;
use tokio::sync::mpsc;
use tonic::Status;
use uuid::Uuid;

use super::auth::AuthContext;
use super::executor::Executor;
use super::export::{Export, ExportBuilder, ExportState, ExportValue};
use super::ticket::{export_id_to_ticket, ticket_to_export_id, NON_EXPORT_ID};

/// Milliseconds since the Unix epoch, from the server's clock.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// A session token and the time it stops working.
///
/// Behavior: `io.deephaven.server.session.SessionService.TokenExpiration`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenExpiration {
    pub token: Uuid,
    pub deadline_millis: i64,
}

impl TokenExpiration {
    /// The `authorization` header value that presents this token.
    pub fn bearer(&self) -> String {
        format!("{}{}", super::auth::BEARER_PREFIX, self.token)
    }
}

/// The receiving end of an `ExportNotifications` subscription.
pub type ExportNotifications = mpsc::UnboundedReceiver<Result<ExportNotification, Status>>;

/// Identifies a callback registered with [`Session::add_on_close`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OnCloseId(u64);

#[derive(Default)]
struct ExportTable {
    exports: BTreeMap<i32, Export>,
    /// Ids that were released. Their exports are gone, but the ids cannot be reused.
    released: BTreeSet<i32>,
}

#[derive(Default)]
struct Listeners {
    /// Incremented for each subscription, so that an export can tell whether a listener has
    /// already heard its current state.
    version: u64,
    senders: Vec<mpsc::UnboundedSender<Result<ExportNotification, Status>>>,
}

#[derive(Default)]
struct OnClose {
    next: u64,
    callbacks: BTreeMap<u64, Box<dyn FnOnce() + Send>>,
}

/// Behavior: `io.deephaven.server.session.SessionState`
pub struct Session {
    id: String,
    auth: AuthContext,
    clock: Clock,
    executor: Arc<dyn Executor>,
    expiration: Mutex<Option<TokenExpiration>>,
    next_server_id: AtomicI32,
    table: Mutex<ExportTable>,
    listeners: Mutex<Listeners>,
    on_close: Mutex<OnClose>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn expired() -> Status {
    Status::unauthenticated("session has expired")
}

impl Session {
    /// A session with no token yet. It counts as expired until the store installs one.
    pub(super) fn new(auth: AuthContext, clock: Clock, executor: Arc<dyn Executor>) -> Arc<Self> {
        Arc::new(Session {
            id: Uuid::new_v4().to_string(),
            auth,
            clock,
            executor,
            expiration: Mutex::new(None),
            next_server_id: AtomicI32::new(-1),
            table: Mutex::default(),
            listeners: Mutex::default(),
            on_close: Mutex::default(),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn auth_context(&self) -> AuthContext {
        self.auth
    }

    pub(super) fn executor(&self) -> &Arc<dyn Executor> {
        &self.executor
    }

    /// The token slot, for the store to rotate under one lock.
    pub(super) fn expiration_slot(&self) -> MutexGuard<'_, Option<TokenExpiration>> {
        lock(&self.expiration)
    }

    /// The current token, or `None` if the session has expired.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#getExpiration`
    pub fn expiration(&self) -> Option<TokenExpiration> {
        let now = (self.clock)();
        (*lock(&self.expiration)).filter(|expiration| expiration.deadline_millis > now)
    }

    /// Behavior: `io.deephaven.server.session.SessionState#isExpired`
    pub fn is_expired(&self) -> bool {
        self.expiration().is_none()
    }

    fn check_live(&self) -> Result<(), Status> {
        if self.is_expired() {
            Err(expired())
        } else {
            Ok(())
        }
    }

    /// The export with this id. A client id (positive) that is not defined yet is created in
    /// the `UNKNOWN` state, so that requests may arrive out of order; a server id (negative)
    /// must already exist. A released id gives a stand-in that fails dependents.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#getExport(int)`
    pub fn get_export(self: &Arc<Self>, export_id: i32) -> Result<Export, Status> {
        self.check_live()?;
        if export_id == NON_EXPORT_ID {
            return Err(Status::invalid_argument(format!(
                "Export id {export_id} refers to a non-export and cannot be requested!"
            )));
        }
        {
            let table = lock(&self.table);
            // the check under the lock dominates: once the session has expired the table
            // has been, or is about to be, cleared
            self.check_live()?;
            if let Some(export) = table.exports.get(&export_id) {
                return Ok(export.clone());
            }
            if table.released.contains(&export_id) {
                return Ok(Export::released(self, export_id));
            }
            if export_id < 0 {
                return Err(Status::failed_precondition(format!(
                    "Export id {export_id} does not exist and cannot be used out-of-order!"
                )));
            }
        }
        self.create_export(export_id)
    }

    /// The export with this id if it exists or was released, otherwise `None`.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#getExportIfExists(int)`
    pub fn get_export_if_exists(
        self: &Arc<Self>,
        export_id: i32,
    ) -> Result<Option<Export>, Status> {
        self.check_live()?;
        let table = lock(&self.table);
        self.check_live()?;
        if let Some(export) = table.exports.get(&export_id) {
            return Ok(Some(export.clone()));
        }
        if table.released.contains(&export_id) {
            return Ok(Some(Export::released(self, export_id)));
        }
        Ok(None)
    }

    /// Puts a new export in the table and tells listeners of it, unless another thread did
    /// so first, in which case that export is returned; or the id was released, which gives
    /// the stand-in.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#createExport`
    fn create_export(self: &Arc<Self>, export_id: i32) -> Result<Export, Status> {
        let created = Export::new(self, export_id);
        {
            let mut table = lock(&self.table);
            self.check_live()?;
            if table.released.contains(&export_id) {
                return Ok(Export::released(self, export_id));
            }
            if let Some(existing) = table.exports.get(&export_id) {
                return Ok(existing.clone());
            }
            table.exports.insert(export_id, created.clone());
        }
        // Java sends this under the new export's monitor, taken before the table's; here the
        // export has no other state to report yet, and its own mutex orders this
        // notification with any that follow.
        created.announce();
        Ok(created)
    }

    /// Starts the definition of the export named by a client ticket.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#newExport(Ticket, String)`
    pub fn new_export_for_ticket(
        self: &Arc<Self>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<ExportBuilder, Status> {
        self.new_export(ticket_to_export_id(ticket, log_id)?)
    }

    /// Starts the definition of a client export. The id must be positive; Java throws an
    /// `IllegalArgumentException` otherwise, which reaches the client as `INVALID_ARGUMENT`.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#newExport(int)`, and the
    /// constructor of `SessionState.ExportBuilder`
    pub fn new_export(self: &Arc<Self>, export_id: i32) -> Result<ExportBuilder, Status> {
        self.check_live()?;
        if export_id <= 0 {
            return Err(Status::invalid_argument(
                "exportIds <= 0 are reserved for server allocation only",
            ));
        }
        let found = {
            let table = lock(&self.table);
            self.check_live()?;
            if table.released.contains(&export_id) {
                Some(Export::released(self, export_id))
            } else {
                table.exports.get(&export_id).cloned()
            }
        };
        let export = match found {
            Some(export) => export,
            None => self.create_export(export_id)?,
        };
        // an export that is released but not yet dropped from the table is gone too
        let export = if export.state() == ExportState::Released {
            Export::released(self, export_id)
        } else {
            export
        };
        Ok(ExportBuilder::new(export))
    }

    /// Starts the definition of work that depends on exports and exports nothing.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#nonExport`
    pub fn non_export(self: &Arc<Self>) -> Result<ExportBuilder, Status> {
        self.check_live()?;
        Ok(ExportBuilder::new(Export::new(self, NON_EXPORT_ID)))
    }

    /// Exports a value under an id the server chooses: -1, -2, and so on.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#newServerSideExport`
    pub fn new_server_side_export(self: &Arc<Self>, value: ExportValue) -> Result<Export, Status> {
        let export_id = self.next_server_id.fetch_sub(1, Ordering::SeqCst);
        let export = self.create_export(export_id)?;
        export.set_result(value);
        Ok(export)
    }

    /// Drops a released export from the table and remembers that its id was used.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#markExportReleased`
    pub(super) fn mark_released(&self, export_id: i32) {
        let mut table = lock(&self.table);
        if self.is_expired() {
            // the end of the session clears the table itself
            return;
        }
        table.exports.remove(&export_id);
        table.released.insert(export_id);
    }

    /// Sends a notification to every listener and returns the listener version it was sent
    /// at. A listener whose client has gone away is dropped.
    pub(super) fn publish_notification(&self, notification: ExportNotification) -> u64 {
        let mut listeners = lock(&self.listeners);
        listeners
            .senders
            .retain(|sender| sender.send(Ok(notification.clone())).is_ok());
        listeners.version
    }

    /// Subscribes to export state changes. The listener first receives the state of every
    /// export that is not terminal, then a notification for export id 0 saying that this
    /// refresh is complete, then every change. A change may arrive before the refresh
    /// completes, and may be a duplicate of a refresh entry.
    ///
    /// The stream ends when the session does. Dropping the receiver unsubscribes.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#addExportListener`,
    /// `SessionState.ExportListener#initialize`
    pub fn add_export_listener(&self) -> Result<ExportNotifications, Status> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let version = {
            let mut listeners = lock(&self.listeners);
            self.check_live()?;
            listeners.senders.push(sender.clone());
            listeners.version += 1;
            listeners.version
        };
        let exports: Vec<Export> = lock(&self.table).exports.values().cloned().collect();
        for export in exports {
            export.replay_to(version, |notification| {
                let _ = sender.send(Ok(notification));
            });
        }
        let _ = sender.send(Ok(ExportNotification {
            ticket: Some(export_id_to_ticket(NON_EXPORT_ID)),
            export_state: ExportState::Exported as i32,
            context: "run is complete".to_string(),
            dependent_handle: String::new(),
        }));
        Ok(receiver)
    }

    /// Registers something to run when the session ends. Fails, without running it, if the
    /// session has already ended.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#addOnCloseCallback`
    pub fn add_on_close(
        &self,
        callback: impl FnOnce() + Send + 'static,
    ) -> Result<OnCloseId, Status> {
        let mut on_close = lock(&self.on_close);
        self.check_live()?;
        let id = on_close.next;
        on_close.next += 1;
        on_close.callbacks.insert(id, Box::new(callback));
        Ok(OnCloseId(id))
    }

    /// Unregisters a callback. Returns whether it was removed; if not, it has run or will.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#removeOnCloseCallback`
    pub fn remove_on_close(&self, id: OnCloseId) -> bool {
        if self.is_expired() {
            return false;
        }
        lock(&self.on_close).callbacks.remove(&id.0).is_some()
    }

    /// Ends the session: cancels or releases every export, completes every listener and
    /// runs the on-close callbacks. Does nothing the second time.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#onExpired`
    pub fn on_expired(&self) {
        if lock(&self.expiration).take().is_none() {
            return;
        }
        tracing::debug!("SessionState{{{}}}: releasing outstanding exports", self.id);
        // Cancelling notifies listeners and runs handlers, so it happens outside the table
        // lock. Nothing can be added meanwhile: the session already reads as expired.
        let exports: Vec<Export> = {
            let mut table = lock(&self.table);
            table.released.clear();
            std::mem::take(&mut table.exports).into_values().collect()
        };
        for export in exports {
            export.cancel();
        }
        // dropping the senders completes the streams
        lock(&self.listeners).senders.clear();
        let callbacks = std::mem::take(&mut lock(&self.on_close).callbacks);
        for callback in callbacks.into_values() {
            callback();
        }
    }

    /// The number of exports in the table, for tests.
    #[cfg(test)]
    pub(super) fn export_count(&self) -> usize {
        lock(&self.table).exports.len()
    }

    /// The number of registered on-close callbacks, for tests.
    #[cfg(test)]
    pub(super) fn on_close_count(&self) -> usize {
        lock(&self.on_close).callbacks.len()
    }

    /// The number of subscribed export listeners, for tests.
    #[cfg(test)]
    pub(super) fn listener_count(&self) -> usize {
        lock(&self.listeners).senders.len()
    }
}
