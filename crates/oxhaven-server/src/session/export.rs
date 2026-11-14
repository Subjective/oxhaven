//! Exports: the values a session holds for a client, each with a state machine and
//! dependencies on other exports.
//!
//! An export is defined once, by an [`ExportBuilder`], with the exports it requires and the
//! work that produces its value. The work runs when every dependency has been exported and is
//! handed their values. A dependency that fails, is cancelled or is released before the work
//! can use it fails the dependent export instead.
//!
//! Locking: each export has one mutex. A transition is computed under it and its consequences
//! for other parties (handlers, dependents, the executor) are collected and carried out after
//! it is released, so no two export mutexes are ever held together. The session's export
//! table and listener list are leaf locks.

use std::any::Any;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_proto::grpc::{ExportNotification, Ticket};
use tonic::Status;
use uuid::Uuid;

use super::state::Session;
use super::ticket::{export_id_to_ticket, NON_EXPORT_ID};

/// The states of an export, with the numbers of the protocol.
pub type ExportState = oxhaven_proto::grpc::export_notification::State;

/// What an export holds once exported.
#[derive(Clone)]
pub enum ExportValue {
    Table(Arc<dyn Table>),
    /// Anything else: a plugin object, a Flight info, or `()` for work with no result.
    Object(Arc<dyn Any + Send + Sync>),
}

impl ExportValue {
    pub fn object<T: Any + Send + Sync>(value: T) -> Self {
        ExportValue::Object(Arc::new(value))
    }

    /// The value of work that produces nothing.
    pub fn unit() -> Self {
        Self::object(())
    }

    pub fn as_table(&self) -> Option<&Arc<dyn Table>> {
        match self {
            ExportValue::Table(table) => Some(table),
            ExportValue::Object(_) => None,
        }
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        match self {
            ExportValue::Table(_) => None,
            ExportValue::Object(object) => object.downcast_ref(),
        }
    }
}

impl fmt::Debug for ExportValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportValue::Table(_) => f.write_str("Table"),
            ExportValue::Object(_) => f.write_str("Object"),
        }
    }
}

/// Why export work failed.
#[derive(Clone, Debug)]
pub enum ExportError {
    /// A failure meant for the client. It reaches the client, and the clients of dependent
    /// exports, unchanged.
    Status(Status),
    /// An internal failure. The text is logged with an error id; the client sees only the id.
    Internal(String),
}

impl From<Status> for ExportError {
    fn from(status: Status) -> Self {
        ExportError::Status(status)
    }
}

/// An engine failure is internal: the client is told where the details were logged.
impl From<TableFailure> for ExportError {
    fn from(failure: TableFailure) -> Self {
        ExportError::Internal(failure.message)
    }
}

/// See [`Export::failure`].
#[derive(Clone, Debug)]
pub struct ExportFailure {
    /// Identifies the failure in the server log. Dependents share the id of the export
    /// that failed them.
    pub error_id: String,
    /// The log name of the dependency that failed this export, when that dependency had no
    /// error of its own: it was released or cancelled.
    pub dependency: Option<String>,
    /// The status this export failed with, or inherited from the dependency that failed
    /// it. `None` for an export that was cancelled or whose dependency failed without a
    /// status for the client.
    pub cause: Option<Status>,
}

type Work = Box<dyn FnOnce(Vec<ExportValue>) -> Result<ExportValue, ExportError> + Send>;
type OnError = Box<dyn FnOnce(Status) + Send>;
type OnSuccess = Box<dyn FnOnce(ExportValue) + Send>;

/// Behavior: `io.deephaven.server.session.SessionState#isExportStateFailure`
pub fn is_failure(state: ExportState) -> bool {
    matches!(
        state,
        ExportState::Failed
            | ExportState::Cancelled
            | ExportState::DependencyFailed
            | ExportState::DependencyNeverFound
            | ExportState::DependencyReleased
            | ExportState::DependencyCancelled
    )
}

/// Behavior: `io.deephaven.server.session.SessionState#isExportStateTerminal`
pub fn is_terminal(state: ExportState) -> bool {
    state == ExportState::Released || is_failure(state)
}

/// A handle to one export, or to one piece of work that depends on exports (a "non-export",
/// with id 0). Cloning the handle does not copy the export.
///
/// Behavior: `io.deephaven.server.session.SessionState.ExportObject`
#[derive(Clone)]
pub struct Export {
    inner: Arc<Inner>,
}

struct Inner {
    id: i32,
    /// The export id, or a process-unique tag for a non-export. Reported to dependents as
    /// the handle of the dependency that failed them.
    log_identity: String,
    /// Dangling for an export that belongs to no session.
    session: Weak<Session>,
    slot: Mutex<Slot>,
}

struct Slot {
    state: ExportState,
    result: Option<ExportValue>,
    /// The session's listener version when listeners were last told of this export.
    listener_version: u64,
    /// Whether work has been submitted. An export is defined once.
    defined: bool,
    serial: bool,
    work: Option<Work>,
    on_error: Option<OnError>,
    on_success: Option<OnSuccess>,
    /// Dependents to notify, each with the index of this export among its dependencies.
    children: Vec<(Export, usize)>,
    /// The values of the dependencies, filled in as they resolve.
    inputs: Vec<Option<ExportValue>>,
    /// How many of `inputs` are still missing.
    pending: usize,
    error_id: Option<String>,
    failed_dependency: Option<String>,
    cause: Option<ExportError>,
}

/// What a dependent needs to know about a dependency that has resolved.
#[derive(Clone)]
struct Resolved {
    state: ExportState,
    value: Option<ExportValue>,
    error_id: Option<String>,
    status: Option<Status>,
    log_identity: String,
}

/// A consequence of a transition, carried out after the export's mutex is released.
enum Effect {
    Call(Box<dyn FnOnce() + Send>),
    Resolve {
        child: Export,
        index: usize,
        parent: Resolved,
    },
    Schedule,
    MarkReleased,
}

static NEXT_NON_EXPORT: AtomicU64 = AtomicU64::new(1);

fn non_export_identity() -> String {
    format!("{:x}", NEXT_NON_EXPORT.fetch_add(1, Ordering::Relaxed))
}

impl Export {
    fn with_state(
        id: i32,
        log_identity: String,
        session: Weak<Session>,
        state: ExportState,
    ) -> Self {
        Export {
            inner: Arc::new(Inner {
                id,
                log_identity,
                session,
                slot: Mutex::new(Slot {
                    state,
                    result: None,
                    listener_version: 0,
                    defined: false,
                    serial: false,
                    work: None,
                    on_error: None,
                    on_success: None,
                    children: Vec::new(),
                    inputs: Vec::new(),
                    pending: 0,
                    error_id: None,
                    failed_dependency: None,
                    cause: None,
                }),
            }),
        }
    }

    /// A new export of `session` in the `UNKNOWN` state. Id 0 makes a non-export.
    pub(super) fn new(session: &Arc<Session>, id: i32) -> Self {
        let log_identity = if id == NON_EXPORT_ID {
            non_export_identity()
        } else {
            id.to_string()
        };
        Self::with_state(
            id,
            log_identity,
            Arc::downgrade(session),
            ExportState::Unknown,
        )
    }

    /// A stand-in for an id that has been released. It is not in the export table; it fails
    /// dependents as `DEPENDENCY_RELEASED` and refuses to be redefined.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#newReleasedExport`
    pub(super) fn released(session: &Arc<Session>, id: i32) -> Self {
        Self::with_state(
            id,
            id.to_string(),
            Arc::downgrade(session),
            ExportState::Released,
        )
    }

    /// A value that is already available, as a dependency for exports of any session.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#wrapAsExport`
    pub fn ready(value: ExportValue) -> Self {
        let export = Self::with_state(
            NON_EXPORT_ID,
            format!("{}-sessionless", non_export_identity()),
            Weak::new(),
            ExportState::Exported,
        );
        {
            let mut slot = export.lock();
            slot.defined = true;
            slot.result = Some(value);
        }
        export
    }

    /// A dependency that has already failed. An export that requires it fails with `error`
    /// if that is a status, and as `DEPENDENCY_FAILED` otherwise.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#wrapAsFailedExport`
    pub fn failed(error: ExportError) -> Self {
        let export = Self::with_state(
            NON_EXPORT_ID,
            format!("{}-sessionless", non_export_identity()),
            Weak::new(),
            ExportState::Failed,
        );
        {
            let mut slot = export.lock();
            slot.defined = true;
            slot.cause = Some(error);
            export.assign_error_id(&mut slot, None, ExportState::Failed);
        }
        export
    }

    fn lock(&self) -> MutexGuard<'_, Slot> {
        self.inner.slot.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The export id; 0 for a non-export.
    pub fn id(&self) -> i32 {
        self.inner.id
    }

    /// The ticket that names this export. For a non-export it resolves to nothing.
    pub fn ticket(&self) -> Ticket {
        export_id_to_ticket(self.inner.id)
    }

    pub fn is_non_export(&self) -> bool {
        self.inner.id == NON_EXPORT_ID
    }

    pub fn state(&self) -> ExportState {
        self.lock().state
    }

    /// The exported value, if this export holds one now. To use the value of an export in
    /// work, require the export instead: this is `None` before the export completes and
    /// after it is released.
    pub fn value(&self) -> Option<ExportValue> {
        self.lock().result.clone()
    }

    /// What an error handler in the reference server is told about this export's failure,
    /// or `None` if the export is not in a failure state. Handlers given to
    /// [`ExportBuilder::on_error`] receive only the status; one that reports more, as
    /// `Batch` does, reads the rest here.
    ///
    /// Behavior: the arguments of
    /// `io.deephaven.server.session.SessionState.ExportErrorHandler#onError`
    pub fn failure(&self) -> Option<ExportFailure> {
        let slot = self.lock();
        if !is_failure(slot.state) {
            return None;
        }
        Some(ExportFailure {
            error_id: slot.error_id.clone().unwrap_or_default(),
            dependency: slot.failed_dependency.clone(),
            cause: slot.cause.as_ref().map(|_| self.failure_status(&slot)),
        })
    }

    /// Releases an exported value; cancels an export that has not completed, which fails the
    /// exports that depend on it. Does nothing to an export already in a terminal state.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#cancel`
    pub fn cancel(&self) {
        let mut effects = Vec::new();
        {
            let mut slot = self.lock();
            if slot.state == ExportState::Exported {
                if self.is_non_export() {
                    return;
                }
                self.set_state(&mut slot, ExportState::Released, &mut effects);
                effects.push(Effect::MarkReleased);
            } else if !is_terminal(slot.state) {
                self.set_state(&mut slot, ExportState::Cancelled, &mut effects);
            }
        }
        self.run(effects);
    }

    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#makeExportNotification`
    fn notification_of(&self, slot: &Slot) -> ExportNotification {
        ExportNotification {
            ticket: Some(self.ticket()),
            export_state: slot.state as i32,
            context: slot.error_id.clone().unwrap_or_default(),
            dependent_handle: slot.failed_dependency.clone().unwrap_or_default(),
        }
    }

    /// Tells the session's listeners of this export's current state.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#notifyListeners`
    fn notify_listeners(&self, slot: &mut Slot) {
        if self.is_non_export() {
            return;
        }
        if let Some(session) = self.inner.session.upgrade() {
            let notification = self.notification_of(slot);
            slot.listener_version = session.publish_notification(notification);
        }
    }

    /// Publishes a newly created export to listeners; the caller has just put it in the
    /// export table.
    pub(super) fn announce(&self) {
        let mut slot = self.lock();
        self.notify_listeners(&mut slot);
    }

    /// Sends the current state to one listener that subscribed at `version`, unless the
    /// export is terminal or has notified all listeners since then.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportListener#initialize`
    pub(super) fn replay_to(&self, version: u64, send: impl FnOnce(ExportNotification)) {
        let slot = self.lock();
        if slot.listener_version >= version || is_terminal(slot.state) {
            return;
        }
        send(self.notification_of(&slot));
    }

    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#setState`
    fn set_state(&self, slot: &mut Slot, state: ExportState, effects: &mut Vec<Effect>) {
        assert!(
            !is_terminal(slot.state)
                && !(slot.state == ExportState::Exported && self.is_non_export()),
            "cannot change state if export is already in terminal state"
        );
        assert!(
            slot.state == ExportState::Unknown || (slot.state as i32) < (state as i32),
            "export object state changes must advance toward a terminal state"
        );
        slot.state = state;
        // listeners hear of the change before dependents do
        self.notify_listeners(slot);

        if is_failure(state) {
            if let Some(on_error) = slot.on_error.take() {
                self.assign_error_id(slot, None, state);
                let status = self.failure_status(slot);
                effects.push(Effect::Call(Box::new(move || on_error(status))));
            }
        }
        if state == ExportState::Exported {
            if let (Some(on_success), Some(value)) = (slot.on_success.take(), slot.result.clone()) {
                effects.push(Effect::Call(Box::new(move || on_success(value))));
            }
        }
        if state == ExportState::Exported || is_terminal(state) {
            slot.work = None;
            slot.on_error = None;
            slot.on_success = None;
            slot.inputs = Vec::new();
            let parent = self.resolved(slot);
            for (child, index) in std::mem::take(&mut slot.children) {
                effects.push(Effect::Resolve {
                    child,
                    index,
                    parent: parent.clone(),
                });
            }
            if is_terminal(state) {
                slot.result = None;
                // A status is kept: dependents defined later still report it. Of an
                // internal failure only the fact is kept; its text is in the log.
                if let Some(ExportError::Internal(text)) = &mut slot.cause {
                    *text = String::new();
                }
            }
        }
    }

    fn resolved(&self, slot: &Slot) -> Resolved {
        Resolved {
            state: slot.state,
            value: slot.result.clone(),
            error_id: slot.error_id.clone(),
            status: match &slot.cause {
                Some(ExportError::Status(status)) => Some(status.clone()),
                _ => None,
            },
            log_identity: self.inner.log_identity.clone(),
        }
    }

    /// Gives this export an error id if it has none, and logs what the id stands for. A
    /// cancel or release, of this export or of a dependency, is the client's doing and is
    /// logged as such.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#maybeAssignErrorId`
    fn assign_error_id(&self, slot: &mut Slot, details: Option<&str>, state: ExportState) {
        if slot.error_id.is_some() {
            return;
        }
        let error_id = Uuid::new_v4().to_string();
        let identity = &self.inner.log_identity;
        let client_driven = slot.cause.is_none()
            && matches!(
                state,
                ExportState::Cancelled
                    | ExportState::DependencyCancelled
                    | ExportState::DependencyReleased
            );
        match (&slot.cause, details) {
            (_, Some(details)) if client_driven => {
                tracing::info!("Export ended by client '{error_id}' for {identity}: {details}")
            }
            (None, None) if client_driven => {
                tracing::info!("Export ended by client '{error_id}' for {identity}")
            }
            (Some(ExportError::Status(status)), _) => {
                tracing::debug!("Error '{error_id}' for {identity}: {status}")
            }
            (Some(ExportError::Internal(cause)), _) => {
                tracing::error!("Internal Error '{error_id}' for {identity}: {cause}")
            }
            (None, details) => tracing::error!(
                "Internal Error '{error_id}' for {identity}: {}",
                details.unwrap_or("no details")
            ),
        }
        slot.error_id = Some(error_id);
    }

    /// The status an error handler receives for the failure this export is in.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#toErrorHandler` applied to the
    /// output of `SessionService.ObfuscatingErrorTransformer#transform`, as
    /// `ExportObject#setState` and `#setWork` combine them.
    fn failure_status(&self, slot: &Slot) -> Status {
        let related = match &slot.failed_dependency {
            Some(parent) => format!(" (related parent export id: {parent})"),
            None => String::new(),
        };
        match &slot.cause {
            Some(ExportError::Status(status)) => status.clone(),
            Some(ExportError::Internal(_)) => Status::invalid_argument(format!(
                "Details Logged w/ID '{}'",
                slot.error_id.as_deref().unwrap_or_default()
            )),
            None if slot.state == ExportState::Cancelled => {
                Status::cancelled(format!("Export is cancelled{related}"))
            }
            None => Status::failed_precondition(format!(
                "Export in state {}{related}",
                slot.state.as_str_name()
            )),
        }
    }

    /// Fails this export because the dependency `parent` cannot be used.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#onDependencyFailure`
    fn dependency_failure(&self, slot: &mut Slot, parent: &Resolved, effects: &mut Vec<Effect>) {
        if is_terminal(slot.state) {
            return;
        }
        slot.error_id = parent.error_id.clone();
        if let Some(status) = &parent.status {
            slot.cause = Some(ExportError::Status(status.clone()));
        }
        let mut terminal = ExportState::DependencyFailed;
        if slot.error_id.is_none() {
            // only a dependency that has no error of its own names itself as the cause
            let details = match parent.state {
                ExportState::Released => {
                    terminal = ExportState::DependencyReleased;
                    "dependency released by user.".to_string()
                }
                ExportState::Cancelled => {
                    terminal = ExportState::DependencyCancelled;
                    "dependency cancelled by user.".to_string()
                }
                other => format!(
                    "dependency does not have its own error defined and is in an unexpected \
                     state: {}",
                    other.as_str_name()
                ),
            };
            self.assign_error_id(slot, Some(&details), terminal);
            slot.failed_dependency = Some(parent.log_identity.clone());
        }
        self.set_state(slot, terminal, effects);
    }

    /// Records that the dependency at `index` has resolved, and queues the work if it was
    /// the last one.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#onResolveOne`
    fn on_parent_resolved(&self, index: usize, parent: Resolved) {
        let mut effects = Vec::new();
        {
            let mut slot = self.lock();
            if is_terminal(slot.state) {
                return;
            }
            match parent.value {
                Some(value) if parent.state == ExportState::Exported => {
                    slot.inputs[index] = Some(value);
                    slot.pending -= 1;
                    if slot.pending == 0 {
                        self.queue(&mut slot, &mut effects);
                    }
                }
                _ => self.dependency_failure(&mut slot, &parent, &mut effects),
            }
        }
        self.run(effects);
    }

    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#scheduleExport`
    fn queue(&self, slot: &mut Slot, effects: &mut Vec<Effect>) {
        if slot.state != ExportState::Pending {
            return;
        }
        self.set_state(slot, ExportState::Queued, effects);
        effects.push(Effect::Schedule);
    }

    /// Runs the work, on an executor thread.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#doExport`,
    /// `#setResult`
    fn do_export(&self) {
        let mut effects = Vec::new();
        let session_alive = self
            .inner
            .session
            .upgrade()
            .is_some_and(|session| !session.is_expired());
        let (work, inputs) = {
            let mut slot = self.lock();
            let work = match slot.work.take() {
                Some(work) if slot.state == ExportState::Queued && session_alive => work,
                _ => {
                    // lost a race with a cancel, a release, or the end of the session
                    if !is_terminal(slot.state) {
                        self.set_state(&mut slot, ExportState::Cancelled, &mut effects);
                    }
                    drop(slot);
                    self.run(effects);
                    return;
                }
            };
            self.set_state(&mut slot, ExportState::Running, &mut effects);
            let inputs: Vec<ExportValue> = std::mem::take(&mut slot.inputs)
                .into_iter()
                .map(|input| input.expect("every dependency resolved before the work is queued"))
                .collect();
            (work, inputs)
        };
        self.run(std::mem::take(&mut effects));

        // A panic in the work (an `unported!` call, or a broken invariant) fails this export
        // rather than leaving it RUNNING forever with a client waiting on it.
        let outcome = catch_unwind(AssertUnwindSafe(|| work(inputs))).unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            Err(ExportError::Internal(format!("panic: {message}")))
        });

        {
            let mut slot = self.lock();
            // the client may have cancelled while the work ran
            if !is_terminal(slot.state) {
                match outcome {
                    Ok(value) => {
                        slot.result = Some(value);
                        self.set_state(&mut slot, ExportState::Exported, &mut effects);
                    }
                    Err(error) => {
                        slot.cause = Some(error);
                        self.assign_error_id(&mut slot, None, ExportState::Failed);
                        self.set_state(&mut slot, ExportState::Failed, &mut effects);
                    }
                }
            }
        }
        self.run(effects);
    }

    /// Makes this export `EXPORTED` with `value` without running work.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState#newServerSideExport`
    pub(super) fn set_result(&self, value: ExportValue) {
        let mut effects = Vec::new();
        {
            let mut slot = self.lock();
            slot.defined = true;
            if !is_terminal(slot.state) {
                slot.result = Some(value);
                self.set_state(&mut slot, ExportState::Exported, &mut effects);
            }
        }
        self.run(effects);
    }

    fn run(&self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Call(call) => call(),
                Effect::Resolve {
                    child,
                    index,
                    parent,
                } => child.on_parent_resolved(index, parent),
                Effect::Schedule => {
                    let export = self.clone();
                    match self.inner.session.upgrade() {
                        Some(session) => {
                            let job = Box::new(move || export.do_export());
                            if self.lock().serial {
                                session.executor().run_serially(job);
                            } else {
                                session.executor().run(job);
                            }
                        }
                        // the session is gone; this cancels the export
                        None => export.do_export(),
                    }
                }
                Effect::MarkReleased => {
                    if let Some(session) = self.inner.session.upgrade() {
                        session.mark_released(self.inner.id);
                    }
                }
            }
        }
    }
}

impl fmt::Debug for Export {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Export")
            .field("id", &self.inner.id)
            .field("state", &self.state())
            .finish()
    }
}

/// Defines an export: its dependencies, its handlers and its work.
///
/// Failures, including the reuse of a released id, are reported to the `on_error` handler,
/// so one should always be set.
///
/// Behavior: `io.deephaven.server.session.SessionState.ExportBuilder`
pub struct ExportBuilder {
    export: Export,
    dependencies: Vec<Export>,
    serial: bool,
    on_error: Option<OnError>,
    on_success: Option<OnSuccess>,
}

impl ExportBuilder {
    pub(super) fn new(export: Export) -> Self {
        Self {
            export,
            dependencies: Vec::new(),
            serial: false,
            on_error: None,
            on_success: None,
        }
    }

    /// The export being defined. It can be required by other work before it is submitted.
    pub fn export(&self) -> &Export {
        &self.export
    }

    /// Adds exports whose values the work needs. The work receives the values in the order
    /// the exports were added.
    pub fn require(mut self, dependencies: impl IntoIterator<Item = Export>) -> Self {
        self.dependencies.extend(dependencies);
        self
    }

    /// Runs the work on the serial queue: after all serial work submitted before it.
    pub fn requires_serial_queue(mut self) -> Self {
        self.serial = true;
        self
    }

    /// Called once, with the status to give the client, if the export ends in a failure
    /// state.
    pub fn on_error(mut self, on_error: impl FnOnce(Status) + Send + 'static) -> Self {
        self.on_error = Some(Box::new(on_error));
        self
    }

    /// Called once with the value when the export becomes `EXPORTED`.
    pub fn on_success(mut self, on_success: impl FnOnce(ExportValue) + Send + 'static) -> Self {
        self.on_success = Some(Box::new(on_success));
        self
    }

    /// Submits the work. It runs on the session's executor once every dependency has been
    /// exported, with the dependencies' values.
    ///
    /// Fails with `INVALID_ARGUMENT` if the export already has work: an export id can be
    /// defined once. In Java that is an `IllegalStateException` out of the request handler,
    /// which the error transformer turns into this status.
    ///
    /// Two interleavings of a dependency and its release differ from Java, each within what
    /// Java itself produces under a different timing. A dependency released between the
    /// first look at it here and its registration fails this export as
    /// `DEPENDENCY_RELEASED`, where Java would still use the value. And a non-export that
    /// has already completed can be required here, where Java fails the dependent because
    /// the completed non-export has dropped its result.
    ///
    /// Behavior: `io.deephaven.server.session.SessionState.ExportObject#setDependencies`,
    /// `#setWork`
    pub fn submit<F>(self, work: F) -> Result<Export, Status>
    where
        F: FnOnce(Vec<ExportValue>) -> Result<ExportValue, ExportError> + Send + 'static,
    {
        let ExportBuilder {
            export,
            dependencies,
            serial,
            on_error,
            on_success,
        } = self;

        // A dependency that is already gone fails this export before it is ever pending.
        // One that is already exported gives up its value now, so that a later release of it
        // cannot take the value away.
        let mut inputs: Vec<Option<ExportValue>> = vec![None; dependencies.len()];
        let mut dead = None;
        for (input, dependency) in inputs.iter_mut().zip(&dependencies) {
            let resolved = dependency.resolved(&dependency.lock());
            if is_terminal(resolved.state) {
                dead = Some(resolved);
                break;
            }
            if resolved.state == ExportState::Exported {
                *input = resolved.value;
            }
        }
        let waiting: Vec<usize> = (0..inputs.len())
            .filter(|&index| inputs[index].is_none())
            .collect();

        let mut effects = Vec::new();
        {
            let mut slot = export.lock();
            if slot.defined {
                return Err(Status::invalid_argument(format!(
                    "export {} can only be defined once",
                    export.inner.log_identity
                )));
            }
            slot.defined = true;
            if let Some(dead) = &dead {
                export.dependency_failure(&mut slot, dead, &mut effects);
            }
            if is_terminal(slot.state) {
                // released, cancelled, expired with its session, or failed by a dead
                // dependency just above; this is the first the export hears of the handler
                if let Some(on_error) = on_error {
                    let state = slot.state;
                    export.assign_error_id(&mut slot, None, state);
                    let status = export.failure_status(&slot);
                    effects.push(Effect::Call(Box::new(move || on_error(status))));
                }
                drop(slot);
                export.run(effects);
                return Ok(export);
            }
            slot.work = Some(Box::new(work));
            slot.on_error = on_error;
            slot.on_success = on_success;
            slot.serial = serial;
            slot.inputs = inputs;
            slot.pending = waiting.len();
            export.set_state(&mut slot, ExportState::Pending, &mut effects);
            if slot.pending == 0 {
                export.queue(&mut slot, &mut effects);
            }
        }
        export.run(effects);

        for index in waiting {
            let dependency = &dependencies[index];
            let resolved = {
                let mut parent = dependency.lock();
                if parent.state == ExportState::Exported || is_terminal(parent.state) {
                    Some(dependency.resolved(&parent))
                } else {
                    parent.children.push((export.clone(), index));
                    None
                }
            };
            if let Some(resolved) = resolved {
                export.on_parent_resolved(index, resolved);
            }
        }
        Ok(export)
    }
}
