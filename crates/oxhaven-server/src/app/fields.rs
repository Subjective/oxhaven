//! The applications of the server, their fields, and the clients that are told about them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use oxhaven_proto::grpc::{FieldInfo, FieldsChangeUpdate, Ticket, TypedTicket};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;

use super::resolver::application_field_ticket;
use super::scope::scope_ticket;
use crate::console::type_name_if_displayable;
use crate::session::{ExportValue, OnCloseId, Session};

/// The application id Java keeps for the variables of the query scope.
///
/// Behavior: `io.deephaven.server.appmode.AppFieldId#SCOPE_ID`
const SCOPE_ID: &str = "scope";

/// The description of every query scope variable in the list of fields.
///
/// Behavior: `ApplicationServiceGrpcImpl#QUERY_SCOPE_DESCRIPTION`
const SCOPE_DESCRIPTION: &str = "query scope variable";

/// The shortest time between two messages to the clients that listen for field changes, so
/// that an application that sets fields in a loop sends one message, not one per field.
///
/// Behavior: `ApplicationServiceGrpcImpl.FieldUpdatePropagationJob#UPDATE_INTERVAL_MS`
const UPDATE_INTERVAL: Duration = Duration::from_millis(250);

/// Why an application or a field was refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ApplicationError {
    /// `scope` names the query scope in the list of fields.
    ReservedId,
    /// There is already an application with this id.
    DuplicateId(String),
    /// A field name has a character outside printable ASCII.
    InvalidFieldName(String),
    /// A description was given and is empty.
    EmptyDescription,
    /// An object type failed to say whether the value of a field is its own.
    TypeLookup(String),
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplicationError::ReservedId => write!(
                f,
                "Applications cannot re-use the id '{SCOPE_ID}'; it is reserved for QueryScope \
                 use only"
            ),
            ApplicationError::DuplicateId(id) => {
                write!(f, "Duplicate application found for app_id {id}")
            }
            ApplicationError::InvalidFieldName(name) => {
                write!(f, "field name {name:?} is invalid, must be printable ascii")
            }
            ApplicationError::EmptyDescription => {
                f.write_str("a field description, when present, must not be empty")
            }
            ApplicationError::TypeLookup(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for ApplicationError {}

/// The stream of one `ListFields` call: every field there is, then each change.
pub type FieldUpdates = mpsc::UnboundedReceiver<Result<FieldsChangeUpdate, Status>>;

struct Field {
    value: ExportValue,
}

struct ApplicationData {
    name: String,
    fields: BTreeMap<String, Field>,
}

/// A field as clients know it: the application's id and the field's name.
#[derive(Clone, PartialEq, Eq)]
struct FieldId {
    application_id: String,
    field_name: String,
}

/// What happened to one field since clients were last told.
struct Pending {
    id: FieldId,
    /// What clients know of the field, if they know it.
    existing: Option<FieldInfo>,
    /// What the field is now, if it exists.
    now: Option<FieldInfo>,
}

struct Subscription {
    id: u64,
    sender: mpsc::UnboundedSender<Result<FieldsChangeUpdate, Status>>,
    session: Weak<Session>,
    on_close: OnCloseId,
}

#[derive(Default)]
struct State {
    applications: BTreeMap<String, ApplicationData>,
    /// The fields as the listening clients know them, in the order they were told.
    known: Vec<(FieldId, FieldInfo)>,
    /// Changes the clients have not been told yet, in the order the fields first changed.
    accumulated: Vec<Pending>,
    subscriptions: Vec<Subscription>,
    next_subscription: u64,
    /// Whether a task is on its way to send `accumulated`.
    scheduled: bool,
    /// When the latest such task was due.
    last_scheduled: Option<Instant>,
}

/// The applications of a server. See the [module documentation](super).
///
/// Behavior: `io.deephaven.server.appmode.ApplicationTicketResolver` (the applications by
/// id) and `io.deephaven.server.appmode.ApplicationServiceGrpcImpl` (what clients are told)
pub struct Applications {
    runtime: tokio::runtime::Handle,
    this: Weak<Applications>,
    state: Mutex<State>,
}

impl Applications {
    /// Must be called inside a tokio runtime: changes are sent to listening clients from it.
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            runtime: tokio::runtime::Handle::current(),
            this: this.clone(),
            state: Mutex::new(State::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds an application without fields. `id` is what tickets name it by and `name` is
    /// what a user interface shows.
    ///
    /// Behavior: `io.deephaven.appmode.ApplicationState#ApplicationState`,
    /// `ApplicationTicketResolver#onApplicationLoad`, `AppFieldId#AppFieldId`
    pub fn create(
        self: &Arc<Self>,
        id: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Application, ApplicationError> {
        let id = id.into();
        if id == SCOPE_ID {
            return Err(ApplicationError::ReservedId);
        }
        let mut state = self.lock();
        if state.applications.contains_key(&id) {
            return Err(ApplicationError::DuplicateId(id));
        }
        state.applications.insert(
            id.clone(),
            ApplicationData {
                name: name.into(),
                fields: BTreeMap::new(),
            },
        );
        Ok(Application {
            applications: self.clone(),
            id,
        })
    }

    /// The application with this id.
    ///
    /// Behavior: `ApplicationTicketResolver#getApplicationState`
    pub fn get(self: &Arc<Self>, id: &str) -> Option<Application> {
        self.lock()
            .applications
            .contains_key(id)
            .then(|| Application {
                applications: self.clone(),
                id: id.to_string(),
            })
    }

    /// Starts a `ListFields` stream for `session`: one message that lists every field as
    /// created, then one message per batch of changes. The stream ends with `CANCELLED`
    /// when the session does.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl#listFields`,
    /// `ApplicationServiceGrpcImpl.Subscription`
    pub fn subscribe(&self, session: &Arc<Session>) -> Result<FieldUpdates, Status> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut state = self.lock();
        let id = state.next_subscription;
        state.next_subscription += 1;
        let on_close = session.add_on_close({
            let this = self.this.clone();
            move || {
                if let Some(this) = this.upgrade() {
                    this.cancel(id);
                }
            }
        })?;
        // what is listed has to include the changes still waiting to be sent
        propagate(&mut state);
        let initial = FieldsChangeUpdate {
            created: state.known.iter().map(|(_, info)| info.clone()).collect(),
            ..Default::default()
        };
        let _ = sender.send(Ok(initial));
        state.subscriptions.push(Subscription {
            id,
            sender: sender.clone(),
            session: Arc::downgrade(session),
            on_close,
        });
        drop(state);

        // a client that goes away is forgotten, and so is its session's callback
        let this = self.this.clone();
        self.runtime.spawn(async move {
            sender.closed().await;
            if let Some(this) = this.upgrade() {
                this.cancel(id);
            }
        });
        Ok(receiver)
    }

    /// Ends a subscription, if it has not ended yet.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl.Subscription#onCancel`,
    /// `ApplicationServiceGrpcImpl#remove(Subscription)`
    fn cancel(&self, id: u64) {
        let removed = {
            let mut state = self.lock();
            let position = state.subscriptions.iter().position(|s| s.id == id);
            position.map(|position| state.subscriptions.remove(position))
        };
        if let Some(subscription) = removed {
            end(subscription);
        }
    }

    /// The number of open `ListFields` streams, for tests.
    #[cfg(test)]
    pub(crate) fn subscription_count(&self) -> usize {
        self.lock().subscriptions.len()
    }

    /// Notes that the query scope variable `name` now holds a value clients can open, of
    /// the type `type_name`, or with `None` that it no longer does. Clients see the
    /// variables as fields of the application id `scope`, with the ticket `s/<name>`.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl#onScopeChanges`,
    /// `io.deephaven.server.appmode.AppFieldId#fromScopeName`
    pub fn scope_changed(&self, name: &str, type_name: Option<&str>) {
        let info = type_name.map(|type_name| FieldInfo {
            typed_ticket: Some(TypedTicket {
                ticket: Some(Ticket {
                    ticket: scope_ticket(name),
                }),
                r#type: type_name.to_string(),
            }),
            field_name: name.to_string(),
            field_description: SCOPE_DESCRIPTION.to_string(),
            application_name: String::new(),
            application_id: SCOPE_ID.to_string(),
        });
        let id = FieldId {
            application_id: SCOPE_ID.to_string(),
            field_name: name.to_string(),
        };
        self.changed(&mut self.lock(), id, info);
    }

    /// Notes that a field changed and sees to it that clients are told: now, if nobody is
    /// listening or the last message is old enough, and otherwise once it is.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl#schedulePropagationOrClearIncrementalState`,
    /// `ApplicationServiceGrpcImpl.FieldUpdatePropagationJob#markUpdates`
    fn changed(&self, state: &mut State, id: FieldId, now: Option<FieldInfo>) {
        match state.accumulated.iter_mut().find(|p| p.id == id) {
            Some(pending) => pending.now = now,
            None => {
                let existing = state
                    .known
                    .iter()
                    .find(|(known, _)| *known == id)
                    .map(|(_, info)| info.clone());
                state.accumulated.push(Pending { id, existing, now });
            }
        }
        if state.subscriptions.is_empty() {
            propagate(state);
            return;
        }
        if state.scheduled {
            return;
        }
        state.scheduled = true;
        let now = Instant::now();
        let due = match state.last_scheduled {
            Some(last) if now < last + UPDATE_INTERVAL => last + UPDATE_INTERVAL,
            _ => now,
        };
        state.last_scheduled = Some(due);
        let this = self.this.clone();
        self.runtime.spawn(async move {
            tokio::time::sleep_until(due).await;
            if let Some(this) = this.upgrade() {
                propagate(&mut this.lock());
            }
        });
    }
}

/// Tells a removed subscription's client that it is over and frees the session's callback.
fn end(subscription: Subscription) {
    if let Some(session) = subscription.session.upgrade() {
        session.remove_on_close(subscription.on_close);
    }
    let _ = subscription
        .sender
        .send(Err(Status::cancelled("subscription cancelled")));
}

/// Applies the accumulated changes to what clients know and sends them to every listening
/// client, as one message. A field that was set and removed in between is not mentioned; a
/// field that was replaced, or removed and set again, is `updated`.
///
/// Behavior: `ApplicationServiceGrpcImpl#propagateUpdates`,
/// `ApplicationServiceGrpcImpl.State#append`, `ApplicationServiceGrpcImpl.Updater`
fn propagate(state: &mut State) {
    state.scheduled = false;
    let mut update = FieldsChangeUpdate::default();
    for pending in std::mem::take(&mut state.accumulated) {
        let position = state.known.iter().position(|(id, _)| *id == pending.id);
        match (pending.existing, pending.now) {
            (None, None) => {}
            (None, Some(info)) => {
                state.known.push((pending.id, info.clone()));
                update.created.push(info);
            }
            (Some(_), Some(info)) => {
                if let Some(position) = position {
                    state.known[position].1 = info.clone();
                }
                update.updated.push(info);
            }
            (Some(existing), None) => {
                if let Some(position) = position {
                    state.known.remove(position);
                }
                update.removed.push(existing);
            }
        }
    }
    if update == FieldsChangeUpdate::default() {
        return;
    }
    // a client that cannot be sent to is gone
    let (listening, gone): (Vec<_>, Vec<_>) = std::mem::take(&mut state.subscriptions)
        .into_iter()
        .partition(|subscription| subscription.sender.send(Ok(update.clone())).is_ok());
    state.subscriptions = listening;
    gone.into_iter().for_each(end);
}

/// Behavior: `io.deephaven.appmode.ApplicationUtil#isAsciiPrintable`
fn is_ascii_printable(text: &str) -> bool {
    text.chars().all(|c| (' '..'\u{7f}').contains(&c))
}

/// One application of a server: a handle that sets, replaces and removes its fields.
///
/// Behavior: `io.deephaven.appmode.ApplicationState`
#[derive(Clone)]
pub struct Application {
    applications: Arc<Applications>,
    id: String,
}

impl Application {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The name shown to users.
    pub fn name(&self) -> String {
        let state = self.applications.lock();
        state.applications[&self.id].name.clone()
    }

    /// Sets the field `name` to `value`, replacing the value it had.
    ///
    /// Behavior: `ApplicationState#setField(String, Object)`
    pub fn set_field(&self, name: &str, value: ExportValue) -> Result<(), ApplicationError> {
        self.set(name, value, None)
    }

    /// As [`set_field`](Self::set_field), with a description for users.
    ///
    /// Behavior: `ApplicationState#setField(String, Object, String)`
    pub fn set_field_described(
        &self,
        name: &str,
        value: ExportValue,
        description: &str,
    ) -> Result<(), ApplicationError> {
        self.set(name, value, Some(description))
    }

    /// The type clients are told is `Table` for a table, the name of its kind for another
    /// value clients can open (`PartitionedTable`), and none for anything else.
    ///
    /// Behavior: `ApplicationState#setField(Field)`, `StandardField#checkName`,
    /// `StandardField#checkDescription`, `ApplicationServiceGrpcImpl#onNewField`,
    /// `ApplicationServiceGrpcImpl.State#fieldInfo`, `io.deephaven.server.object.TypeLookup#type`
    fn set(
        &self,
        name: &str,
        value: ExportValue,
        description: Option<&str>,
    ) -> Result<(), ApplicationError> {
        if !is_ascii_printable(name) {
            return Err(ApplicationError::InvalidFieldName(name.to_string()));
        }
        if description.is_some_and(str::is_empty) {
            return Err(ApplicationError::EmptyDescription);
        }
        // Java asks its object type plugins about anything that is no table, and an object
        // none of them knows has no type and is listed all the same. Asked before the lock:
        // a type of a plugin runs Python to answer.
        let type_name = type_name_if_displayable(&value)
            .map_err(ApplicationError::TypeLookup)?
            .unwrap_or_default();
        let mut state = self.applications.lock();
        let application = state
            .applications
            .get_mut(&self.id)
            .expect("an application is never removed");
        let info = FieldInfo {
            typed_ticket: Some(TypedTicket {
                ticket: Some(Ticket {
                    ticket: application_field_ticket(&self.id, name),
                }),
                r#type: type_name,
            }),
            field_name: name.to_string(),
            field_description: description.unwrap_or_default().to_string(),
            application_name: application.name.clone(),
            application_id: self.id.clone(),
        };
        let replaced = application.fields.insert(name.to_string(), Field { value });
        // Java tells its listener of a replaced field in two steps, removed and new. With
        // clients listening the two are one update; with none each is applied at once, and
        // the field moves behind the others in the list (observed on Java 42.9: a script
        // that sets a, b, c and a again is listed as b, c, a).
        if replaced.is_some() {
            self.applications
                .changed(&mut state, self.field_id(name), None);
        }
        self.applications
            .changed(&mut state, self.field_id(name), Some(info));
        Ok(())
    }

    /// Removes the field `name`. Returns whether there was one.
    ///
    /// Behavior: `ApplicationState#removeField`, `ApplicationServiceGrpcImpl#onRemoveField`
    pub fn remove_field(&self, name: &str) -> bool {
        let mut state = self.applications.lock();
        let application = state
            .applications
            .get_mut(&self.id)
            .expect("an application is never removed");
        if application.fields.remove(name).is_none() {
            return false;
        }
        let id = self.field_id(name);
        self.applications.changed(&mut state, id, None);
        true
    }

    /// The value of the field `name`.
    ///
    /// Behavior: `ApplicationState#getField`
    pub fn field(&self, name: &str) -> Option<ExportValue> {
        let state = self.applications.lock();
        let field = state.applications[&self.id].fields.get(name)?;
        Some(field.value.clone())
    }

    /// The names of the fields, sorted.
    pub fn field_names(&self) -> Vec<String> {
        let state = self.applications.lock();
        state.applications[&self.id]
            .fields
            .keys()
            .cloned()
            .collect()
    }

    fn field_id(&self, name: &str) -> FieldId {
        FieldId {
            application_id: self.id.clone(),
            field_name: name.to_string(),
        }
    }
}
