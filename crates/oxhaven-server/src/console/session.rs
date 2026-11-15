//! A script session: something that runs code in a language and has variables, and the
//! [`Console`] that makes its variables the query scope of the server.
//!
//! The session knows the language and holds the variables. Everything else is here and is
//! the same for every language: when the variables are looked at for changes, which changes
//! clients are told, and the fields that `ApplicationService/ListFields` lists under the
//! application id `scope`. The tickets `s/<name>` resolve by asking the session
//! ([`ScopeHolder`]), so there is one set of variables: what code assigns, what a client
//! publishes and what a ticket names are the same thing.

use std::sync::Arc;
use std::time::Duration;

use oxhaven_contracts::table::Table;

use crate::app::{Applications, FormulaValue, ScopeHolder};
use crate::session::ExportValue;

/// How often the variables are looked at for changes made outside a command.
///
/// Behavior: `io.deephaven.server.runner.DeephavenApiServer#CHECK_SCOPE_CHANGES_INTERVAL_MILLIS`
pub const CHECK_SCOPE_CHANGES_INTERVAL: Duration = Duration::from_millis(100);

/// The name clients are told for the type of a table variable.
pub const TABLE_TYPE_NAME: &str = "Table";

/// The type name clients are told for a variable that holds `value`, or `None` if clients
/// are not told about such a variable: only values a client can open are listed.
///
/// A table is a `Table`. Java leaves out a table with the attribute `NonDisplayTable`; the
/// tables of this engine have no such attribute, so every table is listed. Java's other
/// displayable kinds are `HierarchicalTable`, `PartitionedTable` and the objects a plugin
/// has a type for; this server has none of them yet, and the tasks that add them name
/// them here.
///
/// Behavior: `io.deephaven.engine.util.AbstractScriptSession#getTypeNameIfDisplayable`
pub fn type_name_if_displayable(value: &ExportValue) -> Option<&'static str> {
    match value {
        ExportValue::Table(_) => Some(TABLE_TYPE_NAME),
        ExportValue::Object(_) => None,
    }
}

/// Whether two values of variables are the same object, as Java's `==` on references.
fn same_object(a: &ExportValue, b: &ExportValue) -> bool {
    match (a, b) {
        (ExportValue::Table(a), ExportValue::Table(b)) => Arc::ptr_eq(a, b),
        (ExportValue::Object(a), ExportValue::Object(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

/// A variable that clients can open, as a script session reports it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Variable {
    pub name: String,
    /// What kind of thing the value is, as clients are told; see
    /// [`type_name_if_displayable`].
    pub type_name: String,
}

/// What changed in the variables of a script session. Only variables whose values clients
/// can open count: a variable that holds a number is never mentioned.
///
/// Behavior: `io.deephaven.engine.util.ScriptSession.Changes`
#[derive(Clone, Debug, Default)]
pub struct Changes {
    pub created: Vec<Variable>,
    pub updated: Vec<Variable>,
    pub removed: Vec<Variable>,
    /// The error that ended the code, as the text clients are shown. The variables the
    /// code changed before it failed are still listed.
    pub error: Option<String>,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        self.created.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }

    /// Notes that the variable `name` held `from` and now holds `to`; `None` is a variable
    /// that does not exist, or holds the language's null. A value clients cannot open
    /// counts as none, and a value of another type as one variable removed and one created.
    ///
    /// Behavior: `AbstractScriptSession#applyVariableChangeToDiff`
    pub fn note(&mut self, name: &str, from: Option<&ExportValue>, to: Option<&ExportValue>) {
        let variable = |type_name: &str| Variable {
            name: name.to_string(),
            type_name: type_name.to_string(),
        };
        let from = from.and_then(|value| Some((value, type_name_if_displayable(value)?)));
        let to = to.and_then(|value| Some((value, type_name_if_displayable(value)?)));
        match (from, to) {
            (None, None) => {}
            (None, Some((_, to_type))) => self.created.push(variable(to_type)),
            (Some((_, from_type)), None) => self.removed.push(variable(from_type)),
            (Some((from, _)), Some((to, _))) if same_object(from, to) => {}
            (Some((_, from_type)), Some((_, to_type))) if from_type != to_type => {
                self.created.push(variable(to_type));
                self.removed.push(variable(from_type));
            }
            (Some(_), Some((_, to_type))) => self.updated.push(variable(to_type)),
        }
    }
}

/// The language side of a console, and the holder of its variables.
///
/// Behavior: `io.deephaven.engine.util.ScriptSession`, and the abstract methods of
/// `io.deephaven.engine.util.AbstractScriptSession`
pub trait ScriptSession: Send + Sync {
    /// The name of the language, as Java spells it: `Python`.
    ///
    /// Behavior: `ScriptSession#scriptType`
    fn script_type(&self) -> &str;

    /// Runs `code` and reports what changed in the variables since they were last looked
    /// at, which is what the code changed if [`ScriptSession::observe`] was called just
    /// before. An error in the code is not an error of this call: it is `Changes::error`.
    ///
    /// Behavior: `ScriptSession#evaluateScript(String)`
    fn evaluate(&self, code: &str) -> Changes;

    /// Looks at the variables and reports what changed since they were last looked at, by
    /// this call, by [`ScriptSession::evaluate`] or by [`ScriptSession::put`].
    ///
    /// Behavior: `ScriptSession#observeScopeChanges`
    fn observe(&self) -> Changes;

    /// Makes `value` the value of the variable `name`, in the form code of the language
    /// uses it in, and reports what changed since the variables were last looked at.
    ///
    /// Behavior: `AbstractScriptSession#setVariable`, which observes the scope after it
    fn put(&self, name: &str, value: ExportValue) -> Result<Changes, String>;

    /// The value of the variable `name` as a client may open it: a table as a table,
    /// anything else as an object. `None` if there is no such variable or it holds the
    /// language's null.
    ///
    /// Behavior: `AbstractScriptSession#getVariable` and `ScriptSession#unwrapObject`
    fn get(&self, name: &str) -> Option<ExportValue>;

    /// The names of the variables, sorted.
    ///
    /// Behavior: `AbstractScriptSession#getVariableNames`
    fn names(&self) -> Vec<String>;

    /// See [`ScopeHolder::formula_values`].
    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String>;
}

/// A script session with the place the changes of its variables are published to.
pub struct Console {
    session: Arc<dyn ScriptSession>,
    applications: Arc<Applications>,
}

impl Console {
    pub fn new(session: Arc<dyn ScriptSession>, applications: Arc<Applications>) -> Arc<Self> {
        Arc::new(Self {
            session,
            applications,
        })
    }

    /// The console type clients are told and start a console with.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#getConsoleTypes`
    pub fn console_type(&self) -> String {
        self.session.script_type().to_lowercase()
    }

    /// Whether `session_type` of a `StartConsole` request names this console.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#startConsole`
    pub fn is_type(&self, session_type: &str) -> bool {
        // Java's equalsIgnoreCase compares UTF-16 units; for the ASCII names of the script
        // types that is this
        self.session
            .script_type()
            .eq_ignore_ascii_case(session_type)
    }

    /// Runs `code`. What changed before it, outside any command, is published first, so
    /// that the answer holds what the code changed; that is published too.
    ///
    /// Behavior: `AbstractScriptSession#evaluateScript`
    pub fn evaluate(&self, code: &str) -> Changes {
        self.observe();
        let changes = self.session.evaluate(code);
        self.publish(&changes);
        changes
    }

    /// Publishes what changed in the variables since they were last looked at: what a
    /// thread, a listener or anything else outside a command did.
    ///
    /// Behavior: `ScriptSession#observeScopeChanges`
    pub fn observe(&self) {
        self.publish(&self.session.observe());
    }

    /// Looks at the variables every `interval`, on the runtime this is called in, for as
    /// long as the console exists.
    ///
    /// Behavior: `DeephavenApiServer#checkScopeChanges`
    pub fn watch(self: &Arc<Self>, interval: Duration) {
        let console = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(console) = console.upgrade() else {
                    break;
                };
                // the session may have to wait for a running command
                if tokio::task::spawn_blocking(move || console.observe())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
    }

    /// Behavior: `ConsoleServiceGrpcImpl#bindTableToVariable`
    pub fn bind_table(&self, name: &str, table: Arc<dyn Table>) -> Result<(), String> {
        self.put(name, ExportValue::Table(table))
    }

    /// Tells the clients that list fields.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl#onScopeChanges`
    fn publish(&self, changes: &Changes) {
        for variable in &changes.removed {
            self.applications.scope_changed(&variable.name, None);
        }
        for variable in changes.updated.iter().chain(&changes.created) {
            self.applications
                .scope_changed(&variable.name, Some(&variable.type_name));
        }
    }
}

/// The variables of the script session are the query scope of the server.
impl ScopeHolder for Console {
    fn get(&self, name: &str) -> Option<ExportValue> {
        self.session.get(name)
    }

    fn put(&self, name: &str, value: ExportValue) -> Result<(), String> {
        let changes = self.session.put(name, value)?;
        self.publish(&changes);
        Ok(())
    }

    fn names(&self) -> Vec<String> {
        self.session.names()
    }

    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String> {
        self.session.formula_values(names)
    }
}
