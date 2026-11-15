//! A script session: something that runs code in a language and has variables, and the
//! [`Console`] that publishes its variables to clients.
//!
//! The session knows the language. Everything else is here and is the same for every
//! language: which variable changes clients are told, the scope tickets `s/<name>`, and the
//! fields that `ApplicationService/ListFields` lists under the application id `scope`.

use std::sync::Arc;

use oxhaven_contracts::table::Table;

use crate::app::{Applications, Scope};
use crate::session::ExportValue;

/// A variable that clients can open, as a script session reports it.
#[derive(Clone, Debug)]
pub struct Variable {
    pub name: String,
    /// What kind of thing the value is, as clients are told: `Table` for a table.
    ///
    /// Behavior: `io.deephaven.engine.util.AbstractScriptSession#getTypeNameIfDisplayable`
    pub type_name: String,
    /// The value, for a variable that was created or updated.
    pub value: Option<ExportValue>,
}

/// What running a piece of code changed. Only variables whose values clients can open
/// count: a variable that holds a number is never mentioned.
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
}

/// The language side of a console.
///
/// Behavior: `io.deephaven.engine.util.ScriptSession`
pub trait ScriptSession: Send + Sync {
    /// The name of the language, as Java spells it: `Python`.
    ///
    /// Behavior: `ScriptSession#scriptType`
    fn script_type(&self) -> &str;

    /// Runs `code` and reports what it changed. An error in the code is not an error of
    /// this call: it is `Changes::error`.
    ///
    /// Behavior: `ScriptSession#evaluateScript(String)`
    fn evaluate(&self, code: &str) -> Changes;

    /// Makes `table` the value of the variable `name` and reports the change.
    ///
    /// Behavior: `io.deephaven.engine.context.QueryScope#putParam` on the scope of a
    /// script session
    fn put_table(&self, name: &str, table: Arc<dyn Table>) -> Result<Changes, String>;
}

/// A script session with the places its variables are published to.
pub struct Console {
    session: Arc<dyn ScriptSession>,
    scope: Arc<Scope>,
    applications: Arc<Applications>,
}

impl Console {
    pub fn new(
        session: Arc<dyn ScriptSession>,
        scope: Arc<Scope>,
        applications: Arc<Applications>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session,
            scope,
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

    /// Runs `code`, then publishes what it changed.
    ///
    /// Behavior: `AbstractScriptSession#evaluateScript`
    pub fn evaluate(&self, code: &str) -> Changes {
        let changes = self.session.evaluate(code);
        self.publish(&changes);
        changes
    }

    /// Behavior: `ConsoleServiceGrpcImpl#bindTableToVariable`
    pub fn bind_table(&self, name: &str, table: Arc<dyn Table>) -> Result<(), String> {
        let changes = self.session.put_table(name, table)?;
        self.publish(&changes);
        Ok(())
    }

    /// Makes scope tickets resolve to the new values and tells the clients that list
    /// fields.
    ///
    /// Behavior: `ApplicationServiceGrpcImpl#onScopeChanges`
    fn publish(&self, changes: &Changes) {
        for variable in &changes.removed {
            self.scope.remove(&variable.name);
            self.applications.scope_changed(&variable.name, None);
        }
        for variable in changes.updated.iter().chain(&changes.created) {
            if let Some(value) = &variable.value {
                self.scope.put(variable.name.clone(), value.clone());
            }
            self.applications
                .scope_changed(&variable.name, Some(&variable.type_name));
        }
    }
}
