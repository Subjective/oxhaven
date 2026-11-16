//! The state of an application for the `deephaven.appmode` module: what
//! `get_app_state()` wraps, with the methods the reference module calls on Java's
//! `ApplicationState` through jpy.
//!
//! A value goes into a field as it goes into an export from a variable of the session: a
//! table of the package as the engine's table, a partitioned table as that, anything else
//! as the Python object. It comes back as a variable set by a client would.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{export_of, PythonSession};
use crate::app::{script, Application, ApplicationError};

/// One application, as Python holds it. `ApplicationState.j_app_state` is one.
///
/// Behavior: `io.deephaven.server.appmode.ScriptApplicationState`
#[pyclass(frozen, module = "_oxhaven")]
struct AppState {
    application: Application,
}

/// The exception Python sees for a refused field, with the text of Java's.
///
/// Behavior: `io.deephaven.appmode.StandardField#checkName`, `#checkDescription`
fn refused(error: ApplicationError) -> PyErr {
    let text = match error {
        ApplicationError::InvalidFieldName(_) => {
            "java.lang.IllegalArgumentException: name() is invalid, must be printable ascii"
                .to_string()
        }
        ApplicationError::EmptyDescription => {
            "java.lang.IllegalArgumentException: description(), when present, must not be empty"
                .to_string()
        }
        other => format!("java.lang.IllegalArgumentException: {other}"),
    };
    PyRuntimeError::new_err(text)
}

#[pymethods]
impl AppState {
    /// Behavior: `io.deephaven.appmode.ApplicationState#id`
    fn id(&self) -> &str {
        self.application.id()
    }

    /// Behavior: `ApplicationState#name`
    fn name(&self) -> String {
        self.application.name()
    }

    /// Sets the field `name`, replacing the one there is. `None` is no value.
    ///
    /// Behavior: `ScriptApplicationState#setField`, `StandardField#of`, whose builder
    /// refuses a null value with this text
    #[pyo3(name = "setField", signature = (name, value, description=None))]
    fn set_field(
        &self,
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
        description: Option<&str>,
    ) -> PyResult<()> {
        let unwrapped = py
            .import("_oxhaven_bootstrap")?
            .call_method1("unwrap", (value,))?;
        let value = export_of(&unwrapped)
            .ok_or_else(|| PyRuntimeError::new_err("java.lang.NullPointerException: value"))?;
        match description {
            Some(description) => self
                .application
                .set_field_described(name, value, description),
            None => self.application.set_field(name, value),
        }
        .map_err(refused)
    }

    /// The value of the field `name` as Python code uses it, in a tuple; `None` if there
    /// is no such field.
    ///
    /// Behavior: `ApplicationState#getField`, and `deephaven._wrapper.pythonify` of the
    /// field's value
    #[pyo3(name = "getField")]
    fn get_field<'py>(
        &self,
        py: Python<'py>,
        name: &str,
    ) -> PyResult<Option<(Bound<'py, PyAny>,)>> {
        let Some(value) = self.application.field(name) else {
            return Ok(None);
        };
        Ok(Some((PythonSession::wrap(py, value)?,)))
    }

    /// Removes the field `name` if there is one, and says nothing: Java's method is void.
    ///
    /// Behavior: `ApplicationState#removeField`
    #[pyo3(name = "removeField")]
    fn remove_field(&self, name: &str) {
        self.application.remove_field(name);
    }

    /// The names of the fields, sorted. Java lists the fields in the order of a hash
    /// table.
    ///
    /// Behavior: `ApplicationState#listFields`
    #[pyo3(name = "listFields")]
    fn list_fields(&self) -> Vec<String> {
        self.application.field_names()
    }

    /// Behavior: `ApplicationState#numFieldsExported`
    #[pyo3(name = "numFieldsExported")]
    fn num_fields_exported(&self) -> usize {
        self.application.field_names().len()
    }
}

/// The application whose script runs on this thread.
///
/// Behavior: `io.deephaven.appmode.ApplicationContext#get`, with the text of its exception
#[pyfunction]
fn app_state() -> PyResult<AppState> {
    let application = script::current().ok_or_else(|| {
        PyRuntimeError::new_err(
            "java.lang.IllegalStateException: Should not be getting application state \
             outside runUnderContext",
        )
    })?;
    Ok(AppState { application })
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<AppState>()?;
    module.add_function(wrap_pyfunction!(app_state, module)?)
}
