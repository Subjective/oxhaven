//! Object types that Python plugins register, and the registration when the server
//! starts.
//!
//! A plugin's `ObjectType` is wrapped in Python (`deephaven_internal.plugin.object`) and
//! then here ([`PythonObjectType`]), where it becomes one of the server's object types
//! (`crate::object::types`). The server calls it to ask whether a Python object is of the
//! type, and to open such an object for a client.
//!
//! **Locks.** Every call into a plugin takes the interpreter lock and no other: not the
//! update graph's, as in Java, where a plugin's `to_bytes`, `on_data` and `on_close` were
//! observed to hold neither side of it. A table operation that the plugin calls takes the
//! graph's lock as it does for any Python thread: after it has released the interpreter
//! lock (`table_op`), which keeps the order of `docs/CONSOLE.md`, graph lock first. For
//! the same reason the two directions out of Python, a message to the client and the
//! registration of a type, release the interpreter lock before they touch the server.
//!
//! Behavior: `io.deephaven.server.plugin.python.ObjectTypeAdapter`,
//! `io.deephaven.server.plugin.python.PythonPluginsRegistration`

use std::sync::{Arc, OnceLock};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use super::{export_of, PythonObject, PythonSession};
use crate::object::types::{registry, send_once, MessageStream, ObjectType};
use crate::session::{ExportError, ExportValue};

/// An exception of plugin code, as the server reports it: logged under an error id, which
/// is all the client is told.
///
/// Behavior: the `RuntimeException` jpy makes of a Python exception, "Error in Python
/// interpreter:" and the traceback
fn interpreter_error(py: Python<'_>, error: PyErr) -> ExportError {
    let formatted = py
        .import("traceback")
        .and_then(|traceback| traceback.call_method1("format_exception", (error.value(py),)))
        .and_then(|lines| lines.extract::<Vec<String>>());
    let trace = match formatted {
        Ok(lines) => lines.concat(),
        Err(_) => error.to_string(),
    };
    ExportError::Internal(format!("Error in Python interpreter:\n{trace}"))
}

/// An error of the server, as plugin code sees it: a `RuntimeError`, as jpy raises one for
/// a Java exception.
fn server_error(error: ExportError) -> PyErr {
    PyRuntimeError::new_err(match error {
        ExportError::Status(status) => status.message().to_string(),
        ExportError::Internal(text) => text,
    })
}

/// What a plugin refers a client to, as the value the server exports: a table for the
/// package's `Table`, and any other object as itself.
///
/// Java exports `None` as a null export, which a client cannot use; that is refused here.
///
/// Behavior: `deephaven._wrapper.javaify` in the reference's `ExporterAdapter#reference`
/// and `ClientResponseStreamAdapter#on_data`
fn reference_of(reference: &Bound<'_, PyAny>) -> PyResult<ExportValue> {
    let unwrapped = reference
        .py()
        .import("_oxhaven_bootstrap")?
        .call_method1("unwrap", (reference,))?;
    export_of(&unwrapped).ok_or_else(|| {
        PyRuntimeError::new_err(
            "not ported: io.deephaven.server.session.SessionState#newServerSideExport of null \
             (a reference to None)",
        )
    })
}

/// The stream a plugin sends to a client with. `deephaven_internal.plugin.object` wraps it
/// in the `MessageStream` the plugin is handed.
#[pyclass(frozen, module = "_oxhaven")]
struct ObjectConnection {
    connection: Arc<dyn MessageStream>,
}

#[pymethods]
impl ObjectConnection {
    /// Fails with the text of Java's exception when the stream has ended.
    fn on_data(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyBytes>,
        references: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let payload = payload.as_bytes().to_vec();
        let references = references
            .iter()
            .map(reference_of)
            .collect::<PyResult<Vec<_>>>()?;
        let connection = self.connection.clone();
        py.detach(move || connection.on_data(payload, references))
            .map_err(server_error)
    }

    fn on_close(&self, py: Python<'_>) {
        let connection = self.connection.clone();
        py.detach(move || connection.on_close());
    }
}

/// The stream of a plugin that takes the messages of one client.
///
/// Behavior: `ObjectTypeAdapter.PythonServerMessageStream`
struct PythonStream {
    instance: Py<PyAny>,
}

impl MessageStream for PythonStream {
    /// The plugin is handed the bytes and, for each reference of the client, the Python
    /// value of the export: the package's `Table` for a table, the object itself for a
    /// Python object.
    fn on_data(&self, payload: Vec<u8>, references: Vec<ExportValue>) -> Result<(), ExportError> {
        Python::attach(|py| {
            let call = || {
                let references = references
                    .into_iter()
                    .map(|reference| PythonSession::wrap(py, reference))
                    .collect::<PyResult<Vec<_>>>()?;
                let arguments = (PyBytes::new(py, &payload), PyList::new(py, references)?);
                self.instance.bind(py).call_method1("on_data", arguments)
            };
            call()
                .map(|_| ())
                .map_err(|error| interpreter_error(py, error))
        })
    }

    /// An exception of the plugin is ignored, as in Java
    /// (`SendMessageObserver#closeMessageStream`).
    fn on_close(&self) {
        Python::attach(|py| {
            let _ = self.instance.bind(py).call_method0("on_close");
        });
    }
}

/// An object type of a plugin.
struct PythonObjectType {
    name: String,
    /// The `ObjectTypeAdapter` of `deephaven_internal.plugin.object`.
    adapter: Py<PyAny>,
}

impl ObjectType for PythonObjectType {
    fn name(&self) -> &str {
        &self.name
    }

    /// Only a Python object can be of a plugin's type. What the plugin answers counts as
    /// Python's truth of it, but `None` fails, as does an exception.
    ///
    /// Behavior: `ObjectTypeAdapter#isType`, whose conversion of the answer to a `boolean`
    /// fails for `None` with this `NullPointerException`
    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError> {
        let Some(object) = value.downcast_ref::<PythonObject>() else {
            return Ok(false);
        };
        Python::attach(|py| {
            let answer = self
                .adapter
                .bind(py)
                .call_method1("is_type", (object.0.bind(py),))
                .map_err(|error| interpreter_error(py, error))?;
            if answer.is_none() {
                return Err(ExportError::Internal(
                    "java.lang.NullPointerException: Cannot invoke \
                     \"java.lang.Boolean.booleanValue()\" because the return value of \
                     \"org.jpy.PyObject.call(java.lang.Class, String, java.lang.Class, Object)\" \
                     is null"
                        .to_string(),
                ));
            }
            answer
                .is_truthy()
                .map_err(|error| interpreter_error(py, error))
        })
    }

    /// Behavior: `ObjectServiceGrpcImpl#shouldTransformReferences`, which asks the plugin
    /// for its authorization behaviour and fails on one that is none of the three, then
    /// `ObjectTypeAdapter#compatibleClientConnection`
    fn connect(
        &self,
        value: &ExportValue,
        connection: Arc<dyn MessageStream>,
    ) -> Result<Arc<dyn MessageStream>, ExportError> {
        let object = value
            .downcast_ref::<PythonObject>()
            .ok_or_else(|| ExportError::Internal("not a Python object".to_string()))?;
        Python::attach(|py| {
            let adapter = self.adapter.bind(py);
            let object = object.0.bind(py);
            let failed = |error| interpreter_error(py, error);
            adapter
                .call_method0("authorization_export_behavior")
                .map_err(failed)?;
            let fetch_only = adapter
                .call_method0("is_fetch_only")
                .and_then(|answer| answer.is_truthy())
                .map_err(failed)?;
            if fetch_only {
                let message = || {
                    let (payload, references): (Bound<'_, PyBytes>, Vec<Bound<'_, PyAny>>) =
                        adapter.call_method1("to_bytes", (object,))?.extract()?;
                    let references = references
                        .iter()
                        .map(reference_of)
                        .collect::<PyResult<Vec<_>>>()?;
                    Ok((payload.as_bytes().to_vec(), references))
                };
                let (payload, references) = message().map_err(failed)?;
                py.detach(|| send_once(connection.as_ref(), payload, references))
            } else {
                let opened = || {
                    let connection = Py::new(py, ObjectConnection { connection })?;
                    adapter.call_method1("create_client_connection", (object, connection))
                };
                let instance = opened().map_err(failed)?.unbind();
                Ok(Arc::new(PythonStream { instance }) as Arc<dyn MessageStream>)
            }
        })
    }
}

/// Registers an object type of a plugin under `name`. Fails with the text of Java's
/// exception for a name that cannot be registered.
///
/// Behavior: `io.deephaven.server.plugin.python.CallbackAdapter#registerObjectType`
#[pyfunction]
fn register_object_type(py: Python<'_>, name: String, adapter: Py<PyAny>) -> PyResult<()> {
    let description = adapter.bind(py).str()?.to_string();
    let object_type = Arc::new(PythonObjectType {
        name: name.clone(),
        adapter,
    });
    py.detach(|| registry().register(object_type))
        .map_err(|text| {
            PyRuntimeError::new_err(format!("java.lang.IllegalArgumentException: {text}"))
        })?;
    tracing::info!("Registering object type: {name} / {description}");
    Ok(())
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<ObjectConnection>()?;
    native.add_function(wrap_pyfunction!(register_object_type, native)?)?;
    Ok(())
}

/// Registers the plugins of the environment, once per process: the object types live in
/// the interpreter, and a process has one. An `Err` is the text of what a registration
/// raised, and the server then does not start, as the Java server does not; a later call
/// answers the same.
///
/// Behavior: `PythonPluginsRegistration#registerInto`, which
/// `io.deephaven.server.runner.DeephavenApiServer#run` calls before it accepts clients
pub(super) fn register_plugins() -> Result<(), String> {
    static REGISTERED: OnceLock<Result<(), String>> = OnceLock::new();
    REGISTERED
        .get_or_init(|| {
            Python::attach(|py| {
                py.import("deephaven_internal.plugin")
                    .and_then(|module| module.call_method0("initialize_all_and_register_into"))
                    .map(|_| ())
                    .map_err(|error| match interpreter_error(py, error) {
                        ExportError::Internal(text) => {
                            format!("the plugins could not be registered: {text}")
                        }
                        ExportError::Status(status) => status.message().to_string(),
                    })
            })
        })
        .clone()
}
