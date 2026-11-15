//! The Python script session: CPython embedded in the server process.
//!
//! User code runs in an interpreter inside the server, as it does in the Java server
//! through jpy. The `deephaven` package it imports is this crate's own (`python/deephaven`,
//! compiled into the binary): its `Table` holds a [`TableHandle`], a reference to an engine
//! table, and every operation on it goes through [`table_op`] to the handler the table
//! service has for that operation. `docs/CONSOLE.md` gives the reasons and the limits.
//!
//! One interpreter means one session: a process has one set of imported modules, and the
//! `deephaven` package in them reaches the engine through the [`Runtime`] of the session
//! that was created last. A server creates one.
//!
//! Locks are taken in the order update graph, then interpreter lock. Code runs under the
//! graph's shared lock, so no update cycle runs while a command does (Java holds the
//! exclusive lock, which also keeps other clients' operations out). The interpreter lock is
//! released while an engine operation runs.

#[cfg(test)]
mod tests;

use std::ffi::CString;
use std::sync::{Arc, OnceLock, PoisonError, RwLock};

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use oxhaven_column::FlatSource;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table};
use oxhaven_engine::table::{QueryTable, TableSpec};
use oxhaven_proto::grpc::batch_table_request::operation::Op;
use oxhaven_proto::grpc::batch_table_request::Operation;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::logs::LogBuffer;
use super::session::{Changes, Console, ScriptSession, Variable};
use crate::schema::java_type_name;
use crate::session::{ExportError, ExportValue};
use crate::table::run_local;
use crate::table::support::OpContext;
use crate::ServerState;

/// The modules of the `deephaven` package, compiled into the binary: name, whether it is a
/// package, source. `build.rs` lists every file under `python/deephaven`.
const MODULES: &[(&str, bool, &str)] = include!(concat!(env!("OUT_DIR"), "/python_modules.rs"));

const BOOTSTRAP: &str = include_str!("../../../python/_oxhaven_bootstrap.py");

/// What the Python package reaches the server through.
struct Runtime {
    context: OpContext,
    logs: Arc<LogBuffer>,
}

/// The runtime of the session created last; see the [module documentation](self).
static RUNTIME: RwLock<Option<Arc<Runtime>>> = RwLock::new(None);

fn runtime() -> PyResult<Arc<Runtime>> {
    RUNTIME
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .ok_or_else(|| PyRuntimeError::new_err("the server has no Python session"))
}

/// A reference to an engine table, as Python holds it. The Python `Table` class wraps one.
#[pyclass(frozen, module = "_oxhaven")]
struct TableHandle {
    table: Arc<dyn Table>,
}

#[pymethods]
impl TableHandle {
    /// The number of rows now.
    fn size(&self) -> u64 {
        self.table.row_set().size()
    }

    fn is_refreshing(&self) -> bool {
        self.table.is_refreshing()
    }

    /// The columns as `(name, Java type name)`, the type as the schema on the wire has it.
    fn columns(&self) -> Vec<(String, String)> {
        let columns = self.table.definition().columns().iter();
        columns
            .map(|column| {
                let type_name = java_type_name(&column.column_type).into_owned();
                (column.name.clone(), type_name)
            })
            .collect()
    }

    /// Whether `other` refers to the same table, not an equal one.
    fn is_same(&self, other: &TableHandle) -> bool {
        Arc::ptr_eq(&self.table, &other.table)
    }
}

/// The message type of one table operation.
fn operation_descriptor() -> &'static MessageDescriptor {
    static DESCRIPTOR: OnceLock<MessageDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        DescriptorPool::decode(oxhaven_proto::FILE_DESCRIPTOR_SET)
            .expect("the descriptors the stubs were generated from decode")
            .get_message_by_name("io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation")
            .expect("the table service has batch operations")
    })
}

/// Reads a `BatchTableRequest.Operation` from its protobuf JSON form.
fn parse_operation(json: &str) -> Result<Op, String> {
    let mut deserializer = serde_json::Deserializer::from_str(json);
    let message = DynamicMessage::deserialize(operation_descriptor().clone(), &mut deserializer)
        .map_err(|e| format!("not a table operation: {e}"))?;
    deserializer
        .end()
        .map_err(|e| format!("not a table operation: {e}"))?;
    let operation: Operation = message
        .transcode_to()
        .map_err(|e| format!("not a table operation: {e}"))?;
    operation
        .op
        .ok_or_else(|| "a table operation must name one operation".to_string())
}

/// The text Python is shown for a failed operation: the engine's own text where a gRPC
/// client would be given an error id.
fn error_text(error: ExportError) -> String {
    match error {
        ExportError::Status(status) => {
            format!("{:?}: {}", status.code(), status.message())
        }
        ExportError::Internal(text) => text,
    }
}

/// Runs one table service operation, given in protobuf JSON form, on `sources`, which the
/// operation's table references name by position.
#[pyfunction]
fn table_op(
    py: Python<'_>,
    json: &str,
    sources: Vec<PyRef<'_, TableHandle>>,
) -> PyResult<TableHandle> {
    let op = parse_operation(json).map_err(PyValueError::new_err)?;
    let tables: Vec<Arc<dyn Table>> = sources.iter().map(|handle| handle.table.clone()).collect();
    let runtime = runtime()?;
    py.detach(|| run_local(&runtime.context, op, &tables))
        .map(|table| TableHandle { table })
        .map_err(|error| PyRuntimeError::new_err(error_text(error)))
}

/// The values of one input column as an Arrow array; `None` is null.
fn input_column(type_name: &str, values: &Bound<'_, PyAny>) -> PyResult<(ColumnType, ArrayRef)> {
    fn items<'py, T>(values: &Bound<'py, PyAny>) -> PyResult<Vec<Option<T>>>
    where
        T: for<'a> FromPyObject<'a, 'py>,
    {
        values.extract()
    }
    Ok(match type_name {
        "bool" => (
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(items::<bool>(values)?)),
        ),
        "int" => (
            ColumnType::Int,
            Arc::new(Int32Array::from(items::<i32>(values)?)),
        ),
        "long" => (
            ColumnType::Long,
            Arc::new(Int64Array::from(items::<i64>(values)?)),
        ),
        "double" => (
            ColumnType::Double,
            Arc::new(Float64Array::from(items::<f64>(values)?)),
        ),
        "string" => (
            ColumnType::String,
            Arc::new(StringArray::from(items::<String>(values)?)),
        ),
        other => {
            return Err(PyValueError::new_err(format!(
                "input columns of type {other} are not supported yet"
            )))
        }
    })
}

/// A static table from `(name, type, values)` triples.
///
/// Behavior: `io.deephaven.engine.util.TableTools#newTable(ColumnHolder...)`
#[pyfunction]
fn new_table(columns: Vec<(String, String, Bound<'_, PyAny>)>) -> PyResult<TableHandle> {
    let mut size = None;
    let mut definitions = Vec::with_capacity(columns.len());
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(columns.len());
    for (name, type_name, values) in &columns {
        let (column_type, array) = input_column(type_name, values)?;
        if *size.get_or_insert(array.len()) != array.len() {
            return Err(PyValueError::new_err(format!(
                "column {name} has {} rows, the columns before it have {}",
                array.len(),
                size.unwrap_or_default()
            )));
        }
        definitions.push(ColumnDefinition::new(name.clone(), column_type.clone()));
        let source = FlatSource::new(column_type, array)
            .map_err(|failure| PyRuntimeError::new_err(failure.message))?;
        sources.push(Arc::new(source));
    }
    let definition =
        TableDefinition::new(definitions).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let row_set = RowSet::flat(size.unwrap_or_default() as u64);
    let table = QueryTable::new_static(TableSpec::new(definition, row_set, sources));
    Ok(TableHandle { table })
}

/// Adds a record to the server's log buffer: what Python wrote to a standard stream.
#[pyfunction]
fn log(level: &str, text: &str) -> PyResult<()> {
    runtime()?.logs.record(level, text);
    Ok(())
}

/// Registers the module `_oxhaven`, makes the embedded modules importable and redirects
/// the standard streams, unless a session of this process has done so before.
fn bootstrap(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let modules = py.import("sys")?.getattr("modules")?;
    if !modules.contains("_oxhaven_bootstrap")? {
        let native = PyModule::new(py, "_oxhaven")?;
        native.add_class::<TableHandle>()?;
        native.add_function(wrap_pyfunction!(table_op, &native)?)?;
        native.add_function(wrap_pyfunction!(new_table, &native)?)?;
        native.add_function(wrap_pyfunction!(log, &native)?)?;
        modules.set_item("_oxhaven", &native)?;

        let code = CString::new(BOOTSTRAP).expect("the bootstrap source has no NUL");
        let helpers = PyModule::from_code(
            py,
            &code,
            c"<oxhaven>/_oxhaven_bootstrap.py",
            c"_oxhaven_bootstrap",
        )?;
        let sources = PyDict::new(py);
        for (name, is_package, source) in MODULES {
            sources.set_item(name, (source, is_package))?;
        }
        helpers.call_method1("install", (sources,))?;
        helpers.call_method0("redirect_output")?;
        modules.set_item("_oxhaven_bootstrap", &helpers)?;
    }
    py.import("_oxhaven_bootstrap")
}

/// The Python script session of a server.
///
/// Behavior: `io.deephaven.integrations.python.PythonDeephavenSession`
pub struct PythonSession {
    runtime: Arc<Runtime>,
    /// The module `_oxhaven_bootstrap`.
    helpers: Py<PyModule>,
    /// The global variables of the session.
    globals: Py<PyDict>,
}

impl PythonSession {
    /// Starts the interpreter if this process has none yet and creates a session with no
    /// variables. From now on the `deephaven` package runs its operations in `context`.
    pub fn new(context: OpContext, logs: Arc<LogBuffer>) -> Result<Self, String> {
        let runtime = Arc::new(Runtime { context, logs });
        *RUNTIME.write().unwrap_or_else(PoisonError::into_inner) = Some(runtime.clone());
        Python::attach(|py| {
            let helpers = bootstrap(py)?;
            let globals = helpers.call_method0("new_globals")?.cast_into::<PyDict>()?;
            Ok(Self {
                runtime,
                helpers: helpers.unbind(),
                globals: globals.unbind(),
            })
        })
        .map_err(|error: PyErr| format!("the Python session could not start: {error}"))
    }

    /// What changed between two snapshots of the globals.
    fn changes(
        &self,
        py: Python<'_>,
        before: &Bound<'_, PyDict>,
        after: &Bound<'_, PyDict>,
    ) -> PyResult<Changes> {
        type Handles<'py> = Vec<(String, PyRef<'py, TableHandle>)>;
        let (created, updated, removed): (Handles, Handles, Handles) = self
            .helpers
            .bind(py)
            .call_method1("changes", (before, after))?
            .extract()?;
        let variables = |handles: Handles, keep_value: bool| {
            handles
                .into_iter()
                .map(|(name, handle)| Variable {
                    name,
                    // Behavior: `AbstractScriptSession#getTypeNameIfDisplayable`
                    type_name: "Table".to_string(),
                    value: keep_value.then(|| ExportValue::Table(handle.table.clone())),
                })
                .collect()
        };
        Ok(Changes {
            created: variables(created, true),
            updated: variables(updated, true),
            removed: variables(removed, false),
            error: None,
        })
    }

    fn run(&self, py: Python<'_>, code: &str) -> PyResult<Changes> {
        let globals = self.globals.bind(py);
        let before = globals.copy()?;
        let error: Option<String> = self
            .helpers
            .bind(py)
            .call_method1("run", (code, globals))?
            .extract()?;
        let mut changes = self.changes(py, &before, &globals.copy()?)?;
        // Java: "java.lang.RuntimeException: Error in Python interpreter:", the same
        // traceback, and the stack of the JVM
        changes.error = error.map(|trace| format!("Error in Python interpreter:\n{trace}"));
        Ok(changes)
    }
}

impl ScriptSession for PythonSession {
    /// Behavior: `PythonDeephavenSession#SCRIPT_TYPE`
    fn script_type(&self) -> &str {
        "Python"
    }

    /// Behavior: `PythonDeephavenSession#evaluate`, `AbstractScriptSession#evaluateScript`
    fn evaluate(&self, code: &str) -> Changes {
        tracing::info!("Evaluating command: {code}");
        locked(self.runtime.context.graph.as_ref(), || {
            Python::attach(|py| self.run(py, code)).unwrap_or_else(|error| Changes {
                error: Some(format!("the Python session failed: {error}")),
                ..Default::default()
            })
        })
    }

    /// Behavior: `PythonDeephavenSession#setVariable`
    fn put_table(&self, name: &str, table: Arc<dyn Table>) -> Result<Changes, String> {
        Python::attach(|py| {
            let globals = self.globals.bind(py);
            let before = globals.copy()?;
            let handle = Py::new(py, TableHandle { table })?;
            let wrapped = py
                .import("deephaven.table")?
                .getattr("Table")?
                .call1((handle,))?;
            globals.set_item(name, wrapped)?;
            self.changes(py, &before, &globals.copy()?)
        })
        .map_err(|error: PyErr| format!("the variable {name} could not be set: {error}"))
    }
}

/// Gives `state` a Python console: starts the interpreter and makes a new Python session
/// the script session of the server.
pub fn start(state: &mut ServerState) -> Result<(), String> {
    let session = PythonSession::new(state.op_context(), state.logs.clone())?;
    state.console = Some(Console::new(
        Arc::new(session),
        state.scope.clone(),
        state.applications.clone(),
    ));
    Ok(())
}

/// The version of the interpreter the server embeds, as `platform.python_version()` gives
/// it.
pub fn python_version() -> Result<String, String> {
    Python::attach(|py| {
        py.import("platform")?
            .call_method0("python_version")?
            .extract()
    })
    .map_err(|error: PyErr| error.to_string())
}
