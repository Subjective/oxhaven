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
//! released while an engine operation runs. A command that writes to an input table and
//! waits for the write gives the graph's lock up for that wait ([`input`]), as Java's does.
//!
//! A table listener written in Python runs in the update cycle, under the same order
//! ([`listener`], which also has the graph lock as scripts hold it).
//!
//! Commands run on one thread, the interpreter's main thread, where a command whose
//! client gave up on it can be stopped ([`interrupt`]). Everything else that touches the
//! interpreter does so from the thread it is called on; completion (`completer`) does so
//! while a command runs, without the graph's lock.

mod appmode;
#[cfg(test)]
mod appmode_tests;
mod checks;
#[cfg(test)]
mod column_tests;
mod completer;
#[cfg(test)]
mod completer_tests;
mod csv;
mod data;
pub mod env;
mod hierarchical;
mod input;
pub mod interrupt;
mod listener;
mod ops;
mod parquet;
mod partitioned;
mod plugin;
#[cfg(test)]
mod plugin_tests;
#[cfg(test)]
mod tests;
mod udf;

use std::borrow::Cow;
use std::cell::RefCell;
use std::ffi::CString;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock};

use arrow::array::{
    new_empty_array, Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, StringArray, TimestampNanosecondArray,
    UInt16Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::concat;
use arrow::datatypes::DataType;
use oxhaven_column::FlatSource;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table};
use oxhaven_engine::graph::yielding_to_cycles;
use oxhaven_engine::table::{QueryTable, TableSpec};
use oxhaven_proto::grpc::batch_table_request::operation::Op;
use oxhaven_proto::grpc::batch_table_request::Operation;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};

use super::completer::Completer;
use super::logs::LogBuffer;
use super::session::{Changes, Console, ScriptSession, Stop, CHECK_SCOPE_CHANGES_INTERVAL};
use crate::app::{script, Application, FormulaValue};
use crate::schema::{column_type_for_java_names, java_component_type_name, java_type_name};
use crate::session::{ExportError, ExportValue};
use crate::table::run_local;
use crate::table::support::OpContext;
use crate::ServerState;

/// The modules of the `deephaven` and `deephaven_internal` packages, compiled into the binary:
/// name, whether it is a package, source. `build.rs` lists every file under `python/deephaven`
/// and `python/deephaven_internal`.
const MODULES: &[(&str, bool, &str)] = include!(concat!(env!("OUT_DIR"), "/python_modules.rs"));

const BOOTSTRAP: &str = include_str!("../../../python/_oxhaven_bootstrap.py");

/// What the Python package reaches the server through.
struct Runtime {
    context: OpContext,
    logs: Arc<LogBuffer>,
    /// The global variables of the session, once it has them.
    globals: OnceLock<Py<PyDict>>,
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

    /// How many holders the engine table has, this handle among them: a client's export,
    /// a table derived from it and a listener of it are each one. A table with one holder
    /// ends when this handle does.
    fn holders(&self) -> usize {
        Arc::strong_count(&self.table)
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

thread_local! {
    /// The variables a formula compiled on this thread may name, while a table operation
    /// that Python code called runs on it: `None` for the globals of the session.
    ///
    /// Behavior: the scope stack of `io.deephaven.engine.util.PythonScopeJpyImpl`, which
    /// `PythonDeephavenSession#pushScope` and `#popScope` fill
    static CALLER_SCOPE: RefCell<Option<Py<PyDict>>> = const { RefCell::new(None) };
}

thread_local! {
    /// Whether the code that runs on this thread has no query scope in Java: the function
    /// of a partitioned table's transform.
    static NO_QUERY_SCOPE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Says whether the code that runs on this thread from now on has no query scope, and
/// returns what was said before. While it has none, no formula and no filter can be made.
///
/// Behavior: the `ExecutionContext` a `PartitionedTableImpl#transform` from a script
/// session applies its function in, which has no query scope: observed, every `where` with
/// a filter and every formula in the function fails with
/// `ExecutionContextRegistrationException`
#[pyfunction]
fn without_query_scope(without: bool) -> bool {
    NO_QUERY_SCOPE.replace(without)
}

const NO_QUERY_SCOPE_TEXT: &str = "io.deephaven.util.ExecutionContextRegistrationException: No \
     ExecutionContext registered, or current ExecutionContext has no QueryScope";

/// Fails if the code that runs on this thread has no query scope; see
/// [`without_query_scope`]. For the operations Java makes a filter or a formula for
/// without the server's formula compiler being asked here.
#[pyfunction]
fn require_query_scope() -> PyResult<()> {
    if NO_QUERY_SCOPE.get() {
        return Err(PyRuntimeError::new_err(NO_QUERY_SCOPE_TEXT));
    }
    Ok(())
}

/// Makes `scope` the caller scope of this thread until it is dropped.
struct CallerScope {
    before: Option<Py<PyDict>>,
}

impl CallerScope {
    fn enter(scope: Option<Py<PyDict>>) -> Self {
        Self {
            before: CALLER_SCOPE.replace(scope),
        }
    }
}

impl Drop for CallerScope {
    fn drop(&mut self) {
        CALLER_SCOPE.set(self.before.take());
    }
}

/// The variables a formula compiled now on this thread may name; see
/// [`PythonSession::current_scope`]. `None` before the first session.
fn current_scope(py: Python<'_>) -> Option<Bound<'_, PyDict>> {
    let caller =
        CALLER_SCOPE.with_borrow(|scope| scope.as_ref().map(|scope| scope.bind(py).clone()));
    caller.or_else(|| Some(runtime().ok()?.globals.get()?.bind(py).clone()))
}

/// Runs one table service operation, given in protobuf JSON form, on `sources`, which the
/// operation's table references name by position. A formula of the operation may name the
/// variables of the code that called the package.
#[pyfunction]
fn table_op(
    py: Python<'_>,
    json: &str,
    sources: Vec<PyRef<'_, TableHandle>>,
) -> PyResult<TableHandle> {
    let op = parse_operation(json).map_err(PyValueError::new_err)?;
    let tables: Vec<Arc<dyn Table>> = sources.iter().map(|handle| handle.table.clone()).collect();
    let runtime = runtime()?;
    let scope: Option<Py<PyDict>> = py
        .import("_oxhaven_bootstrap")?
        .call_method0("caller_scope")?
        .extract()?;
    let _scope = CallerScope::enter(scope);
    let _calls = udf::Operation::enter(py);
    py.detach(|| run_local(&runtime.context, op, &tables))
        .map(|table| TableHandle { table })
        .map_err(|error| PyRuntimeError::new_err(error_text(error)))
}

/// The cells of an input column of `column_type` as an Arrow array, from the Python values
/// that `deephaven.dtypes.array` made: `None` or the value Java reserves for null in the
/// type is a null (PARITY.md, "Null representation in the Java engine"), an instant is its
/// nanoseconds since the epoch, and a cell of an array column is a list.
fn input_values(column_type: &ColumnType, values: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    fn items<'py, T>(values: &Bound<'py, PyAny>) -> PyResult<Vec<Option<T>>>
    where
        T: for<'a> FromPyObject<'a, 'py>,
    {
        values.extract()
    }
    fn without<T: PartialEq>(values: Vec<Option<T>>, null: T) -> Vec<Option<T>> {
        let keep = |value: Option<T>| value.filter(|value| *value != null);
        values.into_iter().map(keep).collect()
    }
    Ok(match column_type {
        ColumnType::Boolean => Arc::new(BooleanArray::from(items::<bool>(values)?)),
        ColumnType::Byte => Arc::new(Int8Array::from(without(items(values)?, i8::MIN))),
        ColumnType::Char => Arc::new(UInt16Array::from(without(items(values)?, u16::MAX))),
        ColumnType::Short => Arc::new(Int16Array::from(without(items(values)?, i16::MIN))),
        ColumnType::Int => Arc::new(Int32Array::from(without(items(values)?, i32::MIN))),
        ColumnType::Long => Arc::new(Int64Array::from(without(items(values)?, i64::MIN))),
        ColumnType::Float => {
            // the values are doubles that hold floats: `deephaven.dtypes` has rounded them
            let floats = items::<f64>(values)?.into_iter();
            let floats = floats
                .map(|value| value.map(|value| value as f32))
                .collect();
            Arc::new(Float32Array::from(without(floats, -f32::MAX)))
        }
        ColumnType::Double => Arc::new(Float64Array::from(without(items(values)?, -f64::MAX))),
        ColumnType::String => Arc::new(StringArray::from(items::<String>(values)?)),
        ColumnType::Instant => {
            let nanos = without(items::<i64>(values)?, i64::MIN);
            Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("UTC"))
        }
        ColumnType::Array(element) => {
            let DataType::List(field) = column_type.arrow_type() else {
                unreachable!("an array column is an Arrow list");
            };
            let cells = items::<Bound<'_, PyAny>>(values)?;
            let mut lengths = Vec::with_capacity(cells.len());
            let mut valid = Vec::with_capacity(cells.len());
            let mut parts = Vec::with_capacity(cells.len());
            for cell in &cells {
                valid.push(cell.is_some());
                let part = match cell {
                    Some(cell) => input_values(element, cell)?,
                    None => new_empty_array(field.data_type()),
                };
                lengths.push(part.len());
                parts.push(part);
            }
            let parts: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
            let elements = if parts.is_empty() {
                new_empty_array(field.data_type())
            } else {
                concat(&parts).map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            };
            let list = ListArray::try_new(
                field,
                OffsetBuffer::from_lengths(lengths),
                elements,
                Some(NullBuffer::from(valid)),
            );
            Arc::new(list.map_err(|e| PyRuntimeError::new_err(e.to_string()))?)
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "not ported: io.deephaven.engine.util.TableTools#newTable(ColumnHolder...) \
                 for a column of type {other:?}"
            )))
        }
    })
}

/// A static table from `(name, Java class name of the type, values)` triples; the class
/// name is the canonical one (`int`, `java.lang.String`, `int[]`). Without rows when
/// there are no columns.
///
/// Behavior: `io.deephaven.engine.table.impl.util.TableFactory#newTable(Column...)`, which
/// refuses columns of different sizes with this text
#[pyfunction]
fn new_table(columns: Vec<(String, String, Bound<'_, PyAny>)>) -> PyResult<TableHandle> {
    let mut size = None;
    let mut definitions = Vec::with_capacity(columns.len());
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(columns.len());
    for (name, type_name, values) in &columns {
        let column_type = column_type_for_java_names(type_name, None).ok_or_else(|| {
            PyValueError::new_err(format!("the engine has no column type for {type_name}"))
        })?;
        let array = input_values(&column_type, values)?;
        if *size.get_or_insert(array.len()) != array.len() {
            return Err(PyRuntimeError::new_err(
                "java.lang.IllegalArgumentException: All columns must be the same size",
            ));
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

/// The columns of a table as `(name, Java class name of the type, Java class name of the
/// component type)`, the names as a client is told them in the schema.
///
/// Behavior: `io.deephaven.engine.table.ColumnDefinition#getDataType`, `#getComponentType`
#[pyfunction]
fn column_definitions(source: PyRef<'_, TableHandle>) -> Vec<(String, String, Option<String>)> {
    let columns = source.table.definition().columns().iter();
    columns
        .map(|column| {
            let type_name = java_type_name(&column.column_type).into_owned();
            let component = java_component_type_name(&column.column_type).map(Cow::into_owned);
            (column.name.clone(), type_name, component)
        })
        .collect()
}

/// Refuses a name that cannot be a column's.
///
/// Behavior: `io.deephaven.api.util.NameValidator#validateColumnName`
#[pyfunction]
fn validate_column_name(name: &str) -> PyResult<()> {
    crate::table::support::validate_column_name(name).map_err(|text| {
        PyRuntimeError::new_err(format!(
            "io.deephaven.api.util.NameValidator$InvalidNameException: {text}"
        ))
    })
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
        native.add_function(wrap_pyfunction!(column_definitions, &native)?)?;
        native.add_function(wrap_pyfunction!(validate_column_name, &native)?)?;
        native.add_function(wrap_pyfunction!(log, &native)?)?;
        native.add_function(wrap_pyfunction!(without_query_scope, &native)?)?;
        native.add_function(wrap_pyfunction!(require_query_scope, &native)?)?;
        native.add_function(wrap_pyfunction!(parquet::read_parquet, &native)?)?;
        native.add_function(wrap_pyfunction!(parquet::write_parquet, &native)?)?;
        native.add_function(wrap_pyfunction!(parquet::delete_parquet, &native)?)?;
        native.add_function(wrap_pyfunction!(csv::read_csv, &native)?)?;
        native.add_function(wrap_pyfunction!(csv::write_csv, &native)?)?;
        ops::register(&native)?;
        data::register(&native)?;
        input::register(&native)?;
        listener::register(&native)?;
        partitioned::register(&native)?;
        hierarchical::register(&native)?;
        plugin::register(&native)?;
        checks::register(&native)?;
        appmode::register(&native)?;
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

/// A Python object that is not a table, as the value of an export: what a scope ticket
/// resolves to for a variable that holds a number, a list, a function or anything else.
/// Java exports the Java object its conversion of the variable gives; clients can tell
/// neither from the other, because no call accepts such an export.
pub struct PythonObject(Py<PyAny>);

/// The value of a variable as a client may open it; `None` for `None`.
///
/// Behavior: `PythonDeephavenSession#unwrapObject`
fn export_of(value: &Bound<'_, PyAny>) -> Option<ExportValue> {
    if value.is_none() {
        return None;
    }
    if let Ok(handle) = value.cast::<TableHandle>() {
        return Some(ExportValue::Table(handle.get().table.clone()));
    }
    if let Ok(handle) = value.cast::<partitioned::PartitionedHandle>() {
        let partitioned = handle.get().partitioned.clone();
        return Some(crate::partitioned::export_value(partitioned));
    }
    if let Ok(handle) = value.cast::<hierarchical::HierarchicalHandle>() {
        return Some(crate::hierarchical::export_value(
            handle.get().table.clone(),
        ));
    }
    Some(ExportValue::object(PythonObject(value.clone().unbind())))
}

/// The value of a variable as a formula sees it.
///
/// Behavior: `PythonScopeJpyImpl#convert`; jpy's `PyObject#getObjectValue` gives a `bool`
/// as `Boolean`, an `int` as the smallest of `Byte`, `Short`, `Integer` and `Long` that
/// holds it, a `float` as `Double` and a `str` as `String`
fn formula_value(value: &Bound<'_, PyAny>) -> FormulaValue {
    if let Ok(value) = value.cast::<PyBool>() {
        return FormulaValue::Boolean(value.is_true());
    }
    if value.is_instance_of::<PyInt>() {
        return match value.extract::<i64>() {
            Ok(value) => {
                if let Ok(value) = i8::try_from(value) {
                    FormulaValue::Byte(value)
                } else if let Ok(value) = i16::try_from(value) {
                    FormulaValue::Short(value)
                } else if let Ok(value) = i32::try_from(value) {
                    FormulaValue::Int(value)
                } else {
                    FormulaValue::Long(value)
                }
            }
            Err(_) => FormulaValue::Other("an int beyond 64 bits".to_string()),
        };
    }
    if value.is_instance_of::<PyFloat>() {
        if let Ok(value) = value.extract::<f64>() {
            return FormulaValue::Double(value);
        }
    }
    if value.is_instance_of::<PyString>() {
        return match value.extract::<String>() {
            Ok(value) => FormulaValue::String(value),
            Err(_) => FormulaValue::Other("a str that is not valid Unicode".to_string()),
        };
    }
    if value.is_none() {
        return FormulaValue::Other("None".to_string());
    }
    let type_name = value.get_type().name();
    FormulaValue::Other(match type_name {
        Ok(name) => format!("a Python {name}"),
        Err(_) => "a Python object".to_string(),
    })
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
    /// The globals as they were when they were last looked at for changes. Whoever looks
    /// holds the lock, a command for as long as it runs; the interpreter lock is taken
    /// after it, never before.
    ///
    /// Behavior: `AbstractScriptSession#lastSnapshot`, and the monitor of the session that
    /// `evaluateScript`, `observeScopeChanges` and `setVariable` synchronize on
    snapshot: Arc<Mutex<Py<PyDict>>>,
    /// Whether the completion package could not be imported; see [`completer::ready`].
    no_completer: Arc<AtomicBool>,
}

impl PythonSession {
    /// Starts the interpreter if this process has none yet, in the environment that
    /// [`env::initialize`] was given or else the one the process was started in, and
    /// creates a session with no variables. From now on the `deephaven` package runs its
    /// operations in `context`.
    pub fn new(context: OpContext, logs: Arc<LogBuffer>) -> Result<Self, String> {
        env::interpreter()?;
        let runtime = Arc::new(Runtime {
            context,
            logs,
            globals: OnceLock::new(),
        });
        *RUNTIME.write().unwrap_or_else(PoisonError::into_inner) = Some(runtime.clone());
        Python::attach(|py| {
            let helpers = bootstrap(py)?;
            let globals = helpers.call_method0("new_globals")?.cast_into::<PyDict>()?;
            // a runtime is made for one session, so this is the only value it is given
            let _ = runtime.globals.set(globals.clone().unbind());
            Ok(Self {
                runtime,
                helpers: helpers.unbind(),
                snapshot: Arc::new(Mutex::new(PyDict::new(py).unbind())),
                globals: globals.unbind(),
                no_completer: Arc::default(),
            })
        })
        .map_err(|error: PyErr| format!("the Python session could not start: {error}"))
    }

    /// What changed in the globals since `snapshot`, which becomes the globals of now.
    ///
    /// Behavior: `AbstractScriptSession#observeScopeChanges`,
    /// `PythonDeephavenSession#createDiff`
    fn observed(&self, py: Python<'_>, snapshot: &mut Py<PyDict>) -> PyResult<Changes> {
        let after = self.globals.bind(py).copy()?;
        let changed: Vec<(String, Bound<'_, PyAny>, Bound<'_, PyAny>)> = self
            .helpers
            .bind(py)
            .call_method1("changes", (snapshot.bind(py), &after))?
            .extract()?;
        let mut changes = Changes::default();
        for (name, from, to) in changed {
            changes.note(&name, export_of(&from).as_ref(), export_of(&to).as_ref());
        }
        *snapshot = after.unbind();
        Ok(changes)
    }

    /// The same session, for another thread to hold.
    fn share(&self) -> Self {
        Python::attach(|py| Self {
            runtime: self.runtime.clone(),
            helpers: self.helpers.clone_ref(py),
            globals: self.globals.clone_ref(py),
            snapshot: self.snapshot.clone(),
            no_completer: self.no_completer.clone(),
        })
    }

    fn snapshot(&self) -> MutexGuard<'_, Py<PyDict>> {
        self.snapshot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `code` under the graph's lock on the calling thread, which is the
    /// interpreter's main thread when a request to `stop` is to interrupt the code. The
    /// code gives the lock up while it waits for a cycle to apply its write to an input
    /// table.
    fn evaluate_here(&self, code: &str, stop: &Stop) -> Changes {
        let mut snapshot = self.snapshot();
        locked(self.runtime.context.graph.as_ref(), || {
            yielding_to_cycles(|| {
                listener::running_a_command(|| {
                    Python::attach(|py| self.run(py, code, stop, &mut snapshot)).unwrap_or_else(
                        |error| Changes {
                            error: Some(format!("the Python session failed: {error}")),
                            ..Default::default()
                        },
                    )
                })
            })
        })
    }

    fn run(
        &self,
        py: Python<'_>,
        code: &str,
        stop: &Stop,
        snapshot: &mut Py<PyDict>,
    ) -> PyResult<Changes> {
        let helpers = self.helpers.bind(py);
        let ran = interrupt::stoppable(py, stop, || {
            helpers.call_method1("run", (code, self.globals.bind(py)))
        });
        let error: Option<String> = match ran {
            Some(Ok(error)) => error.extract()?,
            // the interrupt arrived after the code, while its error was put into words
            Some(Err(error)) if error.is_instance_of::<PyKeyboardInterrupt>(py) => {
                Some("KeyboardInterrupt\n".to_string())
            }
            Some(Err(error)) => return Err(error),
            // stopped before it started
            None => Some("KeyboardInterrupt\n".to_string()),
        };
        let mut changes = self.observed(py, snapshot)?;
        // Java: "java.lang.RuntimeException: Error in Python interpreter:", the same
        // traceback, and the stack of the JVM
        changes.error = error.map(|trace| format!("Error in Python interpreter:\n{trace}"));
        Ok(changes)
    }

    /// The variables a formula compiled now on this thread may name: those of the code
    /// that called the table operation running on this thread, or the globals.
    ///
    /// Behavior: `PythonScopeJpyImpl#currentScope`
    fn current_scope<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        CALLER_SCOPE
            .with_borrow(|scope| scope.as_ref().map(|scope| scope.bind(py).clone()))
            .unwrap_or_else(|| self.globals.bind(py).clone())
    }

    /// The value `value` has as a Python global: a table as the package's `Table`.
    ///
    /// Behavior: `PythonDeephavenSession#setVariable`, which wraps a Java object for
    /// Python; `deephaven._wrapper` then gives Python code its wrapper class
    fn wrap<'py>(py: Python<'py>, value: ExportValue) -> PyResult<Bound<'py, PyAny>> {
        match &value {
            ExportValue::Table(table) => {
                let handle = Py::new(
                    py,
                    TableHandle {
                        table: table.clone(),
                    },
                )?;
                py.import("deephaven.table")?
                    .getattr("Table")?
                    .call1((handle,))
            }
            ExportValue::Object(_) => {
                if let Some(object) = value.downcast_ref::<PythonObject>() {
                    return Ok(object.0.bind(py).clone());
                }
                if let Some(partitioned) = crate::partitioned::partitioned_of(&value) {
                    let partitioned = partitioned.clone();
                    let handle = Py::new(py, partitioned::PartitionedHandle { partitioned })?;
                    return py
                        .import("deephaven.table")?
                        .getattr("PartitionedTable")?
                        .call1((handle,));
                }
                if let Some(table) = crate::hierarchical::hierarchical_of(&value) {
                    let table = table.clone();
                    let handle = Py::new(py, hierarchical::HierarchicalHandle { table })?;
                    return py
                        .import("deephaven.table")?
                        .getattr("_wrap_hierarchical")?
                        .call1((handle,));
                }
                // Java makes any Java object a Python variable through jpy
                Err(PyRuntimeError::new_err(
                    "not ported: io.deephaven.engine.util.PythonObjectWrapper#wrap of an \
                     object that is neither a table nor a Python object",
                ))
            }
        }
    }
}

impl ScriptSession for PythonSession {
    /// Behavior: `PythonDeephavenSession#SCRIPT_TYPE`
    fn script_type(&self) -> &str {
        "Python"
    }

    /// The code runs on the interpreter's main thread, and this thread waits for it. A
    /// request to `stop` raises `KeyboardInterrupt` in it; [`interrupt`] says what that
    /// ends and what it does not. Java never stops a command.
    ///
    /// Behavior: `PythonDeephavenSession#evaluate`, `AbstractScriptSession#evaluateScript`
    fn evaluate(&self, code: &str, stop: &Arc<Stop>) -> Changes {
        tracing::info!("Evaluating command: {code}");
        let (session, code, stop) = (self.share(), code.to_string(), stop.clone());
        interrupt::on_main_thread(move || session.evaluate_here(&code, &stop))
    }

    /// The application is the script's on the interpreter's main thread, where the code
    /// runs, and not on this one.
    fn evaluate_script(&self, code: &str, application: &Application, stop: &Arc<Stop>) -> Changes {
        tracing::info!("Evaluating command: {code}");
        let (session, code, stop) = (self.share(), code.to_string(), stop.clone());
        let application = application.clone();
        interrupt::on_main_thread(move || {
            script::under_context(&application, || session.evaluate_here(&code, &stop))
        })
    }

    /// Behavior: `AbstractScriptSession#observeScopeChanges`
    fn observe(&self) -> Changes {
        let mut snapshot = self.snapshot();
        Python::attach(|py| self.observed(py, &mut snapshot)).unwrap_or_else(|error| {
            tracing::error!("the variables of the Python session could not be read: {error}");
            Changes::default()
        })
    }

    /// Behavior: `PythonDeephavenSession#setVariable`
    fn put(&self, name: &str, value: ExportValue) -> Result<Changes, String> {
        let mut snapshot = self.snapshot();
        Python::attach(|py| {
            let wrapped = Self::wrap(py, value)?;
            self.globals.bind(py).set_item(name, wrapped)?;
            self.observed(py, &mut snapshot)
        })
        .map_err(|error: PyErr| format!("the variable {name} could not be set: {error}"))
    }

    /// Behavior: `PythonDeephavenSession#getVariable`, `#unwrapObject`
    fn get(&self, name: &str) -> Option<ExportValue> {
        Python::attach(|py| {
            let value = self.globals.bind(py).get_item(name).ok()??;
            let unwrapped = self
                .helpers
                .bind(py)
                .call_method1("unwrap", (value,))
                .ok()?;
            export_of(&unwrapped)
        })
    }

    /// Behavior: `PythonDeephavenSession#getVariableNames`
    fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = Python::attach(|py| {
            let globals = self.globals.bind(py);
            globals
                .keys()
                .iter()
                .filter_map(|name| name.extract().ok())
                .collect()
        });
        names.sort_unstable();
        names
    }

    /// Fails, as every formula does in Java, while a variable holds an `int` beyond 64
    /// bits: Java converts every variable for every formula, and that conversion fails.
    ///
    /// Behavior: `PythonDeephavenSession#getAllValues`
    fn refuses_formulas(&self) -> Option<String> {
        NO_QUERY_SCOPE
            .get()
            .then(|| NO_QUERY_SCOPE_TEXT.to_string())
    }

    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String> {
        Python::attach(|py| {
            let scope = self.current_scope(py);
            for (_, value) in scope.iter() {
                if value.is_instance_of::<PyInt>() && value.extract::<i64>().is_err() {
                    return Err("OverflowError: int too big to convert".to_string());
                }
            }
            names
                .iter()
                .map(|name| match scope.get_item(*name) {
                    Ok(value) => Ok(value.as_ref().map(formula_value)),
                    Err(error) => Err(error.to_string()),
                })
                .collect()
        })
    }

    /// `jedi` in this interpreter, if the environment has it. Takes neither the update
    /// graph's lock nor the snapshot's, so it does not wait for a running command.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#autoCompleteStream`
    fn completer(&self) -> Result<Option<Arc<dyn Completer>>, String> {
        completer::ready(&self.helpers, &self.globals, &self.no_completer)
    }
}

/// Gives `state` a Python console: starts the interpreter and makes a new Python session
/// the script session of the server and the holder of its query scope. Must be called
/// inside a tokio runtime, which looks at the variables for changes from then on.
pub fn start(state: &mut ServerState) -> Result<(), String> {
    // the operations of the Python package may call the functions of the session; a
    // client's formulas, which are compiled with the server's own context, may not
    let context = OpContext {
        compiler: udf::compiler(state.scope.clone()),
        ..state.op_context()
    };
    listener::install(state.graph.clone());
    let session = PythonSession::new(context, state.logs.clone())?;
    plugin::register_plugins()?;
    let console = Console::new(Arc::new(session), state.applications.clone());
    state.scope.hold(console.clone());
    console.watch(CHECK_SCOPE_CHANGES_INTERVAL);
    state.console = Some(console);
    Ok(())
}

/// The version of the interpreter the server embeds, as `platform.python_version()` gives
/// it.
pub fn python_version() -> Result<String, String> {
    env::interpreter().map(|interpreter| interpreter.version)
}
