//! Python functions called from query strings: `t.update("Y = f(X)")` for a function `f` of
//! the script session. `docs/PYTHON_UDF.md` has the design, what Java does and what is
//! refused.
//!
//! The formula compiler of the Python package's operations ([`compiler`]) asks
//! [`PythonFunctions`] about a call the function library cannot take. A callable of that
//! name in the scope of the code that called the operation becomes a [`PythonCall`]: the
//! Python side (`python/_oxhaven_udf.py`) reads the signature once and hands back a function
//! that runs a chunk of rows.
//!
//! Locks: whoever evaluates a formula takes the interpreter lock here, for a chunk at a
//! time, and holds whatever it held before: the update graph's lock in a cycle or a command,
//! nothing in a reader. So the order is update graph, then interpreter, and the other half
//! of the rule is the callers': a thread that holds the interpreter lock releases it before
//! it calls the engine (`py.detach` in every native function of the package).

use std::cell::RefCell;
use std::ffi::CString;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray,
};
use arrow::compute::concat;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
};
use oxhaven_contracts::expr::{FormulaCompiler, FormulaError, Literal};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use oxhaven_expr::{ExternalArgument, ExternalFunction, ExternalFunctions};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::app::{item_function_parts, Scope, ScopedCompiler};
use crate::schema::java_type_name;

/// The Python half: signatures, conversions and the loop over the rows of a chunk.
const SOURCE: &str = include_str!("../../../python/_oxhaven_udf.py");

/// The most rows a function is called for while the interpreter lock is held once.
const CHUNK: usize = 4096;

/// The module `_oxhaven_udf`, loaded on first use.
fn helpers(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let modules = py.import("sys")?.getattr("modules")?;
    if !modules.contains("_oxhaven_udf")? {
        let code = CString::new(SOURCE).expect("the source has no NUL");
        let module = PyModule::from_code(py, &code, c"<oxhaven>/_oxhaven_udf.py", c"_oxhaven_udf")?;
        modules.set_item("_oxhaven_udf", module)?;
    }
    py.import("_oxhaven_udf")
}

thread_local! {
    /// What the calls compiled for the table operation running on this thread share.
    static OPERATION: RefCell<Option<Py<PyDict>>> = const { RefCell::new(None) };
}

/// One table operation of the Python package, for as long as the value lives: the calls
/// its formulas make to one function are checked against each other.
///
/// Behavior: `QueryCompilerRequestProcessor#getQueryScopeVariables`, which converts the
/// variables once per operation, so that the formulas of one operation share one
/// `PyCallableWrapper` per function
pub(super) struct Operation {
    before: Option<Py<PyDict>>,
}

impl Operation {
    pub(super) fn enter(py: Python<'_>) -> Self {
        Self {
            before: OPERATION.replace(Some(PyDict::new(py).unbind())),
        }
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        OPERATION.set(self.before.take());
    }
}

fn unsupported(what: String, java: &str) -> FormulaError {
    FormulaError::Unsupported(format!("{what}: not ported: {java}"))
}

/// The column type a Java type name of `_oxhaven_udf` stands for.
fn result_type(java: &str) -> Option<ColumnType> {
    Some(match java {
        "byte" => ColumnType::Byte,
        "short" => ColumnType::Short,
        "int" => ColumnType::Int,
        "long" => ColumnType::Long,
        "float" => ColumnType::Float,
        "double" => ColumnType::Double,
        "java.lang.Boolean" => ColumnType::Boolean,
        "java.lang.String" => ColumnType::String,
        _ => return None,
    })
}

/// The callables of the scope a formula is compiled in, as functions of the formula
/// language.
///
/// Behavior: `QueryLanguageParser#getMethod`, for a variable of the query scope that is a
/// `PyCallableWrapper`; `PythonScopeJpyImpl#convert` makes every callable one
pub(super) struct PythonFunctions;

impl ExternalFunctions for PythonFunctions {
    fn resolve(
        &self,
        name: &str,
        arguments: &[ExternalArgument<'_>],
    ) -> Option<Result<Arc<dyn ExternalFunction>, FormulaError>> {
        Python::attach(|py| {
            if let Some((cast, variable)) = item_function_parts(name) {
                let container = super::current_scope(py)?.get_item(variable).ok()??;
                return Some(bind_item(py, cast, variable, &container, arguments));
            }
            let function = super::current_scope(py)?.get_item(name).ok()??;
            if !function.is_callable() {
                return None;
            }
            Some(bind(py, name, &function, arguments))
        })
    }
}

/// The call `name(arguments)` of `function`, with the signature read and the arguments
/// checked against it.
///
/// Behavior: `PyCallableWrapperJpyImpl#parseSignature`, `#verifyArguments`
fn bind(
    py: Python<'_>,
    name: &str,
    function: &Bound<'_, PyAny>,
    arguments: &[ExternalArgument<'_>],
) -> Result<Arc<dyn ExternalFunction>, FormulaError> {
    let mut types = Vec::with_capacity(arguments.len());
    for argument in arguments {
        if matches!(argument.literal, Some(Literal::Char(_))) {
            // Java compiles the call and fails on the first row: ClassCastException
            return Err(unsupported(
                format!("a character literal as an argument of {name}"),
                "io.deephaven.engine.util.PyCallableWrapperJpyImpl#call",
            ));
        }
        types.push(match argument.column_type {
            Some(
                column_type @ (ColumnType::Boolean
                | ColumnType::Byte
                | ColumnType::Char
                | ColumnType::Short
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::String
                | ColumnType::Instant),
            ) => java_type_name(column_type).into_owned(),
            // an array, a vector or another object: the Python side refuses it by this name
            Some(other) => format!("a value of the column type {other:?}"),
            None => "null".to_string(),
        });
    }
    let failed = |error: PyErr| FormulaError::Type(format!("the Python function {name}: {error}"));
    let operation = OPERATION
        .with_borrow(|operation| operation.as_ref().map(|operation| operation.clone_ref(py)));
    let outcome = helpers(py)
        .and_then(|helpers| helpers.call_method1("prepare", (function, name, types, operation)))
        .and_then(|outcome| Ok(outcome.cast_into::<PyTuple>()?))
        .map_err(failed)?;
    let text = |index: usize| -> Result<String, FormulaError> {
        outcome
            .get_item(index)
            .and_then(|item| item.extract())
            .map_err(failed)
    };
    match text(0)?.as_str() {
        "ok" => {
            let java = text(1)?;
            let result = result_type(&java).ok_or_else(|| {
                FormulaError::Type(format!("the Python function {name} returns a {java}"))
            })?;
            let run = outcome.get_item(2).map_err(failed)?.unbind();
            Ok(Arc::new(PythonCall {
                what: format!("the Python function {name}"),
                result,
                run,
            }))
        }
        "unsupported" => Err(FormulaError::Unsupported(text(1)?)),
        _ => Err(FormulaError::Type(text(1)?)),
    }
}

/// The item `(cast) variable[argument]` of `container`, a list or a dict, which is read
/// from the object where a row is computed, as Java reads it: a list by Python's rules
/// for an index, so that a negative one counts from the end and one outside the list
/// fails, a dict with `get`, so that a key it has not is null. `None` is null, and
/// another item is converted as jpy converts it for the cast (`_oxhaven_udf.item_reader`).
///
/// Behavior: `org.jpy.PyListWrapper#get(int)`, `org.jpy.PyDictWrapper#get(Object)`, and
/// `QueryLanguageParser#visit(CastExpr)` for an `org.jpy.PyObject`, which calls
/// `PyObject#getIntValue`, `#getLongValue`, `#getDoubleValue` or `#getStringValue`; a
/// null `PyObject` is the null of the type. Java 42.9 was run for each form.
fn bind_item(
    py: Python<'_>,
    cast: &str,
    variable: &str,
    container: &Bound<'_, PyAny>,
    arguments: &[ExternalArgument<'_>],
) -> Result<Arc<dyn ExternalFunction>, FormulaError> {
    use ColumnType as C;
    let list = container.is_instance_of::<PyList>();
    let class = if list {
        "org.jpy.PyListWrapper"
    } else {
        "org.jpy.PyDictWrapper"
    };
    let [argument] = arguments else {
        return Err(unsupported(
            format!(
                "an item of {variable} read with {} arguments",
                arguments.len()
            ),
            &format!("{class}#get"),
        ));
    };
    match argument.column_type {
        // `get(int)`, for what widens to an `int`
        Some(C::Byte | C::Short | C::Char | C::Int) => {}
        // `get(Object)`, for the value boxed
        Some(C::Boolean | C::Long | C::Float | C::Double | C::String) if !list => {}
        Some(other) if list => {
            return Err(FormulaError::Type(format!(
                "Cannot find method get({}) in class {class}",
                java_type_name(other)
            )))
        }
        _ => {
            return Err(unsupported(
                format!("an item of {variable} read with this key"),
                &format!("{class}#get"),
            ))
        }
    }
    let result = result_type(match cast {
        "String" => "java.lang.String",
        other => other,
    })
    .expect("a cast that is computed");
    let kind = if list { "list" } else { "dict" };
    let run = helpers(py)
        .and_then(|helpers| helpers.call_method1("item_reader", (container, kind, cast)))
        .map_err(|error| FormulaError::Type(format!("the items of {variable}: {error}")))?;
    Ok(Arc::new(PythonCall {
        what: format!("({cast}) {variable}[]"),
        result,
        run: run.unbind(),
    }))
}

/// A Python function bound to the argument types of one call in a formula, or the items
/// of a Python list or dict.
struct PythonCall {
    /// What it is, for the message of a failure.
    what: String,
    result: ColumnType,
    /// `run(rows, columns)` of `_oxhaven_udf`: the cells of a chunk, as a list.
    run: Py<PyAny>,
}

impl ExternalFunction for PythonCall {
    fn result_type(&self) -> &ColumnType {
        &self.result
    }

    /// Calls the function for each row, in order: the interpreter lock is taken once for
    /// every [`CHUNK`] rows, and released between chunks.
    ///
    /// Behavior: `FormulaKernelPythonChunkedFunction#applyFormulaChunk` and
    /// `FilterKernelPythonChunkedFunction#filter`, which call for a chunk at a time, and
    /// `PyCallableWrapperJpyImpl#call` for a call that Java makes row by row
    fn call(&self, arguments: &[ArrayRef], rows: usize) -> Result<ArrayRef, TableFailure> {
        let mut parts = Vec::with_capacity(rows.div_ceil(CHUNK));
        let mut offset = 0;
        while offset < rows {
            let length = CHUNK.min(rows - offset);
            let part = Python::attach(|py| {
                let columns = arguments
                    .iter()
                    .map(|argument| python_values(py, &argument.slice(offset, length)))
                    .collect::<PyResult<Vec<_>>>()?;
                let cells = self.run.bind(py).call1((length, columns))?;
                cells_array(&self.result, &cells)
            });
            // Java: "java.lang.RuntimeException: Error in Python interpreter:" and the
            // traceback, whose last line is this text
            parts.push(part.map_err(|error| TableFailure {
                message: format!("Error in {}: {error}", self.what),
            })?);
            offset += length;
        }
        match parts.len() {
            0 => Ok(arrow::array::new_empty_array(&self.result.arrow_type())),
            1 => Ok(parts.remove(0)),
            _ => {
                let parts: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
                concat(&parts).map_err(|error| TableFailure {
                    message: error.to_string(),
                })
            }
        }
    }
}

/// The values of an argument as Python is given them: a null number is the value Java
/// reserves for null in its type, a `char` is its code unit, a `float` is widened, and a
/// null `Boolean` or `String` is `None`.
///
/// Behavior: jpy's conversion of the arguments of `PyCallableWrapperJpyImpl#call`, which
/// are the primitives of the generated formula class
fn python_values<'py>(py: Python<'py>, values: &ArrayRef) -> PyResult<Bound<'py, PyList>> {
    macro_rules! numbers {
        ($arrow:ty, $null:expr, $python:ty) => {{
            let values = values.as_primitive::<$arrow>().iter();
            PyList::new(
                py,
                values.map(|value| <$python>::from(value.unwrap_or($null))),
            )
        }};
    }
    match values.data_type() {
        DataType::Null => PyList::new(py, (0..values.len()).map(|_| py.None())),
        DataType::Boolean => PyList::new(py, values.as_boolean().iter()),
        DataType::Utf8 => PyList::new(py, values.as_string::<i32>().iter()),
        DataType::Int8 => numbers!(Int8Type, i8::MIN, i64),
        DataType::Int16 => numbers!(Int16Type, i16::MIN, i64),
        DataType::UInt16 => numbers!(UInt16Type, u16::MAX, i64),
        DataType::Int32 => numbers!(Int32Type, i32::MIN, i64),
        DataType::Int64 => numbers!(Int64Type, i64::MIN, i64),
        DataType::Float32 => numbers!(Float32Type, -f32::MAX, f64),
        DataType::Float64 => numbers!(Float64Type, -f64::MAX, f64),
        other => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "a Python function cannot be given a value of the Arrow type {other}"
        ))),
    }
}

/// The cells `_oxhaven_udf` computed for a chunk as a column of `result`: an integer is
/// narrowed as a Java cast narrows it, a `float` as a cast of a `double`, and the value
/// Java reserves for null in the type is null.
///
/// Behavior: the casts of `QueryLanguageParser#makeCastExpressionForPyCallable`, and the
/// Java arrays that `PyCallableWrapperJpyImpl#vectorizedCallable` fills
fn cells_array(result: &ColumnType, cells: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    macro_rules! numbers {
        ($array:ty, $from:ty, $to:ty, $null:expr) => {{
            let cells: Vec<Option<$from>> = cells.extract()?;
            let narrowed = cells
                .into_iter()
                .map(|cell| cell.map(|cell| cell as $to).filter(|cell| *cell != $null));
            Arc::new(narrowed.collect::<$array>())
        }};
    }
    Ok(match result {
        ColumnType::Byte => numbers!(Int8Array, i64, i8, i8::MIN),
        ColumnType::Short => numbers!(Int16Array, i64, i16, i16::MIN),
        ColumnType::Int => numbers!(Int32Array, i64, i32, i32::MIN),
        ColumnType::Long => numbers!(Int64Array, i64, i64, i64::MIN),
        ColumnType::Float => numbers!(Float32Array, f64, f32, -f32::MAX),
        ColumnType::Double => numbers!(Float64Array, f64, f64, -f64::MAX),
        ColumnType::Boolean => Arc::new(BooleanArray::from(cells.extract::<Vec<Option<bool>>>()?)),
        ColumnType::String => Arc::new(StringArray::from(cells.extract::<Vec<Option<String>>>()?)),
        other => unreachable!("no Python function is bound with the result type {other:?}"),
    })
}

/// The formula language of the Python package's operations: the variables of the query
/// scope, and its callables as functions.
pub(super) fn compiler(scope: Arc<Scope>) -> Arc<dyn FormulaCompiler> {
    let language = oxhaven_expr::Compiler::with_functions(Arc::new(PythonFunctions));
    Arc::new(ScopedCompiler::new(Arc::new(language), scope).reading_items())
}

#[cfg(test)]
mod tests;
