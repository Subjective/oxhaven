//! Table data crossing between the engine and Python, in the process.
//!
//! Three forms, for the three kinds of reader the `deephaven` package has:
//!
//! - **Arrow**, for `deephaven.arrow` and what is built on it (`deephaven.pandas` with a
//!   `dtype_backend`). [`arrow_snapshot`] takes a consistent snapshot of a table, and the
//!   snapshot hands its record batch to pyarrow through the Arrow C stream interface
//!   (`__arrow_c_stream__`), so the columns are shared with Python and not copied.
//!   [`table_from_arrow`] is the reverse: it reads the stream of a pyarrow table and makes
//!   the table an upload of the same record batches makes.
//! - **Buffers**, for numpy (`deephaven.numpy`, `deephaven.pandas` without a
//!   `dtype_backend`). [`column_values`] gives a column of a fixed-width type as the bytes
//!   of its values, each null as the value Java reserves for null, which is what the
//!   reference package builds its numpy arrays from: the Java arrays of
//!   `io.deephaven.engine.table.vectors.ColumnVectors`.
//! - **Python values**, for the row iterators of a table and for the columns that have no
//!   fixed width: [`cells`].
//!
//! No Java counterpart as a whole: Java hands jpy its arrays, and serializes Arrow record
//! batches for pyarrow to parse.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, RecordBatch, RecordBatchIterator, RecordBatchOptions,
    RecordBatchReader,
};
use arrow::compute::cast;
use arrow::datatypes::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    SchemaRef, TimestampNanosecondType, ToByteSlice, UInt16Type,
};
use arrow::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use oxhaven_column::sentinel::{
    boolean_to_bytes, nulls_to_sentinels, NULL_BYTE, NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_INT,
    NULL_LONG, NULL_SHORT,
};
use oxhaven_contracts::schema::ColumnType;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyCapsule, PyList};
use tonic::Code;

use super::ops::{failure, guarded};
use super::{error_text, TableHandle};
use crate::flight::{table_of_batches, wire_columns, wire_schema, UploadError};
use crate::schema::java_type_name;
use crate::session::ExportError;
use crate::table::support::not_ported;

/// The name the Arrow PyCapsule interface gives the capsule of a C stream.
const STREAM_CAPSULE: &std::ffi::CStr = c"arrow_array_stream";

/// What the package has in place of jpy's Java objects: the classes of an instant and of
/// a Java array, and the function that gives the `DType` of a Java class name.
struct Wrappers<'py> {
    instant: Bound<'py, PyAny>,
    array: Bound<'py, PyAny>,
    dtype: Bound<'py, PyAny>,
}

impl<'py> Wrappers<'py> {
    fn import(py: Python<'py>) -> PyResult<Self> {
        let dtypes = py.import("deephaven.dtypes")?;
        Ok(Self {
            instant: py.import("deephaven._java_time")?.getattr("Instant")?,
            array: dtypes.getattr("JArray")?,
            dtype: dtypes.getattr("_from_java_name")?,
        })
    }
}

/// The cells of `values` as the Python values jpy gives for the elements of a Java array
/// of `column_type`: a primitive is its Java value, so a null is the null constant of its
/// type and a `char` is its code unit; a `Boolean` or `String` is the value or `None`; an
/// `Instant` is the package's instant or `None`; an array is the package's `JArray` of
/// its elements, converted the same way, or `None`.
///
/// A vector is refused: Java hands out the `io.deephaven.vector.Vector` object, which the
/// package has no class for.
fn java_values<'py>(
    py: Python<'py>,
    wrappers: &Wrappers<'py>,
    column_type: &ColumnType,
    values: &ArrayRef,
) -> PyResult<Bound<'py, PyList>> {
    macro_rules! primitive {
        ($arrow:ty, $null:expr) => {
            PyList::new(
                py,
                values
                    .as_primitive::<$arrow>()
                    .iter()
                    .map(|value| value.unwrap_or($null)),
            )
        };
    }
    match column_type {
        ColumnType::Byte => primitive!(Int8Type, NULL_BYTE),
        ColumnType::Short => primitive!(Int16Type, NULL_SHORT),
        ColumnType::Int => primitive!(Int32Type, NULL_INT),
        ColumnType::Long => primitive!(Int64Type, NULL_LONG),
        ColumnType::Float => primitive!(Float32Type, NULL_FLOAT),
        ColumnType::Double => primitive!(Float64Type, NULL_DOUBLE),
        ColumnType::Char => primitive!(UInt16Type, NULL_CHAR),
        ColumnType::Boolean => PyList::new(py, values.as_boolean().iter()),
        ColumnType::String => PyList::new(py, values.as_string::<i32>().iter()),
        ColumnType::Instant => {
            let instants = values.as_primitive::<TimestampNanosecondType>().iter();
            let instants = instants
                .map(|nanos| match nanos {
                    Some(nanos) if nanos != NULL_LONG => wrappers.instant.call1((nanos,)),
                    _ => Ok(py.None().into_bound(py)),
                })
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, instants)
        }
        ColumnType::Array(element) => {
            let Some(lists) = values.as_list_opt::<i32>() else {
                return Err(PyRuntimeError::new_err(format!(
                    "a column of type {column_type:?} is stored as {}",
                    values.data_type()
                )));
            };
            let dtype = wrappers.dtype.call1((java_type_name(element),))?;
            let arrays = lists
                .iter()
                .map(|list| match list {
                    Some(list) => {
                        let elements = java_values(py, wrappers, element, &list)?;
                        wrappers.array.call1((&dtype, elements))
                    }
                    None => Ok(py.None().into_bound(py)),
                })
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, arrays)
        }
        other => Err(failure(
            not_ported(
                "io.deephaven.integrations.python.PythonTableDataReader#readChunkColumnMajor",
                format!(
                    "the Python values of a column of type {other:?}: Java hands out its \
                     io.deephaven.vector.Vector objects"
                ),
            )
            .into(),
        )),
    }
}

/// The cells of one column as Python values, as the reference package's readers hand
/// them out: see [`java_values`]. With `instants`, a column of `Instant` is its
/// nanoseconds since the epoch, a null as the null of `long`, for a numpy
/// `datetime64[ns]`; the instants inside an array are objects either way.
///
/// Behavior: `io.deephaven.integrations.python.PythonTableDataReader#readChunkColumnMajor`
/// as `deephaven._table_reader` reads its arrays; for `instants`,
/// `io.deephaven.integrations.python.PrimitiveArrayConversionUtility#translateArrayInstantToLong`
pub(super) fn cells<'py>(
    py: Python<'py>,
    column_type: &ColumnType,
    values: &ArrayRef,
    instants: bool,
) -> PyResult<Bound<'py, PyList>> {
    if instants && *column_type == ColumnType::Instant {
        let nanos = values.as_primitive::<TimestampNanosecondType>().iter();
        return PyList::new(py, nanos.map(|nanos| nanos.unwrap_or(NULL_LONG)));
    }
    java_values(py, &Wrappers::import(py)?, column_type, values)
}

/// The bytes of the values of a primitive array, in the byte order of the machine, each
/// null as `null`.
fn value_bytes<'py, T: ArrowPrimitiveType>(
    py: Python<'py>,
    values: &ArrayRef,
) -> PyResult<Bound<'py, PyAny>> {
    let filled = nulls_to_sentinels(values).map_err(|failure| failure_text(failure.message))?;
    let bytes = filled.as_primitive::<T>().values().to_byte_slice();
    Ok(PyByteArray::new(py, bytes).into_any())
}

fn failure_text(text: String) -> PyErr {
    PyRuntimeError::new_err(text)
}

/// One column for numpy: a `bytearray` of the values for a type of fixed width, a list
/// of Python values ([`cells`]) for any other.
///
/// Behavior: `io.deephaven.engine.table.vectors.ColumnVectors#of` and `Vector#copyToArray`,
/// with `PrimitiveArrayConversionUtility#translateArrayBooleanToByte` and
/// `#translateArrayInstantToLong`, as `deephaven.jcompat._j_array_to_numpy_array` reads
/// the arrays
fn numpy_values<'py>(
    py: Python<'py>,
    column_type: &ColumnType,
    values: &ArrayRef,
) -> PyResult<Bound<'py, PyAny>> {
    match column_type {
        ColumnType::Byte => value_bytes::<Int8Type>(py, values),
        ColumnType::Short => value_bytes::<Int16Type>(py, values),
        ColumnType::Int => value_bytes::<Int32Type>(py, values),
        ColumnType::Long => value_bytes::<Int64Type>(py, values),
        ColumnType::Float => value_bytes::<Float32Type>(py, values),
        ColumnType::Double => value_bytes::<Float64Type>(py, values),
        ColumnType::Char => value_bytes::<UInt16Type>(py, values),
        ColumnType::Instant => value_bytes::<TimestampNanosecondType>(py, values),
        ColumnType::Boolean => {
            let bytes = boolean_to_bytes(values.as_boolean());
            Ok(PyByteArray::new(py, bytes.values().to_byte_slice()).into_any())
        }
        other => Ok(cells(py, other, values, false)?.into_any()),
    }
}

/// The values of the columns `columns` of a table, in that order, read at one moment:
/// under the update graph's shared lock if the table is refreshing. See [`numpy_values`]
/// for the form of each.
#[pyfunction]
fn column_values<'py>(
    py: Python<'py>,
    source: PyRef<'py, TableHandle>,
    columns: Vec<String>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let table = source.table.clone();
    let definition = table.definition();
    let indices = columns
        .iter()
        .map(|name| {
            definition
                .index_of(name)
                .ok_or_else(|| failure_text(format!("the table has no column {name}")))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let read = py.detach(|| {
        guarded(std::slice::from_ref(&table), || {
            let rows = table.row_set();
            let chunks = indices
                .iter()
                .map(|index| Ok(table.column(*index).chunk(&rows)?))
                .collect::<Result<Vec<_>, ExportError>>()?;
            Ok(chunks)
        })
    });
    let read = read.map_err(failure)?;
    let types = indices.iter().map(|index| &definition.columns()[*index]);
    types
        .zip(&read)
        .map(|(column, values)| numpy_values(py, &column.column_type, values))
        .collect()
}

/// The rows of a table at one moment, as Arrow: what [`arrow_snapshot`] returns. pyarrow
/// reads it through the Arrow PyCapsule interface, `pyarrow.table(snapshot)`.
#[pyclass(frozen, module = "_oxhaven")]
struct ArrowSnapshot {
    schema: SchemaRef,
    /// Every row; `None` for a table without rows.
    batch: Option<RecordBatch>,
}

#[pymethods]
impl ArrowSnapshot {
    /// The snapshot as a C stream of record batches, in a capsule. The arrays are the
    /// engine's own: the consumer shares them and frees nothing of ours.
    ///
    /// `requested_schema` is a wish the interface allows a producer not to grant.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let batches = self.batch.clone().into_iter().map(Ok);
        let reader = RecordBatchIterator::new(batches, self.schema.clone());
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        // dropping the stream releases it unless the consumer has moved it out, which
        // leaves it released
        PyCapsule::new(py, stream, Some(STREAM_CAPSULE.to_owned()))
    }
}

/// A consistent snapshot of `source` with the schema and the cells a Flight client is sent
/// for the table. Every row is in one record batch.
///
/// Java sends the snapshot in record batches of 4096 rows and more, of sizes that depend
/// on how long the snapshot took, so a pyarrow table made of them has several chunks per
/// column where this one has one.
///
/// Behavior: `io.deephaven.extensions.barrage.util.TableToArrowConverter`
#[pyfunction]
fn arrow_snapshot(py: Python<'_>, source: PyRef<'_, TableHandle>) -> PyResult<ArrowSnapshot> {
    let table = source.table.clone();
    let made = py.detach(|| {
        guarded(std::slice::from_ref(&table), || {
            let schema = Arc::new(wire_schema(&table));
            let rows = table.row_set();
            let columns = wire_columns(&table)
                .iter()
                .enumerate()
                .zip(schema.fields())
                .map(|((index, wire), field)| {
                    let cells = wire.cells(table.column(index).chunk(&rows)?)?;
                    if cells.data_type() == field.data_type() {
                        return Ok(cells);
                    }
                    // an uploaded table is described as it was sent: a timestamp
                    // without a time zone, the element fields under their own names
                    cast(&cells, field.data_type()).map_err(|problem| {
                        ExportError::Internal(format!(
                            "column {} cannot be given as {}: {problem}",
                            field.name(),
                            field.data_type()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, ExportError>>()?;
            let rows = rows.size() as usize;
            let batch = if rows == 0 {
                None
            } else {
                let options = RecordBatchOptions::new().with_row_count(Some(rows));
                let batch = RecordBatch::try_new_with_options(schema.clone(), columns, &options);
                Some(batch.map_err(|problem| ExportError::Internal(problem.to_string()))?)
            };
            Ok(ArrowSnapshot { schema, batch })
        })
    });
    made.map_err(failure)
}

/// A static table of the record batches of `data`, which gives them through the Arrow
/// PyCapsule interface as a pyarrow table does. The fields are read as the fields of an
/// upload are: `deephaven:type` names the column type, and without it the Arrow type
/// decides. What an upload refuses is refused here, and what the server does not read yet
/// is a `NotImplementedError` that names the Java method.
///
/// Behavior: `io.deephaven.extensions.barrage.util.ArrowToTableConverter`
#[pyfunction]
fn table_from_arrow(data: &Bound<'_, PyAny>) -> PyResult<TableHandle> {
    let capsule = data.call_method0("__arrow_c_stream__")?;
    let capsule = capsule.cast::<PyCapsule>().map_err(|_| {
        PyTypeError::new_err("__arrow_c_stream__ must return a capsule of an Arrow C stream")
    })?;
    let pointer = capsule.pointer_checked(Some(STREAM_CAPSULE))?;
    // SAFETY: the capsule is named `arrow_array_stream`, which by the Arrow PyCapsule
    // interface means its pointer is to an initialized `ArrowArrayStream` that the capsule
    // owns, valid and aligned while the capsule lives; `capsule` is alive here and no
    // Python code runs between the check and the read. `from_raw` moves the stream out
    // and leaves a released one behind, as the interface asks of a consumer, so the
    // capsule's destructor has nothing left to release.
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer.cast().as_ptr()) };
    let unreadable = |problem: arrow::error::ArrowError| {
        PyRuntimeError::new_err(format!("unreadable Arrow stream: {problem}"))
    };
    let reader = RecordBatchStreamReader::try_new(stream).map_err(unreadable)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(unreadable)?;
    match table_of_batches(&schema, &batches) {
        Ok(table) => Ok(TableHandle { table }),
        Err(UploadError::Status(status)) if status.code() == Code::Unimplemented => Err(
            PyNotImplementedError::new_err(error_text(ExportError::Status(status))),
        ),
        Err(UploadError::Status(status)) => Err(failure(ExportError::Status(status))),
        Err(UploadError::Internal(text)) => Err(failure(ExportError::Internal(text))),
    }
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<ArrowSnapshot>()?;
    native.add_function(wrap_pyfunction!(arrow_snapshot, native)?)?;
    native.add_function(wrap_pyfunction!(column_values, native)?)?;
    native.add_function(wrap_pyfunction!(table_from_arrow, native)?)?;
    Ok(())
}
