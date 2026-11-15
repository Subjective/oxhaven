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

#[cfg(test)]
mod tests {
    //! The expectations are what the Java reference answered to the same code, through
    //! the script scenarios `convert_*` and by hand; a comment says where Java differs.
    //! Only the standard library is sure to be there (the `python-console` job installs
    //! nothing), so what needs numpy, pandas or pyarrow runs only where they are.

    use oxhaven_testkit::TableSnapshot;

    use super::super::tests::Python;

    const SOURCE: &str = r#"
from deephaven import DHError, _engine, empty_table
from deephaven.table import Table
source = empty_table(3).update([
    "B = (byte)(i == 1 ? null : i)", "H = (short)(i == 1 ? null : i)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long)i", "F = (float)(i == 1 ? null : i * 0.5)",
    "D = i == 1 ? null : i * 0.5", "C = i == 1 ? null : (char)(65 + i)",
    "S = i == 1 ? null : `s` + i", "Z = i == 1 ? null : i == 0",
    "T = i == 1 ? null : '2024-01-01T00:00:00Z' + i * SECOND",
])
"#;

    /// Asserts that each of the scope's tables `names` has the definition and the cells of
    /// the scope's table `source`.
    fn assert_same_as_source(python: &Python, names: &[&str]) {
        let source = python.table("source");
        let expected = TableSnapshot::current(source.as_ref()).unwrap();
        for name in names {
            let table = python.table(name);
            assert_eq!(table.definition(), source.definition(), "{name}");
            let snapshot = TableSnapshot::current(table.as_ref()).unwrap();
            assert_eq!(snapshot.columns, expected.columns, "{name}");
        }
    }

    /// Behavior: `io.deephaven.engine.table.vectors.ColumnVectors#of`, with
    /// `PrimitiveArrayConversionUtility#translateArrayBooleanToByte` and
    /// `#translateArrayInstantToLong`
    #[tokio::test]
    async fn columns_for_numpy_are_the_bytes_of_the_values_with_the_nulls_of_java() {
        let python = Python::start().await;
        python.run(SOURCE);
        python.run(
            r#"
import struct
names = ["I", "B", "H", "L", "F", "D", "C", "Z", "T", "S"]
values = dict(zip(names, _engine.native.column_values(source.j_table, names)))
for name, form, expected in (
    ("B", "=3b", (0, -128, 2)), ("H", "=3h", (0, -32768, 2)), ("I", "=3i", (0, -2147483648, 2)),
    ("L", "=3q", (0, -9223372036854775808, 2)),
    ("F", "=3f", (0.0, -3.4028234663852886e+38, 1.0)),
    ("D", "=3d", (0.0, -1.7976931348623157e+308, 1.0)),
    ("C", "=3H", (65, 65535, 67)),
    # a Boolean is a byte, null -128, and an instant its nanoseconds, null the null long
    ("Z", "=3b", (1, -128, 0)),
    ("T", "=3q", (1704067200000000000, -9223372036854775808, 1704067202000000000)),
):
    assert isinstance(values[name], bytearray), name
    assert struct.unpack(form, values[name]) == expected, (name, struct.unpack(form, values[name]))
assert values["S"] == ["s0", None, "s2"]
assert _engine.native.column_values(source.j_table, []) == []
assert [len(v) for v in _engine.native.column_values(source.head(0).j_table, ["I", "S"])] == [0, 0]
try:
    _engine.native.column_values(source.j_table, ["Nope"])
except RuntimeError as e:
    assert str(e) == "the table has no column Nope", str(e)
else:
    raise AssertionError("read")
"#,
        );
    }

    /// Behavior: `io.deephaven.extensions.barrage.util.TableToArrowConverter` and
    /// `io.deephaven.extensions.barrage.util.ArrowToTableConverter`
    #[tokio::test]
    async fn a_snapshot_read_through_its_capsule_is_the_table_again() {
        let python = Python::start().await;
        python.run(SOURCE);
        python.run(
            r#"
snapshot = _engine.native.arrow_snapshot(source.j_table)
capsule = snapshot.__arrow_c_stream__()
assert type(capsule).__name__ == "PyCapsule" and "arrow_array_stream" in repr(capsule)
again = Table(_engine.native.table_from_arrow(snapshot))
no_rows = Table(_engine.native.table_from_arrow(_engine.native.arrow_snapshot(source.head(0).j_table)))


class Stream:
    def __init__(self, capsule):
        self.capsule = capsule

    def __arrow_c_stream__(self):
        return self.capsule


# a stream is read once: what is left in its capsule is a released stream
spent = Stream(capsule)
once = Table(_engine.native.table_from_arrow(spent))
try:
    _engine.native.table_from_arrow(spent)
except RuntimeError as e:
    assert str(e).startswith("unreadable Arrow stream: "), str(e)
else:
    raise AssertionError("read twice")
try:
    _engine.native.table_from_arrow(Stream(5))
except TypeError as e:
    assert str(e) == "__arrow_c_stream__ must return a capsule of an Arrow C stream", str(e)
else:
    raise AssertionError("read a number")
"#,
        );
        assert_same_as_source(&python, &["again", "once"]);
        let no_rows = python.table("no_rows");
        assert_eq!(no_rows.definition(), python.table("source").definition());
        assert!(no_rows.row_set().is_empty());
    }

    /// Behavior: `io.deephaven.integrations.python.PythonTableDataReader#readChunkColumnMajor`
    #[tokio::test]
    async fn rows_hold_instants_and_arrays_and_refuse_vectors() {
        let python = Python::start().await;
        python.run(SOURCE);
        python.run(
            r#"
from deephaven import constants, dtypes, new_table
from deephaven.column import InputColumn
rows = list(source.iter_dict(["I", "T"]))
assert [str(row["T"]) for row in rows] == ["2024-01-01T00:00:00Z", "None", "2024-01-01T00:00:02Z"]
assert rows[1]["T"] is None and type(rows[0]["T"]).__name__ == "Instant"
assert [str(row.T) for row in source.iter_tuple(["T"], chunk_size=2)][2] == "2024-01-01T00:00:02Z"


def cells(dtype, *values):
    return [None if value is None else dtypes.array(dtype, value) for value in values]


arrays = new_table([
    InputColumn("Ints", dtypes.int32_array, input_data=cells(dtypes.int32, [1, constants.NULL_INT, 3], None, [])),
    InputColumn("Strings", dtypes.string_array, input_data=cells(dtypes.string, ["a", None], None, ["b"])),
    InputColumn("Times", dtypes.instant_array,
                input_data=cells(dtypes.Instant, ["2024-01-01T00:00:00Z", None], None, [])),
    InputColumn("Bools", dtypes.boolean_array, input_data=cells(dtypes.bool_, [True, None], [False], None)),
])
rows = list(arrays.iter_dict())
assert [None if row["Ints"] is None else list(row["Ints"]) for row in rows] == [[1, constants.NULL_INT, 3], None, []]
assert [None if row["Strings"] is None else list(row["Strings"]) for row in rows] == [["a", None], None, ["b"]]
assert [None if row["Bools"] is None else list(row["Bools"]) for row in rows] == [[True, None], [False], None]
first = rows[0]["Times"]
assert len(first) == 2 and str(first[0]) == "2024-01-01T00:00:00Z" and first[1] is None
assert rows[1]["Times"] is None and len(rows[2]["Times"]) == 0
# Java hands out the io.deephaven.vector.Vector object of a grouped column, which has no
# counterpart here
grouped = source.update("K = i % 2").group_by("K")
for read in (lambda: list(grouped.iter_dict()), lambda: list(grouped.iter_tuple(["I"]))):
    try:
        read()
    except DHError as e:
        assert "PythonTableDataReader#readChunkColumnMajor" in str(e), str(e)
    else:
        raise AssertionError("read")
assert list(grouped.iter_dict(["K"])) == [{"K": 0}, {"K": 1}]
"#,
        );
    }

    /// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#convertArrowSchema`
    #[tokio::test]
    async fn the_libraries_convert_where_they_are_installed() {
        let python = Python::start().await;
        python.run(SOURCE);
        python.run(
            r#"
try:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
except ImportError:
    pa = None
from_arrow = from_pandas = source
if pa is not None:
    from deephaven import arrow, numpy, pandas
    both = numpy.to_numpy(source.update("J = i * 10"), ["J", "I"])
    assert both.shape == (3, 2) and both.tolist() == [[0, 0], [10, -2147483648], [20, 2]]
    assert str(numpy.to_numpy(source, ["T"]).dtype) == "datetime64[ns]"
    frame = pandas.to_pandas(source)
    assert [str(kind) for kind in frame.dtypes] == [
        "Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "UInt16", "string", "boolean",
        "datetime64[ns, UTC]"]
    assert frame["I"][1] is pd.NA and frame["T"][1] is pd.NaT
    table = arrow.to_arrow(source)
    assert table.column("I").to_pylist() == [0, None, 2] and table.num_rows == 3
    assert table.schema.field("C").metadata[b"deephaven:type"] == b"char"
    from_arrow = arrow.to_table(table)
    from_pandas = pandas.to_table(frame)
    # an Arrow type Java has no column type for is refused with the Java method
    for refused in (pa.uint64(), pa.date32(), pa.duration("ns")):
        try:
            arrow.to_table(pa.table({"X": pa.array([1], refused)}))
        except DHError as e:
            assert "failed to create a Deephaven table from a pyarrow table." in str(e), str(e)
            assert "BarrageUtil#" in str(e), str(e)
        else:
            raise AssertionError(str(refused))
"#,
        );
        assert_same_as_source(&python, &["from_arrow", "from_pandas"]);
    }
}
