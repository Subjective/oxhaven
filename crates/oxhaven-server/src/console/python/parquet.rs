//! `deephaven.parquet` reaches the Parquet reader here.
//!
//! Java has no gRPC call that reads a file, so this is not a table service operation: the
//! Python module calls [`read_parquet`] with the arguments of `deephaven.parquet.read`,
//! and the table comes from `oxhaven_parquet::read` (`docs/STORAGE.md`).

use oxhaven_contracts::table::Table;
use oxhaven_parquet::{ColumnInstruction, Layout, ReadError, ReadOptions, UnsignedLongTarget};
use pyo3::exceptions::{PyNotImplementedError, PyOSError, PyValueError};
use pyo3::prelude::*;

use super::TableHandle;
use crate::table::support::validate_column_name;

/// The Python exception of a failed read; `deephaven.parquet.read` wraps it in a `DHError`.
fn exception(error: ReadError) -> PyErr {
    let text = error.to_string();
    match error {
        ReadError::NotPorted { .. } => PyNotImplementedError::new_err(text),
        ReadError::Invalid(_) => PyValueError::new_err(text),
        ReadError::Io(_) => PyOSError::new_err(text),
    }
}

fn layout(name: &str) -> PyResult<Layout> {
    Ok(match name {
        "SINGLE_FILE" => Layout::SingleFile,
        "FLAT_PARTITIONED" => Layout::FlatPartitioned,
        "KV_PARTITIONED" => Layout::KvPartitioned,
        "METADATA_PARTITIONED" => Layout::MetadataPartitioned,
        other => return Err(PyValueError::new_err(format!("no file layout {other}"))),
    })
}

fn unsigned_long_target(name: &str) -> PyResult<Option<UnsignedLongTarget>> {
    Ok(match name {
        // Java's default, the same as naming none
        "BIG_INTEGER" => None,
        "LONG" => Some(UnsignedLongTarget::Long),
        "SIGNED_LONG" => Some(UnsignedLongTarget::SignedLong),
        other => {
            return Err(PyValueError::new_err(format!(
                "no unsigned long target {other}"
            )))
        }
    })
}

/// One column instruction: the name in the file, the name in the table, the codec, the
/// unsigned long target.
type Column = (String, Option<String>, Option<String>, Option<String>);

/// The table of the Parquet file or directory at `path`.
///
/// Behavior: `deephaven.parquet.read`, which builds a `ParquetInstructions` from its
/// arguments and calls `io.deephaven.parquet.table.ParquetTools#readTable`
#[pyfunction]
pub(super) fn read_parquet(
    py: Python<'_>,
    path: &str,
    file_layout: Option<&str>,
    is_refreshing: bool,
    is_legacy_parquet: bool,
    columns: Vec<Column>,
) -> PyResult<TableHandle> {
    let mut options = ReadOptions {
        layout: file_layout.map(layout).transpose()?,
        refreshing: is_refreshing,
        legacy: is_legacy_parquet,
        columns: Vec::with_capacity(columns.len()),
    };
    for (parquet_name, name, codec, target) in columns {
        options.columns.push(ColumnInstruction {
            parquet_name,
            name,
            codec,
            unsigned_long: target
                .as_deref()
                .map(unsigned_long_target)
                .transpose()?
                .flatten(),
        });
    }
    let table = py
        .detach(|| oxhaven_parquet::read(path, &options))
        .map_err(exception)?;
    for column in table.definition().columns() {
        // a reserved word is a name Java changes; the reader does not know the words
        if validate_column_name(&column.name).is_err() {
            return Err(exception(ReadError::NotPorted {
                java: "io.deephaven.api.util.NameValidator#legalizeColumnName",
                what: format!("column name '{}'", column.name),
            }));
        }
    }
    Ok(TableHandle { table })
}
