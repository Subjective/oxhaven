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

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array};
    use oxhaven_contracts::table::Table;

    use super::super::tests::ONE_INTERPRETER;
    use crate::test_support::TestServer;

    /// The test files of the reader's own crate.
    const DATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../oxhaven-parquet/tests/data");

    fn ints(table: &dyn Table, name: &str) -> Vec<Option<i32>> {
        let index = table.definition().index_of(name).unwrap();
        let values = table.column(index).chunk(&table.row_set()).unwrap();
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        values.iter().collect()
    }

    #[tokio::test]
    async fn deephaven_parquet_reads_tables_and_raises_dherror() {
        let _turn = ONE_INTERPRETER.lock().await;
        let server = TestServer::start_with(|state| super::super::start(state).unwrap()).await;
        let console = server.state.console.clone().unwrap();
        let table = |name: &str| {
            let value = server.state.scope.get(name);
            value.unwrap().as_table().unwrap().clone()
        };

        let changes = console.evaluate(&format!(
            r#"
from deephaven.parquet import ColumnInstruction, ParquetFileLayout, UnsignedLongTarget, read

flat = read("{DATA}/flat")
doubled = flat.where("P == 1").update("Y = X * 2")
one = read("{DATA}/flat/part2.parquet", file_layout=ParquetFileLayout.SINGLE_FILE)
renamed = read("{DATA}/flat/part0.parquet", col_instructions=[
    ColumnInstruction(column_name="Q", parquet_column_name="X"),
])
signed = read("{DATA}/uint64_big.parquet", col_instructions=[ColumnInstruction(
    column_name="V", parquet_column_name="V", unsigned_long_target=UnsignedLongTarget.SIGNED_LONG,
)])
snappy = read("{DATA}/types_snappy.parquet")
"#
        ));
        assert_eq!(changes.error, None);
        // the codec Java writes by default
        let snappy = ints(&*table("snappy"), "I");
        let expected = [-300000, -200000, -100000, 100000, 200000, 300000].map(Some);
        assert_eq!(snappy[..3], expected[..3]);
        assert_eq!(snappy[3], None);
        assert_eq!(snappy[4..], expected[3..]);
        assert_eq!(table("flat").row_set().size(), 12);
        assert_eq!(ints(&*table("doubled"), "Y"), [0, 2, 4, 6].map(Some));
        assert_eq!(ints(&*table("one"), "X"), [0, 1, 2, 3, 4].map(Some));
        assert_eq!(ints(&*table("renamed"), "Q"), [0, 1, 2].map(Some));
        assert_eq!(table("signed").row_set().size(), 5);

        // every failure is a DHError with the reference's message, then the cause
        let failure = |code: &str| {
            let code = format!("from deephaven.parquet import *\n{code}");
            let error = console.evaluate(&code).error.expect("the read fails");
            let last = error.trim_end().lines().last().unwrap().to_string();
            let text =
                last.strip_prefix("deephaven.dherror.DHError: failed to read parquet data. : ");
            text.unwrap_or_else(|| panic!("not a DHError of a read: {last}"))
                .to_string()
        };
        let missing = failure(&format!(r#"read("{DATA}/nope.parquet")"#));
        assert!(missing.contains("nope.parquet"), "{missing}");
        assert_eq!(
            failure(&format!(
                r#"read("{DATA}/flat/part0.parquet", is_refreshing=True)"#
            )),
            "Unable to have a refreshing single parquet file"
        );
        assert_eq!(
            failure(&format!(
                r#"read("{DATA}/flat", col_instructions=[ColumnInstruction(column_name="Q")])"#
            )),
            "must specify the parquet column name for read."
        );
        // a reserved word is a column name Java changes, which only the server can tell
        assert_eq!(
            failure(&format!(r#"read("{DATA}/reserved.parquet")"#)),
            "not ported yet: io.deephaven.api.util.NameValidator#legalizeColumnName: column name 'int'"
        );
        let zstd = failure(&format!(r#"read("{DATA}/codec_zstd.parquet")"#));
        assert!(
            zstd.starts_with("not ported yet: ") && zstd.ends_with("ZSTD"),
            "{zstd}"
        );
        for (argument, java) in [
            ("table_definition={}", "Builder#setTableDefinition"),
            (
                "special_instructions=object()",
                "Builder#setSpecialInstructions",
            ),
        ] {
            let refused = failure(&format!(r#"read("{DATA}/flat", {argument})"#));
            assert!(
                refused.starts_with("not ported yet: ") && refused.contains(java),
                "{refused}"
            );
        }
        // the reference raises this one itself, a DHError inside the DHError of the read
        let layout = console.evaluate(&format!(
            "from deephaven.parquet import read\nread(\"{DATA}/flat\", file_layout=3)"
        ));
        let layout = layout.error.unwrap();
        assert!(
            layout.contains("Invalid parquet file_layout '3'"),
            "{layout}"
        );
    }
}
