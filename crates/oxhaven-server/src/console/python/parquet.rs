//! `deephaven.parquet` reaches the Parquet reader and writer here.
//!
//! Java has no gRPC call that reads or writes a file, so these are not table service
//! operations: the Python module calls [`read_parquet`] with the arguments of
//! `deephaven.parquet.read`, and the table comes from `oxhaven_parquet::read`; it calls
//! [`write_parquet`] with the arguments of `write` and `batch_write`, and the files come
//! from `oxhaven_parquet::write` (`docs/STORAGE.md`).

use std::sync::Arc;

use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_parquet::{
    ColumnInstruction, Layout, ReadError, ReadOptions, RowGroups, UnsignedLongTarget, WriteColumn,
    WriteOptions,
};
use pyo3::exceptions::{PyNotImplementedError, PyOSError, PyValueError};
use pyo3::prelude::*;

use super::ops::{failure, guarded};
use super::TableHandle;
use crate::schema::column_type_for_java_names;
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

/// One column instruction of a write: the name in the table, the name in the file, the
/// codec, whether the column gets a dictionary whatever its size.
type WrittenColumn = (String, Option<String>, Option<String>, bool);

/// A column of a table definition given in Python: the name and the Java class name of
/// the type, as `new_table` takes them.
type DefinedColumn = (String, String);

/// The row groups of a `deephaven.parquet.RowGroupInfo`, which the package hands over as
/// the name of the method that made it, its number and its columns.
type RowGroupInfo = (String, Option<i64>, Vec<String>);

/// A Python integer as the Java `int` jpy makes of it for a setter of
/// `ParquetInstructions.Builder`: the low 32 bits.
fn java_int(value: Option<i64>) -> Option<i32> {
    value.map(|value| value as i32)
}

fn row_groups((kind, number, columns): RowGroupInfo) -> PyResult<RowGroups> {
    let needed = || PyValueError::new_err(format!("{kind} row groups need a number"));
    Ok(match kind.as_str() {
        "single_group" => RowGroups::Single,
        "max_groups" => RowGroups::MaxGroups(number.ok_or_else(needed)?),
        "max_rows" => RowGroups::MaxRows(number.ok_or_else(needed)?),
        "by_groups" => RowGroups::ByGroups {
            columns,
            max_rows: number,
        },
        other => return Err(PyValueError::new_err(format!("no row groups {other}"))),
    })
}

/// Writes each of `tables` to the Parquet file at the path of `paths` with the same
/// position; all of them or none. The files have the columns of `definition`, or of the
/// table `definition_of`, or without either the columns of the tables.
///
/// The tables are read under the update graph's shared lock when one of them changes,
/// taken while the interpreter lock is released (`docs/PYTHON_UDF.md`, "Locks"), so the
/// files hold the rows of one cycle. Java asks for the same: it refuses to write a
/// refreshing table from a thread that holds no lock of the graph.
///
/// Behavior: `deephaven.parquet.write` and `batch_write`, which build a
/// `ParquetInstructions` from their arguments and call
/// `io.deephaven.parquet.table.ParquetTools#writeTables`
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(super) fn write_parquet(
    py: Python<'_>,
    tables: Vec<PyRef<'_, TableHandle>>,
    paths: Vec<String>,
    definition: Option<Vec<DefinedColumn>>,
    definition_of: Option<PyRef<'_, TableHandle>>,
    columns: Vec<WrittenColumn>,
    compression_codec_name: Option<String>,
    max_dictionary_keys: Option<i64>,
    max_dictionary_size: Option<i64>,
    target_page_size: Option<i64>,
    generate_metadata_files: bool,
    index_columns: Vec<Vec<String>>,
    row_group_info: Option<RowGroupInfo>,
) -> PyResult<()> {
    let definition = match (definition_of, definition) {
        (Some(table), _) => Some(table.table.definition().clone()),
        (None, Some(columns)) => {
            let columns = columns.into_iter().map(|(name, type_name)| {
                let column_type = column_type_for_java_names(&type_name, None);
                let column_type = column_type.ok_or_else(|| {
                    PyValueError::new_err(format!("the engine has no column type for {type_name}"))
                })?;
                Ok(ColumnDefinition::new(name, column_type))
            });
            let columns = columns.collect::<PyResult<Vec<_>>>()?;
            let definition = TableDefinition::new(columns);
            Some(definition.map_err(|duplicate| PyValueError::new_err(duplicate.to_string()))?)
        }
        (None, None) => None,
    };
    let columns = columns.into_iter();
    let options = WriteOptions {
        definition,
        columns: columns
            .map(|(name, parquet_name, codec, use_dictionary)| WriteColumn {
                name,
                parquet_name,
                codec,
                use_dictionary,
            })
            .collect(),
        compression: compression_codec_name,
        max_dictionary_keys: java_int(max_dictionary_keys),
        max_dictionary_size: java_int(max_dictionary_size),
        target_page_size: java_int(target_page_size),
        generate_metadata_files,
        index_columns,
        row_groups: row_group_info
            .map(row_groups)
            .transpose()?
            .unwrap_or_default(),
    };
    let tables: Vec<Arc<dyn Table>> = tables.iter().map(|table| table.table.clone()).collect();
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    let written = py.detach(|| {
        guarded(&tables, || {
            Ok(oxhaven_parquet::write(&tables, &paths, &options))
        })
    });
    written.map_err(failure)?.map_err(exception)
}

/// Removes the file or directory at `path`.
///
/// Behavior: `deephaven.parquet.delete`, which calls
/// `io.deephaven.parquet.table.ParquetTools#deleteTable`
#[pyfunction]
pub(super) fn delete_parquet(py: Python<'_>, path: &str) -> PyResult<()> {
    py.detach(|| oxhaven_parquet::delete(path))
        .map_err(exception)
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
zstd = read("{DATA}/codec_zstd.parquet")
"#
        ));
        assert_eq!(changes.error, None);
        // the codec Java writes by default
        let snappy = ints(&*table("snappy"), "I");
        let expected = [-300000, -200000, -100000, 100000, 200000, 300000].map(Some);
        assert_eq!(snappy[..3], expected[..3]);
        assert_eq!(snappy[3], None);
        assert_eq!(snappy[4..], expected[3..]);
        let expected: Vec<_> = (0..20).map(Some).collect();
        assert_eq!(ints(&*table("zstd"), "X"), expected);
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
        let brotli = failure(&format!(r#"read("{DATA}/codec_brotli.parquet")"#));
        assert!(
            brotli.starts_with("not ported yet: ")
                && brotli.contains("pages compressed with BROTLI"),
            "{brotli}"
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

    #[tokio::test]
    async fn deephaven_parquet_writes_tables_and_raises_dherror() {
        let _turn = ONE_INTERPRETER.lock().await;
        let server = TestServer::start_with(|state| super::super::start(state).unwrap()).await;
        let console = server.state.console.clone().unwrap();
        let table = |name: &str| {
            let value = server.state.scope.get(name);
            value.unwrap().as_table().unwrap().clone()
        };
        let root =
            std::env::temp_dir().join(format!("oxhaven-console-write-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let root = root.to_str().unwrap();

        let changes = console.evaluate(&format!(
            r#"
import os
from deephaven import dtypes, empty_table, time_table
from deephaven.parquet import ColumnInstruction, RowGroupInfo, batch_write, delete, read, write

source = empty_table(10).update(["X = i", "S = `s` + (i % 3)", "K = (int)(i / 4)"])
write(source, "{root}/plain.parquet")
plain = read("{root}/plain.parquet")
write(
    source,
    "{root}/options.parquet",
    table_definition={{"S": dtypes.string, "N": dtypes.int32, "X": dtypes.int32}},
    col_instructions=[ColumnInstruction(column_name="X", parquet_column_name="ex")],
    compression_codec_name="ZSTD",
    max_dictionary_keys=2,
    target_page_size=4096,
    row_group_info=RowGroupInfo.max_rows(4),
)
options = read("{root}/options.parquet")
batch_write([source.head(3), source.tail(2)], ["{root}/a/first.parquet", "{root}/a/second.parquet"])
second = read("{root}/a/second.parquet")
# a table that changes is written as it is
ticking = time_table("PT1H", start_time="2024-01-01T00:00:00Z").update_view("X = (int)ii")
write(ticking, "{root}/ticking.parquet")
written = read("{root}/ticking.parquet")
write(source, "{root}/gone/deep/table.parquet")
delete("{root}/gone")
delete("{root}/nothing")
left = sorted(os.listdir("{root}"))
"#
        ));
        assert_eq!(changes.error, None);
        let expected: Vec<_> = (0..10).map(Some).collect();
        assert_eq!(ints(&*table("plain"), "X"), expected);
        assert_eq!(ints(&*table("options"), "ex"), expected);
        assert_eq!(ints(&*table("options"), "N"), [None; 10]);
        let options = table("options");
        let names: Vec<_> = options
            .definition()
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(names, ["S", "N", "ex"]);
        assert_eq!(ints(&*table("second"), "X"), [8, 9].map(Some));
        let written = ints(&*table("written"), "X");
        assert!(
            !written.is_empty(),
            "the time table has its first row when it is made"
        );
        let expected: Vec<_> = (0..written.len() as i32).map(Some).collect();
        assert_eq!(written, expected);
        let left = console.evaluate(
            "assert left == ['a', 'options.parquet', 'plain.parquet', 'ticking.parquet'], left",
        );
        assert_eq!(left.error, None);

        // every failure is a DHError with the reference's message, then the cause
        let failure = |code: &str, message: &str| {
            let code = format!("from deephaven.parquet import *\nfrom deephaven import empty_table\nsource = empty_table(4).update('X = i')\n{code}");
            let error = console.evaluate(&code).error.expect("the write fails");
            let last = error.trim_end().lines().last().unwrap().to_string();
            let prefix = format!("deephaven.dherror.DHError: {message} : ");
            let text = last.strip_prefix(&prefix);
            text.unwrap_or_else(|| panic!("not a DHError of a write: {last}"))
                .to_string()
        };
        const WRITE: &str = "failed to write to parquet data.";
        const BATCH: &str = "write multiple tables to parquet data failed.";
        let no_extension = failure(&format!(r#"write(source, "{root}/table")"#), WRITE);
        assert!(
            no_extension.ends_with("does not end in .parquet extension"),
            "{no_extension}"
        );
        assert_eq!(
            failure(
                &format!(r#"batch_write([source], ["{root}/a.parquet", "{root}/b.parquet"])"#),
                BATCH
            ),
            "Number of sources and destinations must match"
        );
        assert_eq!(
            failure(
                &format!(
                    r#"write(source.head(0), "{root}/a.parquet", row_group_info=RowGroupInfo.max_rows(2))"#
                ),
                WRITE
            ),
            "Number of groups must be at least 1, got: 0"
        );
        for (argument, java) in [
            (
                "generate_metadata_files=True",
                "ParquetMetadataFileWriterImpl#writeMetadataFiles",
            ),
            ("index_columns=[['X']]", "Builder#addAllIndexColumns"),
            (
                "compression_codec_name='LZO'",
                "DeephavenCompressorAdapterFactory#getByName",
            ),
            (
                "special_instructions=object()",
                "Builder#setSpecialInstructions",
            ),
        ] {
            let refused = failure(
                &format!(r#"write(source, "{root}/refused.parquet", {argument})"#),
                WRITE,
            );
            assert!(
                refused.starts_with("not ported yet: ") && refused.contains(java),
                "{refused}"
            );
        }
        let partitioned = failure(
            &format!(r#"write_partitioned(source, "{root}/partitioned")"#),
            WRITE,
        );
        assert!(
            partitioned.contains("Table must have partitioning columns to write partitioned data"),
            "{partitioned}"
        );
        let constructor = console
            .evaluate("from deephaven.parquet import RowGroupInfo\nRowGroupInfo.max_groups(0)");
        assert!(constructor.error.unwrap().contains("RuntimeError"));
        let mut left: Vec<_> = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        left.sort();
        assert_eq!(
            left,
            ["a", "options.parquet", "plain.parquet", "ticking.parquet"]
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
