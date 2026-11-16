//! The `_metadata` and `_common_metadata` files of a write: the footers of the files just
//! written, as one footer.
//!
//! `_metadata` has the schema of the files and every row group of every file, each with
//! the path of its file from the directory of the metadata files. `_common_metadata` has
//! no row group, and its schema starts with the partitioning columns of a partitioned
//! write, which no file holds. Both have the `deephaven` entry of each file under a key
//! of that file, and one for the table.
//!
//! Behavior: `io.deephaven.parquet.table.ParquetMetadataFileWriterImpl`

use super::{invalid, METADATA_KEY};
use crate::WriteError;
use parquet::file::metadata::{
    FileMetaData, KeyValue, ParquetMetaData, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) const METADATA_FILE: &str = "_metadata";
pub(super) const COMMON_METADATA_FILE: &str = "_common_metadata";

/// Behavior: `org.apache.parquet.hadoop.ParquetFileWriter#MAGIC`
const MAGIC: &[u8; 4] = b"PAR1";

/// Where the metadata files of a write go, and what only they know.
pub(super) struct MetadataFiles {
    /// The directory of the two files; every file of the write is in it or under it.
    pub(super) root: PathBuf,
    /// The fields of the partitioning columns, which come first in `_common_metadata`.
    pub(super) partitioning: Vec<TypePtr>,
}

fn problem(error: parquet::errors::ParquetError) -> WriteError {
    invalid(format!("the metadata files: {error}"))
}

/// The key of the `deephaven` entry of the file at `relative` in a metadata file.
///
/// Behavior: `io.deephaven.parquet.base.ParquetUtils#getPerFileMetadataKey`
fn per_file_key(relative: &str) -> String {
    format!("deephaven_per_file_{}", relative.replace('/', "_"))
}

/// A footer alone as a file: the magic number, then what ends a Parquet file.
///
/// Behavior: `ParquetMetadataFileWriterImpl#writeMetadataFile`
fn file(footer: &ParquetMetaData) -> Result<Vec<u8>, WriteError> {
    let mut bytes = MAGIC.to_vec();
    ParquetMetaDataWriter::new(&mut bytes, footer)
        .finish()
        .map_err(problem)?;
    Ok(bytes)
}

/// The contents of `_metadata` and of `_common_metadata` for the files with the footers
/// `footers`, in the order they were written. `table_info` is the `deephaven` entry of
/// the table: the version and the column types its files share.
///
/// Every file of one write has the same schema here, since one list of columns makes
/// them all; Java merges the schemas, which are the same there too.
///
/// Behavior: `ParquetMetadataFileWriterImpl#writeMetadataFiles` and `#mergeMetadata`
pub(super) fn files(
    metadata: &MetadataFiles,
    footers: &[(PathBuf, ParquetMetaData)],
    table_info: String,
) -> Result<(Vec<u8>, Vec<u8>), WriteError> {
    let Some((_, first)) = footers.first() else {
        return Err(invalid("No parquet files to write metadata for"));
    };
    let mut entries: Vec<KeyValue> = Vec::with_capacity(footers.len() + 1);
    let mut groups: Vec<RowGroupMetaData> = Vec::new();
    for (destination, footer) in footers {
        let relative = relative_path(&metadata.root, destination)?;
        let key = per_file_key(&relative);
        if entries.iter().any(|entry| entry.key == key) {
            return Err(invalid(format!(
                "Could not merge metadata for file {} because it has conflicting file key: {key}",
                destination.display()
            )));
        }
        let own = footer.file_metadata().key_value_metadata();
        let own = own.and_then(|entries| entries.iter().find(|entry| entry.key == METADATA_KEY));
        entries.push(KeyValue::new(
            key,
            own.and_then(|entry| entry.value.clone()),
        ));
        for group in footer.row_groups() {
            let columns = group.columns().iter().map(|column| {
                let column = column.clone().into_builder();
                column.set_file_path(relative.clone()).build()
            });
            let columns = columns.collect::<Result<Vec<_>, _>>().map_err(problem)?;
            let group = group.clone().into_builder().set_column_metadata(columns);
            groups.push(group.build().map_err(problem)?);
        }
    }
    entries.push(KeyValue::new(METADATA_KEY.to_string(), table_info));

    let of_file = first.file_metadata();
    let footer = |schema: Arc<SchemaDescriptor>, groups: Vec<RowGroupMetaData>| {
        let rows = groups.iter().map(RowGroupMetaData::num_rows).sum();
        let file = FileMetaData::new(
            of_file.version(),
            rows,
            of_file.created_by().map(str::to_string),
            Some(entries.clone()),
            schema,
            None,
        );
        ParquetMetaData::new(file, groups)
    };
    let all = file(&footer(of_file.schema_descr_ptr(), groups))?;

    let root = of_file.schema();
    let mut fields = metadata.partitioning.clone();
    for field in root.get_fields() {
        match fields.iter().find(|other| other.name() == field.name()) {
            // Java takes the union of two fields with one name, which is the field when
            // they are the same
            Some(other) if other == field => {}
            Some(_) => {
                return Err(invalid(format!(
                    "can not merge the two fields named {} of the partitioning columns and of \
                     the files",
                    field.name()
                )))
            }
            None => fields.push(field.clone()),
        }
    }
    let with_partitions = Type::group_type_builder(root.name())
        .with_fields(fields)
        .build()
        .map_err(problem)?;
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(with_partitions)));
    let common = file(&footer(schema, Vec::new()))?;
    Ok((all, common))
}

/// The path of `destination` from `root`, with `/` between its parts.
fn relative_path(root: &Path, destination: &Path) -> Result<String, WriteError> {
    let Ok(relative) = destination.strip_prefix(root) else {
        return Err(invalid(format!(
            "All destinations must be nested under the provided metadata root directory, \
             provided destination {} is not under {}",
            destination.display(),
            root.display()
        )));
    };
    let parts = relative.components();
    let parts: Vec<_> = parts
        .map(|part| part.as_os_str().to_string_lossy())
        .collect();
    Ok(parts.join("/"))
}
