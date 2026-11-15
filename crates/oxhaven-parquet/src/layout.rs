//! From a path to the files of a table.

use crate::{not_ported, ReadError};
use std::fs;
use std::path::{Path, PathBuf};

/// How the files of a table are laid out.
///
/// Behavior: `io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// One file.
    SingleFile,
    /// The files directly in one directory.
    FlatPartitioned,
    /// The files under a directory, in directories named `key=value`.
    KvPartitioned,
    /// The files a `_metadata` file lists.
    MetadataPartitioned,
}

/// The files of a table, in the order of their rows.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Files {
    pub paths: Vec<PathBuf>,
    /// Whether the table is one file by its layout, not by how many files were found.
    pub single: bool,
}

const PARQUET_EXTENSION: &str = ".parquet";
const METADATA_FILE: &str = "_metadata";
const COMMON_METADATA_FILE: &str = "_common_metadata";

const KEY_VALUE_LAYOUT: &str =
    "io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout#findKeys";
const METADATA_LAYOUT: &str = "io.deephaven.parquet.table.layout.ParquetMetadataFileLayout#create";

fn io_error(path: &Path, error: std::io::Error) -> ReadError {
    ReadError::Io(format!("{}: {error}", path.display()))
}

/// Whether `path` starts with a URI scheme, `s3:` for example.
///
/// Behavior: `io.deephaven.base.FileUtils#startsWithScheme`
pub(crate) fn has_scheme(path: &str) -> bool {
    let Some((scheme, _)) = path.split_once(':') else {
        return false;
    };
    let mut characters = scheme.chars();
    characters.next().is_some_and(|c| c.is_ascii_alphabetic())
        && characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// The files of the table at `path`, laid out as `layout` says or, without one, as the
/// path does: a name that ends in `.parquet` is one file, `_metadata` and
/// `_common_metadata` are metadata files, and everything else is a directory.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#readTable(String, ParquetInstructions)`
/// and the `read*Table` methods it chooses between
pub(crate) fn find(path: &str, layout: Option<Layout>) -> Result<Files, ReadError> {
    if path.is_empty() {
        // Behavior: `io.deephaven.base.FileUtils#convertToURI`
        let text = "Cannot convert empty source to URI";
        return Err(ReadError::Invalid(text.to_string()));
    }
    if has_scheme(path) {
        return Err(not_ported(
            "io.deephaven.util.channel.SeekableChannelsProviderLoader#load",
            format_args!("a path with a URI scheme: {path}"),
        ));
    }
    // Behavior: `io.deephaven.parquet.base.ParquetUtils#isParquetFile`, `#isMetadataFile`
    let is_parquet_file = path.ends_with(PARQUET_EXTENSION);
    let is_metadata_file = !is_parquet_file
        && [METADATA_FILE, COMMON_METADATA_FILE]
            .iter()
            .any(|name| path.ends_with(&format!("/{name}")));
    let location = Path::new(path);
    let layout = layout.unwrap_or(if is_parquet_file {
        Layout::SingleFile
    } else if is_metadata_file || location.join(METADATA_FILE).exists() {
        Layout::MetadataPartitioned
    } else {
        // Behavior: `ParquetTools#readPartitionedTableDirectory`: the key-value layout
        // also reads a directory that has no partition directories
        Layout::KvPartitioned
    });
    let directory = |paths| Files {
        paths,
        single: false,
    };
    match layout {
        Layout::SingleFile => {
            if !is_parquet_file {
                // Behavior: `ParquetTableLocationKey#validateParquetFile`
                let slash = if is_metadata_file { "" } else { "/" };
                return Err(ReadError::Invalid(format!(
                    "Parquet file must end in .parquet, found: {path}{slash}"
                )));
            }
            fs::metadata(location).map_err(|error| io_error(location, error))?;
            Ok(Files {
                paths: vec![location.to_path_buf()],
                single: true,
            })
        }
        Layout::FlatPartitioned => flat(location).map(directory),
        Layout::KvPartitioned => key_value(location).map(directory),
        Layout::MetadataPartitioned => {
            let metadata = if is_metadata_file {
                location.to_path_buf()
            } else if is_parquet_file {
                location.with_file_name(METADATA_FILE)
            } else {
                location.join(METADATA_FILE)
            };
            if !metadata.exists() {
                return Err(ReadError::Invalid(format!(
                    "Parquet metadata file {} does not exist",
                    metadata.display()
                )));
            }
            Err(not_ported(
                METADATA_LAYOUT,
                format_args!("the metadata file {}", metadata.display()),
            ))
        }
    }
}

/// Whether a file named `name` is a Parquet file that is not hidden.
fn is_visible_parquet_name(name: &str) -> bool {
    name.ends_with(PARQUET_EXTENSION) && !name.starts_with('.')
}

fn file_name(path: &Path) -> String {
    let name = path.file_name().unwrap_or_default();
    name.to_string_lossy().into_owned()
}

/// The entries of `directory`.
fn entries(directory: &Path) -> Result<Vec<PathBuf>, ReadError> {
    let list = fs::read_dir(directory).map_err(|error| io_error(directory, error))?;
    list.map(|entry| {
        let entry = entry.map_err(|error| io_error(directory, error))?;
        Ok(entry.path())
    })
    .collect()
}

/// Files sorted as Java sorts the location keys of a table that does not refresh: by
/// path. Java compares the paths as URIs, which orders names outside ASCII by their
/// percent-encoded form; this compares the paths themselves.
///
/// Behavior: `io.deephaven.engine.table.impl.locations.local.URITableLocationKey#compareTo`
/// through `KnownLocationKeyFinder#copyFrom(keyFinder, Comparator.naturalOrder())`
fn sorted(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    paths
}

/// The visible Parquet files directly in `directory`.
///
/// Behavior: `io.deephaven.parquet.table.layout.ParquetFlatPartitionedLayout#findKeys`
fn flat(directory: &Path) -> Result<Vec<PathBuf>, ReadError> {
    let mut files = entries(directory)?;
    files.retain(|path| is_visible_parquet_name(&file_name(path)));
    Ok(sorted(files))
}

/// The visible Parquet files under `root`, in directories that are not hidden. A file in a
/// directory below the root is in a partition, which is not ported.
///
/// Behavior: `io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout#findKeys`,
/// `io.deephaven.parquet.base.ParquetUtils#isVisibleParquetFile`
fn key_value(root: &Path) -> Result<Vec<PathBuf>, ReadError> {
    let kind = fs::metadata(root).map_err(|error| io_error(root, error))?;
    if !kind.is_dir() {
        // Java walks the path and finds the file itself, which is not below the root:
        // no file, or for a name that ends in `.parquet` a `NullPointerException`
        return if is_visible_parquet_name(&file_name(root)) {
            let text = format!("{} is not a directory", root.display());
            Err(ReadError::Invalid(text))
        } else {
            Ok(Vec::new())
        };
    }
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for path in entries(&directory)? {
            let name = file_name(&path);
            if path.is_dir() {
                if !name.starts_with('.') {
                    pending.push(path);
                }
            } else if is_visible_parquet_name(&name) {
                if directory != root {
                    return Err(not_ported(
                        KEY_VALUE_LAYOUT,
                        format_args!("a file in a partition directory: {}", path.display()),
                    ));
                }
                files.push(path);
            }
        }
    }
    Ok(sorted(files))
}
