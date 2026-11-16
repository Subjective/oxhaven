//! From a path to the files of a table.

use crate::schema::legalize;
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

/// One file of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    pub path: PathBuf,
    /// The values of the partitions the file is in, as its directories write them: one
    /// for each key, but for a file with fewer directories above it than the first file.
    pub values: Vec<String>,
}

/// The files of a table.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Files {
    /// The files: in the order of their paths, or for a metadata file in the order it
    /// lists them, which is the order of their rows.
    pub files: Vec<Found>,
    /// The names of the partitioning columns, from the directories named `key=value`.
    pub keys: Vec<String>,
    /// How the files were found to be laid out.
    pub layout: Layout,
    /// The metadata file the files were read from, which also has the table's columns.
    pub metadata: Option<Metadata>,
}

/// A `_metadata` file and what it says of the files it lists.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Metadata {
    pub path: PathBuf,
    /// The `_common_metadata` file beside it, if there is one.
    pub common: Option<PathBuf>,
    /// For each file, in order, how many of its row groups the metadata file lists.
    pub row_groups: Vec<usize>,
}

impl Files {
    fn unpartitioned(paths: Vec<PathBuf>, layout: Layout) -> Self {
        let found = |path| Found {
            path,
            values: Vec::new(),
        };
        Files {
            files: paths.into_iter().map(found).collect(),
            keys: Vec::new(),
            layout,
            metadata: None,
        }
    }
}

const PARQUET_EXTENSION: &str = ".parquet";
const METADATA_FILE: &str = "_metadata";
const COMMON_METADATA_FILE: &str = "_common_metadata";

/// The most levels of partition directories Java follows when it infers the columns.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#MAX_PARTITIONING_LEVELS_INFERENCE`
const MAX_PARTITIONING_LEVELS: usize = 32;

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
/// `refreshing` says that the table is to follow files that appear later, which a
/// metadata file cannot.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#readTable(String, ParquetInstructions)`
/// and the `read*Table` methods it chooses between
pub(crate) fn find(
    path: &str,
    layout: Option<Layout>,
    refreshing: bool,
) -> Result<Files, ReadError> {
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
            Ok(Files::unpartitioned(vec![location.to_path_buf()], layout))
        }
        Layout::FlatPartitioned => Ok(Files::unpartitioned(flat(location)?, layout)),
        Layout::KvPartitioned => key_value(location),
        Layout::MetadataPartitioned => {
            if refreshing {
                // Behavior: the constructor of `ParquetMetadataFileLayout`
                let text = "ParquetMetadataFileLayout does not support refreshing";
                return Err(ReadError::Invalid(text.to_string()));
            }
            let metadata = if path.ends_with(&format!("/{METADATA_FILE}")) {
                location.to_path_buf()
            } else if is_metadata_file || is_parquet_file {
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
            metadata_file(&metadata)
        }
    }
}

/// The files the metadata file at `path` lists, in the order it first names them, which
/// is the order of their rows.
///
/// Behavior: the constructor of
/// `io.deephaven.parquet.table.layout.ParquetMetadataFileLayout`, for the files; the
/// columns are read by the caller
fn metadata_file(path: &Path) -> Result<Files, ReadError> {
    let file = fs::File::open(path).map_err(|error| io_error(path, error))?;
    let footer = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(|error| ReadError::Io(format!("{}: {error}", path.display())))?;
    let root = path.parent().unwrap_or(Path::new(""));
    let mut files: Vec<Found> = Vec::new();
    let mut row_groups: Vec<usize> = Vec::new();
    for (index, group) in footer.row_groups().iter().enumerate() {
        let name = group.columns().first().and_then(|chunk| chunk.file_path());
        let Some(name) = name.filter(|name| !name.is_empty()) else {
            return Err(ReadError::Invalid(format!(
                "Missing parquet file name for row groups [{index}] in {}",
                path.display()
            )));
        };
        let file = root.join(name);
        match files.iter().position(|found| found.path == file) {
            Some(position) => row_groups[position] += 1,
            None => {
                files.push(Found {
                    path: file,
                    values: Vec::new(),
                });
                row_groups.push(1);
            }
        }
    }
    let common = root.join(COMMON_METADATA_FILE);
    Ok(Files {
        files,
        keys: Vec::new(),
        layout: Layout::MetadataPartitioned,
        metadata: Some(Metadata {
            path: path.to_path_buf(),
            common: common.exists().then_some(common),
            row_groups,
        }),
    })
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

/// The visible Parquet files under `root`, in directories that are not hidden, with the
/// partitions the directories between the root and each file name.
///
/// The first file, by path, says how many partitioning columns there are and names them:
/// each directory above it is `key=value`, and the key becomes a column name as a column
/// of a file does, without the space made an underscore. Every other file must be under
/// directories of the same keys, and no more of them.
///
/// Java takes the files in the order its walk of the directory finds them, which is the
/// file system's; this takes them by path.
///
/// Behavior: `io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout#findKeys`,
/// `io.deephaven.engine.table.impl.locations.local.URIStreamKeyValuePartitionLayout#findKeys`
/// and `#getPartitions`, `KeyValuePartitionLayout#processSubdirectoryInternal`,
/// `io.deephaven.parquet.base.ParquetUtils#isVisibleParquetFile`
fn key_value(root: &Path) -> Result<Files, ReadError> {
    let kind = fs::metadata(root).map_err(|error| io_error(root, error))?;
    if !kind.is_dir() {
        // Java walks the path and finds the file itself, which is not below the root:
        // no file, or for a name that ends in `.parquet` a `NullPointerException`
        return if is_visible_parquet_name(&file_name(root)) {
            let text = format!("{} is not a directory", root.display());
            Err(ReadError::Invalid(text))
        } else {
            Ok(Files::unpartitioned(Vec::new(), Layout::KvPartitioned))
        };
    }
    let mut paths = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for path in entries(&directory)? {
            let name = file_name(&path);
            if path.is_dir() {
                if !name.starts_with('.') {
                    pending.push(path);
                }
            } else if is_visible_parquet_name(&name) {
                paths.push(path);
            }
        }
    }
    let mut files = Vec::with_capacity(paths.len());
    // the name of each key as its directories write it, and as a column
    let mut written: Vec<String> = Vec::new();
    let mut keys: Vec<String> = Vec::new();
    for (index, path) in sorted(paths).into_iter().enumerate() {
        let relative = path.strip_prefix(root).unwrap_or(&path);
        let shown = relative.display();
        let mut directories: Vec<String> = relative
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        directories.pop();
        let invalid = |text: String| Err(ReadError::Invalid(text));
        if index == 0 && directories.len() > MAX_PARTITIONING_LEVELS {
            return invalid(format!(
                "Too many partitioning levels at {shown}, count = {}, maximum expected are \
                 {MAX_PARTITIONING_LEVELS}",
                directories.len()
            ));
        }
        if index > 0 && directories.len() > keys.len() {
            return invalid(format!(
                "Too many partitioning levels at {shown} (expected {}) based on earlier leaf \
                 nodes in the tree.",
                keys.len()
            ));
        }
        if index > 0 && directories.len() < keys.len() {
            // Java gives the CSV reader a short row, or meets this file first and says
            // the deeper ones have too many levels: which one is up to its walk
            return Err(crate::not_ported(
                "io.deephaven.engine.table.impl.locations.local.URIStreamKeyValuePartitionLayout#getPartitions",
                format!(
                    "{shown} has {} partition directories and an earlier file has {}",
                    directories.len(),
                    keys.len()
                ),
            ));
        }
        let mut values = Vec::with_capacity(directories.len());
        for (level, directory) in directories.iter().enumerate() {
            let Some((key, value)) = directory.split_once('=') else {
                return invalid(format!(
                    "Unexpected directory name format (not key=value) at {shown}"
                ));
            };
            match written.get(level) {
                Some(expected) if expected != key => {
                    return invalid(format!(
                        "Column name mismatch at column index {level}: expected {expected} \
                         found {key} at {shown}"
                    ));
                }
                Some(_) => {}
                None => {
                    keys.push(legalize(key, false, &keys)?);
                    written.push(key.to_string());
                }
            }
            values.push(value.to_string());
        }
        files.push(Found { path, values });
    }
    Ok(Files {
        files,
        keys,
        layout: Layout::KvPartitioned,
        metadata: None,
    })
}
