//! `snapshot` and `snapshot_when`. Task 4.9.
//!
//! | Function | Result |
//! | --- | --- |
//! | [`snapshot`] | A static copy of a table as it is now |
//! | [`snapshot_when`] | A copy of a base table that is renewed when a trigger table changes |
//!
//! `snapshot_when` has three forms, chosen by [`SnapshotWhenOptions`]:
//!
//! | Flags | Rows of the result | Columns |
//! | --- | --- | --- |
//! | neither | the base as of the last trigger, at row keys `0..size` | base, then stamps |
//! | `incremental` | the base as of the last trigger, at the base's row keys; only rows that changed since the trigger before are copied and stamped again | base, then stamps |
//! | `history` | one copy of the base per trigger row, appended | stamps, then base |
//!
//! The stamp columns hold the values of the trigger's last row at the time of the copy, or
//! in a history, of the trigger row the copy was made for.
//!
//! The trigger is what makes the result change. The result is refreshing exactly when the
//! trigger is, except that an incremental result with `initial` is static unless base and
//! trigger are both refreshing.

mod history;
mod incremental;
mod plain;

#[cfg(test)]
mod corpus;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

use arrow::array::{new_null_array, ArrayRef, UInt64Array};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{locked, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use crate::table::tracked_source;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// Why a snapshot was refused. The variants name the kind of Java exception the reference
/// throws, so that a gRPC handler can map each to the status Java reports: `UNIMPLEMENTED`
/// for [`SnapshotError::Unsupported`], `INVALID_ARGUMENT` for the others (recorded in
/// `fixtures/snapshot_static.json`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SnapshotError {
    /// Java's `UnsupportedOperationException`: `history` together with `initial` or
    /// `incremental`.
    Unsupported(String),
    /// Java's `InvalidNameException`: a stamp column string that names no legal column.
    InvalidName(String),
    /// Java's `NoSuchColumnException`: a stamp column the trigger does not have.
    NoSuchColumn(String),
    /// A stamp column has the name of a base column, or in a history has the name of a base
    /// column of another type and a value that the base column cannot hold.
    ColumnConflict(String),
    /// Java's `IllegalArgumentException`: base and trigger belong to different update graphs.
    IllegalArgument(String),
    /// A table has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotError::Unsupported(message)
            | SnapshotError::InvalidName(message)
            | SnapshotError::NoSuchColumn(message)
            | SnapshotError::ColumnConflict(message)
            | SnapshotError::IllegalArgument(message) => f.write_str(message),
            SnapshotError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<TableFailure> for SnapshotError {
    fn from(failure: TableFailure) -> Self {
        SnapshotError::Table(failure)
    }
}

/// One stamp column: the trigger column `source` under the name `name`.
///
/// Behavior: io.deephaven.api.JoinAddition
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StampColumn {
    pub name: String,
    pub source: String,
}

impl StampColumn {
    pub fn new(name: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: source.into(),
        }
    }

    /// Reads `Name` or `Name=Source`, as the gRPC request carries stamp columns. Each side is
    /// trimmed and must be a legal column name.
    ///
    /// Behavior: io.deephaven.api.JoinAddition#parse and io.deephaven.api.ColumnName#parse
    pub fn parse(text: &str) -> Result<Self, SnapshotError> {
        let (name, source) = match text.split_once('=') {
            Some((name, source)) => (name.trim(), source.trim()),
            None => (text.trim(), text.trim()),
        };
        validate_name(name)?;
        validate_name(source)?;
        Ok(Self::new(name, source))
    }
}

/// Behavior: io.deephaven.api.util.NameValidator#validateColumnName: not empty, not a
/// variable of the formula language, and a name by `javax.lang.model.SourceVersion#isName`,
/// that is a Java identifier that is no keyword and no boolean or null literal (Java
/// Language Specification, section 3.9).
///
/// Outside ASCII this accepts letters and digits as Unicode defines them, which is close to
/// but not the same set as `Character#isJavaIdentifierStart` and `#isJavaIdentifierPart`.
fn validate_name(name: &str) -> Result<(), SnapshotError> {
    const RESERVED: &[&str] = &[
        "in",
        "not",
        "i",
        "ii",
        "k",
        "abstract",
        "assert",
        "boolean",
        "break",
        "byte",
        "case",
        "catch",
        "char",
        "class",
        "const",
        "continue",
        "default",
        "do",
        "double",
        "else",
        "enum",
        "extends",
        "final",
        "finally",
        "float",
        "for",
        "goto",
        "if",
        "implements",
        "import",
        "instanceof",
        "int",
        "interface",
        "long",
        "native",
        "new",
        "package",
        "private",
        "protected",
        "public",
        "return",
        "short",
        "static",
        "strictfp",
        "super",
        "switch",
        "synchronized",
        "this",
        "throw",
        "throws",
        "transient",
        "try",
        "void",
        "volatile",
        "while",
        "_",
        "true",
        "false",
        "null",
    ];
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err(SnapshotError::InvalidName(
            "Invalid column name: name can not be null or empty".into(),
        ));
    };
    let starts = first.is_alphabetic() || first == '_' || first == '$';
    if !starts || !characters.all(|c| c.is_alphanumeric() || c == '_' || c == '$') {
        return Err(SnapshotError::InvalidName(format!(
            "Invalid column name \"{name}\""
        )));
    }
    if RESERVED.contains(&name) {
        return Err(SnapshotError::InvalidName(format!(
            "Invalid column name \"{name}\": \"{name}\" is a reserved keyword"
        )));
    }
    Ok(())
}

/// What `snapshot_when` is to do.
///
/// Behavior: io.deephaven.api.snapshot.SnapshotWhenOptions
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SnapshotWhenOptions {
    /// Take a first snapshot when the result is made. Without it the result is empty until
    /// the trigger first changes.
    pub initial: bool,
    /// Copy and stamp again only the rows of the base that changed since the last trigger.
    pub incremental: bool,
    /// Append a copy of the base for every row the trigger gets. The trigger must be
    /// append-only. Not allowed together with `initial` or `incremental`.
    pub history: bool,
    /// The trigger columns to stamp the copied rows with. Empty: all of them.
    pub stamp_columns: Vec<StampColumn>,
}

/// A static copy of `base` as it is now, at the row keys `0..size`. The result is flat and
/// has none of the attributes of `base`.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#snapshot
pub fn snapshot(base: &Arc<dyn Table>) -> Result<Arc<dyn Table>, SnapshotError> {
    plain::snapshot(base)
}

/// A copy of `base` that is renewed whenever `trigger` changes, with the stamp columns of
/// `trigger` added. See the [module documentation](self) for the three forms.
///
/// A change of the trigger that only modifies columns that are not stamp columns does not
/// count as a change when stamp columns are named.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#snapshotWhen
pub fn snapshot_when(
    base: &Arc<dyn Table>,
    trigger: &Arc<dyn Table>,
    options: &SnapshotWhenOptions,
) -> Result<Arc<dyn Table>, SnapshotError> {
    // Behavior: io.deephaven.api.snapshot.SnapshotWhenOptions#checkHistory
    if options.history && (options.initial || options.incremental) {
        return Err(SnapshotError::Unsupported(
            "snapshotWhen with history does not currently support incremental nor initial".into(),
        ));
    }
    if let (Some(a), Some(b)) = (graph_of(base), graph_of(trigger)) {
        if !Arc::ptr_eq(&a, &b) {
            return Err(SnapshotError::IllegalArgument(
                "the base and the trigger of a snapshot must share an update graph".into(),
            ));
        }
    }
    let stamps = Stamps::plan(trigger, &options.stamp_columns)?;
    let parts = Parts {
        base: base.clone(),
        trigger: trigger.clone(),
        stamps,
    };
    if options.history {
        history::snapshot_history(parts)
    } else if options.incremental {
        incremental::snapshot_incremental(parts, options.initial)
    } else {
        plain::snapshot_when(parts, options.initial)
    }
}

/// The update graph of `table` if it is refreshing.
fn graph_of(table: &Arc<dyn Table>) -> Option<Arc<dyn UpdateGraph>> {
    table.update_graph().filter(|_| table.is_refreshing())
}

/// Runs `read` under the graph lock of the refreshing tables among `tables`, which share
/// one graph, and refuses a table that has failed.
fn reading<T>(
    tables: &[&Arc<dyn Table>],
    read: impl FnOnce() -> Result<T, SnapshotError>,
) -> Result<T, SnapshotError> {
    let checked = || {
        if tables.iter().any(|table| table.is_failed()) {
            return Err(SnapshotError::Table(failure("the parent table has failed")));
        }
        read()
    };
    match tables.iter().find_map(|table| graph_of(table)) {
        Some(graph) => locked(&*graph, checked),
        None => checked(),
    }
}

/// The stamp columns of a snapshot: the table Java makes with `view` of the trigger.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#maybeViewForSnapshot
struct Stamps {
    /// The stamp columns as the result names them.
    columns: Vec<ColumnDefinition>,
    /// For each stamp column, the trigger column it reads.
    sources: Vec<usize>,
    /// The trigger columns a modify must touch to count as a change of the trigger; `None`
    /// when no stamp columns were named and the trigger itself is used.
    watched: Option<BTreeSet<usize>>,
}

impl Stamps {
    /// A named stamp column reads a trigger column or a stamp column named before it; a
    /// name given twice keeps its first place and its last source.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable#view over
    /// io.deephaven.engine.table.impl.select.SourceColumn, as the Java server answers in
    /// `fixtures/snapshot_static.json` (`duplicate_stamp`, `stamp_chain`)
    fn plan(trigger: &Arc<dyn Table>, named: &[StampColumn]) -> Result<Self, SnapshotError> {
        let declared = trigger.definition().columns();
        if named.is_empty() {
            return Ok(Self {
                columns: declared.to_vec(),
                sources: (0..declared.len()).collect(),
                watched: None,
            });
        }
        // What a name means at this point of the list: trigger columns, shadowed by the
        // stamp columns named so far.
        let mut scope: Vec<(String, usize)> = declared
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.clone(), index))
            .collect();
        let mut listed: Vec<(String, usize)> = Vec::new();
        for stamp in named {
            let Some(&(_, source)) = scope.iter().find(|(name, _)| *name == stamp.source) else {
                let available: Vec<&str> = scope.iter().map(|(name, _)| name.as_str()).collect();
                return Err(SnapshotError::NoSuchColumn(format!(
                    "Unknown column names [{}], available column names are [{}]",
                    stamp.source,
                    available.join(", ")
                )));
            };
            for names in [&mut scope, &mut listed] {
                match names.iter_mut().find(|(name, _)| *name == stamp.name) {
                    Some(existing) => existing.1 = source,
                    None => names.push((stamp.name.clone(), source)),
                }
            }
        }
        Ok(Self {
            columns: listed
                .iter()
                .map(|(name, source)| {
                    ColumnDefinition::new(name.clone(), declared[*source].column_type.clone())
                })
                .collect(),
            sources: listed.iter().map(|(_, source)| *source).collect(),
            watched: Some(listed.iter().map(|(_, source)| *source).collect()),
        })
    }

    /// Whether the table of stamp columns delivers an update when the trigger delivers
    /// `update`: a view drops modifies of columns it does not show, and a table delivers
    /// nothing when nothing is left.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable.ViewOrUpdateViewListener#onUpdate
    /// with io.deephaven.engine.table.impl.BaseTable#notifyListeners
    fn delivers(&self, update: &TableUpdate) -> bool {
        let Some(watched) = &self.watched else {
            return true;
        };
        let modifies = !update.modified.is_empty()
            && watched
                .iter()
                .any(|column| update.modified_columns.contains(*column));
        modifies
            || !update.added.is_empty()
            || !update.removed.is_empty()
            || !update.shifted.is_empty()
    }

    /// The stamp of the trigger's last row, one array of one value per stamp column, or
    /// `None` if the trigger has no rows.
    fn last(&self, trigger: &dyn Table) -> Result<Option<Vec<ArrayRef>>, TableFailure> {
        let Some(last) = trigger.row_set().last_row_key() else {
            return Ok(None);
        };
        let row = RowSet::from_keys([last]);
        let values = self.sources.iter();
        let values = values.map(|&source| trigger.column(source).chunk(&row));
        values.collect::<Result<Vec<_>, _>>().map(Some)
    }
}

/// The tables of a `snapshot_when` and its stamp columns.
struct Parts {
    base: Arc<dyn Table>,
    trigger: Arc<dyn Table>,
    stamps: Stamps,
}

impl Parts {
    /// The base's columns followed by the stamp columns, or the stamp columns that have the
    /// name of a base column.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable#throwColumnConflictMessage
    fn base_then_stamps(&self) -> Result<TableDefinition, SnapshotError> {
        let base = self.base.definition();
        let conflicts: Vec<&str> = self
            .stamps
            .columns
            .iter()
            .filter(|stamp| base.index_of(&stamp.name).is_some())
            .map(|stamp| stamp.name.as_str())
            .collect();
        if !conflicts.is_empty() {
            return Err(SnapshotError::ColumnConflict(format!(
                "Column name conflicts: {}",
                conflicts.join(", ")
            )));
        }
        let mut columns = base.columns().to_vec();
        columns.extend(self.stamps.columns.iter().cloned());
        Ok(definition(columns))
    }

    /// Writes the current values of the base's `rows` into `data` at `rows_to`, which has as
    /// many rows.
    ///
    /// Behavior: io.deephaven.engine.table.impl.snapshot.SnapshotUtils#copyDataColumns
    fn copy_data(
        &self,
        rows: &RowSet,
        data: &[Arc<WritableSource>],
        rows_to: &RowSet,
    ) -> Result<(), TableFailure> {
        if rows.is_empty() {
            return Ok(());
        }
        for (index, source) in data.iter().enumerate() {
            source.fill(rows_to, &self.base.column(index).chunk(rows)?)?;
        }
        Ok(())
    }
}

fn definition(columns: Vec<ColumnDefinition>) -> TableDefinition {
    match TableDefinition::new(columns) {
        Ok(definition) => definition,
        Err(e) => panic!("the columns of a snapshot were checked for conflicts: {e}"),
    }
}

/// One empty writable source per column, keeping previous values if the result is
/// refreshing.
fn sources<'a>(
    graph: Option<&Arc<dyn UpdateGraph>>,
    columns: impl IntoIterator<Item = &'a ColumnDefinition>,
) -> Vec<Arc<WritableSource>> {
    let columns = columns.into_iter();
    columns
        .map(|column| match graph {
            Some(graph) => tracked_source(graph, column.column_type.clone()),
            None => Arc::new(WritableSource::new(column.column_type.clone())),
        })
        .collect()
}

fn readable(sources: &[Arc<WritableSource>]) -> impl Iterator<Item = Arc<dyn ColumnSource>> + '_ {
    let sources = sources.iter();
    sources.map(|source| source.clone() as Arc<dyn ColumnSource>)
}

/// `size` copies of the one value of `value`.
fn spread(value: &ArrayRef, size: u64) -> Result<ArrayRef, TableFailure> {
    let zeros = UInt64Array::from(vec![0u64; size as usize]);
    arrow::compute::take(value.as_ref(), &zeros, None).map_err(|e| failure(e.to_string()))
}

fn nulls(column_type: &ColumnType, size: u64) -> ArrayRef {
    new_null_array(&column_type.arrow_type(), size as usize)
}
