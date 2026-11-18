//! Columns to rows. Task 11.3.
//!
//! [`columns_to_rows`] turns value columns into labeled rows: every row of the parent
//! becomes one row per label, with a label column that names the label and one value
//! column per group of transposed parent columns, which holds the value of the group's
//! column for that label. The parent's other columns repeat the row's value. It is what
//! `ColumnsToRowsTransform.columnsToRows` of Java does, which the Python console reaches
//! through `jpy` (`deephaven.plot.express`'s `dx.data.marketing` calls it).
//!
//! The rows of a parent row `k` are at the row keys `k << bits | slot`, `slot` the place
//! of the label, `bits` the fewest that hold the last slot ([`sources::Layout`]): as in
//! Java, so the keys and the update a listener sees are Java's. The columns read the
//! parent at `k`; nothing is copied.
//!
//! | File | Holds |
//! | --- | --- |
//! | `mod.rs` | The arguments, Java's refusals, the result and the operator that keeps it up to date |
//! | `sources.rs` | The layout of the row keys and the three kinds of column |

mod sources;

#[cfg(test)]
mod tests;

use crate::ops::agg::java_class;
use crate::table::{QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use sources::{Expanded, Labels, Layout, Transposed};
use std::fmt;
use std::sync::Arc;

/// The Java method this module ports, for refusals.
const JAVA: &str = "io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows";

/// Why a columns-to-rows transform was refused, or why its result failed.
///
/// [`fmt::Display`] gives the message of Java's exception and
/// [`java_exception`](Self::java_exception) its class.
#[derive(Clone, PartialEq, Debug)]
pub enum ColumnsToRowsError {
    /// No value column. Java: `IllegalArgumentException`.
    NoValueColumns,
    /// Not one group of transposed columns per value column. Java:
    /// `IllegalArgumentException`.
    InconsistentDefinition { value_columns: usize, groups: usize },
    /// A group of transposed columns has not one column per label. Java:
    /// `IllegalArgumentException`.
    LabelCount {
        labels: usize,
        columns: usize,
        value_column: String,
    },
    /// No labels. Java fails an assertion of its key arithmetic
    /// (`io.deephaven.engine.table.impl.CrossJoinShiftState#setNumShiftBits`) with
    /// `io.deephaven.base.verify.AssertionFailure`.
    NoLabels,
    /// A transposed column the parent does not have. Java: `NoSuchColumnException`.
    NoSuchColumn {
        name: String,
        /// The names of the parent's columns.
        available: Vec<String>,
    },
    /// Two columns of one group have different types: the first column of the group in
    /// the parent's order, and the first that differs from it. Java:
    /// `IllegalArgumentException`.
    IncompatibleTypes {
        first: String,
        first_type: ColumnType,
        column: String,
        column_type: ColumnType,
    },
    /// With more than one label, every column of a group is in an earlier group too, so
    /// Java finds no type for the value column. Java: `NullPointerException`.
    NoValueType,
    /// What Java does here is undefined or inconsistent: refused by name, with what it is.
    NotPorted(String),
    /// The parent has failed.
    Table(TableFailure),
}

impl ColumnsToRowsError {
    /// The class of the exception Java throws.
    pub fn java_exception(&self) -> &'static str {
        match self {
            ColumnsToRowsError::NoValueColumns
            | ColumnsToRowsError::InconsistentDefinition { .. }
            | ColumnsToRowsError::LabelCount { .. }
            | ColumnsToRowsError::IncompatibleTypes { .. } => "java.lang.IllegalArgumentException",
            ColumnsToRowsError::NoLabels => "io.deephaven.base.verify.AssertionFailure",
            ColumnsToRowsError::NoSuchColumn { .. } => {
                "io.deephaven.engine.table.impl.NoSuchColumnException"
            }
            ColumnsToRowsError::NoValueType => "java.lang.NullPointerException",
            ColumnsToRowsError::NotPorted(_) | ColumnsToRowsError::Table(_) => {
                "java.lang.UnsupportedOperationException"
            }
        }
    }
}

impl fmt::Display for ColumnsToRowsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnsToRowsError::NoValueColumns => write!(f, "No columns to transpose defined!"),
            ColumnsToRowsError::InconsistentDefinition {
                value_columns,
                groups,
            } => write!(
                f,
                "Inconsistent transpose column definition, {value_columns} names defined, \
                 {groups} columns defined."
            ),
            ColumnsToRowsError::LabelCount {
                labels,
                columns,
                value_column,
            } => write!(
                f,
                "{labels} labels defined, but {columns} transpose columns defined for \
                 {value_column}."
            ),
            ColumnsToRowsError::NoLabels => write!(
                f,
                "Assertion failed: asserted newNumShiftBits < 63, instead newNumShiftBits == \
                 64, 63 == 63."
            ),
            ColumnsToRowsError::NoSuchColumn { name, available } => write!(
                f,
                "Unknown column names [{name}], available column names are [{}]",
                available.join(", ")
            ),
            ColumnsToRowsError::IncompatibleTypes {
                first,
                first_type,
                column,
                column_type,
            } => write!(
                f,
                "Incompatible transpose types {first} is {}, {column} is {}",
                java_class(first_type),
                java_class(column_type)
            ),
            ColumnsToRowsError::NoValueType => write!(
                f,
                "Cannot invoke \"java.lang.Class.isPrimitive()\" because \"type\" is null"
            ),
            ColumnsToRowsError::NotPorted(what) => write!(f, "not ported: {JAVA}: {what}"),
            ColumnsToRowsError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for ColumnsToRowsError {}

impl From<TableFailure> for ColumnsToRowsError {
    fn from(failure: TableFailure) -> Self {
        ColumnsToRowsError::Table(failure)
    }
}

impl From<ColumnsToRowsError> for TableFailure {
    fn from(error: ColumnsToRowsError) -> Self {
        match error {
            ColumnsToRowsError::Table(failure) => failure,
            other => TableFailure {
                message: other.to_string(),
            },
        }
    }
}

/// `source` with the columns of `transpose_columns` turned into rows.
///
/// Every row of `source` becomes one row per label, in the order of `labels`. The result
/// has the columns of `source` that are not transposed, in their order, then
/// `label_column`, which holds the row's label, then one column per name of
/// `value_columns`. Value column `v` holds, in the row of label `i`, the value of the
/// column `transpose_columns[v][i]` of the parent row. A name the result already has
/// replaces that column where it is, as Java's map of columns does: a label column named
/// as a column that is not transposed takes its place, and of two value columns of one
/// name the last is kept.
///
/// Refused as Java refuses: no value column, not one group of `transpose_columns` per
/// value column, a group without one column per label, no labels, a column `source` does
/// not have (the first, group by group), two columns of different types in a group (the
/// first in the order of `source`'s columns), and a value column with more than one label
/// whose columns are all in earlier groups.
///
/// Refused as not ported, where Java's result is undefined or inconsistent (owner
/// question 7.11 of `review-items-2.md`): a column in two groups whose value columns
/// have different types; for a refreshing `source`, a column that is transposed twice or
/// a group whose columns are not in the order of `source`'s columns, for which Java's
/// update misses changed rows; and row keys of the result beyond the last row key.
///
/// The Java overloads `columnsToRows(Table, String, String, String...)` and
/// `columnsToRows(Table, String, String, String[], String[])` are this with one value
/// column, the first with the column names as labels.
///
/// Behavior: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows,
/// all three overloads
pub fn columns_to_rows(
    source: &Arc<dyn Table>,
    label_column: &str,
    value_columns: &[String],
    labels: &[String],
    transpose_columns: &[Vec<String>],
) -> Result<Arc<dyn Table>, ColumnsToRowsError> {
    if value_columns.is_empty() {
        return Err(ColumnsToRowsError::NoValueColumns);
    }
    if value_columns.len() != transpose_columns.len() {
        return Err(ColumnsToRowsError::InconsistentDefinition {
            value_columns: value_columns.len(),
            groups: transpose_columns.len(),
        });
    }
    for (group, value_column) in transpose_columns.iter().zip(value_columns) {
        if group.len() != labels.len() {
            return Err(ColumnsToRowsError::LabelCount {
                labels: labels.len(),
                columns: group.len(),
                value_column: value_column.clone(),
            });
        }
    }
    if labels.is_empty() {
        return Err(ColumnsToRowsError::NoLabels);
    }
    let plan = Plan::new(
        source.definition(),
        label_column,
        value_columns,
        transpose_columns,
    )?;
    if source.is_refreshing() {
        plan.check_updates_are_consistent(source.definition())?;
    }
    let layout = Layout::new(labels.len());
    check_keys_fit(&source.row_set(), layout)?;
    let labels = labels.to_vec();
    let parent = source.clone();
    let result = QueryTable::derive(source, move || {
        // the parent may have changed since
        check_keys_fit(&parent.row_set(), layout).map_err(TableFailure::from)?;
        let spec = plan.spec(&*parent, layout, labels, &parent.row_set());
        let operator = plan.operator(parent.clone(), layout);
        Ok((spec, move |_: &QueryTable, upstream: &TableUpdate| {
            operator.on_update(upstream)
        }))
    })?;
    Ok(result)
}

/// The refusal of row keys that do not fit: Java multiplies the parent's key and
/// overflows.
fn check_keys_fit(rows: &RowSet, layout: Layout) -> Result<(), ColumnsToRowsError> {
    match rows.last_row_key() {
        Some(last) if last > layout.last_parent() => Err(ColumnsToRowsError::NotPorted(format!(
            "the row key {last} of the parent does not fit {} bits of labels; Java's row \
                 keys overflow",
            layout.bits
        ))),
        _ => Ok(()),
    }
}

/// Where each column of the result comes from.
#[derive(Clone, Debug)]
enum Origin {
    /// The parent's column at this index, not transposed.
    Kept(usize),
    Label,
    /// The value column of this group.
    Value(usize),
}

/// What the arguments make of the parent's columns.
struct Plan {
    /// The result's columns, by name, in order, and where each comes from.
    columns: Vec<(String, Origin)>,
    /// For each group, the parent's columns in the order of the labels.
    groups: Vec<Vec<usize>>,
    /// For each group, the type of its value column.
    value_types: Vec<ColumnType>,
    /// For each group, the name of its value column.
    value_names: Vec<String>,
}

impl Plan {
    /// The result's columns, or Java's refusal.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows,
    /// from the lookup of the transposed columns to the result's map of columns
    fn new(
        parent: &TableDefinition,
        label_column: &str,
        value_columns: &[String],
        transpose_columns: &[Vec<String>],
    ) -> Result<Self, ColumnsToRowsError> {
        let declared = parent.columns();
        let mut groups = Vec::with_capacity(transpose_columns.len());
        for group in transpose_columns {
            let mut indices = Vec::with_capacity(group.len());
            for name in group {
                let index =
                    parent
                        .index_of(name)
                        .ok_or_else(|| ColumnsToRowsError::NoSuchColumn {
                            name: name.clone(),
                            available: declared.iter().map(|c| c.name.clone()).collect(),
                        })?;
                indices.push(index);
            }
            groups.push(indices);
        }
        // Java takes the type of a group from its first column in the parent's order that
        // is in no earlier group, and checks that group's columns against it.
        let owner = |index: usize| groups.iter().position(|group| group.contains(&index));
        let mut typed: Vec<Option<usize>> = vec![None; groups.len()];
        let mut columns = Vec::with_capacity(declared.len() + 1 + value_columns.len());
        for (index, column) in declared.iter().enumerate() {
            let Some(group) = owner(index) else {
                columns.push((column.name.clone(), Origin::Kept(index)));
                continue;
            };
            match typed[group] {
                None => typed[group] = Some(index),
                Some(first) if declared[first].column_type != column.column_type => {
                    return Err(ColumnsToRowsError::IncompatibleTypes {
                        first: declared[first].name.clone(),
                        first_type: declared[first].column_type.clone(),
                        column: column.name.clone(),
                        column_type: column.column_type.clone(),
                    });
                }
                Some(_) => {}
            }
        }
        let mut put = |name: &str, origin: Origin| match columns.iter().position(|(n, _)| n == name)
        {
            Some(at) => columns[at].1 = origin,
            None => columns.push((name.to_string(), origin)),
        };
        put(label_column, Origin::Label);
        let single = groups[0].len() == 1;
        let mut value_types = Vec::with_capacity(groups.len());
        for (group, name) in value_columns.iter().enumerate() {
            let column_type = match typed[group] {
                // with one label Java hands the parent's column on as the value column
                _ if single => declared[groups[group][0]].column_type.clone(),
                None => return Err(ColumnsToRowsError::NoValueType),
                Some(first) => declared[first].column_type.clone(),
            };
            let differs = groups[group]
                .iter()
                .find(|&&index| declared[index].column_type != column_type);
            if let Some(&index) = differs {
                // only a column of an earlier group can differ: Java reads it as this type
                return Err(ColumnsToRowsError::NotPorted(format!(
                    "the column {} is {} and in an earlier group, and the value column {name} \
                     is {}",
                    declared[index].name,
                    java_class(&declared[index].column_type),
                    java_class(&column_type)
                )));
            }
            value_types.push(column_type);
            put(name, Origin::Value(group));
        }
        Ok(Self {
            columns,
            groups,
            value_types,
            value_names: value_columns.to_vec(),
        })
    }

    /// Refuses what Java keeps up to date inconsistently: a column transposed twice, or a
    /// group whose columns are not in the parent's order. Java finds the rows a change of
    /// a transposed column modifies by counting the columns of each group in the parent's
    /// order, and maps a column to the value column of the last group it is in, so its
    /// update then names other rows, or other columns, than those that changed.
    ///
    /// Behavior: the `sourcesForRow` and `resultColumnSets` of the listener of
    /// io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows
    fn check_updates_are_consistent(
        &self,
        parent: &TableDefinition,
    ) -> Result<(), ColumnsToRowsError> {
        let name = |index: usize| parent.columns()[index].name.clone();
        let mut seen = Vec::new();
        for group in &self.groups {
            for &index in group {
                if seen.contains(&index) {
                    return Err(ColumnsToRowsError::NotPorted(format!(
                        "the column {} is transposed twice in a refreshing table, whose \
                         updates Java reports inconsistently",
                        name(index)
                    )));
                }
                seen.push(index);
            }
            if let Some(pair) = group.windows(2).find(|pair| pair[0] > pair[1]) {
                return Err(ColumnsToRowsError::NotPorted(format!(
                    "the transposed columns {} and {} are not in the order of the columns of \
                     the refreshing table, whose updates Java then reports inconsistently",
                    name(pair[0]),
                    name(pair[1])
                )));
            }
        }
        Ok(())
    }

    /// The result's initial state.
    fn spec(
        &self,
        parent: &dyn Table,
        layout: Layout,
        labels: Vec<String>,
        rows: &RowSet,
    ) -> TableSpec {
        let labels: Arc<dyn ColumnSource> = Arc::new(Labels { labels, layout });
        let mut definitions = Vec::with_capacity(self.columns.len());
        let mut sources = Vec::with_capacity(self.columns.len());
        for (name, origin) in &self.columns {
            let source: Arc<dyn ColumnSource> = match origin {
                Origin::Kept(index) if layout.bits == 0 => parent.column(*index),
                Origin::Kept(index) => Arc::new(Expanded {
                    parent: parent.column(*index),
                    layout,
                }),
                Origin::Label => labels.clone(),
                Origin::Value(group) if layout.bits == 0 => parent.column(self.groups[*group][0]),
                Origin::Value(group) => Arc::new(Transposed {
                    column_type: self.value_types[*group].clone(),
                    parents: self.groups[*group]
                        .iter()
                        .map(|&index| parent.column(index))
                        .collect(),
                    layout,
                }),
            };
            definitions.push(ColumnDefinition::new(
                name.clone(),
                source.column_type().clone(),
            ));
            sources.push(source);
        }
        let definition = match TableDefinition::new(definitions) {
            Ok(definition) => definition,
            Err(duplicate) => unreachable!("{duplicate}: the names were made distinct"),
        };
        TableSpec::new(definition, layout.expand(rows, None), sources)
    }

    /// What keeps the result up to date.
    fn operator(&self, parent: Arc<dyn Table>, layout: Layout) -> Operator {
        let result_index = |name: &str| self.columns.iter().position(|(n, _)| n == name);
        let declared = parent.definition().columns();
        let mut kept = Vec::new();
        let mut mapping = Vec::with_capacity(declared.len());
        for (index, column) in declared.iter().enumerate() {
            let group = self.groups.iter().position(|group| group.contains(&index));
            // Java names the result's column: a later column of the same name may have
            // taken the place of the one this column feeds
            let child = match group {
                Some(group) => result_index(&self.value_names[group]),
                None => {
                    kept.push(index);
                    result_index(&column.name)
                }
            };
            mapping.push((index, child));
        }
        let slots = (0..layout.fanout)
            .map(|slot| self.groups.iter().map(|group| group[slot]).collect())
            .collect();
        Operator {
            parent,
            layout,
            kept,
            slots,
            transformer: ModifiedColumnTransformer::new(mapping),
        }
    }
}

/// Keeps a result up to date.
struct Operator {
    parent: Arc<dyn Table>,
    layout: Layout,
    /// The parent's columns that are not transposed.
    kept: Vec<usize>,
    /// For each slot, the parent's columns its value columns read.
    slots: Vec<Vec<usize>>,
    transformer: ModifiedColumnTransformer,
}

impl Operator {
    /// The result's change for the parent's change `upstream`: every row of an added,
    /// removed or shifted parent row; for a modified parent row every row when a column
    /// that is not transposed changed, and otherwise the rows of the labels whose
    /// columns changed. The label column changes only when it took the place of a
    /// changed column.
    ///
    /// Behavior: the listener of
    /// io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows
    fn on_update(&self, upstream: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let layout = self.layout;
        check_keys_fit(&self.parent.row_set(), layout).map_err(TableFailure::from)?;
        let changed = &upstream.modified_columns;
        let modified = if upstream.modified.is_empty() {
            RowSet::empty()
        } else if changed.contains_any(&self.kept) {
            layout.expand(&upstream.modified, None)
        } else {
            let slots: Vec<bool> = self.slots.iter().map(|s| changed.contains_any(s)).collect();
            layout.expand(&upstream.modified, Some(&slots))
        };
        let last = layout.last_parent();
        let mut shifts = Vec::with_capacity(upstream.shifted.ranges().len());
        for range in upstream.shifted.ranges() {
            // no parent row past `last` exists before or after the shift
            let end = range.last.min(last).min(last - range.delta.max(0));
            if range.first > end {
                continue;
            }
            shifts.push(ShiftRange {
                first: layout.first_row(range.first),
                last: layout.last_row(end),
                delta: range.delta << layout.bits,
            });
        }
        let shifted = RowSetShiftData::new(shifts).map_err(|e| TableFailure {
            message: e.to_string(),
        })?;
        Ok(TableUpdate {
            added: layout.expand(&upstream.added, None),
            removed: layout.expand(&upstream.removed, None),
            modified,
            shifted,
            modified_columns: self.transformer.transform(changed),
        })
    }
}
