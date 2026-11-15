//! `head_by` and `tail_by`. Task 4.11.
//!
//! [`head_by`] keeps the first rows of every group of a table and [`tail_by`] the last
//! ones, where a group is the rows that have one key in the group-by columns. The result
//! has the group-by columns first, in the order they were given, and then the other
//! columns in the order of the source. Its rows are group after group, the groups in the
//! order their keys were first seen, and within a group in the order of the source
//! (PARITY.md, "Order guarantees per operation", for aggregations and `ungroup`).
//!
//! Java builds the two from three other operations: a group aggregation of every column
//! that is not a key, an `updateView` that cuts each array to its first or last `nRows`
//! elements, and an `ungroup` of all of them. The result here is made the same way as far
//! as it shows: [`groups`] keeps the rows of each group and reports the groups that Java's
//! aggregation reports, and the `ungroup` of `ops/ungroup` turns the kept rows of the
//! groups into rows of the result, with the row keys and the updates of Java's. The
//! arrays are never built: `ungroup` reads an array column only through the sizes of its
//! arrays and their elements, which are rows of the source.
//!
//! A column of the source that holds vectors or arrays is kept like any other when it is
//! not a group-by column: Java's `ungroup` takes apart the arrays of the aggregation, not
//! the cells in them. As a group-by column an array is refused, and a vector is not
//! ported, see [`vector_key`].
//!
//! What follows from that construction, as in Java:
//!
//! - A key column shows, in every row of a group, the key of the first row ever seen in
//!   the group, which matters for keys that are equal without being the same: `-0.0` and
//!   `0.0`, and NaNs.
//! - When a row joins or leaves a group, or a row of it is modified in a column that is
//!   not a key, all the rows that the group has in the result before and after are
//!   reported as modified, whether the row is among the kept ones or not.

mod groups;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod tests;

use crate::ops::ungroup::keys::MINIMUM_BASE;
use crate::ops::ungroup::operator::{ungroup_arrays, Ungrouped};
use crate::table::{tracked_source, QueryTable, TableSpec};
use groups::{Groups, Kept};
use oxhaven_column::WritableSource;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::Arc;

/// Why a `head_by` or `tail_by` was refused, in the order Java checks. The text is Java's
/// message, as recorded in `fixtures/java_refusals.txt`; the Java server answers
/// `INVALID_ARGUMENT` for each but `Failed`, as recorded there and in
/// `fixtures/head_tail_by_static.json`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HeadByError {
    /// The number of rows is zero or negative.
    ///
    /// Behavior: the `Require.gtZero(nRows, "nRows")` of
    /// io.deephaven.engine.table.impl.QueryTable#headOrTailBy, a `RequirementFailure`
    RowsNotPositive(i64),
    /// The group-by columns are not distinct columns of the source: one is named twice,
    /// or is not a column of it.
    ///
    /// Java does not look at the names. It makes room for as many formulas as the source
    /// has columns less the number of group-by columns, and fails when the columns that
    /// are not among them outnumber that: with an `ArrayIndexOutOfBoundsException`, or
    /// with a `NegativeArraySizeException` if there are more group-by columns than columns.
    ///
    /// Behavior: the `updates` and `casting` arrays of
    /// io.deephaven.engine.table.impl.QueryTable#headOrTailBy
    GroupColumns {
        /// The group-by columns as given.
        columns: Vec<String>,
        /// The number of formulas there was room for, which is negative when Java could
        /// not make the arrays.
        room: i64,
    },
    /// Every column of the source is a group-by column, so there is nothing to keep.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable#aggBy, an
    /// `IllegalArgumentException`
    NoOtherColumns(Vec<String>),
    /// A group-by column holds Java arrays; the first such column of those given. A column
    /// of vectors is not refused here.
    ///
    /// Behavior: the key sources of
    /// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation, an
    /// `IllegalArgumentException`
    ArrayKey {
        /// The name of the column.
        column: String,
        /// The type of the elements of its arrays.
        element: ColumnType,
    },
    /// The source is a blink table.
    ///
    /// Behavior: the `visit(AggSpecGroup)` of
    /// io.deephaven.engine.table.impl.by.AggregationProcessor, an
    /// `UnsupportedOperationException`
    Blink,
    /// A column that is not grouped by holds tables. Java puts every such column back in
    /// its type with a cast in an `updateView`, which refuses to make a column of tables.
    ///
    /// Behavior:
    /// io.deephaven.engine.table.impl.select.analyzers.ViewColumnLayer#checkResultType, an
    /// `UnsupportedOperationException`
    TableColumn(TableClass),
    /// The source has failed, or could not be read, or the row keys of the result do not
    /// fit in 63 bits.
    Failed(TableFailure),
}

impl fmt::Display for HeadByError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeadByError::RowsNotPositive(rows) => write!(
                f,
                "Requirement failed: required nRows > 0, instead nRows == {rows}."
            ),
            HeadByError::GroupColumns { room, .. } if *room < 0 => write!(f, "{room}"),
            HeadByError::GroupColumns { room, .. } => {
                write!(f, "Index {room} out of bounds for length {room}")
            }
            HeadByError::NoOtherColumns(columns) => write!(
                f,
                "aggBy must have at least one aggregation, none specified. \
                 groupByColumns=[{}]",
                columns.join(",")
            ),
            HeadByError::ArrayKey { column, element } => write!(
                f,
                "Cannot aggregate using an array column: {column}, column type is an array \
                 of {}",
                component(element)
            ),
            HeadByError::Blink => write!(
                f,
                "Blink tables do not support AggGroup; use BlinkTableTools.blinkToAppendOnly \
                 to accumulate full history"
            ),
            HeadByError::TableColumn(class) => write!(
                f,
                "Cannot use view or updateView to produce results of type class {}; use \
                 select or update instead",
                match class {
                    TableClass::QueryTable => "io.deephaven.engine.table.impl.QueryTable",
                    _ => "io.deephaven.engine.table.Table",
                }
            ),
            HeadByError::Failed(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for HeadByError {}

impl From<TableFailure> for HeadByError {
    fn from(failure: TableFailure) -> Self {
        HeadByError::Failed(failure)
    }
}

/// The first `rows` rows of every group of `source` by `columns`; all the rows of a group
/// that has fewer. Without `columns` the whole table is one group.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#headBy
pub fn head_by(
    source: &Arc<dyn Table>,
    rows: i64,
    columns: &[String],
) -> Result<Arc<dyn Table>, HeadByError> {
    head_or_tail_by(source, rows, true, columns)
}

/// The last `rows` rows of every group of `source` by `columns`; all the rows of a group
/// that has fewer. Without `columns` the whole table is one group.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#tailBy
pub fn tail_by(
    source: &Arc<dyn Table>,
    rows: i64,
    columns: &[String],
) -> Result<Arc<dyn Table>, HeadByError> {
    head_or_tail_by(source, rows, false, columns)
}

/// The name Java prints for the class of the elements of an array, which is the name of
/// a primitive, and `class` and the binary name of any other class: `int`,
/// `class java.lang.String`, `class [I` for an `int[]`.
///
/// Behavior: `java.lang.Class#toString` of the `getComponentType()` in
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation
fn component(element: &ColumnType) -> String {
    match element {
        ColumnType::Byte => "byte".to_string(),
        ColumnType::Char => "char".to_string(),
        ColumnType::Short => "short".to_string(),
        ColumnType::Int => "int".to_string(),
        ColumnType::Long => "long".to_string(),
        ColumnType::Float => "float".to_string(),
        ColumnType::Double => "double".to_string(),
        other => format!("class {}", binary_name(other)),
    }
}

/// The name of the Java class of a column type as `java.lang.Class#getName` gives it,
/// which for an array is `[` and the descriptor of its elements.
///
/// Behavior: `java.lang.Class#getName`; the classes are those the contracts name for
/// each column type
fn binary_name(column_type: &ColumnType) -> String {
    let name = match column_type {
        ColumnType::Boolean => "java.lang.Boolean",
        ColumnType::Byte => "byte",
        ColumnType::Char => "char",
        ColumnType::Short => "short",
        ColumnType::Int => "int",
        ColumnType::Long => "long",
        ColumnType::Float => "float",
        ColumnType::Double => "double",
        ColumnType::String => "java.lang.String",
        ColumnType::Instant => "java.time.Instant",
        ColumnType::Vector(element) => match **element {
            ColumnType::Byte => "io.deephaven.vector.ByteVector",
            ColumnType::Char => "io.deephaven.vector.CharVector",
            ColumnType::Short => "io.deephaven.vector.ShortVector",
            ColumnType::Int => "io.deephaven.vector.IntVector",
            ColumnType::Long => "io.deephaven.vector.LongVector",
            ColumnType::Float => "io.deephaven.vector.FloatVector",
            ColumnType::Double => "io.deephaven.vector.DoubleVector",
            _ => "io.deephaven.vector.ObjectVector",
        },
        ColumnType::Array(element) => {
            let descriptor = match &**element {
                ColumnType::Byte => "B".to_string(),
                ColumnType::Char => "C".to_string(),
                ColumnType::Short => "S".to_string(),
                ColumnType::Int => "I".to_string(),
                ColumnType::Long => "J".to_string(),
                ColumnType::Float => "F".to_string(),
                ColumnType::Double => "D".to_string(),
                array @ ColumnType::Array(_) => binary_name(array),
                other => format!("L{};", binary_name(other)),
            };
            return format!("[{descriptor}");
        }
        other => unported!(
            "io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation",
            "the Java class of column type {other:?}"
        ),
    };
    name.to_string()
}

/// Stops at a group-by column that holds vectors.
///
/// Java does not refuse one. It groups by the vectors, and its `ungroup()` of the
/// aggregation then takes apart every array column, the group-by column among them: the
/// result has a row for each element of the key, and the request fails with an
/// `IllegalStateException` ("Array sizes differ at row key 0 (position 0), V has size 2,
/// K has size 1", in `fixtures/java_refusals.txt`) when a key does not have as many
/// elements as its group has kept rows, at once or in a later cycle.
///
/// Behavior: the `ungroup()` of io.deephaven.engine.table.impl.QueryTable#headOrTailBy
fn vector_key(name: &str, column_type: &ColumnType) {
    if let ColumnType::Vector(_) = column_type {
        unported!(
            "io.deephaven.engine.table.impl.QueryTable#headOrTailBy",
            "the group-by column {name} of type {column_type:?}, which Java ungroups"
        );
    }
}

/// The indexes in `source` of the group-by columns, in the order given, or the refusal.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#headOrTailBy up to its `aggBy`,
/// and the checks of that `aggBy` that a `head_by` can fail
fn checked(source: &dyn Table, rows: i64, columns: &[String]) -> Result<Vec<usize>, HeadByError> {
    if rows <= 0 {
        return Err(HeadByError::RowsNotPositive(rows));
    }
    let declared = source.definition().columns();
    let room = declared.len() as i64 - columns.len() as i64;
    let others = declared.iter().filter(|c| !columns.contains(&c.name));
    if others.count() as i64 > room {
        return Err(HeadByError::GroupColumns {
            columns: columns.to_vec(),
            room,
        });
    }
    // Now every group-by column is a column of the source, and none is named twice.
    if room == 0 {
        return Err(HeadByError::NoOtherColumns(columns.to_vec()));
    }
    let index = |name: &String| source.definition().index_of(name).expect("a column");
    let indexes: Vec<usize> = columns.iter().map(index).collect();
    for &index in &indexes {
        if let ColumnType::Array(element) = &declared[index].column_type {
            return Err(HeadByError::ArrayKey {
                column: declared[index].name.clone(),
                element: (**element).clone(),
            });
        }
    }
    if source.attributes().blink {
        return Err(HeadByError::Blink);
    }
    for &index in &indexes {
        vector_key(&declared[index].name, &declared[index].column_type);
    }
    Ok(indexes)
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#headOrTailBy
fn head_or_tail_by(
    source: &Arc<dyn Table>,
    rows: i64,
    head: bool,
    columns: &[String],
) -> Result<Arc<dyn Table>, HeadByError> {
    let key_columns = checked(&**source, rows, columns)?;
    let declared = source.definition().columns();
    let value_columns: Vec<usize> = (0..declared.len())
        .filter(|column| !key_columns.contains(column))
        .collect();

    // The grouped table: a row for each group, with its key. Its other columns stand for
    // the arrays of the group, which `ungroup` reads through `Kept`, not through them.
    let graph = source.update_graph().filter(|_| source.is_refreshing());
    let new_source = |column_type: &ColumnType| match &graph {
        Some(graph) => tracked_source(graph, column_type.clone()),
        None => Arc::new(WritableSource::new(column_type.clone())),
    };
    let ordered = key_columns.iter().chain(&value_columns);
    let defined: Vec<ColumnDefinition> = ordered.map(|&column| declared[column].clone()).collect();
    let definition = TableDefinition::new(defined).expect("the source's names");
    let key_sources: Vec<Arc<WritableSource>> = key_columns
        .iter()
        .map(|&column| new_source(&declared[column].column_type))
        .collect();
    let keys = key_sources.iter();
    let keys = keys.map(|source| source.clone() as Arc<dyn ColumnSource>);
    let stand_ins = value_columns.iter().map(|&column| {
        Arc::new(WritableSource::new(declared[column].column_type.clone())) as Arc<dyn ColumnSource>
    });
    let sources: Vec<Arc<dyn ColumnSource>> = keys.chain(stand_ins).collect();

    let groups = Groups::new(
        source.clone(),
        key_columns.clone(),
        key_sources,
        rows as u64,
        head,
    )?;
    // Java fails here, in its last step, after everything the grouping refuses.
    for &column in &value_columns {
        if let ColumnType::Table(class) = &declared[column].column_type {
            return Err(HeadByError::TableColumn(*class));
        }
    }
    let groups = Arc::new(groups);
    let grouped = QueryTable::derive(source, || {
        let spec = TableSpec::new(definition, groups.initial()?, sources);
        let groups = groups.clone();
        let operator = move |_: &QueryTable, upstream: &TableUpdate| groups.advance(upstream);
        Ok((spec, operator))
    })?;
    let grouped: Arc<dyn Table> = grouped;

    let kept = value_columns.iter().enumerate().map(|(place, &column)| {
        let arrays = Arc::new(Kept {
            groups: groups.clone(),
            column: source.column(column),
        });
        Ungrouped {
            index: key_columns.len() + place,
            arrays,
        }
    });
    ungroup_arrays(&grouped, false, kept.collect(), MINIMUM_BASE)
        .map_err(|error| HeadByError::Failed(error.into()))
}
