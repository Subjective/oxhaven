//! `rename_columns` and `move_columns`: the parent's column sources and rows under another
//! definition, and what the operations of this module keep of a restriction of sorting.

use super::plan::{validate_name, Kind, Plan};
use super::{spec, SelectError};
use crate::table::{java_hash_order, mark, marks, CopyType, Marks, QueryTable};
use crate::update::ModifiedColumnTransformer;
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

/// One column of a rename or a move: the parent's column `input` under the name `output`.
///
/// Behavior: io.deephaven.api.Pair
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ColumnPair {
    pub input: String,
    pub output: String,
}

impl ColumnPair {
    /// The pair of a text: `Output = Input`, or a column name for a column that keeps its
    /// name. Both names must be valid column names.
    ///
    /// Behavior: io.deephaven.api.Pair#parse, io.deephaven.api.ColumnName#parse and
    /// io.deephaven.api.util.NameValidator#validateColumnName
    pub fn parse(text: &str) -> Result<ColumnPair, SelectError> {
        let name = |text: &str| {
            let name = text.trim();
            validate_column_name(name)?;
            Ok::<_, SelectError>(name.to_string())
        };
        match text.split_once('=') {
            None => {
                let name = name(text)?;
                Ok(ColumnPair {
                    input: name.clone(),
                    output: name,
                })
            }
            Some((output, input)) => {
                let output = name(output)?;
                Ok(ColumnPair {
                    input: name(input)?,
                    output,
                })
            }
        }
    }
}

/// Behavior: io.deephaven.api.util.NameValidator#validateColumnName: a name is refused
/// when it is empty, then when it is reserved, then when it is not a Java identifier
/// (`javax.lang.model.SourceVersion#isIdentifier`). The identifier is checked as the
/// stamp columns of `snapshot_when` are: a letter, `_` or `$`, then letters, digits, `_`
/// and `$`.
fn validate_column_name(name: &str) -> Result<(), SelectError> {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err(SelectError::InvalidName(
            "Invalid column name: name can not be null or empty".into(),
        ));
    };
    validate_name(name)?;
    let starts = first.is_alphabetic() || first == '_' || first == '$';
    if !starts || !characters.all(|c| c.is_alphanumeric() || c == '_' || c == '$') {
        return Err(SelectError::InvalidName(format!(
            "Invalid column name \"{name}\""
        )));
    }
    Ok(())
}

/// Behavior: io.deephaven.engine.table.impl.RenameColumnHelper#createLookupAndValidate,
/// which reports every missing source, every source named twice and every destination
/// named twice in one message
fn validate(definition: &TableDefinition, pairs: &[ColumnPair]) -> Result<(), SelectError> {
    fn note<'a>(names: &mut Vec<&'a str>, name: &'a str) {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    let (mut missing, mut twice_from, mut twice_to) = (Vec::new(), Vec::new(), Vec::new());
    for (index, pair) in pairs.iter().enumerate() {
        if definition.index_of(&pair.input).is_none() {
            note(&mut missing, &pair.input);
        }
        if pairs[..index].iter().any(|p| p.input == pair.input) {
            note(&mut twice_from, &pair.input);
        }
        if pairs[..index].iter().any(|p| p.output == pair.output) {
            note(&mut twice_to, &pair.output);
        }
    }
    let problems: Vec<String> = [
        ("Column(s) not found: ", missing),
        ("Duplicate source column(s): ", twice_from),
        ("Duplicate destination column(s): ", twice_to),
    ]
    .into_iter()
    .filter(|(_, names)| !names.is_empty())
    .map(|(what, names)| format!("{what}{}", names.join(", ")))
    .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(SelectError::IllegalArgument(problems.join("\n")))
    }
}

/// The columns of the result as `(parent column, name)`, in the result's order. With a
/// `position` the renamed columns stand together, in the order of `pairs`, before the
/// column that would otherwise be at that position, or at the end; without one each stays
/// where it is. A column whose name a pair gives to another column is left out.
///
/// Behavior: the loop of io.deephaven.engine.table.impl.QueryTable#renameColumnsImpl
fn arrange(
    definition: &TableDefinition,
    pairs: &[ColumnPair],
    position: Option<usize>,
) -> Vec<(usize, String)> {
    let moved = |arranged: &mut Vec<(usize, String)>| {
        for pair in pairs {
            let index = definition
                .index_of(&pair.input)
                .unwrap_or_else(|| panic!("validated pairs name columns of the table"));
            arranged.push((index, pair.output.clone()));
        }
    };
    let mut arranged = Vec::new();
    for (index, column) in definition.columns().iter().enumerate() {
        let renamed = pairs.iter().find(|pair| pair.input == column.name);
        let name = match renamed {
            // the column is replaced by a renamed one
            None if pairs.iter().any(|pair| pair.output == column.name) => continue,
            None => column.name.clone(),
            // moved when the position is reached
            Some(_) if position.is_some() => continue,
            Some(pair) => pair.output.clone(),
        };
        if position == Some(arranged.len()) {
            moved(&mut arranged);
        }
        arranged.push((index, name));
    }
    if position.is_some_and(|position| arranged.len() <= position) {
        moved(&mut arranged);
    }
    arranged
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#renameColumnsImpl. The result has
/// the parent's column sources and row set; it is flat when the parent is; it has the
/// attributes io.deephaven.engine.table.impl.BaseTable#attributeToCopySet lists for
/// `CopyAttributeOperation.RenameColumns` (add-only, append-only, blink, merged) and the
/// sortable columns under their new names; and its modified columns are the parent's
/// under their new names.
fn rename_impl(
    parent: &Arc<dyn Table>,
    pairs: &[ColumnPair],
    position: Option<usize>,
) -> Result<Arc<dyn Table>, SelectError> {
    if pairs.is_empty() {
        return Ok(parent.clone());
    }
    validate(parent.definition(), pairs)?;
    let arranged = arrange(parent.definition(), pairs, position);
    let declared = parent.definition().columns();
    let columns: Vec<ColumnDefinition> = arranged
        .iter()
        .map(|(index, name)| ColumnDefinition {
            name: name.clone(),
            ..declared[*index].clone()
        })
        .collect();
    let definition = match TableDefinition::new(columns) {
        Ok(definition) => definition,
        Err(e) => panic!("the columns of a validated rename have distinct names: {e}"),
    };
    let of_parent = marks(&**parent);
    let mut result_marks = CopyType::RenameColumns.sort_marks(&of_parent);
    result_marks.sortable = sortable_after_rename(&of_parent, &definition, pairs);

    let attributes = CopyType::RenameColumns.attributes(parent.attributes());
    let transformer = ModifiedColumnTransformer::new(
        arranged
            .iter()
            .enumerate()
            .map(|(to, (from, _))| (*from, [to])),
    );
    let table = QueryTable::derive(parent, || {
        let sources = arranged
            .iter()
            .map(|(index, _)| parent.column(*index))
            .collect();
        Ok((
            spec(parent, definition, sources, attributes),
            move |_result: &QueryTable, upstream: &TableUpdate| {
                Ok(TableUpdate {
                    modified_columns: transformer.transform(&upstream.modified_columns),
                    ..upstream.clone()
                })
            },
        ))
    })?;
    mark(&table, result_marks);
    Ok(table)
}

/// The parent with columns renamed: each pair's input column under its output name, where
/// the input column stands. The renames happen at once, so two columns can swap names, and
/// a column whose name is given to another column and that is not renamed itself is left
/// out. With no pairs, the parent itself.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#renameColumns
pub fn rename_columns(
    parent: &Arc<dyn Table>,
    pairs: &[ColumnPair],
) -> Result<Arc<dyn Table>, SelectError> {
    rename_impl(parent, pairs, None)
}

/// The parent with the columns of `pairs`, renamed as in [`rename_columns`], moved to
/// stand together at `index` among the columns, in the order of `pairs`. An index below 0
/// is 0, and one past the other columns is the end. With no pairs, the parent itself.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#moveColumns
pub fn move_columns(
    parent: &Arc<dyn Table>,
    index: i32,
    pairs: &[ColumnPair],
) -> Result<Arc<dyn Table>, SelectError> {
    rename_impl(parent, pairs, Some(index.max(0) as usize))
}

/// [`move_columns`] to the front.
///
/// Behavior: io.deephaven.engine.table.impl.TableDefaults#moveColumnsUp
pub fn move_columns_up(
    parent: &Arc<dyn Table>,
    pairs: &[ColumnPair],
) -> Result<Arc<dyn Table>, SelectError> {
    move_columns(parent, 0, pairs)
}

/// [`move_columns`] to the index that is the number of the parent's columns less the
/// number of pairs: the end, unless a pair gives its column the name of a column that
/// then leaves the table.
///
/// Behavior: io.deephaven.engine.table.impl.TableDefaults#moveColumnsDown
pub fn move_columns_down(
    parent: &Arc<dyn Table>,
    pairs: &[ColumnPair],
) -> Result<Arc<dyn Table>, SelectError> {
    let columns = parent.definition().columns().len() as i64;
    let index = columns - pairs.len() as i64;
    move_columns(parent, index.clamp(0, i64::from(i32::MAX)) as i32, pairs)
}

/// The attribute value of a set of sortable columns that Java collected in a `HashSet`.
///
/// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#setSortableColumns
fn joined(added_in_order: &[&str]) -> Option<String> {
    Some(java_hash_order(added_in_order, false).join(","))
}

/// The sortable columns of the result of a rename: a sortable column that is still there
/// under its name, and the new name of one that was renamed.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#copySortableColumns(BaseTable, Collection)
fn sortable_after_rename(
    of_parent: &Marks,
    result: &TableDefinition,
    pairs: &[ColumnPair],
) -> Option<String> {
    let sortable = of_parent.sortable_columns()?;
    let mut kept = Vec::new();
    for name in sortable {
        let renamed_from = pairs.iter().find(|pair| pair.output == name);
        if result.index_of(name).is_some() && renamed_from.is_none_or(|pair| pair.input == name) {
            kept.push(name);
        } else if let Some(pair) = pairs.iter().find(|pair| pair.input == name) {
            kept.push(pair.output.as_str());
        }
    }
    joined(&kept)
}

/// The sortable columns of the result of `drop_columns`: those that are left.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#copySortableColumns(BaseGridAttributes, Predicate)
pub(super) fn sortable_after_drop(of_parent: &Marks, result: &TableDefinition) -> Option<String> {
    let sortable = of_parent.sortable_columns()?;
    let kept: Vec<&str> = sortable
        .into_iter()
        .filter(|name| result.index_of(name).is_some())
        .collect();
    Some(kept.join(","))
}

/// The sortable columns of the result of a formula operation: a listed column that is a
/// sortable column under this or another name, and a sortable column that no formula
/// replaced and that is still there.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#copySortableColumns(BaseTable, SelectColumn[])
pub(super) fn sortable_after_select(of_parent: &Marks, plan: &Plan) -> Option<String> {
    let sortable = of_parent.sortable_columns()?;
    // Java's two sets: the sortable columns less those a formula replaces, and the result's
    // sortable columns in the order they are added
    let in_set_order = java_hash_order(&sortable, true);
    let mut current: Vec<&str> = sortable;
    let mut added: Vec<&str> = Vec::new();
    for step in &plan.steps {
        match &step.kind {
            Kind::Source(source) => {
                if current.contains(&source.name.as_str()) && !added.contains(&step.name.as_str()) {
                    added.push(&step.name);
                }
            }
            Kind::Constant(_) | Kind::Formula(_) => {
                added.retain(|name| *name != step.name);
                current.retain(|name| *name != step.name);
            }
        }
    }
    let result = plan.definition();
    for name in &in_set_order {
        let Some(name) = current.iter().find(|kept| *kept == name) else {
            continue;
        };
        if result.index_of(name).is_some() && !added.contains(name) {
            added.push(name);
        }
    }
    joined(&added)
}
