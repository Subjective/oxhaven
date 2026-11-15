//! Copies of tables, and what Java remembers of a sort in a table's attributes.
//!
//! Java answers some operations with a copy of a table: a `QueryTable.CopiedTable`, which
//! has the rows, the columns and the attributes of the table it is a copy of. A static
//! table that is sorted and already in order is answered with one, and so is a static
//! table that is flattened and whose row set is already flat. A copy differs from another
//! table in one way that shows: an operation on it whose result Java memoizes is answered
//! with a copy of that result, with the attributes of the copy the operation was asked of
//! that Java's attribute table lists for the operation, and none of the result's own. So
//! `where` of a sort that changed nothing is not add-only, and `where` of the same table
//! is.
//!
//! The `Table` contract cannot say that a table is a copy, or by which columns Java holds
//! it sorted. Until it can, the tables that have something to say are recognized by
//! address, as the sources of [`super::storage`] are: a table leaves when it is dropped.

use super::{QueryTable, TableSpec};
use oxhaven_contracts::table::{InputTable, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

/// What is known of a table beyond the `Table` contract.
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub(crate) struct Marks {
    /// For a copy, whether the table it is a copy of is a blink table.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable.CopiedTable and its `parent`
    pub copy_of_blink: Option<bool>,
    /// The columns Java holds the table sorted by, and whether each is descending.
    ///
    /// Behavior: `Table#SORTED_COLUMNS_ATTRIBUTE` as
    /// io.deephaven.engine.table.impl.SortedColumnsAttribute reads and writes it
    pub sorted: BTreeMap<String, bool>,
    /// Whether the table has the attribute that names the row redirection of a sort, and
    /// whether it says that there is none.
    ///
    /// Behavior: `Table#SORT_ROW_REDIRECTION_ATTRIBUTE` and
    /// io.deephaven.engine.table.impl.SortOperation#IDENTITY_REDIRECTION_VALUE
    pub identity_sort: Option<bool>,
}

static MARKS: Mutex<BTreeMap<usize, Marks>> = Mutex::new(BTreeMap::new());

fn address(table: &dyn Table) -> usize {
    table as *const dyn Table as *const () as usize
}

/// The marks of `table`: none unless it is a [`QueryTable`] that [`mark`] was called for.
pub(crate) fn marks(table: &dyn Table) -> Marks {
    MARKS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&address(table))
        .cloned()
        .unwrap_or_default()
}

/// Gives `table` its marks. Call it before the table is shared.
pub(crate) fn mark(table: &Arc<QueryTable>, marks: Marks) {
    let address = Arc::as_ptr(table) as usize;
    let mut all = MARKS.lock().unwrap_or_else(PoisonError::into_inner);
    if marks == Marks::default() {
        all.remove(&address);
    } else {
        all.insert(address, marks);
    }
}

/// Called when a [`QueryTable`] is dropped.
pub(super) fn forget(table: &QueryTable) {
    MARKS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&(table as *const QueryTable as usize));
}

/// The operations Java copies attributes for, of those that have a result here.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CopyType {
    None,
    Filter,
    Sort,
    Flatten,
    Reverse,
    DropColumns,
    View,
    UpdateView,
}

impl CopyType {
    /// Whether an attribute Java's table does not list is copied, which is also what the
    /// table says of the input table attribute.
    ///
    /// Behavior: `LEGACY_COPY_ATTRIBUTES` in io.deephaven.engine.table.impl.BaseTable
    fn copies_unlisted(self) -> bool {
        matches!(
            self,
            CopyType::Flatten | CopyType::Sort | CopyType::UpdateView | CopyType::Filter
        )
    }

    /// The attributes of `from` that are copied for this operation.
    ///
    /// Behavior: the `ADD_ONLY_TABLE_ATTRIBUTE`, `APPEND_ONLY_TABLE_ATTRIBUTE` and
    /// `BLINK_TABLE_ATTRIBUTE` entries of
    /// io.deephaven.engine.table.impl.BaseTable#attributeToCopySet
    fn attributes(self, from: TableAttributes) -> TableAttributes {
        let mut copied = TableAttributes::default();
        copied.add_only = from.add_only && self == CopyType::DropColumns;
        copied.append_only =
            from.append_only && matches!(self, CopyType::DropColumns | CopyType::Flatten);
        copied.blink = from.blink
            && matches!(
                self,
                CopyType::Filter
                    | CopyType::Sort
                    | CopyType::Reverse
                    | CopyType::Flatten
                    | CopyType::DropColumns
            );
        copied
    }

    /// The marks of a sort in `from` that are copied for this operation.
    ///
    /// Behavior: the `SORTED_COLUMNS_ATTRIBUTE` entry of `attributeToCopySet`, and
    /// `LEGACY_COPY_ATTRIBUTES` for `SORT_ROW_REDIRECTION_ATTRIBUTE`, which has no entry
    pub(crate) fn sort_marks(self, from: &Marks) -> Marks {
        Marks {
            copy_of_blink: None,
            sorted: if matches!(self, CopyType::Flatten | CopyType::Filter) {
                from.sorted.clone()
            } else {
                BTreeMap::new()
            },
            identity_sort: from.identity_sort.filter(|_| self.copies_unlisted()),
        }
    }
}

/// A table with the rows and the columns of `table`, which follows it if it changes.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#copy(TableDefinition, Predicate)
fn copy(
    table: &Arc<dyn Table>,
    attributes: TableAttributes,
    input: Option<Arc<dyn InputTable>>,
    flat: bool,
    marks: Marks,
) -> Result<Arc<dyn Table>, TableFailure> {
    let result = QueryTable::derive(table, || {
        let definition = table.definition().clone();
        let columns = (0..definition.columns().len())
            .map(|index| table.column(index))
            .collect();
        let mut spec = TableSpec::new(definition, (*table.row_set()).clone(), columns)
            .with_attributes(attributes);
        spec.flat = flat;
        spec.input = input;
        let operator = |_result: &QueryTable, upstream: &TableUpdate| Ok(upstream.clone());
        Ok((spec, operator))
    })?;
    mark(&result, marks);
    Ok(result)
}

/// A copy of `table` with all of its attributes, and with its marks as `change` leaves
/// them. It is flat if `table` is or `flat` says so.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#copy() and the
/// `withAttributes` of io.deephaven.engine.table.impl.LiveAttributeMap, which is a copy
/// with attributes added
pub(crate) fn copy_of(
    table: &Arc<dyn Table>,
    flat: bool,
    change: impl FnOnce(&mut Marks),
) -> Result<Arc<dyn Table>, TableFailure> {
    let mut marks = marks(&**table);
    marks.copy_of_blink = Some(table.attributes().blink);
    change(&mut marks);
    copy(
        table,
        table.attributes(),
        table.input_table(),
        flat || table.is_flat(),
        marks,
    )
}

/// What an operation whose result Java memoizes answers when it is asked of `parent` and
/// computed `result`: `result`, unless `parent` is a copy. Asked of a copy the answer is a
/// copy of `result` that is flat if `result` is, with the attributes of `parent` that are
/// copied for `copy_type` and none of the attributes of `result`.
///
/// `blink_must_agree` is for the operations whose memoized result Java does not use for a
/// copy that is a blink table when the table it is a copy of is not, or the reverse.
///
/// Java memoizes the result on the table that `parent` is a copy of, so a result of the
/// same operation on that table that is still alive is what Java copies. Here it is always
/// the result of the operation on `parent`. The two have the same rows and attributes; one
/// can be flat when the other is not, when `parent` is the copy that `flatten` makes.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable.CopiedTable#memoizeResult and the
/// `attributesCompatible` of io.deephaven.engine.table.impl.MemoizedOperationKey. Of the
/// keys with a result here only the `Sort` key compares another attribute, the sortable
/// columns, which no table has here.
pub(crate) fn memoized(
    parent: &Arc<dyn Table>,
    copy_type: CopyType,
    blink_must_agree: bool,
    result: Arc<dyn Table>,
) -> Result<Arc<dyn Table>, TableFailure> {
    let of_parent = marks(&**parent);
    let Some(copy_of_blink) = of_parent.copy_of_blink else {
        return Ok(result);
    };
    if blink_must_agree && copy_of_blink != parent.attributes().blink {
        return Ok(result);
    }
    let mut marks = copy_type.sort_marks(&of_parent);
    marks.copy_of_blink = Some(result.attributes().blink);
    let input = if copy_type.copies_unlisted() {
        parent.input_table()
    } else {
        None
    };
    copy(
        &result,
        copy_type.attributes(parent.attributes()),
        input,
        result.is_flat(),
        marks,
    )
}

/// Whether `table` is a copy.
#[cfg(test)]
pub(crate) fn is_copy(table: &dyn Table) -> bool {
    marks(table).copy_of_blink.is_some()
}
