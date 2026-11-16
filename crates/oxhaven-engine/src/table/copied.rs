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

/// What is known of a table beyond the `Table` contract: the attributes of Java's attribute
/// map that `TableAttributes` has no field for.
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub(crate) struct Marks {
    /// For a copy, whether the table it is a copy of is a blink table.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable.CopiedTable and its `parent`
    pub copy_of_blink: Option<bool>,
    /// For a copy, the `sortable` of the table it is a copy of.
    pub copy_of_sortable: Option<String>,
    /// The columns Java holds the table sorted by, and whether each is descending, in the
    /// order of Java's map of them. Change it with [`Marks::set_sorted`].
    ///
    /// Behavior: `Table#SORTED_COLUMNS_ATTRIBUTE` as
    /// io.deephaven.engine.table.impl.SortedColumnsAttribute reads and writes it
    pub sorted: Vec<(String, bool)>,
    /// The attribute that names a column of a sort result that is read through the sort's
    /// row redirection, or [`IDENTITY_REDIRECTION`] when there is none.
    ///
    /// Behavior: `Table#SORT_ROW_REDIRECTION_ATTRIBUTE` and
    /// io.deephaven.engine.table.impl.SortOperation#populateRedirectedColumns
    pub sort_redirection: Option<String>,
    /// Whether the table has the attribute that holds the reverse lookup of a sort.
    ///
    /// Behavior: `Table#SORT_REVERSE_LOOKUP_ATTRIBUTE`, set by
    /// io.deephaven.engine.table.impl.SortOperation#setReverseLookup
    pub reverse_lookup: bool,
    /// The columns the table may be sorted by, joined by commas, when that is restricted.
    ///
    /// Behavior: `GridAttributes#SORTABLE_COLUMNS_ATTRIBUTE`
    pub sortable: Option<String>,
    /// Whether the table is the result of a merge, or has the columns of one.
    ///
    /// Behavior: `Table#MERGED_TABLE_ATTRIBUTE`
    pub merged: bool,
}

/// The value of the row redirection attribute of a sort that changed no order.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#IDENTITY_REDIRECTION_VALUE
pub(crate) const IDENTITY_REDIRECTION: &str = "+IDENTITY_REDIRECTION";

/// The order in which a `java.util.HashSet` or `HashMap` iterates over `keys` after they
/// were added to it in the order given. `presized` is for a set made of a collection of
/// the keys (`new HashSet<>(collection)`), which starts with room for them.
///
/// The buckets are visited in order and the keys of a bucket in the order they were added.
/// A bucket that Java turns into a tree (eight keys in one bucket of a table of 64) is
/// not modelled.
///
/// Behavior: `java.lang.String#hashCode`, `java.util.HashMap#hash`, `#putVal`, `#resize`
/// and `#tableSizeFor`
pub(crate) fn java_hash_order<S: AsRef<str>>(keys: &[S], presized: bool) -> Vec<String> {
    let mut distinct: Vec<&str> = Vec::with_capacity(keys.len());
    for key in keys {
        if !distinct.contains(&key.as_ref()) {
            distinct.push(key.as_ref());
        }
    }
    let mut capacity: usize = 16;
    if presized {
        // `Math.max((int) (size / .75f) + 1, 16)`, rounded up to a power of two
        let wanted = (keys.len() as f32 / 0.75f32) as usize + 1;
        capacity = wanted.max(16).next_power_of_two();
    }
    while distinct.len() > capacity / 4 * 3 {
        capacity *= 2;
    }
    let bucket = |key: &str| {
        let hash = key.encode_utf16().fold(0i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        }) as u32;
        ((hash ^ (hash >> 16)) as usize) & (capacity - 1)
    };
    // a stable sort keeps the keys of one bucket in the order they were added
    distinct.sort_by_key(|key| bucket(key));
    distinct.into_iter().map(str::to_string).collect()
}

/// The names of a comma separated attribute value.
///
/// Behavior: `java.lang.String#split(String)`, which leaves out trailing empty strings
/// and gives one empty string for an empty text
pub(crate) fn java_split(text: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = text.split(',').collect();
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    parts
}

impl Marks {
    /// Whether Java holds the table sorted by `column` in this direction.
    ///
    /// Behavior: io.deephaven.engine.table.impl.SortedColumnsAttribute#isSortedBy
    pub(crate) fn is_sorted_by(&self, column: &str, descending: bool) -> bool {
        self.sorted
            .iter()
            .any(|(name, order)| name == column && *order == descending)
    }

    /// Notes that the table is sorted by `column`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.SortedColumnsAttribute#setOrderForColumn,
    /// which reads the attribute into a `HashMap`, puts the column and writes the map
    pub(crate) fn set_sorted(&mut self, column: &str, descending: bool) {
        if self.is_sorted_by(column, descending) {
            return;
        }
        match self.sorted.iter_mut().find(|(name, _)| name == column) {
            Some(entry) => entry.1 = descending,
            None => self.sorted.push((column.to_string(), descending)),
        }
        let names: Vec<&str> = self.sorted.iter().map(|(name, _)| name.as_str()).collect();
        let order = java_hash_order(&names, false);
        self.sorted
            .sort_by_key(|(name, _)| order.iter().position(|ordered| ordered == name));
    }

    /// The columns the table may be sorted by, or `None` when any may be.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#getSortableColumns, a
    /// `LinkedHashSet` of the names
    pub(crate) fn sortable_columns(&self) -> Option<Vec<&str>> {
        let mut names: Vec<&str> = Vec::new();
        for name in java_split(self.sortable.as_deref()?) {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        Some(names)
    }
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
    RenameColumns,
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
    pub(crate) fn attributes(self, from: TableAttributes) -> TableAttributes {
        let renames = matches!(self, CopyType::DropColumns | CopyType::RenameColumns);
        let mut copied = TableAttributes::default();
        copied.add_only = from.add_only && renames;
        copied.append_only = from.append_only && (renames || self == CopyType::Flatten);
        copied.blink = from.blink
            && (renames
                || matches!(
                    self,
                    CopyType::Filter | CopyType::Sort | CopyType::Reverse | CopyType::Flatten
                ));
        copied
    }

    /// The marks of `from` that are copied for this operation.
    ///
    /// Behavior: the `SORTED_COLUMNS_ATTRIBUTE`, `SORTABLE_COLUMNS_ATTRIBUTE` and
    /// `MERGED_TABLE_ATTRIBUTE` entries of `attributeToCopySet`, and
    /// `LEGACY_COPY_ATTRIBUTES` for `SORT_ROW_REDIRECTION_ATTRIBUTE` and
    /// `SORT_REVERSE_LOOKUP_ATTRIBUTE`, which have no entry
    pub(crate) fn sort_marks(self, from: &Marks) -> Marks {
        Marks {
            copy_of_blink: None,
            copy_of_sortable: None,
            sorted: if matches!(self, CopyType::Flatten | CopyType::Filter) {
                from.sorted.clone()
            } else {
                Vec::new()
            },
            sort_redirection: from
                .sort_redirection
                .clone()
                .filter(|_| self.copies_unlisted()),
            reverse_lookup: from.reverse_lookup && self.copies_unlisted(),
            sortable: from.sortable.clone().filter(|_| {
                matches!(
                    self,
                    CopyType::Flatten | CopyType::Sort | CopyType::Filter | CopyType::Reverse
                )
            }),
            merged: from.merged
                && matches!(
                    self,
                    CopyType::DropColumns | CopyType::RenameColumns | CopyType::View
                ),
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
            .keeping_partitioning()
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
    marks.copy_of_sortable = marks.sortable.clone();
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
/// columns: `sort` asks before it calls this.
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
    marks.copy_of_sortable = self::marks(&*result).sortable;
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

/// The value of one attribute of a table.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AttributeValue {
    Bool(bool),
    Text(String),
    /// An attribute whose value in Java is an object that has no counterpart here: the
    /// name of its Java class or interface.
    Object(&'static str),
}

/// The attributes of `table` by Java's names, of those that an operation of this engine
/// leaves: `AddOnly`, `AppendOnly`, `BlinkTable`, `SortedColumns`, `SortRowRedirection`,
/// `SortReverseLookup`, `SortableColumns`, `MergedTable` and `InputTable`. Java's map has
/// no order; this one is in the order of that list.
///
/// Not among them, though Java has it: `AggregationRowLookup` of the result of an
/// aggregation, whose value is the lookup function of the aggregation's operator.
///
/// Behavior: io.deephaven.engine.table.AttributeMap#getAttributes
pub fn attribute_map(table: &dyn Table) -> Vec<(&'static str, AttributeValue)> {
    let attributes = table.attributes();
    let marks = marks(table);
    let mut map = Vec::new();
    let mut flag = |name, set: bool| {
        if set {
            map.push((name, AttributeValue::Bool(true)));
        }
    };
    flag("AddOnly", attributes.add_only);
    flag("AppendOnly", attributes.append_only);
    flag("BlinkTable", attributes.blink);
    if !marks.sorted.is_empty() {
        let columns: Vec<String> = marks
            .sorted
            .iter()
            .map(|(name, descending)| {
                let order = if *descending {
                    "Descending"
                } else {
                    "Ascending"
                };
                format!("{name}={order}")
            })
            .collect();
        map.push(("SortedColumns", AttributeValue::Text(columns.join(","))));
    }
    if let Some(column) = marks.sort_redirection {
        map.push(("SortRowRedirection", AttributeValue::Text(column)));
    }
    if marks.reverse_lookup {
        let lookup = AttributeValue::Object("java.util.function.LongUnaryOperator");
        map.push(("SortReverseLookup", lookup));
    }
    if let Some(sortable) = marks.sortable {
        map.push(("SortableColumns", AttributeValue::Text(sortable)));
    }
    if marks.merged {
        map.push(("MergedTable", AttributeValue::Bool(true)));
    }
    if table.input_table().is_some() {
        let updater = AttributeValue::Object("io.deephaven.engine.util.input.InputTableUpdater");
        map.push(("InputTable", updater));
    }
    map
}

/// The columns `table` may be sorted by, when that is restricted: the names of its
/// `SortableColumns` attribute, each once, in the attribute's order. A name need not be a
/// column of the table.
///
/// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#getSortableColumns
pub fn sortable_columns(table: &dyn Table) -> Option<Vec<String>> {
    let marks = marks(table);
    let names = marks.sortable_columns()?;
    Some(names.into_iter().map(str::to_string).collect())
}
