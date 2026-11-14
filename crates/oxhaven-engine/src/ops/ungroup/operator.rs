//! `ungroup` of array columns: the initial result and its change for a change of the
//! parent.

use super::keys::{self, required_base};
use super::sources::{ArrayColumn, Bases, Elements, Repeated};
use super::UngroupError;
use crate::table::{reads_through, QueryTable, TableSpec};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::ops::Range;
use std::sync::Arc;

/// How many rows Java reads at a time. It decides which of two differences in size is
/// reported.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation#CHUNK_SIZE, the default of
/// the property `UngroupOperation.chunkSize`
const CHUNK_SIZE: usize = 1 << 10;

/// A column to ungroup.
pub(crate) struct Ungrouped {
    /// Its place in the parent's definition.
    pub(crate) index: usize,
    pub(crate) arrays: Arc<dyn ArrayColumn>,
}

/// One `ungroup`.
struct Ungroup {
    parent: Arc<dyn Table>,
    /// At least one column, in the order they were asked for.
    columns: Vec<Ungrouped>,
    null_fill: bool,
    minimum_base: u32,
    bases: Arc<Bases>,
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn largest(sizes: &[u64]) -> u64 {
    sizes.iter().copied().max().unwrap_or(0)
}

/// Sizes of the arrays of one column, to compare with the sizes of a reference column.
struct Compared<'a> {
    reference: &'a Ungrouped,
    sizes: &'a [u64],
    column: &'a Ungrouped,
    found: &'a [u64],
}

impl Ungroup {
    fn name(&self, column: &Ungrouped) -> String {
        self.parent.definition().columns()[column.index]
            .name
            .clone()
    }

    /// The error for the first of the positions `chunk` of `rows` at which the sizes
    /// differ.
    ///
    /// Behavior: the message of io.deephaven.engine.table.impl.UngroupOperation#computeMaxSize
    fn differ(
        &self,
        rows: &RowSet,
        previous: bool,
        compared: &Compared,
        chunk: Range<usize>,
    ) -> Result<(), UngroupError> {
        let mut chunk = chunk;
        let Some(at) = chunk.find(|at| compared.found[*at] != compared.sizes[*at]) else {
            return Ok(());
        };
        let row_key = rows.get(at as u64).expect("a row of the sizes");
        let of_parent = if previous {
            self.parent.prev_row_set()
        } else {
            self.parent.row_set()
        };
        // Java's `find`: the position, or the insertion point as a negative number.
        let position = match of_parent.find_or_insertion(row_key) {
            Ok(position) => position as i64,
            Err(position) => -(position as i64) - 1,
        };
        Err(UngroupError::SizesDiffer {
            row_key,
            previous,
            position,
            reference: self.name(compared.reference),
            reference_size: compared.sizes[at],
            column: self.name(compared.column),
            size: compared.found[at],
        })
    }

    /// The number of elements of each of `rows`, over all the columns: with `null_fill`
    /// the size of the largest array of the row, and without it the size they all have,
    /// or an error if they differ.
    ///
    /// Java reads `CHUNK_SIZE` rows at a time and within them one column after the other,
    /// so the difference it reports is the first of the first chunk that has one, of the
    /// first column that differs there.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation#computeMaxSize without
    /// `computeSizeForColumn`
    fn sizes(&self, rows: &RowSet, previous: bool) -> Result<Vec<u64>, UngroupError> {
        let (first, others) = self.columns.split_first().expect("a column to ungroup");
        let mut sizes = first.arrays.sizes(rows, previous)?;
        let mut found = Vec::with_capacity(others.len());
        for column in others {
            found.push(column.arrays.sizes(rows, previous)?);
        }
        if self.null_fill {
            for found in &found {
                for (size, found) in sizes.iter_mut().zip(found) {
                    *size = (*size).max(*found);
                }
            }
            return Ok(sizes);
        }
        for start in (0..sizes.len()).step_by(CHUNK_SIZE) {
            let chunk = start..sizes.len().min(start + CHUNK_SIZE);
            for (column, found) in others.iter().zip(&found) {
                let compared = Compared {
                    reference: first,
                    sizes: &sizes,
                    column,
                    found,
                };
                self.differ(rows, previous, &compared, chunk.clone())?;
            }
        }
        Ok(sizes)
    }

    /// Checks that the arrays of the columns in `modified` have the sizes `expected` at
    /// `rows`, which are the sizes of the columns that were not modified.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation#computeMaxSize with
    /// `computeSizeForColumn`, which Java calls without `nullFill` alone
    fn check_modified(
        &self,
        rows: &RowSet,
        expected: &[u64],
        modified: &ModifiedColumnSet,
    ) -> Result<(), UngroupError> {
        let is_modified = |column: &&Ungrouped| modified.contains(column.index);
        let unmodified = self.columns.iter().find(|column| !is_modified(column));
        let reference = unmodified.unwrap_or(&self.columns[0]);
        let mut found = Vec::new();
        for column in self.columns.iter().filter(is_modified) {
            found.push((column, column.arrays.sizes(rows, false)?));
        }
        for start in (0..expected.len()).step_by(CHUNK_SIZE) {
            let chunk = start..expected.len().min(start + CHUNK_SIZE);
            for (column, found) in &found {
                let compared = Compared {
                    reference,
                    sizes: expected,
                    column,
                    found,
                };
                self.differ(rows, false, &compared, chunk.clone())?;
            }
        }
        Ok(())
    }

    /// The initial rows of the result.
    ///
    /// Java can also build the result from the parent's previous values, for a result
    /// made while the parent updates; `QueryTable::derive` does not support that.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation#initialize
    fn initial(&self) -> Result<RowSet, UngroupError> {
        let rows = self.parent.row_set();
        let sizes = self.sizes(&rows, false)?;
        let base = required_base(largest(&sizes)).max(self.minimum_base);
        self.bases.initialize(base);
        keys::slots(&rows, &sizes, base)
    }

    /// The sizes of the rows the parent added, and the base that holds them, which is at
    /// least `base`. An error if the slot of the last of them does not fit at that base.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation.UngroupListener#evaluateAdded
    fn added(&self, rows: &RowSet, base: u32) -> Result<(Vec<u64>, u32), UngroupError> {
        if rows.is_empty() {
            return Ok((Vec::new(), base));
        }
        let sizes = self.sizes(rows, false)?;
        let base = base.max(required_base(largest(&sizes)));
        keys::slots(rows, &sizes, base)?;
        Ok((sizes, base))
    }

    /// The parent removed every row it had, which includes the parent that had none: the
    /// result starts again from the rows added, at the base of a new table.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation.UngroupListener#clearAndRecompute
    fn restart(
        &self,
        before: &RowSet,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, UngroupError> {
        let (sizes, base) = self.added(&upstream.added, self.minimum_base)?;
        let added = keys::slots(&upstream.added, &sizes, base)?;
        self.bases.set(base);
        Ok(TableUpdate {
            added,
            removed: before.clone(),
            ..TableUpdate::empty()
        })
    }

    /// The sizes the modified rows have now, and the base that holds them, which is at
    /// least `added_base`, the base that holds the added rows.
    ///
    /// The sizes are read from the arrays if every ungrouped column was modified, or with
    /// `null_fill` if any was. Otherwise an unmodified column says that the sizes are
    /// those the rows had, which the result's rows `before` show; the modified columns
    /// are then checked against them, but not, in Java, in a cycle whose added rows
    /// need a larger base: there a modified array of another size goes unnoticed and
    /// the row keeps its number of elements. With `null_fill` and a larger base for the
    /// added rows, Java reads the arrays even if no ungrouped column was modified.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation.UngroupListener#evaluateModified
    /// when it is given no sizes, and the branch of #onUpdate for a base the added rows
    /// changed
    fn modified(
        &self,
        before: &RowSet,
        old_base: u32,
        added_base: u32,
        pre_shift: &RowSet,
        upstream: &TableUpdate,
    ) -> Result<(Vec<u64>, u32), UngroupError> {
        let columns = &upstream.modified_columns;
        let is_modified = |column: &Ungrouped| columns.contains(column.index);
        let all = self.columns.iter().all(is_modified);
        let none = !self.columns.iter().any(is_modified);
        let rebased = added_base != old_base;
        let read = if rebased {
            all || self.null_fill
        } else {
            !none && (all || self.null_fill)
        };
        if read {
            let sizes = self.sizes(&upstream.modified, false)?;
            let base = added_base.max(required_base(largest(&sizes)));
            return Ok((sizes, base));
        }
        let sizes = keys::sizes_in(before, old_base, pre_shift);
        if !rebased && !none {
            self.check_modified(&upstream.modified, &sizes, columns)?;
        }
        Ok((sizes, added_base))
    }

    /// The change of the result for the parent's change `upstream`; `before` are the
    /// result's rows before it.
    ///
    /// - A parent that removed all its rows starts again ([`Ungroup::restart`]).
    /// - A removed row takes its elements with it, which `before` shows.
    /// - An added row brings the elements of its arrays.
    /// - A modified row keeps the elements it had before and has now, as modified,
    ///   whatever column was modified; the result adds the elements it has beyond those
    ///   and removes the ones it has no more.
    /// - A shifted row moves its elements with it.
    /// - If an added or modified row has more elements than a slot holds, the base grows
    ///   and every element that stays moves to its new slot, as a shift. The base never
    ///   shrinks, except by starting again.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation.UngroupListener#onUpdate,
    /// #evaluateRemoved, #evaluateModified, #rebase and
    /// io.deephaven.engine.table.impl.UngroupOperation#updateRowsetForRow
    fn update(&self, before: &RowSet, upstream: &TableUpdate) -> Result<TableUpdate, UngroupError> {
        if upstream.removed.size() == self.parent.prev_row_set().size() {
            return self.restart(before, upstream);
        }
        let old_base = self.bases.current();
        let (added_sizes, added_base) = self.added(&upstream.added, old_base)?;
        let pre_shift = modified_pre_shift(upstream).map_err(|e| failure(e.to_string()))?;
        let (modified_sizes, base) = if upstream.modified.is_empty() {
            (Vec::new(), added_base)
        } else {
            self.modified(before, old_base, added_base, &pre_shift, upstream)?
        };

        let mut added = if base == added_base {
            keys::slots(&upstream.added, &added_sizes, base)?
        } else {
            keys::moved_slots(&upstream.added, &added_sizes, base)?
        };
        let removed_sizes = keys::sizes_in(before, old_base, &upstream.removed);
        let mut removed = keys::slots(&upstream.removed, &removed_sizes, old_base)?;

        // The modified rows, one by one: where they were, with the old base, and where
        // they are, with the new one.
        let was = keys::sizes_in(before, old_base, &pre_shift);
        let mut modified = Vec::new();
        let mut grown = Vec::new();
        let mut shrunk = Vec::new();
        let rows = upstream.modified.iter().zip(pre_shift.iter());
        for ((now, then), (size, was)) in rows.zip(modified_sizes.iter().zip(&was)) {
            let now = keys::modified_start(now, base, *size)?;
            // Only a row that had elements is looked up at its old place.
            let then = then.wrapping_shl(old_base);
            let (size, was) = (*size as RowKey, *was as RowKey);
            let kept = size.min(was);
            if kept > 0 {
                modified.push(RowRange::new(now, now + kept - 1));
            }
            if size < was {
                shrunk.push(RowRange::new(then + size, then + was - 1));
            } else if size > was {
                grown.push(RowRange::new(now + was, now + size - 1));
            }
        }
        added.insert(&RowSet::from_ranges(grown));
        removed.insert(&RowSet::from_ranges(shrunk));

        let staying = before.minus(&removed);
        let shifted = if base == old_base {
            keys::shifts(&staying, base, &upstream.shifted)?
        } else {
            keys::rebase(&staying, old_base, base, &upstream.shifted)?
        };
        if base != old_base {
            self.bases.set(base);
        }
        // Every column of the result is the parent's column of the same place.
        let width = self.parent.definition().columns().len();
        let modified_columns =
            ModifiedColumnTransformer::identity(width).transform(&upstream.modified_columns);
        Ok(TableUpdate {
            added,
            removed,
            modified: RowSet::from_ranges(modified),
            shifted,
            modified_columns,
        })
    }
}

/// `parent` with each of `columns`, of which there is at least one, replaced by the
/// elements of its arrays: a row for every element, in the order of the parent's rows and
/// within a row of the array. The other columns repeat the row's value. A new table has
/// slots of at least `1 << minimum_base` keys.
///
/// The result is not flat, and is a blink table if the parent is one; Java copies no other
/// attribute. A static parent gives a static result.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation#initialize
pub(crate) fn ungroup_arrays(
    parent: &Arc<dyn Table>,
    null_fill: bool,
    columns: Vec<Ungrouped>,
    minimum_base: u32,
) -> Result<Arc<dyn Table>, UngroupError> {
    assert!(!columns.is_empty() && minimum_base >= 1);
    let graph = parent.update_graph().filter(|_| parent.is_refreshing());
    let bases = Arc::new(Bases::new(graph, minimum_base));

    let mut defined = Vec::new();
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::new();
    for (index, column) in parent.definition().columns().iter().enumerate() {
        match columns.iter().find(|ungrouped| ungrouped.index == index) {
            Some(ungrouped) => {
                let arrays = ungrouped.arrays.clone();
                let element_type = arrays.element_type().clone();
                defined.push(ColumnDefinition::new(column.name.clone(), element_type));
                sources.push(reads_through(Elements {
                    arrays,
                    bases: bases.clone(),
                }));
            }
            None => {
                defined.push(column.clone());
                sources.push(reads_through(Repeated {
                    inner: parent.column(index),
                    bases: bases.clone(),
                }));
            }
        }
    }
    let definition = TableDefinition::new(defined).expect("the parent's names");
    let mut attributes = TableAttributes::default();
    attributes.blink = parent.attributes().blink;

    let ungroup = Ungroup {
        parent: parent.clone(),
        columns,
        null_fill,
        minimum_base,
        bases,
    };
    // `derive` passes a failure on; the reason for a refusal is kept beside it.
    let mut refused = None;
    let derived = QueryTable::derive(parent, || match ungroup.initial() {
        Ok(rows) => {
            let spec = TableSpec::new(definition, rows, sources).with_attributes(attributes);
            let operator = move |result: &QueryTable, upstream: &TableUpdate| {
                let update = ungroup.update(&result.row_set(), upstream);
                update.map_err(TableFailure::from)
            };
            Ok((spec, operator))
        }
        Err(error) => {
            let failure = failure(error.to_string());
            refused = Some(error);
            Err(failure)
        }
    });
    match derived {
        Ok(table) => Ok(table),
        Err(failure) => Err(refused.unwrap_or(UngroupError::Table(failure))),
    }
}
