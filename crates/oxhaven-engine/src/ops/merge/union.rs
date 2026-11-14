//! `merge`: the rows of several tables, one table after the other.
//!
//! Each constituent gets a range of the result's key space, a slot, and its rows keep their
//! own row keys inside it. A slot is sized in whole allocation units and only ever grows;
//! when a constituent outgrows its slot, the slots after it move up and their rows shift.

use super::MergeError;
use crate::table::{reads_through, QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use arrow::array::{new_empty_array, Array, ArrayRef};
use oxhaven_contracts::graph::{ClockPhase, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{ColumnSource, InputTable, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_rowset::{RowSetBuilderRandom, ShiftBuilder};
use std::sync::{Arc, PoisonError, RwLock};

/// Each constituent is allocated row key space in multiples of this unit.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UnionRedirection#ALLOCATION_UNIT_ROW_KEYS
const ALLOCATION_UNIT_ROW_KEYS: i64 = 1 << 16;

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

fn overflow() -> TableFailure {
    failure(
        "Failure to insert row set into UnionRedirection, row keys exceed max long.  If you \
         have several recursive merges, consider rewriting your query to do a single merge of \
         many tables.",
    )
}

/// The key space a constituent whose last row key is `last_row_key` needs. An empty
/// constituent gets one unit, so that every slot holds at least one key.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UnionRedirection#keySpaceFor
fn key_space_for(last_row_key: Option<RowKey>) -> Result<i64, TableFailure> {
    let units = last_row_key.unwrap_or(0) / ALLOCATION_UNIT_ROW_KEYS + 1;
    units
        .checked_mul(ALLOCATION_UNIT_ROW_KEYS)
        .ok_or_else(overflow)
}

/// Behavior: io.deephaven.engine.table.impl.sources.UnionRedirection#checkOverflow
fn next_slot(first: RowKey, key_space: i64) -> Result<RowKey, TableFailure> {
    first.checked_add(key_space).ok_or_else(overflow)
}

/// Where each slot starts: `firsts[i]` is the first row key of slot `i`, and the last
/// element is the end of the allocated key space.
struct Slots {
    current: Vec<RowKey>,
    /// The starts before the change made on step `changed_step`.
    previous: Vec<RowKey>,
    changed_step: Option<u64>,
}

/// The slot layout of one merge, shared by its columns and its operator.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UnionRedirection
struct Layout {
    /// `None` for a merge of static tables.
    graph: Option<Arc<dyn UpdateGraph>>,
    slots: RwLock<Slots>,
}

impl Layout {
    /// The starts now, or with `use_prev` as of the end of the previous cycle.
    fn firsts(&self, use_prev: bool) -> Vec<RowKey> {
        let slots = self.slots.read().unwrap_or_else(PoisonError::into_inner);
        let changed_this_cycle = match (&self.graph, slots.changed_step) {
            (Some(graph), Some(changed)) => {
                let clock = graph.clock();
                clock.phase == ClockPhase::Updating && clock.step == changed
            }
            _ => false,
        };
        if use_prev && changed_this_cycle {
            slots.previous.clone()
        } else {
            slots.current.clone()
        }
    }

    /// Replaces the starts during a cycle; the old ones stay readable as previous values
    /// until the cycle ends.
    fn replace(&self, firsts: Vec<RowKey>) {
        let mut slots = self.slots.write().unwrap_or_else(PoisonError::into_inner);
        if slots.current == firsts {
            return;
        }
        slots.previous = std::mem::replace(&mut slots.current, firsts);
        slots.changed_step = self.graph.as_ref().map(|graph| graph.clock().step);
    }
}

/// One column of a merge result: the constituents' columns, each read through its slot.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UnionColumnSource#fillChunk and
/// #fillPrevChunk
struct UnionSource {
    column_type: ColumnType,
    layout: Arc<Layout>,
    /// The column of this name in each constituent, by slot.
    parts: Vec<Arc<dyn ColumnSource>>,
}

impl UnionSource {
    fn read(&self, rows: &RowSet, use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let firsts = self.layout.firsts(use_prev);
        // The rows of one slot at a time, in the constituent's own key space.
        let mut chunks: Vec<ArrayRef> = Vec::new();
        let mut slot = 0;
        let mut inner: Vec<RowRange> = Vec::new();
        let mut flush = |slot: usize, inner: &mut Vec<RowRange>| -> Result<(), TableFailure> {
            if inner.is_empty() {
                return Ok(());
            }
            let keys = RowSet::from_ranges(inner.drain(..));
            let part = &self.parts[slot];
            chunks.push(if use_prev {
                part.prev_chunk(&keys)?
            } else {
                part.chunk(&keys)?
            });
            Ok(())
        };
        for range in rows.ranges() {
            let mut first = range.first();
            while first <= range.last() {
                // The slot holding `first`: the last one that starts at or before it.
                let holder = firsts.partition_point(|&start| start <= first) - 1;
                if holder >= self.parts.len() {
                    return Err(failure(format!(
                        "row key {first} is outside the merged table"
                    )));
                }
                if holder != slot {
                    flush(slot, &mut inner)?;
                    slot = holder;
                }
                let last = range.last().min(firsts[holder + 1] - 1);
                inner.push(RowRange::new(first - firsts[holder], last - firsts[holder]));
                first = last + 1;
            }
        }
        flush(slot, &mut inner)?;
        if chunks.is_empty() {
            return Ok(new_empty_array(&self.column_type.arrow_type()));
        }
        let chunks: Vec<&dyn Array> = chunks.iter().map(|chunk| chunk.as_ref()).collect();
        arrow::compute::concat(&chunks).map_err(failure)
    }
}

impl ColumnSource for UnionSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, true)
    }
}

/// Adds to `builder` the shifts of one constituent, whose slot moved from `prev_offset` to
/// `curr_offset`, in the result's key space. `inner` is the constituent's own shift data;
/// the stretches between its ranges move with the slot. Ranges are cut where they would
/// leave the slot or run into each other, which loses nothing because valid upstream shifts
/// have no rows there.
///
/// Behavior: io.deephaven.engine.rowset.RowSetShiftData.Builder#appendShiftData
fn append_shift_data(
    builder: &mut ShiftBuilder,
    inner: &RowSetShiftData,
    prev_offset: RowKey,
    prev_cardinality: i64,
    curr_offset: RowKey,
    curr_cardinality: i64,
) {
    let slot_delta = curr_offset - prev_offset;
    let ranges = inner.ranges();
    let mut watermark = 0;
    for index in 0..=ranges.len() {
        let (start, end, delta) = match ranges.get(index) {
            Some(range) => {
                let delta = range.delta;
                // A shift to a key below zero or beyond the slot cannot hold a row.
                let start = range.first.max(if delta < 0 { -delta } else { 0 });
                let end = (prev_cardinality - 1)
                    .min(curr_cardinality - 1 - delta)
                    .min(range.last);
                (start, end, delta)
            }
            None => (prev_cardinality, prev_cardinality, 0),
        };
        // The stretch before this range moves with the slot only.
        let stretch_end = (prev_cardinality - 1).min(start - 1) + delta.min(0);
        builder.shift_range(
            watermark + prev_offset,
            stretch_end + prev_offset,
            slot_delta,
        );
        if index >= ranges.len() || start > prev_cardinality {
            break;
        }
        builder.shift_range(start + prev_offset, end + prev_offset, slot_delta + delta);
        watermark = end + 1 + delta.max(0);
    }
}

/// The incremental half of `merge`.
struct MergeOperator {
    tables: Vec<Arc<dyn Table>>,
    layout: Arc<Layout>,
    /// For each constituent, its columns mapped to the result's columns.
    transformers: Vec<ModifiedColumnTransformer>,
}

impl MergeOperator {
    /// Behavior: io.deephaven.engine.table.impl.sources.UnionSourceManager.ChangeProcessingContext#processChanges
    /// and #processExisting, for a fixed list of constituents
    fn on_update(
        &self,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        let prev_firsts = self.layout.firsts(false);
        let mut firsts = Vec::with_capacity(prev_firsts.len());
        firsts.push(0);

        let mut added = RowSetBuilderRandom::new();
        let mut removed = RowSetBuilderRandom::new();
        let mut modified = RowSetBuilderRandom::new();
        let mut shifts = ShiftBuilder::new();
        let mut modified_columns = ModifiedColumnSet::empty();
        let place = |builder: &mut RowSetBuilderRandom, rows: &RowSet, offset: RowKey| {
            builder.add_ranges(
                rows.ranges()
                    .iter()
                    .map(|range| RowRange::new(range.first() + offset, range.last() + offset)),
            );
        };

        for (slot, table) in self.tables.iter().enumerate() {
            let prev_first = prev_firsts[slot];
            let prev_allocation = prev_firsts[slot + 1] - prev_first;
            let prev_last = prev_firsts[slot + 1] - 1;
            let curr_first = firsts[slot];
            let slot_delta = curr_first - prev_first;

            let changes = match &upstream[slot] {
                Some(changes) if !changes.is_empty() => changes,
                _ => {
                    // Static, or unchanged this cycle: it only moves with its slot.
                    firsts.push(next_slot(curr_first, prev_allocation)?);
                    if !table.row_set().is_empty() {
                        shifts.shift_range(prev_first, prev_last, slot_delta);
                    }
                    continue;
                }
            };

            // A slot grows when its constituent needs more; it never shrinks, so that a
            // constituent that grows back does not move the slots after it again.
            let needed = key_space_for(table.row_set().last_row_key())?;
            let allocation = needed.max(prev_allocation);
            firsts.push(next_slot(curr_first, allocation)?);

            place(&mut added, &changes.added, curr_first);
            place(&mut removed, &changes.removed, prev_first);
            place(&mut modified, &changes.modified, curr_first);
            modified_columns = modified_columns
                .union(&self.transformers[slot].transform(&changes.modified_columns));

            // A constituent that removed all of its previous rows has nothing to shift.
            if table.prev_row_set().size() == changes.removed.size() {
                continue;
            }
            if changes.shifted.is_empty() {
                shifts.shift_range(prev_first, prev_last, slot_delta);
            } else {
                append_shift_data(
                    &mut shifts,
                    &changes.shifted,
                    prev_first,
                    prev_allocation,
                    curr_first,
                    allocation,
                );
            }
        }

        let shifted = shifts.build().map_err(failure)?;
        self.layout.replace(firsts);
        Ok(TableUpdate {
            added: added.build(),
            removed: removed.build(),
            modified: modified.build(),
            shifted,
            modified_columns,
        })
    }
}

/// Checks that `other` has the columns of `expected`, in any order, with the same types.
///
/// Behavior: io.deephaven.engine.table.TableDefinition#checkMutualCompatibility
fn check_compatible(expected: &TableDefinition, other: &TableDefinition) -> Result<(), MergeError> {
    let mut differences = Vec::new();
    for column in expected.columns() {
        match other.index_of(&column.name) {
            None => differences.push(format!("constituent column '{}' is missing", column.name)),
            Some(index) => {
                let found = &other.columns()[index].column_type;
                if *found != column.column_type {
                    differences.push(format!(
                        "column '{}' is {:?} in expected and {found:?} in constituent",
                        column.name, column.column_type
                    ));
                }
            }
        }
    }
    for column in other.columns() {
        if expected.index_of(&column.name).is_none() {
            differences.push(format!("expected column '{}' is missing", column.name));
        }
    }
    if differences.is_empty() {
        Ok(())
    } else {
        Err(MergeError::IncompatibleDefinitions(format!(
            "Table definition incompatibilities: {}",
            differences.join("; ")
        )))
    }
}

/// The attributes all constituents share, with add-only and append-only decided by the
/// last constituent when nothing before it can change.
///
/// Behavior: io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#computeSharedAttributes
fn shared_attributes(tables: &[Arc<dyn Table>]) -> TableAttributes {
    let mut attributes = TableAttributes::default();
    let Some((last, before)) = tables.split_last() else {
        return attributes;
    };
    attributes.blink = tables.iter().all(|table| table.attributes().blink);
    // A constituent that may grow can shift the ones after it.
    if !before.iter().any(|table| table.is_refreshing()) {
        attributes.add_only = last.attributes().add_only;
        attributes.append_only = last.attributes().append_only;
    }
    attributes
}

/// The input table every constituent has, if they all have the same one.
///
/// Behavior: io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#computeSharedAttributes,
/// which keeps an attribute that every constituent has with an equal value. The value of
/// `INPUT_TABLE_ATTRIBUTE` is an `InputTableUpdater`, equal only to itself.
fn shared_input_table(tables: &[Arc<dyn Table>]) -> Option<Arc<dyn InputTable>> {
    let (first, rest) = tables.split_first()?;
    let shared = first.input_table()?;
    let same = |other: &Arc<dyn InputTable>| {
        std::ptr::eq(
            Arc::as_ptr(other) as *const (),
            Arc::as_ptr(&shared) as *const (),
        )
    };
    rest.iter()
        .all(|table| table.input_table().is_some_and(|other| same(&other)))
        .then_some(shared)
}

/// The rows of `tables`, all rows of the first table, then of the second, and so on, each
/// table's rows in its own order. The result has the first table's definition; every other
/// table must have the same columns with the same types, in any order.
///
/// Java also unpacks a constituent that is itself a merge result into its own constituents
/// (`TableToolsMergeHelper#getTablesToMerge`). That changes row keys only and is not done
/// here.
///
/// Behavior: io.deephaven.engine.util.TableTools#merge,
/// io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#merge and
/// io.deephaven.engine.table.impl.sources.UnionSourceManager
pub fn merge(tables: &[Arc<dyn Table>]) -> Result<Arc<dyn Table>, MergeError> {
    let Some(first) = tables.first() else {
        return Err(MergeError::IllegalArgument(
            "No non-null tables provided to merge".into(),
        ));
    };
    let definition = first.definition().clone();
    for table in &tables[1..] {
        check_compatible(&definition, table.definition())?;
    }
    let mut graphs = tables
        .iter()
        .filter(|table| table.is_refreshing())
        .filter_map(|table| table.update_graph());
    let graph = graphs.next();
    if let Some(graph) = &graph {
        if !graphs.all(|other| Arc::ptr_eq(&other, graph)) {
            return Err(MergeError::IllegalArgument(
                "the tables to merge do not share an update graph".into(),
            ));
        }
    }

    let result = QueryTable::derive_merged(tables, Vec::new(), || {
        let mut firsts = vec![0];
        let mut rows = RowSetBuilderRandom::new();
        for table in tables {
            let own = table.row_set();
            let first = firsts[firsts.len() - 1];
            firsts.push(next_slot(first, key_space_for(own.last_row_key())?)?);
            rows.add_ranges(
                own.ranges()
                    .iter()
                    .map(|range| RowRange::new(range.first() + first, range.last() + first)),
            );
        }
        let layout = Arc::new(Layout {
            graph: graph.clone(),
            slots: RwLock::new(Slots {
                current: firsts,
                previous: Vec::new(),
                changed_step: None,
            }),
        });

        // Column `c` of the result is the column of that name in each constituent.
        let column_in = |table: &Arc<dyn Table>, name: &str| match table.definition().index_of(name)
        {
            Some(index) => index,
            None => panic!("column {name} passed the compatibility check but is missing"),
        };
        let columns = definition
            .columns()
            .iter()
            .map(|column| {
                reads_through(UnionSource {
                    column_type: column.column_type.clone(),
                    layout: layout.clone(),
                    parts: tables
                        .iter()
                        .map(|table| table.column(column_in(table, &column.name)))
                        .collect(),
                })
            })
            .collect();
        let transformers = tables
            .iter()
            .map(|table| {
                ModifiedColumnTransformer::new(
                    definition
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(result, column)| (column_in(table, &column.name), [result])),
                )
            })
            .collect();

        let mut spec = TableSpec::new(definition.clone(), rows.build(), columns)
            .with_attributes(shared_attributes(tables));
        spec.input = shared_input_table(tables);
        let operator = MergeOperator {
            tables: tables.to_vec(),
            layout,
            transformers,
        };
        Ok((
            spec,
            move |_result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
                operator.on_update(upstream)
            },
        ))
    })?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxhaven_contracts::shift::ShiftRange;

    fn shift(first: RowKey, last: RowKey, delta: i64) -> ShiftRange {
        ShiftRange { first, last, delta }
    }

    #[test]
    fn key_space_is_whole_units_and_never_empty() {
        let unit = ALLOCATION_UNIT_ROW_KEYS;
        assert_eq!(key_space_for(None).unwrap(), unit);
        assert_eq!(key_space_for(Some(0)).unwrap(), unit);
        assert_eq!(key_space_for(Some(unit - 1)).unwrap(), unit);
        assert_eq!(key_space_for(Some(unit)).unwrap(), 2 * unit);
        assert!(key_space_for(Some(i64::MAX)).is_err());
        assert!(next_slot(i64::MAX - 5, unit).is_err());
    }

    #[test]
    fn inner_shifts_move_with_their_slot() {
        // A slot of 100 keys moving from 1000 to 1500 whose constituent shifts 10..=19 up
        // by 5 and 40..=49 down by 3.
        let inner = RowSetShiftData::new(vec![shift(10, 19, 5), shift(40, 49, -3)]).unwrap();
        let mut builder = ShiftBuilder::new();
        append_shift_data(&mut builder, &inner, 1000, 100, 1500, 100);
        let data = builder.build().unwrap();
        assert_eq!(
            data.ranges(),
            [
                shift(1000, 1009, 500),
                shift(1010, 1019, 505),
                // Keys 20..=24 are where the first range landed, and 37..=39 where the
                // second lands: no row can be there.
                shift(1025, 1036, 500),
                shift(1040, 1049, 497),
                shift(1050, 1099, 500),
            ]
        );
    }

    #[test]
    fn inner_shifts_in_a_slot_that_stays_are_only_offset() {
        let inner = RowSetShiftData::new(vec![shift(10, 19, 5)]).unwrap();
        let mut builder = ShiftBuilder::new();
        append_shift_data(&mut builder, &inner, 1000, 100, 1000, 200);
        assert_eq!(builder.build().unwrap().ranges(), [shift(1010, 1019, 5)]);
    }

    #[test]
    fn inner_shifts_are_cut_at_the_slot_boundaries() {
        // The range would start below zero and end beyond the previous slot.
        let inner = RowSetShiftData::new(vec![shift(0, 9, -4), shift(90, 150, 2)]).unwrap();
        let mut builder = ShiftBuilder::new();
        append_shift_data(&mut builder, &inner, 0, 100, 0, 100);
        assert_eq!(
            builder.build().unwrap().ranges(),
            [shift(4, 9, -4), shift(90, 97, 2)]
        );
    }
}
