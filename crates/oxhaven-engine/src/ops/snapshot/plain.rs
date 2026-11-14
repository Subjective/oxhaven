//! `snapshot`, and `snapshot_when` without `incremental` or `history`: the whole base is
//! copied anew at every trigger.

use super::{definition, nulls, readable, reading, sources, spread, Parts, SnapshotError};
use crate::table::{QueryTable, TableSpec};
use oxhaven_column::{FlatSource, WritableSource};
use oxhaven_contracts::graph::Dependency;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

/// Behavior: io.deephaven.engine.table.impl.QueryTable#snapshot, which is
/// `snapshotInternal` with an initial snapshot and a static one-row trigger without columns
pub(super) fn snapshot(base: &Arc<dyn Table>) -> Result<Arc<dyn Table>, SnapshotError> {
    let declared = base.definition().columns();
    let (size, values) = reading(&[base], || {
        let rows = base.row_set();
        let values = (0..declared.len()).map(|index| base.column(index).chunk(&rows));
        let values = values.collect::<Result<Vec<_>, _>>()?;
        Ok((rows.size(), values))
    })?;
    let mut columns: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(declared.len());
    for (column, values) in declared.iter().zip(values) {
        columns.push(Arc::new(FlatSource::new(
            column.column_type.clone(),
            values,
        )?));
    }
    let spec = TableSpec::new(definition(declared.to_vec()), RowSet::flat(size), columns).flat();
    Ok(QueryTable::new_static(spec))
}

/// The copy of the base and the stamp, held at the row keys `0..size`.
struct Copy {
    parts: Parts,
    data: Vec<Arc<WritableSource>>,
    stamps: Vec<Arc<WritableSource>>,
}

impl Copy {
    /// Replaces the copy of `old_size` rows by the base and the trigger's last row as they
    /// are now, and returns the new size. Without a trigger row the stamps are null.
    ///
    /// Behavior: io.deephaven.engine.table.impl.snapshot.SnapshotInternalListener#doSnapshot
    fn take(&self, old_size: u64) -> Result<u64, TableFailure> {
        let rows = self.parts.base.row_set();
        let size = rows.size();
        let held = RowSet::flat(size);
        let dropped = RowSet::flat(old_size).minus(&held);
        for source in self.data.iter().chain(&self.stamps) {
            source.remove(&dropped);
        }
        self.parts.copy_data(&rows, &self.data, &held)?;
        let stamp = self.parts.stamps.last(&*self.parts.trigger)?;
        for (index, source) in self.stamps.iter().enumerate() {
            let values = match &stamp {
                Some(stamp) => spread(&stamp[index], size)?,
                None => nulls(source.column_type(), size),
            };
            source.fill(&held, &values)?;
        }
        Ok(size)
    }
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#snapshotInternal with the listener
/// io.deephaven.engine.table.impl.snapshot.SnapshotInternalListener
pub(super) fn snapshot_when(parts: Parts, initial: bool) -> Result<Arc<dyn Table>, SnapshotError> {
    let definition = parts.base_then_stamps()?;
    let base = parts.base.clone();
    let trigger = parts.trigger.clone();
    let graph = super::graph_of(&trigger);
    let width = base.definition().columns().len();
    let copy = Copy {
        data: sources(graph.as_ref(), &definition.columns()[..width]),
        stamps: sources(graph.as_ref(), &definition.columns()[width..]),
        parts,
    };
    let columns: Vec<Arc<dyn ColumnSource>> =
        readable(&copy.data).chain(readable(&copy.stamps)).collect();

    if graph.is_none() {
        // A static trigger never asks for another snapshot.
        let size = reading(&[&base], || Ok(if initial { copy.take(0)? } else { 0 }))?;
        let spec = TableSpec::new(definition, RowSet::flat(size), columns).flat();
        return Ok(QueryTable::new_static(spec));
    }

    // The base is read at every trigger but its own changes ask for nothing: it is a
    // dependency, so that a trigger of the same cycle sees the base after its update.
    let waits_for: Vec<Arc<dyn Dependency>> = if base.is_refreshing() {
        vec![base.clone()]
    } else {
        Vec::new()
    };
    let result = QueryTable::derive_merged(&[trigger], waits_for, || {
        if base.is_failed() {
            return Err(super::failure("the parent table has failed"));
        }
        let size = if initial { copy.take(0)? } else { 0 };
        let spec = TableSpec::new(definition, RowSet::flat(size), columns).flat();
        let operator = move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
            let triggered = upstream[0]
                .as_ref()
                .is_some_and(|update| copy.parts.stamps.delivers(update));
            if !triggered {
                return Ok(TableUpdate::empty());
            }
            // Every row is replaced: the rows held go, the rows of the new copy come.
            let old_size = result.row_set().size();
            let size = copy.take(old_size)?;
            Ok(TableUpdate {
                added: RowSet::flat(size),
                removed: RowSet::flat(old_size),
                ..TableUpdate::empty()
            })
        };
        Ok((spec, operator))
    })?;
    Ok(result)
}
