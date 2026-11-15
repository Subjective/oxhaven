//! The schema a client is sent for a table, and a consistent copy of the table's data.

use std::sync::{Arc, Mutex, PoisonError, Weak};

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::table::{Table, TableFailure};

use crate::schema::table_schema;
use crate::wire_columns::WireColumn;

/// The schemas clients uploaded, by the table made from each.
///
/// Java keeps the uploaded schema as the table attribute `BarrageSchema`. A table here has
/// no place for it, so it is kept beside the table, found by the table's address, and
/// forgotten when the table is dropped.
static UPLOADED: Mutex<Vec<Uploaded>> = Mutex::new(Vec::new());

/// A table and the schema it was uploaded with.
type Uploaded = (Weak<dyn Table>, Arc<Schema>);

fn address(table: *const dyn Table) -> *const () {
    table.cast()
}

/// Remembers that `table` was made from data a client sent with `schema`.
///
/// Behavior: `io.deephaven.extensions.barrage.table.BarrageTable#make` (the
/// `BARRAGE_SCHEMA_ATTRIBUTE` it sets)
pub(super) fn remember_uploaded(table: &Arc<dyn Table>, schema: Schema) {
    let mut uploaded = UPLOADED.lock().unwrap_or_else(PoisonError::into_inner);
    uploaded.retain(|(table, _)| table.strong_count() > 0);
    uploaded.push((Arc::downgrade(table), Arc::new(schema)));
}

fn uploaded_schema(table: &Arc<dyn Table>) -> Option<Arc<Schema>> {
    let uploaded = UPLOADED.lock().unwrap_or_else(PoisonError::into_inner);
    uploaded.iter().find_map(|(candidate, schema)| {
        // a dropped table's address may belong to another table by now
        let live = candidate.upgrade()?;
        (address(Arc::as_ptr(&live)) == address(Arc::as_ptr(table))).then(|| schema.clone())
    })
}

/// The schema a client is sent for `table`: for `GetSchema`, `GetFlightInfo` and the first
/// message of `DoGet`.
///
/// For a table a client uploaded, each field keeps the Arrow type and the nullability the
/// client declared, so that the client gets back what it sent (a timestamp without a time
/// zone stays one), and the client's own field metadata wins over the server's. A table
/// derived from an uploaded one is described from its definition alone; in Java a filter,
/// a sort and a few other operations pass the uploaded schema on.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#schemaFromTable`,
/// `BarrageUtil#columnDefinitionsToFields` (the `wireFormatSpecified` branch)
pub fn wire_schema(table: &Arc<dyn Table>) -> Schema {
    let schema = table_schema(table.as_ref());
    let Some(uploaded) = uploaded_schema(table) else {
        return schema;
    };
    let fields: Vec<Field> = uploaded
        .fields()
        .iter()
        .filter_map(|sent| {
            let ours = schema.field_with_name(sent.name()).ok()?;
            let mut metadata = ours.metadata().clone();
            metadata.extend(sent.metadata().clone());
            Some(sent.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    let mut metadata = schema.metadata().clone();
    // the attribute holds the schema object, which Java cannot send and marks as such
    metadata.insert(
        "deephaven:unsent.attribute.BarrageSchema".to_owned(),
        String::new(),
    );
    Schema::new_with_metadata(fields, metadata)
}

/// How each column of `table` is sent, in the order of its definition: in the Arrow type
/// [`wire_schema`] declares for it. For a table a client uploaded that is the layout the
/// table stores, which is the layout the client sent.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl` (the chunk
/// writers it makes from the fields of the table's schema)
pub fn wire_columns(table: &Arc<dyn Table>) -> Vec<WireColumn> {
    let uploaded = uploaded_schema(table).is_some();
    table
        .definition()
        .columns()
        .iter()
        .map(|column| {
            if uploaded {
                WireColumn::stored(&column.column_type)
            } else {
                WireColumn::described(&column.column_type)
            }
        })
        .collect()
}

/// The rows of a table at one moment, in row set order.
pub struct Snapshot {
    pub rows: usize,
    /// One array per column, each of `rows` elements.
    pub columns: Vec<ArrayRef>,
}

/// Reads every row of every column of `table`. A refreshing table is read under the shared
/// lock of its update graph, so no cycle changes it meanwhile.
///
/// Java first tries to read without the lock and checks afterwards whether a cycle
/// interfered; the result is the same consistent state.
///
/// Behavior: `io.deephaven.engine.table.impl.remote.ConstructSnapshot#constructBackplaneSnapshotInPositionSpace`
pub fn snapshot(table: &Arc<dyn Table>) -> Result<Snapshot, TableFailure> {
    let read = || {
        let rows = table.row_set();
        let columns = (0..table.definition().columns().len())
            .map(|index| table.column(index).chunk(&rows))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Snapshot {
            rows: rows.size() as usize,
            columns,
        })
    };
    match table.update_graph() {
        Some(graph) if table.is_refreshing() => locked(graph.as_ref(), read),
        _ => read(),
    }
}
