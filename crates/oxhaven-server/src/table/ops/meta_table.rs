//! `MetaTable`: a static table describing the columns of another table.

use super::prelude::*;
use crate::schema::java_type_name;
use arrow::array::{ArrayRef, BooleanArray, StringArray};
use oxhaven_column::FlatSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use oxhaven_engine::table::{QueryTable, TableSpec};
use oxhaven_proto::grpc::MetaTableRequest;

/// Behavior: `io.deephaven.server.table.ops.MetaTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = MetaTableRequest;

    fn result_id(request: &MetaTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &MetaTableRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        _context: &OpContext,
        _request: &MetaTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(meta_table(sources[0].definition())?)
    }
}

/// One row per column of `definition`: its `Name`, its Java `DataType`, its `ColumnType`
/// and whether it `IsPartitioning`: `Partitioning` and `true` for a partitioning column,
/// `Normal` and `false` for any other.
///
/// The result is static and, as in Java, not marked flat.
///
/// Behavior: `io.deephaven.engine.util.TableTools#metaTable`
pub fn meta_table(definition: &TableDefinition) -> Result<Arc<QueryTable>, TableFailure> {
    let described = definition.columns();
    let names: StringArray = described.iter().map(|c| Some(c.name.as_str())).collect();
    let data_types: StringArray = described
        .iter()
        .map(|c| Some(java_type_name(&c.column_type).into_owned()))
        .collect();
    let column_types: StringArray = described
        .iter()
        .map(|c| Some(if c.partitioning { "Partitioning" } else { "Normal" }))
        .collect();
    let partitioning: BooleanArray = described.iter().map(|c| Some(c.partitioning)).collect();

    let columns: [(&str, ColumnType, ArrayRef); 4] = [
        ("Name", ColumnType::String, Arc::new(names)),
        ("DataType", ColumnType::String, Arc::new(data_types)),
        ("ColumnType", ColumnType::String, Arc::new(column_types)),
        (
            "IsPartitioning",
            ColumnType::Boolean,
            Arc::new(partitioning),
        ),
    ];
    let mut definitions = Vec::with_capacity(columns.len());
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(columns.len());
    for (name, column_type, values) in columns {
        definitions.push(ColumnDefinition::new(name, column_type.clone()));
        sources.push(Arc::new(FlatSource::new(column_type, values)?));
    }
    let definition = TableDefinition::new(definitions).map_err(|e| TableFailure {
        message: e.to_string(),
    })?;
    Ok(QueryTable::new_static(TableSpec::new(
        definition,
        RowSet::flat(described.len() as u64),
        sources,
    )))
}
