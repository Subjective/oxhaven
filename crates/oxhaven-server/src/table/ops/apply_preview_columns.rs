//! `ApplyPreviewColumns`: the table a user interface shows in place of another, with the
//! columns it could not display replaced by text.
//!
//! The web IDE runs this on every table before it subscribes to it. For a table whose
//! columns are all of types a client can display, which is every table of this engine today,
//! the result is the source table itself.

use super::prelude::*;
use crate::schema::java_type_name;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::unported;
use oxhaven_proto::grpc::ApplyPreviewColumnsRequest;

/// Behavior: `io.deephaven.server.table.ops.ApplyPreviewColumnsGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = ApplyPreviewColumnsRequest;

    fn result_id(request: &ApplyPreviewColumnsRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &ApplyPreviewColumnsRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Behavior: `ApplyPreviewColumnsGrpcImpl#validateRequest`
    fn validate(request: &ApplyPreviewColumnsRequest) -> Result<(), Status> {
        if request.unpreviewed_types.is_empty() && request.convert_arrays {
            return Err(Status::invalid_argument(
                "convert_arrays is true but no types were provided",
            ));
        }
        Ok(())
    }

    /// Behavior: `ApplyPreviewColumnsGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &ApplyPreviewColumnsRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let source = &sources[0];
        let wrapped = if request.unpreviewed_types.is_empty() {
            Vec::new()
        } else {
            wrapped_columns(source.definition(), &request.unpreviewed_types)
        };
        if wrapped.is_empty() {
            return Ok(source.clone());
        }
        // The result has a column of type `DisplayWrapper` for each of these, and the
        // attributes `PreviewParentTable` and `ColumnDescriptions`. The contracts have
        // neither that column type nor those attributes.
        unported!(
            "io.deephaven.engine.table.impl.preview.ColumnPreviewManager#makePreviewTable",
            "columns {wrapped:?} have types the request does not list as unpreviewed"
        )
    }
}

/// The columns Java replaces by their text because the client did not name their type in
/// `unpreviewed_types`: the names of every column whose Java type name is not in the list.
///
/// This is the whole of Java's decision for the column types of this engine. None of them
/// is an array, a vector or a Python list, which `convert_arrays` is about, and none has a
/// preview type registered: nothing in the reference calls `ColumnPreviewManager#addPreview`.
///
/// Without a list of types Java replaces the columns that
/// `ColumnPreviewManager#isColumnTypeDisplayable` refuses. It accepts primitives, their
/// boxes, `String` and `Instant`, so no column of this engine is replaced; the handler does
/// not call this function then.
///
/// Behavior: `ColumnPreviewManager#applyPreview(Table, boolean, List)`,
/// `ColumnPreviewManager#applyPreview(Table)`
fn wrapped_columns<'a>(definition: &'a TableDefinition, unpreviewed: &[String]) -> Vec<&'a str> {
    definition
        .columns()
        .iter()
        .filter(|column| {
            let type_name = java_type_name(&column.column_type);
            !unpreviewed.iter().any(|listed| listed == type_name)
        })
        .map(|column| column.name.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxhaven_contracts::schema::{ColumnDefinition, ColumnType};

    fn definition() -> TableDefinition {
        TableDefinition::new(vec![
            ColumnDefinition::new("Flag", ColumnType::Boolean),
            ColumnDefinition::new("Id", ColumnType::Int),
            ColumnDefinition::new("Sym", ColumnType::String),
            ColumnDefinition::new("When", ColumnType::Instant),
        ])
        .unwrap()
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn a_column_is_wrapped_unless_the_java_name_of_its_type_is_listed() {
        let definition = definition();
        let all = names(&[
            "java.lang.Boolean",
            "int",
            "java.lang.String",
            "java.time.Instant",
        ]);
        assert!(wrapped_columns(&definition, &all).is_empty());
        // the primitive name does not cover a Boolean column, nor the box an int column
        let near = names(&["boolean", "java.lang.Integer", "java.lang.String"]);
        assert_eq!(
            wrapped_columns(&definition, &near),
            vec!["Flag", "Id", "When"]
        );
    }
}
