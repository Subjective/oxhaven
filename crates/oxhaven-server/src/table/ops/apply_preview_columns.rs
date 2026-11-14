//! `ApplyPreviewColumns`: the table a user interface shows in place of another, with the
//! columns it could not display replaced by text.
//!
//! The web IDE runs this on every table before it subscribes to it. For a table whose
//! columns are all of types a client can display as they are, the result is the source
//! table itself. A table with an array or vector column that the request converts is not
//! ported.

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
        let wrapped = wrapped_columns(
            source.definition(),
            &request.unpreviewed_types,
            request.convert_arrays,
        );
        if wrapped.is_empty() {
            return Ok(source.clone());
        }
        // The result has a column of type `DisplayWrapper` or `ArrayPreview` for each of
        // these, and the attributes `PreviewParentTable` and `ColumnDescriptions`. The
        // contracts have neither those column types nor those attributes.
        unported!(
            "io.deephaven.engine.table.impl.preview.ColumnPreviewManager#makePreviewTable",
            "columns {wrapped:?} are replaced by a preview of their values"
        )
    }
}

/// The columns Java replaces by a preview of their values.
///
/// With a list of `unpreviewed` types, an array or vector column is replaced exactly when
/// `convert_arrays` is set, whether or not its type is listed, and any other column when
/// the Java name of its type is not in the list. No column type of this engine has a
/// preview type registered: nothing in the reference calls
/// `ColumnPreviewManager#addPreview`.
///
/// Without a list, every array and vector column is replaced, and of the others those that
/// `ColumnPreviewManager#isColumnTypeDisplayable` refuses. It accepts primitives, their
/// boxes, `String` and `Instant`, so no other column of this engine is replaced.
///
/// Behavior: `ColumnPreviewManager#applyPreview(Table, boolean, List)`,
/// `ColumnPreviewManager#applyPreview(Table)`
fn wrapped_columns<'a>(
    definition: &'a TableDefinition,
    unpreviewed: &[String],
    convert_arrays: bool,
) -> Vec<&'a str> {
    definition
        .columns()
        .iter()
        .filter(|column| {
            let list = column.column_type.list_element().is_some();
            if unpreviewed.is_empty() {
                list
            } else if list {
                convert_arrays
            } else {
                let type_name = java_type_name(&column.column_type);
                !unpreviewed.iter().any(|listed| *listed == type_name)
            }
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
        assert!(wrapped_columns(&definition, &all, false).is_empty());
        // the primitive name does not cover a Boolean column, nor the box an int column
        let near = names(&["boolean", "java.lang.Integer", "java.lang.String"]);
        assert_eq!(
            wrapped_columns(&definition, &near, false),
            vec!["Flag", "Id", "When"]
        );
        assert!(wrapped_columns(&definition, &[], false).is_empty());
    }

    #[test]
    fn an_array_or_vector_column_is_wrapped_by_convert_arrays_and_not_by_the_list() {
        let definition = TableDefinition::new(vec![
            ColumnDefinition::new("Id", ColumnType::Int),
            ColumnDefinition::new("Vec", ColumnType::vector(ColumnType::Int)),
            ColumnDefinition::new("Arr", ColumnType::array(ColumnType::String)),
        ])
        .unwrap();
        // Listing the type of a list column does not keep it from being converted,
        let listed = names(&["int", "io.deephaven.vector.IntVector"]);
        assert_eq!(
            wrapped_columns(&definition, &listed, true),
            vec!["Vec", "Arr"]
        );
        // and leaving it out does not convert it.
        let only_int = names(&["int"]);
        assert!(wrapped_columns(&definition, &only_int, false).is_empty());
        // Without a list of types Java always converts them.
        assert_eq!(wrapped_columns(&definition, &[], false), vec!["Vec", "Arr"]);
    }
}
