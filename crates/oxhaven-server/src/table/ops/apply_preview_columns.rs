//! `ApplyPreviewColumns`: the table a user interface shows in place of another, with the
//! columns it could not display replaced by text.
//!
//! The web IDE runs this on every table before it subscribes to it. For a table whose
//! columns are all of types a client can display as they are, the result is the source
//! table itself. Otherwise it is the source with each such column computed on read as the
//! text of its value: an array or vector as its first five elements, any other value as
//! Java's `toString` of it.
//!
//! In Java the new columns have classes of their own, `ArrayPreview` and `DisplayWrapper`,
//! which reach a client as strings whose `deephaven:type` is that class. Here they are
//! `String` columns, and [`crate::schema::remember_previews`] keeps what the client is
//! told about them.

use super::prelude::*;
use crate::java_text::{object_texts, vector_text};
use crate::schema::{java_type_name, previews_of, remember_previews, PreviewColumn};
use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use oxhaven_contracts::expr::{ColumnAssignment, Expr, Formula, FormulaCompiler, FormulaError};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::TableFailure;
use oxhaven_contracts::unported;
use oxhaven_engine::ops::select;
use oxhaven_proto::grpc::ApplyPreviewColumnsRequest;
use std::collections::HashMap;

/// `ArrayPreview.ARRAY_SIZE_CUTOFF`: the elements of an array or vector a preview shows.
const ARRAY_SIZE_CUTOFF: usize = 5;

/// `DisplayWrapper.MAX_CHARACTERS`, the default of the configuration property
/// `DisplayWrapper.maxCharacters`: the UTF-16 code units of a value's text that are kept.
const MAX_CHARACTERS: usize = 10_000;

/// The Java class of the preview of an array or vector.
const ARRAY_PREVIEW: &str = "io.deephaven.engine.table.impl.preview.ArrayPreview";

/// The Java class of the text of a value of a type the client did not list.
const DISPLAY_WRAPPER: &str = "io.deephaven.engine.table.impl.preview.DisplayWrapper";

/// The function the preview columns are written with, for the engine's `update_view`.
const PREVIEW_FUNCTION: &str = "preview";

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
        preview_table(
            &sources[0],
            &request.unpreviewed_types,
            request.convert_arrays,
        )
    }
}

/// How a column's values become text.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// The first [`ARRAY_SIZE_CUTOFF`] elements of an array or vector.
    ///
    /// Behavior: `ArrayPreview#fromVector`, `ArrayPreview#fromArray`
    Array,
    /// The first [`MAX_CHARACTERS`] of `toString`.
    ///
    /// Behavior: `DisplayWrapper#make`
    Display,
}

impl Kind {
    fn java_type(self) -> &'static str {
        match self {
            Kind::Array => ARRAY_PREVIEW,
            Kind::Display => DISPLAY_WRAPPER,
        }
    }
}

/// A column Java replaces by a preview of its values.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Wrapped {
    name: String,
    kind: Kind,
    /// The column's entry in the result's `ColumnDescriptions`.
    description: String,
}

/// `source` with the columns of [`wrapped_columns`] replaced by text, or `source` itself
/// when there are none.
///
/// # Panics
/// By `unported!`, for an array or vector whose elements are arrays, or vectors of
/// arrays: Java prints such an element as its class and identity hash (`[I@1b6d3586`).
///
/// Behavior: `ColumnPreviewManager#applyPreview(Table, boolean, List)`,
/// `ColumnPreviewManager#applyPreview(Table)`, `ColumnPreviewManager#makePreviewTable`
fn preview_table(
    source: &Arc<dyn Table>,
    unpreviewed: &[String],
    convert_arrays: bool,
) -> Result<Arc<dyn Table>, ExportError> {
    let wrapped = wrapped_columns(
        source.definition(),
        &previews_of(&**source),
        unpreviewed,
        convert_arrays,
    );
    if wrapped.is_empty() {
        return Ok(source.clone());
    }
    for column in &wrapped {
        let index = source
            .definition()
            .index_of(&column.name)
            .expect("a wrapped column is a column of the source");
        let column_type = &source.definition().columns()[index].column_type;
        if let (Kind::Array, Some(element)) = (column.kind, column_type.list_element()) {
            if holds_array(element) {
                unported!(
                    "java.lang.Object#toString",
                    "the preview of column {} of type {column_type:?}, whose text holds the \
                     identity hash of each array",
                    column.name
                );
            }
        }
    }
    let compiler = PreviewCompiler {
        kinds: wrapped
            .iter()
            .map(|column| (column.name.clone(), column.kind))
            .collect(),
    };
    let formulas: Vec<String> = wrapped
        .iter()
        .map(|column| format!("{0} = {PREVIEW_FUNCTION}({0})", column.name))
        .collect();
    // `makePreviewTable` then copies the attributes Java lists for a preview. Of the
    // attributes a table has here, `updateView` has copied them all already.
    let result = select::update_view(source, &formulas, &compiler)?;
    remember_previews(
        &result,
        wrapped
            .into_iter()
            .map(|column| {
                let preview = PreviewColumn {
                    java_type: column.kind.java_type(),
                    description: column.description,
                };
                (column.name, preview)
            })
            .collect(),
    );
    Ok(result)
}

/// Whether the text of a value of this type holds the text of an array.
fn holds_array(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Array(_) => true,
        ColumnType::Vector(element) => holds_array(element),
        _ => false,
    }
}

/// Whether a client is sent the values of a column of `column_type` as they are when it
/// lists no types: primitives, their boxes, `String`, `BigInteger`, `BigDecimal`,
/// `Instant`, `ZonedDateTime`, `LocalDate` and `LocalTime`. Seen on Deephaven 42.9: a
/// `Duration`, `Period`, `ZoneId`, `LocalDateTime`, `DayOfWeek`, `Object` or table column
/// is replaced by its text. A list column never reaches this. No class is on the white
/// list: its property, `ColumnPreviewManager.whiteListClasses`, is empty by default.
///
/// Behavior: `ColumnPreviewManager#isColumnTypeDisplayable`
fn is_displayable(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Boolean
            | ColumnType::Byte
            | ColumnType::Char
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::String
            | ColumnType::BigInteger
            | ColumnType::BigDecimal
            | ColumnType::Instant
            | ColumnType::ZonedDateTime
            | ColumnType::LocalDate
            | ColumnType::LocalTime
            | ColumnType::PrimitiveBoolean
    )
}

/// The columns Java replaces by a preview of their values, in the order of the definition.
/// `previews` are the columns of the table that are previews already; such a column has
/// the preview class for its type, which is neither an array nor a vector.
///
/// With a list of `unpreviewed` types, an array or vector column is replaced exactly when
/// `convert_arrays` is set, whether or not its type is listed, and any other column when
/// the Java name of its type is not in the list. No column type of this engine has a
/// preview type registered: nothing in the reference calls
/// `ColumnPreviewManager#addPreview`.
///
/// Without a list, every array and vector column is replaced, and of the others a preview
/// and those that [`is_displayable`] refuses.
///
/// The description of a replaced column names the type it had, followed on a new line by
/// the description it had, if any.
///
/// Behavior: `ColumnPreviewManager#applyPreview(Table, boolean, List)`,
/// `ColumnPreviewManager#applyPreview(Table)`, `ColumnPreviewManager#makePreviewTable`
fn wrapped_columns(
    definition: &TableDefinition,
    previews: &HashMap<String, PreviewColumn>,
    unpreviewed: &[String],
    convert_arrays: bool,
) -> Vec<Wrapped> {
    definition
        .columns()
        .iter()
        .filter_map(|column| {
            let preview = previews.get(&column.name);
            // a preview is a `String` column to the engine, so never a list
            let list = column.column_type.list_element().is_some();
            let type_name = match preview {
                Some(preview) => preview.java_type.into(),
                None => java_type_name(&column.column_type),
            };
            let kind = if list {
                (unpreviewed.is_empty() || convert_arrays).then_some(Kind::Array)
            } else if unpreviewed.is_empty() {
                (preview.is_some() || !is_displayable(&column.column_type))
                    .then_some(Kind::Display)
            } else {
                (!unpreviewed.iter().any(|listed| *listed == type_name)).then_some(Kind::Display)
            }?;
            let mut description = format!("Preview of type: {type_name}");
            if let Some(current) = preview.filter(|preview| !preview.description.is_empty()) {
                description.push('\n');
                description.push_str(&current.description);
            }
            Some(Wrapped {
                name: column.name.clone(),
                kind,
                description,
            })
        })
        .collect()
}

/// The text of each of `values`, the cells of a column of `column_type`; null for a null
/// cell.
///
/// Behavior: `ArrayPreview#fromVector`, `ArrayPreview#fromArray` (which wraps the array
/// as a vector), `DisplayWrapper#make`
fn preview_texts(kind: Kind, column_type: &ColumnType, values: &ArrayRef) -> StringArray {
    match (kind, column_type.list_element()) {
        (Kind::Array, Some(element)) => {
            let cells = values.as_list::<i32>();
            cells
                .iter()
                .map(|cell| cell.map(|cell| vector_text(element, &cell, ARRAY_SIZE_CUTOFF)))
                .collect()
        }
        _ => object_texts(column_type, values)
            .into_iter()
            .map(|text| text.map(first_characters))
            .collect(),
    }
}

/// The first [`MAX_CHARACTERS`] UTF-16 code units of `text`, as Java's `substring` cuts
/// it. A character outside the basic plane is two units; cut between them, Java keeps
/// half a character, which it sends as `?`.
///
/// Behavior: `DisplayWrapper#DisplayWrapper`
fn first_characters(text: String) -> String {
    let mut units = 0;
    for (index, character) in text.char_indices() {
        let before = units;
        units += character.len_utf16();
        if units > MAX_CHARACTERS {
            let mut cut = text[..index].to_string();
            if before < MAX_CHARACTERS {
                cut.push('?');
            }
            return cut;
        }
    }
    text
}

/// Compiles `Name = preview(Name)`, the only formula a preview table has.
struct PreviewCompiler {
    /// The columns to preview and how.
    kinds: HashMap<String, Kind>,
}

impl FormulaCompiler for PreviewCompiler {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        let column = text
            .strip_prefix(PREVIEW_FUNCTION)
            .and_then(|rest| rest.strip_prefix('('))
            .and_then(|rest| rest.strip_suffix(')'))
            .ok_or_else(|| FormulaError::Unsupported(text.to_string()))?;
        Ok(Expr::Call {
            function: PREVIEW_FUNCTION.to_string(),
            arguments: vec![Expr::Column(column.to_string())],
        })
    }

    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        let (name, expr) = text
            .split_once(" = ")
            .ok_or_else(|| FormulaError::Unsupported(text.to_string()))?;
        Ok(ColumnAssignment {
            name: name.to_string(),
            expr: self.parse(expr)?,
        })
    }

    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        let column = match expr {
            Expr::Call {
                function,
                arguments,
            } if function == PREVIEW_FUNCTION => match arguments.as_slice() {
                [Expr::Column(column)] => column,
                _ => return Err(FormulaError::Unsupported(format!("{expr:?}"))),
            },
            _ => return Err(FormulaError::Unsupported(format!("{expr:?}"))),
        };
        let unknown = || FormulaError::Type(format!("no column {column} to preview"));
        let kind = *self.kinds.get(column).ok_or_else(unknown)?;
        let index = definition.index_of(column).ok_or_else(unknown)?;
        Ok(Arc::new(PreviewFormula {
            columns: vec![column.clone()],
            source_type: definition.columns()[index].column_type.clone(),
            kind,
        }))
    }
}

/// The text of one column's values.
///
/// Behavior: the `FunctionalColumn` of `ColumnPreviewManager.PreviewColumnFactory#makeColumn`
struct PreviewFormula {
    /// The one column read.
    columns: Vec<String>,
    source_type: ColumnType,
    kind: Kind,
}

impl Formula for PreviewFormula {
    fn result_type(&self) -> &ColumnType {
        &ColumnType::String
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn column_arrays(&self) -> &[String] {
        &[]
    }

    fn uses_row_variables(&self) -> bool {
        false
    }

    fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure> {
        let index = table
            .definition()
            .index_of(&self.columns[0])
            .expect("the formula is evaluated against the definition it was compiled for");
        let source = table.column(index);
        let values = if use_prev {
            source.prev_chunk(rows)?
        } else {
            source.chunk(rows)?
        };
        debug_assert_eq!(values.len() as u64, rows.size());
        Ok(Arc::new(preview_texts(
            self.kind,
            &self.source_type,
            &values,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxhaven_contracts::schema::ColumnDefinition;

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

    /// The names and kinds of the wrapped columns of a table without previews.
    fn wrapped(
        definition: &TableDefinition,
        unpreviewed: &[String],
        convert_arrays: bool,
    ) -> Vec<(String, Kind)> {
        wrapped_columns(definition, &HashMap::new(), unpreviewed, convert_arrays)
            .into_iter()
            .map(|column| (column.name, column.kind))
            .collect()
    }

    fn of(kind: Kind, names: &[&str]) -> Vec<(String, Kind)> {
        names.iter().map(|name| ((*name).to_string(), kind)).collect()
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
        assert!(wrapped(&definition, &all, false).is_empty());
        // the primitive name does not cover a Boolean column, nor the box an int column
        let near = names(&["boolean", "java.lang.Integer", "java.lang.String"]);
        assert_eq!(
            wrapped(&definition, &near, false),
            of(Kind::Display, &["Flag", "Id", "When"])
        );
        assert!(wrapped(&definition, &[], false).is_empty());
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
            wrapped(&definition, &listed, true),
            of(Kind::Array, &["Vec", "Arr"])
        );
        // and leaving it out does not convert it.
        let only_int = names(&["int"]);
        assert!(wrapped(&definition, &only_int, false).is_empty());
        // Without a list of types Java always converts them.
        assert_eq!(
            wrapped(&definition, &[], false),
            of(Kind::Array, &["Vec", "Arr"])
        );
    }

    /// Recorded from Java 2026-11-15: the `deephaven:description` of the columns of
    /// `ApplyPreviewColumns` over uploaded `int[]`, `IntVector`, `ObjectVector` and `int`
    /// columns, and over the result of `ApplyPreviewColumns` itself.
    #[test]
    fn the_description_names_the_type_and_keeps_the_description_before_it() {
        let definition = TableDefinition::new(vec![
            ColumnDefinition::new("Arr", ColumnType::array(ColumnType::Int)),
            ColumnDefinition::new("Vec", ColumnType::vector(ColumnType::Int)),
            ColumnDefinition::new("Obj", ColumnType::vector(ColumnType::String)),
            ColumnDefinition::new("Id", ColumnType::Int),
        ])
        .unwrap();
        let first = wrapped_columns(&definition, &HashMap::new(), &names(&["long"]), true);
        let descriptions: Vec<&str> = first.iter().map(|c| c.description.as_str()).collect();
        assert_eq!(
            descriptions,
            [
                "Preview of type: int[]",
                "Preview of type: io.deephaven.vector.IntVector",
                "Preview of type: io.deephaven.vector.ObjectVector",
                "Preview of type: int",
            ]
        );

        // the result of that, previewed again: the columns are strings to the engine
        let strings = TableDefinition::new(
            ["Arr", "Vec", "Obj", "Id"]
                .map(|name| ColumnDefinition::new(name, ColumnType::String))
                .to_vec(),
        )
        .unwrap();
        let previews: HashMap<String, PreviewColumn> = first
            .iter()
            .filter(|column| column.name != "Obj")
            .map(|column| {
                let preview = PreviewColumn {
                    java_type: column.kind.java_type(),
                    description: column.description.clone(),
                };
                (column.name.clone(), preview)
            })
            .collect();
        let listed = names(&["java.lang.String"]);
        let second = wrapped_columns(&strings, &previews, &listed, true);
        let second: Vec<(&str, Kind, &str)> = second
            .iter()
            .map(|c| (c.name.as_str(), c.kind, c.description.as_str()))
            .collect();
        assert_eq!(
            second,
            [
                (
                    "Arr",
                    Kind::Display,
                    "Preview of type: io.deephaven.engine.table.impl.preview.ArrayPreview\n\
                     Preview of type: int[]"
                ),
                (
                    "Vec",
                    Kind::Display,
                    "Preview of type: io.deephaven.engine.table.impl.preview.ArrayPreview\n\
                     Preview of type: io.deephaven.vector.IntVector"
                ),
                (
                    "Id",
                    Kind::Display,
                    "Preview of type: io.deephaven.engine.table.impl.preview.DisplayWrapper\n\
                     Preview of type: int"
                ),
            ]
        );
        // without a list a preview class is not one a client can display
        let legacy = wrapped_columns(&strings, &previews, &[], false);
        let legacy: Vec<&str> = legacy.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(legacy, ["Arr", "Vec", "Id"]);
    }

    #[test]
    fn the_text_of_a_value_is_cut_at_ten_thousand_utf16_units() {
        let exact = "x".repeat(MAX_CHARACTERS);
        assert_eq!(first_characters(exact.clone()), exact);
        assert_eq!(first_characters(format!("{exact}y")), exact);
        // two units each: 5000 fit
        let wide = "\u{1F600}".repeat(5001);
        assert_eq!(first_characters(wide), "\u{1F600}".repeat(5000));
        // a pair cut in two is sent as a question mark
        let split = format!("{}\u{1F600}z", "x".repeat(MAX_CHARACTERS - 1));
        assert_eq!(
            first_characters(split),
            format!("{}?", "x".repeat(MAX_CHARACTERS - 1))
        );
    }

    fn texts(array: &StringArray) -> Vec<Option<&str>> {
        array.iter().collect()
    }

    /// Recorded from Java 2026-11-15, `ApplyPreviewColumns` with `convert_arrays` over
    /// uploaded `list<int32>` cells of 0, 1, 2, 5, 6 and 12 elements and a null cell, as
    /// `int[]` and as `IntVector`, and over an `ObjectVector` of `IntVector`.
    #[test]
    fn an_array_or_vector_is_previewed_by_its_first_five_elements() {
        use oxhaven_testkit::{ints, list_of};
        let elements = |n: usize| (0..n).map(|i| [Some(1), None, Some(-3)][i % 3]);
        let lengths = [0, 1, 2, 5, 6, 12];
        let values: ArrayRef = Arc::new(arrow::array::Int32Array::from_iter(
            lengths.iter().flat_map(|n| elements(*n)),
        ));
        let expected = [
            Some("[]"),
            Some("[1]"),
            Some("[1, ]"),
            Some("[1, ,-3,1, ]"),
            Some("[1, ,-3,1, , ...]"),
            Some("[1, ,-3,1, , ...]"),
            None,
        ];
        for column_type in [
            ColumnType::array(ColumnType::Int),
            ColumnType::vector(ColumnType::Int),
        ] {
            let cells = list_of(
                &column_type,
                lengths.iter().map(|n| Some(*n)).chain([None]),
                values.clone(),
            );
            let previewed = preview_texts(Kind::Array, &column_type, &cells);
            assert_eq!(texts(&previewed), expected, "{column_type:?}");
        }

        // a vector inside shows ten of its elements
        let inner = ColumnType::vector(ColumnType::Int);
        let outer = ColumnType::vector(inner.clone());
        let cells = list_of(
            &outer,
            [Some(2), Some(6)],
            list_of(
                &inner,
                [Some(12), Some(0), Some(1), None, Some(1), Some(1), Some(1), Some(1)],
                ints((0..17).map(|i| i % 12)),
            ),
        );
        let previewed = preview_texts(Kind::Array, &outer, &cells);
        assert_eq!(
            texts(&previewed),
            [
                Some("[[0,1,2,3,4,5,6,7,8,9, ...],[]]"),
                Some("[[0], ,[1],[2],[3], ...]")
            ]
        );
    }

    /// Recorded from Java 2026-11-15: `ApplyPreviewColumns` that lists only `int`, over
    /// `float`, `char`, `Boolean` and `Instant` columns.
    #[test]
    fn any_other_value_is_previewed_by_its_text() {
        use arrow::array::{
            BooleanArray, Float32Array, TimestampNanosecondArray, UInt16Array,
        };
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![Some(1.0e7), None, Some(0.1)]));
        let previewed = preview_texts(Kind::Display, &ColumnType::Float, &floats);
        assert_eq!(texts(&previewed), [Some("1.0E7"), None, Some("0.1")]);
        let chars: ArrayRef = Arc::new(UInt16Array::from(vec![Some(0x41), Some(0xe9), None]));
        let previewed = preview_texts(Kind::Display, &ColumnType::Char, &chars);
        assert_eq!(texts(&previewed), [Some("A"), Some("\u{e9}"), None]);
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
        let previewed = preview_texts(Kind::Display, &ColumnType::Boolean, &flags);
        assert_eq!(texts(&previewed), [Some("true"), None]);
        let long: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(MAX_CHARACTERS + 5)]));
        let previewed = preview_texts(Kind::Display, &ColumnType::String, &long);
        assert_eq!(previewed.value(0), "x".repeat(MAX_CHARACTERS));
        let instants: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![Some(0), None]).with_timezone("UTC"));
        let previewed = preview_texts(Kind::Display, &ColumnType::Instant, &instants);
        assert_eq!(texts(&previewed), [Some("1970-01-01T00:00:00Z"), None]);
    }

    /// Seen on Deephaven 42.9: `ColumnPreviewManager#applyPreview` with the types the web
    /// client lists, and without a list, over columns made by `java.time` factories.
    #[test]
    fn time_values_are_previewed_by_their_text() {
        use arrow::array::{
            DurationNanosecondArray, Int32Array, Int8Array, StructArray, TimestampNanosecondArray,
        };
        use arrow::datatypes::DataType;
        let preview = |column_type: ColumnType, values: ArrayRef| {
            texts(&preview_texts(Kind::Display, &column_type, &values))
                .into_iter()
                .map(|text| text.map(str::to_string))
                .collect::<Vec<_>>()
        };
        let owned = |expected: &[Option<&str>]| {
            expected
                .iter()
                .map(|text| text.map(str::to_string))
                .collect::<Vec<_>>()
        };
        let durations: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(1_500_000_000),
            Some(-1_500_000_000),
            Some(3_600_000_000_000 * 25 + 61_000_000_123),
            Some(-500_000_000),
            None,
            Some(-60_500_000_000),
            Some(-3_661_000_000_001),
            Some(-3_600_000_000_000),
            Some(-500),
            Some(3_600_000_000_001),
            Some(-i64::MAX),
            Some(-119_999_999_999),
        ]));
        assert_eq!(
            preview(ColumnType::Duration, durations),
            owned(&[
                Some("PT0S"),
                Some("PT0.000000001S"),
                Some("PT-0.000000001S"),
                Some("PT1.5S"),
                Some("PT-1.5S"),
                Some("PT25H1M1.000000123S"),
                Some("PT-0.5S"),
                None,
                Some("PT-1M-0.5S"),
                Some("PT-1H-1M-1.000000001S"),
                Some("PT-1H"),
                Some("PT-0.0000005S"),
                Some("PT1H0.000000001S"),
                Some("PT-2562047H-47M-16.854775807S"),
                Some("PT-1M-59.999999999S"),
            ])
        );

        let DataType::Struct(fields) = ColumnType::Period.arrow_type() else {
            unreachable!("a Period is a struct")
        };
        let part = |values: [i32; 8]| Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
        let periods: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                part([0, 1, -1, 0, 2, 0, 0, 0]),
                part([0, 2, 0, -3, 0, 0, 14, 0]),
                part([0, 3, 0, 0, 0, -40, 0, 0]),
            ],
            Some(vec![true, true, true, true, true, true, true, false].into()),
        ));
        assert_eq!(
            preview(ColumnType::Period, periods),
            owned(&[
                Some("P0D"),
                Some("P1Y2M3D"),
                Some("P-1Y"),
                Some("P-3M"),
                Some("P2Y"),
                Some("P-40D"),
                Some("P14M"),
                None,
            ])
        );

        let zones: ArrayRef = Arc::new(StringArray::from(vec![
            Some("UTC"),
            Some("Z"),
            Some("America/New_York"),
            Some("Etc/GMT+5"),
            None,
        ]));
        assert_eq!(
            preview(ColumnType::ZoneId, zones),
            owned(&[
                Some("UTC"),
                Some("Z"),
                Some("America/New_York"),
                Some("Etc/GMT+5"),
                None
            ])
        );

        let DataType::Struct(fields) = ColumnType::ZonedDateTime.arrow_type() else {
            unreachable!("a ZonedDateTime is a struct")
        };
        let instants = TimestampNanosecondArray::from(vec![
            1_710_055_800_000_000_000,
            1_710_021_600_000_001_000,
            1_710_045_000_000_002_000,
            1_710_048_600_000_003_000,
            1_710_012_600_000_004_000,
            0,
        ])
        .with_timezone("UTC");
        let ids = StringArray::from(vec![
            "America/New_York",
            "Asia/Kolkata",
            "UTC",
            "Z",
            "Australia/Lord_Howe",
            "UTC",
        ]);
        let zoned: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(instants), Arc::new(ids)],
            Some(vec![true, true, true, true, true, false].into()),
        ));
        assert_eq!(
            preview(ColumnType::ZonedDateTime, zoned),
            owned(&[
                Some("2024-03-10T03:30-04:00[America/New_York]"),
                Some("2024-03-10T03:30:00.000001+05:30[Asia/Kolkata]"),
                Some("2024-03-10T04:30:00.000002Z[UTC]"),
                Some("2024-03-10T05:30:00.000003Z"),
                Some("2024-03-10T06:30:00.000004+11:00[Australia/Lord_Howe]"),
                None,
            ])
        );

        let days: ArrayRef = Arc::new(Int8Array::from(vec![Some(1), Some(7), None]));
        assert_eq!(
            preview(ColumnType::DayOfWeek, days),
            owned(&[Some("MONDAY"), Some("SUNDAY"), None])
        );
    }

    /// Seen on Deephaven 42.9: a column declared `Object` previews the text of each value,
    /// and a column of the tables of a partition aggregation `QueryTable` for each.
    #[test]
    fn objects_and_tables_are_previewed_by_their_text() {
        use arrow::array::{Int32Array, Int64Array};
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(3)]));
        let previewed = preview_texts(
            Kind::Display,
            &ColumnType::object(Some(ColumnType::Int)),
            &ints,
        );
        assert_eq!(texts(&previewed), [Some("0"), Some("3")]);
        let nulls = arrow::array::new_null_array(&arrow::datatypes::DataType::Utf8, 2);
        let previewed = preview_texts(Kind::Display, &ColumnType::object(None), &nulls);
        assert_eq!(texts(&previewed), [None, None]);
        let handles: ArrayRef = Arc::new(Int64Array::from(vec![Some(4), None]));
        let tables = ColumnType::Table(oxhaven_contracts::schema::TableClass::QueryTable);
        let previewed = preview_texts(Kind::Display, &tables, &handles);
        assert_eq!(texts(&previewed), [Some("QueryTable"), None]);
    }

    /// Seen on Deephaven 42.9: `ColumnPreviewManager#applyPreview(Table)` (no list of
    /// types) replaces the columns of these types by their text, and keeps the others.
    #[test]
    fn without_a_list_the_types_a_client_cannot_display_are_wrapped() {
        let definition = TableDefinition::new(vec![
            ColumnDefinition::new("D", ColumnType::Duration),
            ColumnDefinition::new("P", ColumnType::Period),
            ColumnDefinition::new("Z", ColumnType::ZoneId),
            ColumnDefinition::new("LDT", ColumnType::LocalDateTime),
            ColumnDefinition::new("DOW", ColumnType::DayOfWeek),
            ColumnDefinition::new("ZDT", ColumnType::ZonedDateTime),
            ColumnDefinition::new("LD", ColumnType::LocalDate),
            ColumnDefinition::new("LT", ColumnType::LocalTime),
            ColumnDefinition::new("BD", ColumnType::BigDecimal),
            ColumnDefinition::new("BI", ColumnType::BigInteger),
            ColumnDefinition::new("C", ColumnType::Char),
            ColumnDefinition::new("O", ColumnType::object(Some(ColumnType::Int))),
            ColumnDefinition::new(
                "Tab",
                ColumnType::Table(oxhaven_contracts::schema::TableClass::QueryTable),
            ),
        ])
        .unwrap();
        assert_eq!(
            wrapped(&definition, &[], false),
            of(Kind::Display, &["D", "P", "Z", "LDT", "DOW", "O", "Tab"])
        );
        let descriptions: Vec<String> = wrapped_columns(&definition, &HashMap::new(), &[], false)
            .into_iter()
            .map(|column| column.description)
            .collect();
        assert_eq!(
            descriptions,
            [
                "Preview of type: java.time.Duration",
                "Preview of type: java.time.Period",
                "Preview of type: java.time.ZoneId",
                "Preview of type: java.time.LocalDateTime",
                "Preview of type: java.time.DayOfWeek",
                "Preview of type: java.lang.Object",
                "Preview of type: io.deephaven.engine.table.impl.QueryTable",
            ]
        );
    }

    fn lists_table(column_type: ColumnType) -> Arc<dyn Table> {
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        use oxhaven_testkit::{ints, list_of};
        let definition = TableDefinition::new(vec![
            ColumnDefinition::new("Id", ColumnType::Int),
            ColumnDefinition::new("Xs", column_type.clone()),
        ])
        .unwrap();
        let element = column_type.list_element().unwrap();
        let elements = if element.list_element().is_some() {
            list_of(element, [Some(1), Some(2), Some(3)], ints(1..=6))
        } else {
            ints(1..=3)
        };
        let cells = list_of(&column_type, [Some(2), None, Some(1)], elements);
        let schema = Schema::new(vec![
            Field::new("Id", arrow::datatypes::DataType::Int32, true),
            Field::new("Xs", cells.data_type().clone(), true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![ints([7, 8, 9]), cells]).unwrap();
        oxhaven_engine::table::in_memory_table(definition, &[batch]).unwrap()
    }

    fn column_texts(table: &dyn Table, name: &str) -> Vec<Option<String>> {
        let index = table.definition().index_of(name).unwrap();
        let values = table.column(index).chunk(&table.row_set()).unwrap();
        let values = values.as_string::<i32>();
        values.iter().map(|text| text.map(str::to_owned)).collect()
    }

    #[test]
    fn the_preview_table_has_text_columns_and_says_what_they_were() {
        let source = lists_table(ColumnType::vector(ColumnType::Int));
        let listed = names(&["int"]);

        // nothing to replace: the table itself
        let same = preview_table(&source, &listed, false).unwrap();
        assert!(Arc::ptr_eq(&same, &source));
        assert!(previews_of(&*same).is_empty());

        let result = preview_table(&source, &listed, true).unwrap();
        let types: Vec<&ColumnType> = result
            .definition()
            .columns()
            .iter()
            .map(|column| &column.column_type)
            .collect();
        assert_eq!(types, [&ColumnType::Int, &ColumnType::String]);
        assert_eq!(
            column_texts(&*result, "Xs"),
            [Some("[1,2]".to_owned()), None, Some("[3]".to_owned())]
        );
        let previews = previews_of(&*result);
        assert_eq!(previews.len(), 1);
        assert_eq!(
            previews["Xs"],
            PreviewColumn {
                java_type: ARRAY_PREVIEW,
                description: "Preview of type: io.deephaven.vector.IntVector".to_owned(),
            }
        );

        // the preview of that: both columns as the text of their values
        let again = preview_table(&result, &names(&["long"]), true).unwrap();
        assert_eq!(
            column_texts(&*again, "Id"),
            ["7", "8", "9"].map(|text| Some(text.to_owned()))
        );
        assert_eq!(column_texts(&*again, "Xs"), column_texts(&*result, "Xs"));
        let previews = previews_of(&*again);
        assert_eq!(previews["Id"].java_type, DISPLAY_WRAPPER);
        assert_eq!(previews["Xs"].java_type, DISPLAY_WRAPPER);
        assert_eq!(
            previews["Xs"].description,
            "Preview of type: io.deephaven.engine.table.impl.preview.ArrayPreview\n\
             Preview of type: io.deephaven.vector.IntVector"
        );
        // and the first preview is still what it was
        assert_eq!(previews_of(&*result)["Xs"].java_type, ARRAY_PREVIEW);
    }

    /// A preview column of a refreshing table reads the previous values of its source
    /// when asked for previous values.
    #[test]
    fn the_preview_of_a_refreshing_table_follows_it() {
        use oxhaven_contracts::graph::UpdateGraph;
        use oxhaven_engine::graph::SteppedUpdateGraph;
        use oxhaven_testkit::{definition, ints, list_of, ScriptedTable, TableChange};
        let stepper = SteppedUpdateGraph::new();
        let graph: Arc<dyn UpdateGraph> = stepper.clone();
        let vector = ColumnType::vector(ColumnType::Int);
        let source = ScriptedTable::new(
            graph,
            definition(&[("Xs", vector.clone())]),
            RowSet::from_keys([0, 1]),
            vec![list_of(&vector, [Some(1), Some(2)], ints([1, 2, 3]))],
        );
        let table: Arc<dyn Table> = source.table();
        let formula = PreviewCompiler {
            kinds: [("Xs".to_owned(), Kind::Array)].into(),
        }
        .compile(
            &PreviewCompiler {
                kinds: HashMap::new(),
            }
            .parse("preview(Xs)")
            .unwrap(),
            table.definition(),
        )
        .unwrap();
        assert_eq!(formula.columns(), ["Xs".to_owned()]);
        assert_eq!(formula.result_type(), &ColumnType::String);

        struct Seen {
            formula: Arc<dyn Formula>,
            table: Arc<dyn Table>,
            texts: std::sync::Mutex<Vec<Vec<Option<String>>>>,
        }
        impl oxhaven_contracts::graph::Dependency for Seen {
            fn satisfied(&self, _step: u64) -> bool {
                true
            }
        }
        impl oxhaven_contracts::table::UpdateListener for Seen {
            fn on_update(
                &self,
                _update: &oxhaven_contracts::update::TableUpdate,
            ) -> Result<(), TableFailure> {
                let read = |use_prev: bool| {
                    let rows = if use_prev {
                        self.table.prev_row_set()
                    } else {
                        self.table.row_set()
                    };
                    let values = self.formula.evaluate(&*self.table, &rows, use_prev).unwrap();
                    let values = values.as_string::<i32>();
                    values.iter().map(|text| text.map(str::to_owned)).collect()
                };
                let mut texts = self.texts.lock().unwrap();
                texts.push(read(true));
                texts.push(read(false));
                Ok(())
            }
            fn on_failure(&self, _failure: &TableFailure) {}
        }
        let seen = Arc::new(Seen {
            formula,
            table: table.clone(),
            texts: Default::default(),
        });
        let listener: Arc<dyn oxhaven_contracts::table::UpdateListener> = seen.clone();
        table.add_listener(&listener);
        source.push(
            TableChange::new().modify([1], [(0, list_of(&vector, [Some(0)], ints([])))]),
        );
        stepper.run_cycle();
        let text = |value: &str| Some(value.to_owned());
        assert_eq!(
            *seen.texts.lock().unwrap(),
            [
                vec![text("[1]"), text("[2,3]")],
                vec![text("[1]"), text("[]")]
            ]
        );
    }

    #[test]
    fn a_vector_of_vectors_is_previewed() {
        let source = lists_table(ColumnType::vector(ColumnType::vector(ColumnType::Int)));
        let result = preview_table(&source, &names(&["int"]), true).unwrap();
        assert_eq!(
            column_texts(&*result, "Xs"),
            [Some("[[1],[2,3]]".to_owned()), None, Some("[[4,5,6]]".to_owned())]
        );
    }

    #[test]
    #[should_panic(
        expected = "unported: java.lang.Object#toString: the preview of column Xs of type \
                    Array(Array(Int))"
    )]
    fn the_preview_of_arrays_in_an_array_is_not_ported() {
        let source = lists_table(ColumnType::array(ColumnType::array(ColumnType::Int)));
        let _ = preview_table(&source, &names(&["int"]), true);
    }

    #[test]
    #[should_panic(expected = "unported: java.lang.Object#toString")]
    fn the_preview_of_arrays_in_a_vector_of_vectors_is_not_ported() {
        let inner = ColumnType::vector(ColumnType::array(ColumnType::Int));
        assert!(holds_array(&inner));
        assert!(!holds_array(&ColumnType::vector(ColumnType::Int)));
        let source = lists_table(ColumnType::vector(ColumnType::array(ColumnType::Int)));
        let _ = preview_table(&source, &[], false);
    }
}
