//! Array and vector columns as clients see them: the schema, against every schema of such
//! a column that the oracle corpus recorded from Java, and the record batches of a `DoGet`
//! and of a Barrage snapshot.
//!
//! No fixture in `flight/fixtures` or `barrage/fixtures` was captured from a table with an
//! array or vector column, so the record batches are not compared with Java's bytes. They
//! are decoded as a client decodes them and compared with the table, and their layout is
//! compared with what `io.deephaven.extensions.barrage.chunk.ListChunkWriter` writes.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    make_array, new_empty_array, Array, ArrayData, ArrayRef, Float64Array, Int32Array, ListArray,
    RecordBatch, StringArray, UInt32Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::flight::FlightData;
use oxhaven_testkit::{definition, ints, list_of, ScriptedTable};

use crate::barrage::{snapshot_request_messages, WireOptions};
use crate::flight::tests::support::{batch_of, schema_of_message, Batch};
use crate::flight::{do_get_messages, encode_batch};
use crate::schema::{
    arrow_schema, column_type_for_java_names, java_component_type_name, java_type_name,
    SchemaContext, COMPONENT_TYPE_KEY, TYPE_KEY,
};

// ---------------------------------------------------------------------------------------
// The schema
// ---------------------------------------------------------------------------------------

fn arrow_files(directory: &Path, into: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(directory)
        .unwrap_or_else(|problem| panic!("{}: {problem}", directory.display()));
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            arrow_files(&path, into);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "arrow")
        {
            into.push(path);
        }
    }
}

fn is_list_class(name: &str) -> bool {
    name.ends_with("[]") || name.starts_with("io.deephaven.vector.")
}

/// Every field of an array or vector column in the corpus is the field this server sends
/// for the column type its `deephaven:type` and `deephaven:componentType` name: the same
/// Arrow type, element field included, and the same metadata.
#[test]
fn list_fields_are_described_as_java_recorded_them() {
    let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle/corpus");
    let mut files = Vec::new();
    arrow_files(&corpus, &mut files);
    let mut seen = BTreeSet::new();
    let mut fields = 0;
    for path in files {
        let file = std::fs::File::open(&path).unwrap();
        let schema = FileReader::try_new(file, None).unwrap().schema();
        for recorded in schema.fields() {
            let Some(name) = recorded.metadata().get(TYPE_KEY) else {
                continue;
            };
            if !is_list_class(name) {
                continue;
            }
            let component = recorded.metadata().get(COMPONENT_TYPE_KEY);
            let at = format!("{}: {}", path.display(), recorded.name());
            let column_type = column_type_for_java_names(name, component.map(String::as_str))
                .unwrap_or_else(|| panic!("{at}: no column type for {name} of {component:?}"));
            let ours = arrow_schema(
                &definition(&[(recorded.name().as_str(), column_type.clone())]),
                &SchemaContext::default(),
            );
            assert_eq!(ours.field(0), recorded.as_ref(), "{at}: {column_type:?}");
            seen.insert((name.clone(), component.cloned().unwrap_or_default()));
            fields += 1;
        }
    }
    // The kinds of array and vector the corpus has. One that is added shows up here, and
    // a walk that finds no files does not pass.
    let expected: BTreeSet<(String, String)> = [
        ("double[]", "double"),
        ("int[]", "int"),
        ("io.deephaven.vector.DoubleVector", "double"),
        ("io.deephaven.vector.FloatVector", "float"),
        ("io.deephaven.vector.IntVector", "int"),
        ("io.deephaven.vector.LongVector", "long"),
        ("io.deephaven.vector.ObjectVector", "java.lang.Boolean"),
        ("io.deephaven.vector.ObjectVector", "java.lang.String"),
        ("io.deephaven.vector.ObjectVector", "java.time.Instant"),
        ("java.lang.Boolean[]", "java.lang.Boolean"),
        ("java.lang.String[]", "java.lang.String"),
        ("java.time.Instant[]", "java.time.Instant"),
        ("long[]", "long"),
    ]
    .into_iter()
    .map(|(name, component)| (name.to_owned(), component.to_owned()))
    .collect();
    assert_eq!(seen, expected);
    assert!(fields > expected.len(), "{fields} fields");
}

#[test]
fn names_of_array_and_vector_types_round_trip() {
    let elements = [
        ColumnType::Boolean,
        ColumnType::Byte,
        ColumnType::Char,
        ColumnType::Short,
        ColumnType::Int,
        ColumnType::Long,
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::String,
        ColumnType::Instant,
        ColumnType::array(ColumnType::Double),
        ColumnType::vector(ColumnType::Int),
        ColumnType::array(ColumnType::array(ColumnType::String)),
    ];
    for element in elements {
        for column_type in [
            ColumnType::vector(element.clone()),
            ColumnType::array(element.clone()),
        ] {
            let name = java_type_name(&column_type);
            let component = java_component_type_name(&column_type);
            assert_eq!(component.as_deref(), Some(&*java_type_name(&element)));
            assert_eq!(
                column_type_for_java_names(&name, component.as_deref()),
                Some(column_type.clone()),
                "{name} of {component:?}"
            );
        }
    }
    let name = |column_type: ColumnType| java_type_name(&column_type).into_owned();
    assert_eq!(
        name(ColumnType::vector(ColumnType::Char)),
        "io.deephaven.vector.CharVector"
    );
    assert_eq!(
        name(ColumnType::vector(ColumnType::Boolean)),
        "io.deephaven.vector.ObjectVector"
    );
    assert_eq!(
        name(ColumnType::array(ColumnType::array(ColumnType::Int))),
        "int[][]"
    );
    assert_eq!(java_component_type_name(&ColumnType::Int), None);

    // An array or a primitive vector says its elements without a component type,
    let named = |name, component| column_type_for_java_names(name, component);
    assert_eq!(
        named("io.deephaven.vector.LongVector", None),
        Some(ColumnType::vector(ColumnType::Long))
    );
    assert_eq!(
        named("java.time.Instant[]", None),
        Some(ColumnType::array(ColumnType::Instant))
    );
    // an ObjectVector does not,
    assert_eq!(named("io.deephaven.vector.ObjectVector", None), None);
    // and one of ObjectVectors does not say the elements of those.
    assert_eq!(
        named(
            "io.deephaven.vector.ObjectVector",
            Some("io.deephaven.vector.ObjectVector")
        ),
        None
    );
    // A component type that contradicts the type is no column type,
    assert_eq!(named("io.deephaven.vector.IntVector", Some("long")), None);
    assert_eq!(named("int[]", Some("long")), None);
    // nor is an ObjectVector of a primitive, nor an array of a class that is none.
    assert_eq!(named("io.deephaven.vector.ObjectVector", Some("int")), None);
    assert_eq!(named("java.math.BigDecimal[]", None), None);
    assert_eq!(named("int", None), Some(ColumnType::Int));
}

fn field_of(column_type: ColumnType) -> Field {
    let schema = arrow_schema(
        &definition(&[("X", column_type)]),
        &SchemaContext::default(),
    );
    schema.field(0).clone()
}

#[test]
fn a_list_field_names_its_elements_and_has_an_unnamed_nullable_element_field() {
    let field = field_of(ColumnType::vector(ColumnType::array(ColumnType::Double)));
    let pairs: BTreeSet<(&str, &str)> = field
        .metadata()
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    assert_eq!(
        pairs,
        BTreeSet::from([
            ("deephaven:componentType", "double[]"),
            ("deephaven:isSortable", "true"),
            ("deephaven:type", "io.deephaven.vector.ObjectVector"),
        ])
    );
    let element = |data_type| Arc::new(Field::new("", data_type, true));
    assert_eq!(
        *field.data_type(),
        DataType::List(element(DataType::List(element(DataType::Float64))))
    );
    assert!(field.is_nullable());
}

/// Java sorts an array only by a registered comparator, and has none for `Boolean[]` or
/// for an array of arrays; a vector is `Comparable`. No recorded schema has an array that
/// is not sortable: this is `ComparatorRegistry#reset` as read.
#[test]
fn an_array_without_a_comparator_is_not_marked_sortable() {
    let sortable = |column_type| {
        field_of(column_type)
            .metadata()
            .get("deephaven:isSortable")
            .cloned()
    };
    let yes = Some("true".to_owned());
    assert_eq!(sortable(ColumnType::array(ColumnType::Boolean)), None);
    assert_eq!(
        sortable(ColumnType::array(ColumnType::array(ColumnType::Int))),
        None
    );
    assert_eq!(sortable(ColumnType::array(ColumnType::Instant)), yes);
    assert_eq!(sortable(ColumnType::vector(ColumnType::Boolean)), yes);
    assert_eq!(
        sortable(ColumnType::vector(ColumnType::array(ColumnType::Boolean))),
        yes
    );
}

/// As Java sent them 2026-11-15: `head` of an uploaded `list<int8>` column came back as
/// `binary`, and `group_by` of `group_by` of a `String` column as `list<list<utf8>>`, both
/// with the class names of the column type.
#[test]
fn a_byte_array_is_binary_and_an_inner_object_vector_holds_text() {
    let bytes = field_of(ColumnType::array(ColumnType::Byte));
    assert_eq!(bytes.data_type(), &DataType::Binary);
    assert_eq!(bytes.metadata()[TYPE_KEY], "byte[]");
    assert_eq!(bytes.metadata()[COMPONENT_TYPE_KEY], "byte");

    let element = |data_type| Arc::new(Field::new("", data_type, true));
    let byte_arrays = field_of(ColumnType::vector(ColumnType::array(ColumnType::Byte)));
    assert_eq!(
        byte_arrays.data_type(),
        &DataType::List(element(DataType::Binary))
    );
    assert_eq!(byte_arrays.metadata()[COMPONENT_TYPE_KEY], "byte[]");

    let texts = DataType::List(element(DataType::List(element(DataType::Utf8))));
    for inner in [ColumnType::String, ColumnType::Boolean, ColumnType::Instant] {
        let vectors = field_of(ColumnType::vector(ColumnType::vector(inner.clone())));
        assert_eq!(vectors.data_type(), &texts, "{inner:?}");
        assert_eq!(
            vectors.metadata()[TYPE_KEY],
            "io.deephaven.vector.ObjectVector"
        );
        assert_eq!(
            vectors.metadata()[COMPONENT_TYPE_KEY],
            "io.deephaven.vector.ObjectVector"
        );
        let arrays = field_of(ColumnType::array(ColumnType::vector(inner)));
        assert_eq!(arrays.data_type(), &texts);
        assert_eq!(
            arrays.metadata()[TYPE_KEY],
            "io.deephaven.vector.ObjectVector[]"
        );
    }
}

#[test]
fn a_vector_of_primitives_may_be_an_element() {
    let field = field_of(ColumnType::array(ColumnType::vector(ColumnType::Int)));
    assert_eq!(
        field.metadata()[TYPE_KEY],
        "io.deephaven.vector.IntVector[]"
    );
    assert_eq!(
        field.metadata()[COMPONENT_TYPE_KEY],
        "io.deephaven.vector.IntVector"
    );
}

// ---------------------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------------------

fn columns() -> Vec<(&'static str, ColumnType)> {
    vec![
        ("K", ColumnType::Int),
        ("V", ColumnType::vector(ColumnType::Int)),
        ("A", ColumnType::array(ColumnType::String)),
        (
            "N",
            ColumnType::vector(ColumnType::array(ColumnType::Double)),
        ),
    ]
}

fn int_elements(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<Int32Array>())
}

/// Five rows:
///
/// | K | V | A | N |
/// | - | - | - | - |
/// | 1 | `[1, 2]` | `["a", "bc"]` | `[[1.5], []]` |
/// | 2 | null | `[]` | null |
/// | 3 | `[]` | null | `[null]` |
/// | 4 | `[null, 5]` | `[null]` | `[]` |
/// | 5 | `[6, 7, 8]` | `["d"]` | `[[2.5, 3.5]]` |
///
/// The null cell of `V` lies over an element, 99, which is no part of the column.
fn cells() -> Vec<ArrayRef> {
    let [_, (_, vector), (_, array), (_, nested)] = &columns()[..] else {
        unreachable!()
    };
    let DataType::List(element) = vector.arrow_type() else {
        unreachable!()
    };
    let over_an_element = ListArray::new(
        element,
        OffsetBuffer::new(vec![0, 2, 3, 3, 5, 8].into()),
        int_elements([
            Some(1),
            Some(2),
            Some(99),
            None,
            Some(5),
            Some(6),
            Some(7),
            Some(8),
        ]),
        Some(NullBuffer::from(vec![true, false, true, true, true])),
    );
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("bc"),
        None,
        Some("d"),
    ]));
    let inner = list_of(
        nested.list_element().unwrap(),
        [Some(1), Some(0), None, Some(2)],
        Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
    );
    vec![
        ints([1, 2, 3, 4, 5]),
        Arc::new(over_an_element),
        list_of(array, [Some(2), Some(0), None, Some(1), Some(1)], strings),
        list_of(nested, [Some(2), None, Some(1), Some(0), Some(1)], inner),
    ]
}

fn static_table() -> Arc<dyn Table> {
    let definition = definition(&columns());
    let fields: Vec<Field> = columns()
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cells()).unwrap();
    in_memory_table(definition, &[batch]).unwrap()
}

/// One column of a record batch, read as the Arrow format says: its field node, its
/// validity buffer, the buffers of its type, and for a list its element column.
fn read_column(
    data_type: &DataType,
    nodes: &mut std::slice::Iter<'_, (i64, i64)>,
    buffers: &mut std::slice::Iter<'_, (i64, i64)>,
    body: &[u8],
) -> ArrayRef {
    let mut buffer = || {
        let (offset, length) = *buffers.next().expect("a buffer");
        Buffer::from(&body[offset as usize..(offset + length) as usize])
    };
    let (length, null_count) = *nodes.next().expect("a field node");
    let validity = buffer();
    let (own, children) = match data_type {
        DataType::List(element) => {
            let offsets = buffer();
            let elements = read_column(element.data_type(), nodes, buffers, body);
            (vec![offsets], vec![elements.to_data()])
        }
        DataType::Utf8 | DataType::Binary => (vec![buffer(), buffer()], vec![]),
        _ => (vec![buffer()], vec![]),
    };
    if length == 0 {
        assert!(own.iter().all(Buffer::is_empty), "{data_type}: no rows");
        return new_empty_array(data_type);
    }
    assert_eq!(validity.is_empty(), null_count == 0, "{data_type}");
    let nulls = (null_count > 0).then_some(validity);
    let data = ArrayData::try_new(data_type.clone(), length as usize, nulls, 0, own, children)
        .unwrap_or_else(|problem| panic!("{data_type}: {problem}"));
    assert_eq!(data.null_count() as i64, null_count, "{data_type}");
    make_array(data)
}

/// The columns of a record batch message of a table with the columns of [`columns`].
fn decode(batch: &Batch) -> Vec<ArrayRef> {
    let mut nodes = batch.nodes.iter();
    let mut buffers = batch.buffers.iter();
    let decoded: Vec<ArrayRef> = columns()
        .iter()
        .map(|(_, column_type)| {
            read_column(
                &column_type.arrow_type(),
                &mut nodes,
                &mut buffers,
                &batch.body,
            )
        })
        .collect();
    assert!(nodes.next().is_none() && buffers.next().is_none());
    decoded
}

fn assert_same_cells(decoded: &[ArrayRef], expected: &[ArrayRef], context: &str) {
    assert_eq!(decoded.len(), expected.len(), "{context}");
    for ((name, _), (ours, expected)) in columns().iter().zip(decoded.iter().zip(expected)) {
        assert_eq!(ours.to_data(), expected.to_data(), "{context}: {name}");
    }
}

fn int_cells(body: &[u8], (offset, length): (i64, i64)) -> Vec<i32> {
    body[offset as usize..(offset + length) as usize]
        .chunks(4)
        .map(|cell| i32::from_le_bytes(cell.try_into().unwrap()))
        .collect()
}

#[test]
fn do_get_sends_the_cells_of_list_columns_laid_out_as_java_lays_them_out() {
    let table = static_table();
    let messages = do_get_messages(&table).unwrap();
    assert_eq!(messages.len(), 2);

    let schema = schema_of_message(&messages[0]);
    assert_eq!(
        schema,
        arrow_schema(table.definition(), &SchemaContext::of(table.as_ref()))
    );
    let described: Vec<(&str, Option<&str>)> = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.metadata()[TYPE_KEY].as_str(),
                field.metadata().get(COMPONENT_TYPE_KEY).map(String::as_str),
            )
        })
        .collect();
    assert_eq!(
        described,
        [
            ("int", None),
            ("io.deephaven.vector.IntVector", Some("int")),
            ("java.lang.String[]", Some("java.lang.String")),
            ("io.deephaven.vector.ObjectVector", Some("double[]")),
        ]
    );

    let batch = batch_of(&messages[1]);
    assert_same_cells(&decode(&batch), &cells(), "DoGet");

    // A list column is its node and then the node of its elements: of V the seven
    // elements of the cells that are not null, one of them null.
    assert_eq!(batch.rows, 5);
    assert_eq!(
        batch.nodes,
        [
            (5, 0),
            (5, 1),
            (7, 1),
            (5, 1),
            (4, 1),
            (5, 1),
            (4, 1),
            (3, 0)
        ]
    );
    // Validity, then offsets, then the element column; everything padded to 8 bytes,
    // and no validity buffer where nothing is null.
    assert_eq!(
        batch.buffers,
        [
            // K
            (0, 0),
            (0, 24),
            // V: validity, 6 offsets, and of its elements validity and 7 ints
            (24, 8),
            (32, 24),
            (56, 8),
            (64, 32),
            // A: validity, 6 offsets, and of its elements validity, 5 offsets, 4 bytes
            (96, 8),
            (104, 24),
            (128, 8),
            (136, 24),
            (160, 8),
            // N: validity, 6 offsets, of its elements validity and 5 offsets, and of
            // theirs no validity and 3 doubles
            (168, 8),
            (176, 24),
            (200, 8),
            (208, 24),
            (232, 0),
            (232, 24),
        ]
    );
    assert_eq!(batch.body.len(), 256);
    assert_eq!(batch.body[24], 0b11101, "the second cell of V is null");
    // The offsets count the elements sent: the null cell has none, though it lay over one.
    assert_eq!(int_cells(&batch.body, batch.buffers[3]), [0, 2, 2, 2, 4, 7]);
    assert_eq!(batch.body[56], 0b111_1011, "the third element is null");
    assert_eq!(
        int_cells(&batch.body, batch.buffers[5]),
        [1, 2, i32::MIN, 5, 6, 7, 8, 0],
        "the elements, Java's null value under the null, and padding"
    );
}

#[test]
fn a_list_column_without_rows_has_nodes_and_no_bytes() {
    let cells = cells();
    let batch = encode_batch(
        &[cells[1].slice(0, 0), cells[3].slice(0, 0)],
        &[true; 2],
        0,
        0,
    );
    let batch = batch.unwrap();
    let batch = batch_of(&FlightData {
        data_header: batch.header.into(),
        data_body: batch.body.into(),
        ..Default::default()
    });
    assert_eq!(batch.nodes, [(0, 0); 5]);
    // validity and offsets per list, validity and values of the innermost elements
    assert_eq!(batch.buffers, [(0, 0); 10]);
    assert!(batch.body.is_empty());
}

#[test]
fn a_slice_of_a_list_column_is_sent_with_offsets_from_zero() {
    let cells = cells();
    // rows 3 and 4 of V: [null, 5] and [6, 7, 8]
    let batch = encode_batch(&[cells[1].clone()], &[true], 3, 2).unwrap();
    let batch = batch_of(&FlightData {
        data_header: batch.header.into(),
        data_body: batch.body.into(),
        ..Default::default()
    });
    assert_eq!(batch.nodes, [(2, 0), (5, 1)]);
    assert_eq!(batch.buffers, [(0, 0), (0, 16), (16, 8), (24, 24)]);
    assert_eq!(int_cells(&batch.body, batch.buffers[1]), [0, 2, 5, 0]);
    assert_eq!(
        int_cells(&batch.body, batch.buffers[3]),
        [i32::MIN, 5, 6, 7, 8, 0]
    );
}

/// A `byte[]` column and a vector of vectors of strings are stored as lists and sent as
/// `Binary` and as lists of lists of text, by `DoGet` and in a Barrage snapshot.
#[test]
fn columns_sent_in_another_type_are_converted_by_do_get_and_in_a_snapshot() {
    let bytes = ColumnType::array(ColumnType::Byte);
    let inner = ColumnType::vector(ColumnType::Boolean);
    let vectors = ColumnType::vector(inner.clone());
    let definition = definition(&[("B", bytes.clone()), ("V", vectors.clone())]);
    let stored: Vec<ArrayRef> = vec![
        list_of(
            &bytes,
            [Some(2), None, Some(0)],
            Arc::new(arrow::array::Int8Array::from(vec![Some(1), None])),
        ),
        list_of(
            &vectors,
            [Some(2), Some(0), None],
            list_of(
                &inner,
                [Some(2), None],
                Arc::new(arrow::array::BooleanArray::from(vec![Some(true), None])),
            ),
        ),
    ];
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), stored).unwrap();
    let table: Arc<dyn Table> = in_memory_table(definition, &[batch]).unwrap();

    let element = |data_type| Arc::new(Field::new("", data_type, true));
    let texts = DataType::List(element(DataType::List(element(DataType::Utf8))));
    let check = |batch: &Batch, context: &str| {
        use arrow::array::AsArray;
        let mut nodes = batch.nodes.iter();
        let mut buffers = batch.buffers.iter();
        let binary = read_column(&DataType::Binary, &mut nodes, &mut buffers, &batch.body);
        let lists = read_column(&texts, &mut nodes, &mut buffers, &batch.body);
        assert!(
            nodes.next().is_none() && buffers.next().is_none(),
            "{context}"
        );
        let binary = binary.as_binary::<i32>();
        let binary: Vec<Option<&[u8]>> = binary.iter().collect();
        assert_eq!(
            binary,
            [Some([1u8, 0x80].as_slice()), None, Some([].as_slice())],
            "{context}"
        );
        let rows = lists.as_list::<i32>();
        assert!(rows.is_valid(1) && rows.value(1).is_empty() && rows.is_null(2));
        let first = rows.value(0);
        let first = first.as_list::<i32>();
        assert!(first.is_null(1), "{context}");
        let elements = first.value(0);
        let elements: Vec<Option<&str>> = elements.as_string::<i32>().iter().collect();
        assert_eq!(elements, [Some("true"), None], "{context}");
    };

    let messages = do_get_messages(&table).unwrap();
    assert_eq!(messages.len(), 2);
    let schema = schema_of_message(&messages[0]);
    assert_eq!(schema.field(0).data_type(), &DataType::Binary);
    assert_eq!(schema.field(1).data_type(), &texts);
    check(&batch_of(&messages[1]), "DoGet");

    let options = WireOptions::default();
    let messages =
        snapshot_request_messages(&table, &[true, true], None, None, false, options).unwrap();
    assert_eq!(messages.len(), 1);
    check(&batch_of(&messages[0]), "snapshot");
}

fn nullable() -> Vec<bool> {
    vec![true; columns().len()]
}

fn snapshot_batches(
    table: &Arc<dyn Table>,
    viewport: Option<&RowSet>,
    options: WireOptions,
) -> Vec<Batch> {
    let messages = snapshot_request_messages(table, &nullable(), None, viewport, false, options);
    messages.unwrap().iter().map(batch_of).collect()
}

fn rows_of(cells: &[ArrayRef], rows: &[u32]) -> Vec<ArrayRef> {
    let positions = UInt32Array::from(rows.to_vec());
    cells
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &positions, None).unwrap())
        .collect()
}

#[test]
fn a_barrage_snapshot_carries_the_cells_of_list_columns() {
    let cells = cells();
    // a static table, which is read in parts,
    let batches = snapshot_batches(&static_table(), None, WireOptions::default());
    assert_eq!(batches.len(), 1);
    assert_same_cells(&decode(&batches[0]), &cells, "static");

    // a viewport of it, whose rows are picked out of the columns,
    let viewport = RowSet::from_keys([1, 3, 4]);
    let batches = snapshot_batches(&static_table(), Some(&viewport), WireOptions::default());
    assert_eq!(batches.len(), 1);
    assert_same_cells(
        &decode(&batches[0]),
        &rows_of(&cells, &[1, 3, 4]),
        "viewport",
    );

    // and a refreshing table, which is read at once under the lock of its graph.
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let source = ScriptedTable::new(
        graph,
        definition(&columns()),
        RowSet::from_keys([2, 3, 5, 8, 13]),
        cells.clone(),
    );
    let refreshing: Arc<dyn Table> = source.table();
    let batches = snapshot_batches(&refreshing, None, WireOptions::default());
    assert_eq!(batches.len(), 1);
    assert_same_cells(&decode(&batches[0]), &cells, "refreshing");
    let viewport = RowSet::from_keys([0, 1]);
    let batches = snapshot_batches(&refreshing, Some(&viewport), WireOptions::default());
    assert_same_cells(
        &decode(&batches[0]),
        &rows_of(&cells, &[0, 1]),
        "refreshing viewport",
    );
}

/// With Deephaven nulls a primitive element is null by its value and its column has no
/// validity buffer, as a primitive column has none; a null cell and a null `String`
/// element are still told by a validity buffer.
#[test]
fn deephaven_nulls_reach_the_elements_of_a_list_and_not_its_cells() {
    let options = WireOptions {
        use_deephaven_nulls: true,
        ..Default::default()
    };
    let batch = &snapshot_batches(&static_table(), None, options)[0];
    assert_eq!(
        batch.nodes,
        [
            (5, 0),
            (5, 1),
            (7, 0),
            (5, 1),
            (4, 1),
            (5, 1),
            (4, 1),
            (3, 0)
        ]
    );
    let lengths: Vec<i64> = batch.buffers.iter().map(|(_, length)| *length).collect();
    assert_eq!(lengths[2..6], [8, 24, 0, 32], "V");
    assert_eq!(lengths[6..11], [8, 24, 8, 24, 8], "A");
    assert_eq!(
        int_cells(&batch.body, batch.buffers[5])[..7],
        [1, 2, i32::MIN, 5, 6, 7, 8]
    );
}

#[test]
fn the_preview_limit_cuts_every_list_to_its_first_or_last_elements() {
    let [_, (_, vector), _, (_, nested)] = &columns()[..] else {
        unreachable!()
    };
    let limited = |limit| {
        let options = WireOptions {
            preview_list_length_limit: limit,
            ..Default::default()
        };
        let batches = snapshot_batches(&static_table(), None, options);
        assert_eq!(batches.len(), 1);
        decode(&batches[0])
    };

    // the first element of each cell, and of each list within a cell
    let first = limited(1);
    let expected = list_of(
        vector,
        [Some(1), None, Some(0), Some(1), Some(1)],
        int_elements([Some(1), None, Some(6)]),
    );
    assert_eq!(first[1].to_data(), expected.to_data());
    let inner = list_of(
        nested.list_element().unwrap(),
        [Some(1), None, Some(1)],
        Arc::new(Float64Array::from(vec![1.5, 2.5])),
    );
    let expected = list_of(nested, [Some(1), None, Some(1), Some(0), Some(1)], inner);
    assert_eq!(first[3].to_data(), expected.to_data());

    // the last two
    let last = limited(-2);
    let expected = list_of(
        vector,
        [Some(2), None, Some(0), Some(2), Some(2)],
        int_elements([Some(1), Some(2), None, Some(5), Some(7), Some(8)]),
    );
    assert_eq!(last[1].to_data(), expected.to_data());

    // and a limit no cell reaches changes nothing
    assert_same_cells(&limited(3), &cells(), "limit 3");
    assert_same_cells(&limited(-3), &cells(), "limit -3");
}
