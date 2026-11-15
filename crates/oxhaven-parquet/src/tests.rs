//! Reads of the files in `tests/data`, against what Java 42.9 reads from the same files
//! (`tests/data/README.md` says how each file was made and how Java was asked).

use crate::{read, ColumnInstruction, Layout, ReadError, ReadOptions, UnsignedLongTarget};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int64Type, TimestampNanosecondType};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use oxhaven_contracts::rowset::{RowRange, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::QueryTable;
use std::sync::Arc;

fn data(name: &str) -> String {
    format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn read_default(name: &str) -> Result<Arc<QueryTable>, ReadError> {
    read(&data(name), &ReadOptions::default())
}

fn with_layout(layout: Layout) -> ReadOptions {
    ReadOptions {
        layout: Some(layout),
        ..ReadOptions::default()
    }
}

fn instruction(parquet_name: &str, name: &str) -> ColumnInstruction {
    ColumnInstruction {
        parquet_name: parquet_name.to_string(),
        name: Some(name.to_string()),
        ..ColumnInstruction::default()
    }
}

fn with_columns(columns: Vec<ColumnInstruction>) -> ReadOptions {
    ReadOptions {
        columns,
        ..ReadOptions::default()
    }
}

/// The columns of `table` as `(name, type)`.
fn columns(table: &QueryTable) -> Vec<(String, ColumnType)> {
    let columns = table.definition().columns().iter();
    columns
        .map(|column| (column.name.clone(), column.column_type.clone()))
        .collect()
}

/// The cells of the rows `rows` of a column as text, `null` for a null. An instant is its
/// nanoseconds since the epoch. The array must have the Arrow type of the column's type.
fn cells_of(table: &QueryTable, name: &str, rows: &RowSet) -> Vec<String> {
    let index = table.definition().index_of(name).unwrap();
    let values = table.column(index).chunk(rows).unwrap();
    let column_type = &table.definition().columns()[index].column_type;
    assert_eq!(values.data_type(), &column_type.arrow_type(), "{name}");
    let values = match values.data_type() {
        DataType::Timestamp(_, _) => {
            let nanoseconds = values.as_primitive::<TimestampNanosecondType>();
            Arc::new(nanoseconds.reinterpret_cast::<Int64Type>())
        }
        _ => values,
    };
    let options = FormatOptions::default().with_null("null");
    let formatter = ArrayFormatter::try_new(values.as_ref(), &options).unwrap();
    (0..values.len())
        .map(|row| formatter.value(row).to_string())
        .collect()
}

fn cells(table: &QueryTable, name: &str) -> Vec<String> {
    cells_of(table, name, &table.row_set())
}

fn not_ported(result: Result<Arc<QueryTable>, ReadError>) -> (&'static str, String) {
    match result {
        Err(ReadError::NotPorted { java, what }) => (java, what),
        Err(other) => panic!("not refused as not ported: {other}"),
        Ok(_) => panic!("not refused"),
    }
}

fn invalid(result: Result<Arc<QueryTable>, ReadError>) -> String {
    match result {
        Err(ReadError::Invalid(text)) => text,
        Err(other) => panic!("not refused as invalid: {other}"),
        Ok(_) => panic!("not refused"),
    }
}

const REGION: i64 = 1 << 43;

/// The types and values of `types*.parquet`, which Java wrote.
fn assert_java_types(table: &QueryTable) {
    use ColumnType::*;
    let expected = [
        ("B", Boolean),
        ("Y", Byte),
        ("H", Short),
        ("I", Int),
        ("L", Long),
        ("C", Char),
        ("F", Float),
        ("D", Double),
        ("S", String),
        ("T", Instant),
    ];
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    assert_eq!(columns(table), expected);
    assert_eq!(*table.row_set(), RowSet::flat(7));
    let cells = |name| cells(table, name);
    assert_eq!(
        cells("B"),
        ["null", "false", "true", "null", "true", "false", "null"]
    );
    assert_eq!(cells("Y"), ["-3", "null", "-1", "0", "1", "2", "3"]);
    assert_eq!(
        cells("H"),
        ["-3000", "-2000", "null", "0", "1000", "2000", "3000"]
    );
    let ints = [
        "-300000", "-200000", "-100000", "null", "100000", "200000", "300000",
    ];
    assert_eq!(cells("I"), ints);
    let longs = [
        "-30000000000",
        "-20000000000",
        "-10000000000",
        "0",
        "null",
        "20000000000",
        "30000000000",
    ];
    assert_eq!(cells("L"), longs);
    assert_eq!(cells("C"), ["97", "98", "99", "100", "101", "null", "103"]);
    assert_eq!(
        cells("F"),
        ["-1.0", "-0.5", "0.0", "0.5", "1.0", "1.5", "null"]
    );
    assert_eq!(
        cells("D"),
        ["null", "NaN", "-0.5", "-0.25", "0.0", "0.25", "0.5"]
    );
    assert_eq!(cells("S"), ["s0", "s1", "null", "", "s4", "s5", "s6"]);
    // 2024-01-01T00:00:00Z and a second and a nanosecond more per row
    let start = 1_704_067_200_000_000_000_i64;
    let instants: Vec<_> = (0..7)
        .map(|row| match row {
            4 => "null".to_string(),
            _ => (start + row * 1_000_000_001).to_string(),
        })
        .collect();
    assert_eq!(cells("T"), instants);
}

#[test]
fn a_file_java_wrote_reads_with_its_types_and_nulls() {
    let table = read_default("types.parquet").unwrap();
    assert_java_types(&table);
    assert!(!table.is_refreshing());
    assert!(table.attributes().add_only);
}

#[test]
fn compressed_pages_are_read() {
    // Snappy is what Java writes when it is not told a codec
    for name in [
        "types_snappy.parquet",
        "types_gzip.parquet",
        "types_lz4.parquet",
    ] {
        assert_java_types(&read_default(name).unwrap());
    }
    // pyarrow's LZ4 is the framed codec, Java's the raw one
    for codec in ["snappy", "brotli", "gzip", "lz4"] {
        let name = format!("codec_{codec}.parquet");
        let table = read_default(&name).unwrap();
        let expected: Vec<_> = (0..20).map(|row| row.to_string()).collect();
        assert_eq!(cells(&table, "X"), expected, "{name}");
        let expected: Vec<_> = (0..20).map(|row| format!("s{}", row % 3)).collect();
        assert_eq!(cells(&table, "S"), expected, "{name}");
    }
}

#[test]
fn a_codec_that_is_not_built_in_is_refused_by_name() {
    let (java, what) = not_ported(read_default("codec_zstd.parquet"));
    assert!(java.ends_with("DeephavenCompressorAdapterFactory#getByName"));
    assert!(
        what.contains("ZSTD") && what.contains("codec_zstd.parquet"),
        "{what}"
    );
    // no file of the test data has LZO pages
    let path = std::path::Path::new("lzo.parquet");
    let lzo = crate::location::check_codec(path, parquet::basic::Compression::LZO);
    let Err(ReadError::NotPorted { what, .. }) = lzo else {
        panic!("LZO pages were accepted");
    };
    assert!(what.ends_with("LZO"), "{what}");
}

#[test]
fn vectors_and_arrays_are_told_apart_by_the_deephaven_footer_entry() {
    use ColumnType::*;
    let table = read_default("lists.parquet").unwrap();
    let expected = [
        ("K", Int),
        ("V", ColumnType::vector(Int)),
        ("W", ColumnType::vector(Double)),
        ("S", ColumnType::vector(String)),
        ("A", ColumnType::array(Int)),
        ("E", ColumnType::array(String)),
    ];
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    assert_eq!(columns(&table), expected);
    assert_eq!(cells(&table, "K"), ["0", "1"]);
    assert_eq!(cells(&table, "V"), ["[0, 2, 4]", "[1, 3]"]);
    assert_eq!(cells(&table, "W"), ["[0.0, 1.0, 2.0]", "[0.5, 1.5]"]);
    assert_eq!(cells(&table, "S"), ["[s0, s2, s4]", "[s1, s3]"]);
    assert_eq!(cells(&table, "A"), ["[0, 2]", "[1, 2]"]);
    assert_eq!(cells(&table, "E"), ["[x, null]", "[x, null]"]);
}

#[test]
fn a_file_of_another_writer_reads_as_java_reads_it() {
    use ColumnType::*;
    let table = read_default("foreign.parquet").unwrap();
    let expected = [
        ("U8", Char),
        ("U16", Char),
        ("U32", Long),
        ("I32", Int),
        ("I64", Long),
        ("I8", Byte),
        ("Ms", Instant),
        ("Us", Instant),
        ("Ns", Instant),
        ("Str", String),
        ("Dict", String),
        ("Flag", Boolean),
        ("Ints", ColumnType::array(Int)),
    ];
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    assert_eq!(columns(&table), expected);
    let cells = |name| cells(&table, name);
    // 255 is a char like any other; 65535 is Java's null char
    assert_eq!(cells("U8"), ["0", "255", "null", "7", "8", "9", "10"]);
    assert_eq!(cells("U16"), ["0", "null", "null", "7", "65534", "9", "10"]);
    assert_eq!(
        cells("U32"),
        ["0", "4294967295", "null", "7", "8", "9", "10"]
    );
    // the least value of each signed type is Java's null of it
    assert_eq!(
        cells("I32"),
        ["null", "1", "null", "2147483647", "8", "9", "10"]
    );
    let longs = ["null", "1", "null", "9223372036854775807", "8", "9", "10"];
    assert_eq!(cells("I64"), longs);
    assert_eq!(cells("I8"), ["null", "127", "null", "7", "8", "9", "10"]);
    let milliseconds = [
        "0",
        "1700000000123000000",
        "null",
        "-1000000",
        "8000000",
        "9000000",
        "10000000",
    ];
    assert_eq!(cells("Ms"), milliseconds);
    let microseconds = [
        "0",
        "1700000000123456000",
        "null",
        "-1000",
        "8000",
        "9000",
        "10000",
    ];
    assert_eq!(cells("Us"), microseconds);
    let nanoseconds = ["0", "1700000000123456789", "null", "-1", "8", "9", "10"];
    assert_eq!(cells("Ns"), nanoseconds);
    assert_eq!(cells("Str"), ["a", "", "null", "δ", "e", "f", "g"]);
    assert_eq!(cells("Dict"), ["x", "y", "null", "x", "y", "x", "z"]);
    let flags = ["true", "false", "null", "true", "false", "true", "false"];
    assert_eq!(cells("Flag"), flags);
    let lists = ["[1, 2]", "[]", "null", "[null, 3]", "[4]", "[5]", "[6]"];
    assert_eq!(cells("Ints"), lists);
}

#[test]
fn rows_are_read_across_row_groups() {
    // three row groups, of 3, 3 and 1 rows
    let table = read_default("foreign.parquet").unwrap();
    let rows = RowSet::from_ranges([
        RowRange::new(1, 1),
        RowRange::new(2, 4),
        RowRange::new(6, 6),
    ]);
    assert_eq!(
        cells_of(&table, "U8", &rows),
        ["255", "null", "7", "8", "10"]
    );
    assert_eq!(cells_of(&table, "Str", &rows), ["", "null", "δ", "e", "g"]);
    let last_group = RowSet::from_range(6, 6);
    assert_eq!(cells_of(&table, "Ints", &last_group), ["[6]"]);
    assert_eq!(cells_of(&table, "Ints", &RowSet::empty()), [""; 0]);
}

#[test]
fn an_int96_is_an_instant() {
    let table = read_default("int96.parquet").unwrap();
    assert_eq!(columns(&table), [("When".to_string(), ColumnType::Instant)]);
    assert_eq!(cells(&table, "When"), ["0", "1700000000123456789", "null"]);
}

#[test]
fn the_files_of_a_directory_are_regions_in_the_order_of_their_names() {
    for options in [
        ReadOptions::default(),
        with_layout(Layout::FlatPartitioned),
        with_layout(Layout::KvPartitioned),
    ] {
        let table = read(&data("flat"), &options).unwrap();
        // empty.parquet sorts first and has no rows, so it is no region
        let expected = RowSet::from_ranges([
            RowRange::new(0, 2),
            RowRange::new(REGION, REGION + 3),
            RowRange::new(2 * REGION, 2 * REGION + 4),
        ]);
        assert_eq!(*table.row_set(), expected);
        let p = ["0", "0", "0", "1", "1", "1", "1", "2", "2", "2", "2", "2"];
        assert_eq!(cells(&table, "P"), p);
        let x = ["0", "1", "2", "0", "1", "2", "3", "0", "1", "2", "3", "4"];
        assert_eq!(cells(&table, "X"), x);
        let s = cells(&table, "S");
        assert_eq!(s[..4], ["f0_0", "f0_1", "f0_2", "f1_0"]);
        assert_eq!(s[11], "f2_4");
    }
}

#[test]
fn hidden_files_and_directories_are_not_part_of_a_directory() {
    for layout in [Layout::KvPartitioned, Layout::FlatPartitioned] {
        let table = read(&data("hidden"), &with_layout(layout)).unwrap();
        // a.parquet, then b.parquet; not .c.parquet, .dir/d.parquet or notes.txt
        assert_eq!(cells(&table, "X"), ["2", "1"]);
    }
}

#[test]
fn the_last_file_decides_the_columns_and_a_missing_column_is_null() {
    use ColumnType::*;
    let table = read_default("evolve").unwrap();
    let expected = [("Y", String), ("X", Int), ("Z", Double)];
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    assert_eq!(columns(&table), expected);
    assert_eq!(cells(&table, "Y"), ["null", "null", "p", "q"]);
    assert_eq!(cells(&table, "X"), ["1", "2", "3", "4"]);
    assert_eq!(cells(&table, "Z"), ["null", "null", "0.5", "1.5"]);
}

#[test]
fn a_file_whose_column_has_another_type_than_the_table_is_refused() {
    // Java makes the table and fails when the column is read:
    // "Cannot convert parquet DOUBLE column to int"
    let (java, what) = not_ported(read_default("mismatch"));
    assert!(java.ends_with("ParquetColumnLocation#makeToPage"));
    assert!(
        what.contains("column X is Double in this file and Int in the table"),
        "{what}"
    );
}

#[test]
fn a_directory_of_files_without_rows_is_a_table_without_rows() {
    let directory = scratch("only_empty");
    std::fs::copy(data("flat/empty.parquet"), directory.join("empty.parquet")).unwrap();
    let table = read(directory.to_str().unwrap(), &ReadOptions::default()).unwrap();
    assert!(table.row_set().is_empty());
    let names: Vec<_> = columns(&table).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["P", "X", "S"]);
    assert!(cells(&table, "X").is_empty());
}

#[test]
fn types_without_a_column_type_are_refused_by_their_java_type() {
    let files = [
        ("no_decimal.parquet", "java.math.BigDecimal"),
        ("no_date.parquet", "java.time.LocalDate"),
        ("no_time.parquet", "java.time.LocalTime"),
        ("no_local_timestamp.parquet", "java.time.LocalDateTime"),
        ("no_uint64.parquet", "java.math.BigInteger"),
        ("no_binary.parquet", "byte[]"),
        ("no_struct.parquet", "a struct with the one field a"),
    ];
    for (name, java_type) in files {
        let (java, what) = not_ported(read_default(name));
        assert!(
            java.ends_with("ParquetSchemaReader#readParquetSchema"),
            "{name}"
        );
        assert!(
            what.starts_with("column V") && what.ends_with(java_type),
            "{what}"
        );
    }
    let legacy = ReadOptions {
        legacy: true,
        ..ReadOptions::default()
    };
    let (_, what) = not_ported(read(&data("no_binary.parquet"), &legacy));
    assert!(what.contains("java.lang.String"), "{what}");
}

#[test]
fn what_java_refuses_is_refused_with_its_reason() {
    assert_eq!(
        invalid(read_default("no_map.parquet")),
        "Encountered unsupported multi-column field V: found columns V/key_value/key and \
         V/key_value/value"
    );
    assert_eq!(
        invalid(read_default("no_nested_list.parquet")),
        "Unsupported maximum repetition level 2 in column V/list/element/list/element"
    );
}

#[test]
fn an_unsigned_long_is_read_as_the_instruction_says() {
    let target = |target| {
        with_columns(vec![ColumnInstruction {
            unsigned_long: Some(target),
            ..instruction("V", "V")
        }])
    };
    let path = data("uint64_big.parquet");
    // the file holds 1, 2^63, 2^64 - 1, null, 2^63 - 1
    let signed = read(&path, &target(UnsignedLongTarget::SignedLong)).unwrap();
    assert_eq!(columns(&signed), [("V".to_string(), ColumnType::Long)]);
    // 2^63 has the bits of Java's null long
    let expected = ["1", "null", "-1", "null", "9223372036854775807"];
    assert_eq!(cells(&signed, "V"), expected);

    // Java too makes the table and fails when the value is read
    let checked = read(&path, &target(UnsignedLongTarget::Long)).unwrap();
    let error = checked.column(0).chunk(&checked.row_set()).unwrap_err();
    let text = "the unsigned value 9223372036854775808 does not fit a long";
    assert!(error.message.ends_with(text), "{}", error.message);

    let small = read(
        &data("no_uint64.parquet"),
        &target(UnsignedLongTarget::Long),
    )
    .unwrap();
    assert_eq!(cells(&small, "V"), ["1", "null"]);
}

#[test]
fn a_column_is_renamed_by_an_instruction() {
    let renamed = with_columns(vec![instruction("X", "Q"), instruction("Nope", "R")]);
    let table = read(&data("flat/part0.parquet"), &renamed).unwrap();
    let names: Vec<_> = columns(&table).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["P", "Q", "S"]);
    assert_eq!(cells(&table, "Q"), ["0", "1", "2"]);

    // a name Java would change is refused, unless an instruction names the column
    let (java, what) = not_ported(read_default("names.parquet"));
    assert!(java.ends_with("NameValidator#legalizeColumnName"));
    assert_eq!(what, "column name 'a b'");
    let named = with_columns(vec![instruction("a b", "A"), instruction("2x", "B")]);
    let table = read(&data("names.parquet"), &named).unwrap();
    let names: Vec<_> = columns(&table).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["A", "B", "ok"]);
    assert_eq!(cells(&table, "B"), ["2"]);
}

#[test]
fn instructions_java_refuses_are_refused() {
    let path = data("flat/part0.parquet");
    let twice = with_columns(vec![instruction("X", "A"), instruction("X", "B")]);
    assert_eq!(
        invalid(read(&path, &twice)),
        "Cannot add new mapping from parquetColumnName=X to columnName=B: already mapped to \
         columnName=A"
    );
    let same = with_columns(vec![instruction("X", "A"), instruction("X", "A")]);
    assert!(read(&path, &same).is_ok());
    let duplicate = with_columns(vec![instruction("X", "P")]);
    assert_eq!(invalid(read(&path, &duplicate)), "duplicate column name: P");
    let unnamed = with_columns(vec![ColumnInstruction {
        parquet_name: "X".to_string(),
        ..ColumnInstruction::default()
    }]);
    assert!(invalid(read(&path, &unnamed)).contains("names no column"));
    let codec = with_columns(vec![ColumnInstruction {
        codec: Some("io.deephaven.util.codec.SimpleByteArrayCodec".to_string()),
        ..instruction("X", "X")
    }]);
    let (java, _) = not_ported(read(&path, &codec));
    assert!(java.ends_with("Builder#addColumnCodec"));
}

#[test]
fn a_layout_that_does_not_fit_the_path_is_refused() {
    let file = data("types.parquet");
    let directory = data("flat");
    assert_eq!(
        invalid(read(&directory, &with_layout(Layout::SingleFile))),
        format!("Parquet file must end in .parquet, found: {directory}/")
    );
    let flat = read(&file, &with_layout(Layout::FlatPartitioned));
    assert!(matches!(flat, Err(ReadError::Io(_))));
    let key_value = read(&file, &with_layout(Layout::KvPartitioned));
    assert!(invalid(key_value).ends_with("is not a directory"));
    for path in [&file, &directory] {
        let metadata = invalid(read(path, &with_layout(Layout::MetadataPartitioned)));
        assert!(metadata.starts_with("Parquet metadata file "), "{metadata}");
        assert!(metadata.ends_with("_metadata does not exist"), "{metadata}");
    }
}

#[test]
fn what_is_missing_or_empty_is_an_error() {
    let missing = read_default("nope.parquet");
    assert!(matches!(missing, Err(ReadError::Io(text)) if text.contains("nope.parquet")));
    assert!(matches!(read_default("nope"), Err(ReadError::Io(_))));
    assert_eq!(
        invalid(read("", &ReadOptions::default())),
        "Cannot convert empty source to URI"
    );

    let no_files = "Unable to infer schema for a partitioned parquet table when there are no \
                    initial parquet files";
    let empty = scratch("empty");
    for options in [ReadOptions::default(), with_layout(Layout::FlatPartitioned)] {
        assert_eq!(invalid(read(empty.to_str().unwrap(), &options)), no_files);
    }
    // a file that is not named as a Parquet file is a directory without files to Java
    let text = empty.join("notes.txt");
    std::fs::write(&text, "not parquet").unwrap();
    assert_eq!(
        invalid(read(text.to_str().unwrap(), &ReadOptions::default())),
        no_files
    );
    // and the files of a key-value layout are not seen by the flat one
    assert_eq!(
        invalid(read(&data("kv"), &with_layout(Layout::FlatPartitioned))),
        no_files
    );

    let garbage = empty.join("garbage.parquet");
    std::fs::write(&garbage, "not a parquet file at all").unwrap();
    let garbage = read(garbage.to_str().unwrap(), &ReadOptions::default());
    assert!(matches!(garbage, Err(ReadError::Io(_))));
}

#[test]
fn a_refreshing_table_is_refused() {
    let refreshing = ReadOptions {
        refreshing: true,
        ..ReadOptions::default()
    };
    assert_eq!(
        invalid(read(&data("types.parquet"), &refreshing)),
        "Unable to have a refreshing single parquet file"
    );
    // the missing file is found first, as in Java
    let missing = read(&data("nope.parquet"), &refreshing);
    assert!(matches!(missing, Err(ReadError::Io(_))));
    let (java, _) = not_ported(read(&data("flat"), &refreshing));
    assert!(java.ends_with("PollingTableLocationProvider#refresh"));
}

#[test]
fn layouts_that_are_not_ported_are_refused() {
    let (java, what) = not_ported(read_default("kv"));
    assert!(java.ends_with("ParquetKeyValuePartitionedLayout#findKeys"));
    assert!(
        what.contains("K=1/a.parquet") || what.contains("K=2/a.parquet"),
        "{what}"
    );

    for path in [data("meta"), data("meta/_metadata")] {
        let (java, _) = not_ported(read(&path, &ReadOptions::default()));
        assert!(java.ends_with("ParquetMetadataFileLayout#create"), "{path}");
    }
    let (java, _) = not_ported(read("s3://bucket/table.parquet", &ReadOptions::default()));
    assert!(java.ends_with("SeekableChannelsProviderLoader#load"));
}

/// A new empty directory for one test.
fn scratch(name: &str) -> std::path::PathBuf {
    let directory =
        std::env::temp_dir().join(format!("oxhaven-parquet-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory
}
