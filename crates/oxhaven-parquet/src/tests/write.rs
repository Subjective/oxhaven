//! Files written from the tables of `tests/data`, against the files Java 42.9 wrote from
//! the same tables: the Parquet schema, the `deephaven` footer entry and the table read
//! back. `oracle/scenarios/python/parquet_write_*.py` compare with a running reference.

use super::{assert_java_types, cells, columns, data, read_default, scratch};
use crate::{delete, read, write, ReadOptions, RowGroups, WriteColumn, WriteError, WriteOptions};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::QueryTable;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::path::Path;
use std::sync::Arc;

fn footer(path: &str) -> ParquetMetaData {
    let file = std::fs::File::open(path).unwrap();
    SerializedFileReader::new(file).unwrap().metadata().clone()
}

/// The Parquet schema of a file, as text.
fn schema(footer: &ParquetMetaData) -> String {
    let mut text = Vec::new();
    let root = footer.file_metadata().schema();
    parquet::schema::printer::print_schema(&mut text, root);
    String::from_utf8(text).unwrap()
}

/// The `deephaven` entry of a file's footer.
fn deephaven(footer: &ParquetMetaData) -> String {
    let entries = footer.file_metadata().key_value_metadata().unwrap();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].key, "deephaven");
    entries[0].value.clone().unwrap()
}

fn row_groups(footer: &ParquetMetaData) -> Vec<i64> {
    let groups = footer.row_groups().iter();
    groups.map(|group| group.num_rows()).collect()
}

/// Whether the chunk of the column at `path` in row group `group` has a dictionary.
fn has_dictionary(footer: &ParquetMetaData, group: usize, path: &str) -> bool {
    let columns = footer.row_group(group).columns().iter();
    let column = columns
        .filter(|column| column.column_path().string() == path)
        .next()
        .unwrap();
    column.dictionary_page_offset().is_some()
}

fn target(directory: &Path, name: &str) -> String {
    directory.join(name).to_str().unwrap().to_string()
}

fn write_one(
    table: &Arc<QueryTable>,
    path: &str,
    options: &WriteOptions,
) -> Result<(), WriteError> {
    let table: Arc<dyn Table> = table.clone();
    write(&[table], &[path], options)
}

fn read_back(path: &str) -> Arc<QueryTable> {
    read(path, &ReadOptions::default()).unwrap()
}

fn invalid(result: Result<(), WriteError>) -> String {
    match result {
        Err(WriteError::Invalid(text)) => text,
        Err(other) => panic!("not refused as invalid: {other}"),
        Ok(()) => panic!("not refused"),
    }
}

fn not_ported(result: Result<(), WriteError>) -> &'static str {
    match result {
        Err(WriteError::NotPorted { java, .. }) => java,
        Err(other) => panic!("not refused as not ported: {other}"),
        Ok(()) => panic!("not refused"),
    }
}

fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn with_groups(row_groups: RowGroups) -> WriteOptions {
    WriteOptions {
        row_groups,
        ..WriteOptions::default()
    }
}

#[test]
fn every_type_is_written_as_java_writes_it() {
    let directory = scratch("write-types");
    let path = target(&directory, "types.parquet");
    let table = read_default("types.parquet").unwrap();
    write_one(&table, &path, &WriteOptions::default()).unwrap();

    let java = footer(&data("types_snappy.parquet"));
    let ours = footer(&path);
    assert_eq!(schema(&ours), schema(&java));
    assert_eq!(deephaven(&ours), deephaven(&java));
    assert_eq!(deephaven(&ours), r#"{"version":"42.9"}"#);
    assert_eq!(row_groups(&ours), [7]);
    for column in ours.row_group(0).columns() {
        assert_eq!(column.compression(), Compression::SNAPPY);
        // only a string column has a dictionary
        let path = column.column_path().string();
        assert_eq!(
            column.dictionary_page_offset().is_some(),
            path == "S",
            "{path}"
        );
    }
    assert_java_types(&read_back(&path));
}

#[test]
fn vectors_and_arrays_are_written_as_lists_with_the_vectors_named_in_the_footer() {
    let directory = scratch("write-lists");
    let path = target(&directory, "lists.parquet");
    let table = read_default("lists.parquet").unwrap();
    write_one(&table, &path, &WriteOptions::default()).unwrap();

    let java = footer(&data("lists.parquet"));
    let ours = footer(&path);
    assert_eq!(schema(&ours), schema(&java));
    assert_eq!(deephaven(&ours), deephaven(&java));
    assert!(deephaven(&ours).contains(r#"{"columnName":"V","specialType":"Vector"}"#));

    let back = read_back(&path);
    assert_eq!(columns(&back), columns(&table));
    for (name, _) in columns(&table) {
        assert_eq!(cells(&back, &name), cells(&table, &name), "{name}");
    }
}

#[test]
fn a_table_without_rows_is_a_file_without_row_groups() {
    let directory = scratch("write-empty");
    let path = target(&directory, "empty.parquet");
    let table = read(&data("flat/empty.parquet"), &ReadOptions::default());
    let table = match table {
        Ok(table) if table.row_set().is_empty() => table,
        _ => panic!("flat/empty.parquet is the file without rows"),
    };
    for groups in [RowGroups::Single, RowGroups::MaxGroups(4)] {
        write_one(&table, &path, &with_groups(groups)).unwrap();
        assert_eq!(row_groups(&footer(&path)), [0i64; 0]);
        let back = read_back(&path);
        assert_eq!(columns(&back), columns(&table));
        assert!(back.row_set().is_empty());
    }
    let refused = write_one(&table, &path, &with_groups(RowGroups::MaxRows(4)));
    assert_eq!(
        invalid(refused),
        "Number of groups must be at least 1, got: 0"
    );
}

#[test]
fn rows_are_split_into_row_groups_as_the_instructions_say() {
    let directory = scratch("write-groups");
    let path = target(&directory, "groups.parquet");
    let table = read_default("types.parquet").unwrap();
    let by = |columns: &[&str], max_rows| RowGroups::ByGroups {
        columns: columns.iter().map(|name| name.to_string()).collect(),
        max_rows,
    };
    let cases = [
        (RowGroups::Single, vec![7]),
        (RowGroups::MaxGroups(1), vec![7]),
        (RowGroups::MaxGroups(2), vec![4, 3]),
        (RowGroups::MaxGroups(3), vec![3, 2, 2]),
        (RowGroups::MaxGroups(20), vec![1; 7]),
        (RowGroups::MaxRows(3), vec![3, 2, 2]),
        (RowGroups::MaxRows(4), vec![4, 3]),
        (RowGroups::MaxRows(7), vec![7]),
        (RowGroups::MaxRows(100), vec![7]),
        (by(&["I"], None), vec![1; 7]),
        (by(&["I", "S"], Some(3)), vec![1; 7]),
    ];
    for (groups, expected) in cases {
        write_one(&table, &path, &with_groups(groups.clone())).unwrap();
        assert_eq!(row_groups(&footer(&path)), expected, "{groups:?}");
        assert_java_types(&read_back(&path));
    }
    // B is null, false, true, null, ...: the nulls are not next to each other
    let refused = write_one(&table, &path, &with_groups(by(&["B"], None)));
    assert!(invalid(refused).starts_with("Misordered for Grouping column(s)"));
    let refused = write_one(&table, &path, &with_groups(by(&["Nope"], None)));
    assert!(
        matches!(refused, Err(WriteError::Invalid(_))),
        "{refused:?}"
    );
    for groups in [
        RowGroups::MaxGroups(0),
        RowGroups::MaxRows(0),
        by(&[], None),
    ] {
        let refused = write_one(&table, &path, &with_groups(groups.clone()));
        assert!(matches!(refused, Err(WriteError::Invalid(_))), "{groups:?}");
    }
    // the last file written stands
    assert_eq!(row_groups(&footer(&path)), [1; 7]);
}

#[test]
fn a_string_column_has_a_dictionary_while_it_fits_the_limits() {
    let directory = scratch("write-dictionary");
    let path = target(&directory, "dictionary.parquet");
    let table = read_default("types.parquet").unwrap();
    // S has six distinct strings, "" among them, of 10 bytes: five as "s0", and ""
    let distinct: std::collections::HashSet<String> = cells(&table, "S").into_iter().collect();
    assert_eq!(distinct.len(), 7, "six strings and the null");
    let limits = |keys, size| WriteOptions {
        max_dictionary_keys: keys,
        max_dictionary_size: size,
        ..WriteOptions::default()
    };
    let cases = [
        (limits(None, None), true),
        (limits(Some(6), None), true),
        (limits(Some(5), None), false),
        (limits(Some(0), None), false),
        (limits(None, Some(10)), true),
        (limits(None, Some(9)), false),
    ];
    for (options, expected) in cases {
        write_one(&table, &path, &options).unwrap();
        assert_eq!(
            has_dictionary(&footer(&path), 0, "S"),
            expected,
            "{options:?}"
        );
        assert_java_types(&read_back(&path));
    }
    // a column that is asked for a dictionary has one whatever the limits
    let hinted = WriteOptions {
        columns: vec![WriteColumn {
            name: "S".to_string(),
            parquet_name: Some("S".to_string()),
            use_dictionary: true,
            ..WriteColumn::default()
        }],
        ..limits(Some(0), None)
    };
    write_one(&table, &path, &hinted).unwrap();
    assert!(has_dictionary(&footer(&path), 0, "S"));
    for refused in [limits(Some(-2), None), limits(None, Some(-2))] {
        assert!(matches!(
            write_one(&table, &path, &refused),
            Err(WriteError::Invalid(_))
        ));
    }
}

#[test]
fn pages_are_compressed_with_the_codec_java_knows_by_the_name() {
    let directory = scratch("write-codecs");
    let path = target(&directory, "codec.parquet");
    let table = read_default("types.parquet").unwrap();
    let named = |name: &str| WriteOptions {
        compression: Some(name.to_string()),
        ..WriteOptions::default()
    };
    let cases = [
        ("SNAPPY", Compression::SNAPPY),
        ("SnappyCodec", Compression::SNAPPY),
        ("uncompressed", Compression::UNCOMPRESSED),
        ("LZ4", Compression::LZ4),
        ("lz4raw", Compression::LZ4_RAW),
        ("LZ4_RAW", Compression::LZ4_RAW),
    ];
    for (name, expected) in cases {
        write_one(&table, &path, &named(name)).unwrap();
        let footer = footer(&path);
        for column in footer.row_group(0).columns() {
            assert_eq!(column.compression(), expected, "{name}");
        }
        assert_java_types(&read_back(&path));
    }
    for name in ["Gzip", "jdkgzip"] {
        write_one(&table, &path, &named(name)).unwrap();
        let codec = footer(&path).row_group(0).column(0).compression();
        assert!(matches!(codec, Compression::GZIP(_)), "{name}");
        assert_java_types(&read_back(&path));
    }
    write_one(&table, &path, &named("zstdcodec")).unwrap();
    let codec = footer(&path).row_group(0).column(0).compression();
    assert!(matches!(codec, Compression::ZSTD(_)));
    assert_java_types(&read_back(&path));

    // names Java does not know
    for name in ["zstandard", "bogus", "LZ4 RAW", "BROTLI"] {
        let refused = write_one(&table, &path, &named(name));
        assert!(refused.is_err(), "{name}");
    }
    // a codec Java writes and this crate does not
    let java = not_ported(write_one(&table, &path, &named("LZO")));
    assert!(
        java.ends_with("DeephavenCompressorAdapterFactory#getByName"),
        "{java}"
    );
}

#[test]
fn columns_are_named_in_the_file_as_the_instructions_say() {
    let directory = scratch("write-names");
    let path = target(&directory, "names.parquet");
    let table = read_default("lists.parquet").unwrap();
    let column = |name: &str, parquet_name: &str| WriteColumn {
        name: name.to_string(),
        parquet_name: Some(parquet_name.to_string()),
        ..WriteColumn::default()
    };
    let with = |columns| WriteOptions {
        columns,
        ..WriteOptions::default()
    };
    // an instruction for a column the table does not have is ignored
    let renamed = with(vec![
        column("K", "kay"),
        column("V", "vee"),
        column("Nope", "nope"),
    ]);
    write_one(&table, &path, &renamed).unwrap();
    let footer = footer(&path);
    let schema = schema(&footer);
    assert!(
        schema.contains("OPTIONAL INT32 kay (INTEGER(32,true));"),
        "{schema}"
    );
    assert!(schema.contains("OPTIONAL group vee (LIST)"), "{schema}");
    // the entry names the column of the file
    assert!(deephaven(&footer).contains(r#"{"columnName":"vee","specialType":"Vector"}"#));
    let back = read_back(&path);
    let names: Vec<String> = columns(&back).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["kay", "vee", "W", "S", "A", "E"]);
    assert_eq!(cells(&back, "vee"), cells(&table, "V"));

    let twice = with(vec![column("K", "a"), column("V", "a")]);
    assert!(matches!(
        write_one(&table, &path, &twice),
        Err(WriteError::Invalid(_))
    ));
    let taken = with(vec![column("K", "V")]);
    assert!(matches!(
        write_one(&table, &path, &taken),
        Err(WriteError::Invalid(_))
    ));
    let again = with(vec![column("K", "a"), column("K", "b")]);
    assert!(matches!(
        write_one(&table, &path, &again),
        Err(WriteError::Invalid(_))
    ));
    let unnamed = with(vec![WriteColumn {
        name: "K".to_string(),
        ..WriteColumn::default()
    }]);
    assert!(matches!(
        write_one(&table, &path, &unnamed),
        Err(WriteError::Invalid(_))
    ));
}

#[test]
fn a_definition_chooses_orders_and_adds_columns() {
    let directory = scratch("write-definition");
    let path = target(&directory, "defined.parquet");
    let table = read_default("types.parquet").unwrap();
    let definition = |columns: &[(&str, ColumnType)]| {
        let columns = columns.iter();
        let columns =
            columns.map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()));
        WriteOptions {
            definition: Some(TableDefinition::new(columns.collect()).unwrap()),
            ..WriteOptions::default()
        }
    };
    let options = definition(&[
        ("S", ColumnType::String),
        ("N", ColumnType::Long),
        ("I", ColumnType::Int),
        ("A", ColumnType::array(ColumnType::Double)),
    ]);
    write_one(&table, &path, &options).unwrap();
    let back = read_back(&path);
    let expected = options.definition.as_ref().unwrap().columns().iter();
    let expected: Vec<_> = expected
        .map(|column| (column.name.clone(), column.column_type.clone()))
        .collect();
    assert_eq!(columns(&back), expected);
    assert_eq!(cells(&back, "S"), cells(&table, "S"));
    assert_eq!(cells(&back, "I"), cells(&table, "I"));
    assert_eq!(cells(&back, "N"), ["null"; 7]);
    assert_eq!(cells(&back, "A"), ["null"; 7]);

    // a column of the table with another type: Java casts and fails, or by accident not
    let other = definition(&[("I", ColumnType::String)]);
    let java = not_ported(write_one(&table, &path, &other));
    assert!(java.ends_with("TransferObject#create"), "{java}");
    let none = definition(&[]);
    assert_eq!(
        invalid(write_one(&table, &path, &none)),
        "Cannot write a parquet table with zero columns"
    );
}

#[test]
fn several_tables_are_written_together_or_not_at_all() {
    let directory = scratch("write-batch");
    let types = read_default("types.parquet").unwrap();
    let same = read_default("types_gzip.parquet").unwrap();
    let lists = read_default("lists.parquet").unwrap();
    let all = |tables: &[&Arc<QueryTable>], paths: &[&str], options: &WriteOptions| {
        let tables = tables.iter();
        let tables: Vec<Arc<dyn Table>> = tables
            .map(|table| -> Arc<dyn Table> { (*table).clone() })
            .collect();
        let paths: Vec<String> = paths.iter().map(|name| target(&directory, name)).collect();
        let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
        write(&tables, &paths, options)
    };
    let default = WriteOptions::default();

    all(
        &[&types, &same],
        &["one.parquet", "a/b/two.parquet"],
        &default,
    )
    .unwrap();
    assert_java_types(&read_back(&target(&directory, "one.parquet")));
    assert_java_types(&read_back(&target(&directory, "a/b/two.parquet")));
    assert_eq!(names(&directory), ["a", "one.parquet"]);
    assert_eq!(names(&directory.join("a/b")), ["two.parquet"]);

    assert_eq!(
        invalid(all(&[], &[], &default)),
        "No source tables provided for writing"
    );
    assert_eq!(
        invalid(all(&[&types], &["x.parquet", "y.parquet"], &default)),
        "Number of sources and destinations must match"
    );
    assert_eq!(
        invalid(all(
            &[&types, &lists],
            &["x.parquet", "y.parquet"],
            &default
        )),
        "Table definition must be provided when writing multiple tables with different definitions"
    );
    assert_eq!(names(&directory), ["a", "one.parquet"]);

    // the second table fails: the first is not left behind, nor the directories made for
    // it, and the file that was there is the file it was
    let misordered = with_groups(RowGroups::ByGroups {
        columns: vec!["V".to_string()],
        max_rows: None,
    });
    let by_list = all(&[&lists], &["made/deep/x.parquet"], &misordered);
    let java = not_ported(by_list);
    assert!(java.ends_with("SplitByGroupsIterator#next"), "{java}");
    assert_eq!(names(&directory), ["a", "one.parquet"]);

    std::fs::create_dir(directory.join("directory.parquet")).unwrap();
    let onto_directory = all(
        &[&lists, &lists],
        &["made/deep/x.parquet", "directory.parquet"],
        &default,
    );
    assert!(invalid(onto_directory).ends_with("directory.parquet exists and is a directory"));
    let under_file = all(
        &[&lists, &lists],
        &["one.parquet", "one.parquet/under.parquet"],
        &default,
    );
    assert!(invalid(under_file).ends_with("is not a directory"));
    assert_eq!(names(&directory), ["a", "directory.parquet", "one.parquet"]);
    assert_java_types(&read_back(&target(&directory, "one.parquet")));

    let no_extension = all(&[&lists], &["x.txt"], &default);
    assert!(invalid(no_extension).ends_with("x.txt does not end in .parquet extension"));
}

#[test]
fn a_file_that_is_there_is_replaced_and_nothing_else_is_left() {
    let directory = scratch("write-replace");
    let path = target(&directory, "table.parquet");
    let types = read_default("types.parquet").unwrap();
    let lists = read_default("lists.parquet").unwrap();
    write_one(&lists, &path, &WriteOptions::default()).unwrap();
    assert_eq!(read_back(&path).row_set().size(), 2);
    write_one(&types, &path, &WriteOptions::default()).unwrap();
    assert_java_types(&read_back(&path));
    assert_eq!(names(&directory), ["table.parquet"]);
}

/// Java writes both tables to the one unfinished file, puts it in place, and fails to put
/// it in place again; what it takes back leaves the last table at the destination.
#[test]
fn a_destination_given_twice_fails_and_is_left_as_the_last_table() {
    let directory = scratch("write-twice");
    let path = target(&directory, "table.parquet");
    let types: Arc<dyn Table> = read_default("types.parquet").unwrap();
    let other: Arc<dyn Table> = read_default("types_gzip.parquet").unwrap();
    let options = with_groups(RowGroups::MaxRows(4));
    let refused = write(&[other, types], &[&path, &path], &options);
    let message = invalid(refused);
    assert!(
        message.starts_with("Failed to install shadow file at"),
        "{message}"
    );
    assert!(
        message.contains("couldn't rename temporary shadow file"),
        "{message}"
    );
    assert_eq!(names(&directory), ["table.parquet"]);
    assert_java_types(&read_back(&path));
}

#[test]
fn what_is_not_ported_is_refused_by_the_java_method() {
    let directory = scratch("write-refused");
    let path = target(&directory, "table.parquet");
    let table = read_default("types.parquet").unwrap();
    let default = WriteOptions::default;
    let cases = [
        (
            WriteOptions {
                generate_metadata_files: true,
                ..default()
            },
            "ParquetMetadataFileWriterImpl#writeMetadataFiles",
        ),
        (
            WriteOptions {
                index_columns: vec![vec!["I".to_string()]],
                ..default()
            },
            "ParquetInstructions.Builder#addAllIndexColumns",
        ),
        (
            WriteOptions {
                columns: vec![WriteColumn {
                    name: "I".to_string(),
                    parquet_name: Some("I".to_string()),
                    codec: Some("io.deephaven.util.codec.SimpleByteArrayCodec".to_string()),
                    ..WriteColumn::default()
                }],
                ..default()
            },
            "ParquetInstructions.Builder#addColumnCodec",
        ),
    ];
    for (options, expected) in cases {
        let java = not_ported(write_one(&table, &path, &options));
        assert!(java.ends_with(expected), "{java}");
    }
    let java = not_ported(write_one(&table, "s3://bucket/table.parquet", &default()));
    assert!(
        java.ends_with("SeekableChannelsProviderLoader#load"),
        "{java}"
    );

    // a column whose cells hold lists, which Java writes with a codec: refused by its
    // type, before a row is read
    let lists = read_default("lists.parquet").unwrap();
    let nested = WriteOptions {
        definition: Some(
            TableDefinition::new(vec![ColumnDefinition::new(
                "N",
                ColumnType::vector(ColumnType::array(ColumnType::Int)),
            )])
            .unwrap(),
        ),
        ..default()
    };
    let java = not_ported(write_one(
        &lists,
        &target(&directory, "made/nested.parquet"),
        &nested,
    ));
    assert!(java.ends_with("TypeInfos#getCodecAndArgs"), "{java}");
    assert!(names(&directory).is_empty());
}

#[test]
fn delete_removes_a_file_a_directory_and_nothing() {
    let directory = scratch("write-delete");
    let table = read_default("types.parquet").unwrap();
    let file = target(&directory, "file.parquet");
    let deep = target(&directory, "gone/deep/down.parquet");
    write_one(&table, &file, &WriteOptions::default()).unwrap();
    write_one(&table, &deep, &WriteOptions::default()).unwrap();
    delete(&file).unwrap();
    assert_eq!(names(&directory), ["gone"]);
    delete(&target(&directory, "gone")).unwrap();
    assert!(names(&directory).is_empty());
    delete(&target(&directory, "nothing.parquet")).unwrap();
    delete(&file).unwrap();
}
