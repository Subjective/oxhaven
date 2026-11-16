//! Files written from the tables of `tests/data`, against the files Java 42.9 wrote from
//! the same tables: the Parquet schema, the `deephaven` footer entry and the table read
//! back. `oracle/scenarios/python/parquet_write_*.py` compare with a running reference.

use super::{assert_java_types, cells, columns, data, read_default, scratch};
use crate::{
    delete, read, write, write_partitioned, PartitionedSource, ReadOptions, RowGroups, WriteColumn,
    WriteError, WriteOptions,
};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::ops::partitioned::partition_by;
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
    let mut columns = footer.row_group(group).columns().iter();
    let column = columns
        .find(|column| column.column_path().string() == path)
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
        // a class is known by its whole name, in the case it is spelt in
        (
            "org.apache.parquet.hadoop.codec.SnappyCodec",
            Compression::SNAPPY,
        ),
        (
            "org.apache.parquet.hadoop.codec.Lz4RawCodec",
            Compression::LZ4_RAW,
        ),
        ("org.apache.hadoop.io.compress.Lz4Codec", Compression::LZ4),
        ("io.airlift.compress.lz4.Lz4Codec", Compression::LZ4),
    ];
    for (name, expected) in cases {
        write_one(&table, &path, &named(name)).unwrap();
        let footer = footer(&path);
        for column in footer.row_group(0).columns() {
            assert_eq!(column.compression(), expected, "{name}");
        }
        assert_java_types(&read_back(&path));
    }
    let gzip = [
        "Gzip",
        "jdkgzip",
        "org.apache.hadoop.io.compress.GzipCodec",
        "io.airlift.compress.gzip.JdkGzipCodec",
    ];
    for name in gzip {
        write_one(&table, &path, &named(name)).unwrap();
        let codec = footer(&path).row_group(0).column(0).compression();
        assert!(matches!(codec, Compression::GZIP(_)), "{name}");
        assert_java_types(&read_back(&path));
    }
    for name in ["zstdcodec", "io.airlift.compress.zstd.ZstdCodec"] {
        write_one(&table, &path, &named(name)).unwrap();
        let codec = footer(&path).row_group(0).column(0).compression();
        assert!(matches!(codec, Compression::ZSTD(_)), "{name}");
        assert_java_types(&read_back(&path));
    }

    // names Java does not know
    let unknown = [
        "zstandard",
        "bogus",
        "LZ4 RAW",
        "BROTLI",
        "org.apache.hadoop.io.compress.gzipcodec",
        "org.apache.hadoop.io.compress.SnappyCodec",
        "org.apache.parquet.hadoop.codec.ZstandardCodec",
    ];
    for name in unknown {
        let refused = write_one(&table, &path, &named(name));
        assert!(invalid(refused).contains(name), "{name}");
    }
    // a codec Java writes and this crate does not
    for name in ["LZO", "io.airlift.compress.lzo.LzoCodec"] {
        let java = not_ported(write_one(&table, &path, &named(name)));
        assert!(
            java.ends_with("DeephavenCompressorAdapterFactory#getByName"),
            "{java}"
        );
    }
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

/// Every file under `directory`, by its path from it, in order.
fn files_under(directory: &Path) -> Vec<String> {
    fn walk(root: &Path, directory: &Path, found: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, found);
            } else {
                let relative = path.strip_prefix(root).unwrap();
                found.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    let mut found = Vec::new();
    walk(directory, directory, &mut found);
    found.sort();
    found
}

/// The physical type and the annotation of each leaf column of a file.
fn leaves(footer: &ParquetMetaData) -> Vec<String> {
    let columns = footer.file_metadata().schema_descr().columns().iter();
    columns
        .map(|column| {
            use parquet::basic::LogicalType;
            let annotation = match column.logical_type_ref() {
                Some(LogicalType::Decimal(of)) => {
                    format!("DECIMAL({},{})", of.precision, of.scale)
                }
                Some(LogicalType::Date) => "DATE".to_string(),
                Some(LogicalType::Time(of)) => {
                    format!("TIME({},{:?})", of.is_adjusted_to_u_t_c, of.unit)
                }
                Some(LogicalType::Timestamp(of)) => {
                    format!("TIMESTAMP({},{:?})", of.is_adjusted_to_u_t_c, of.unit)
                }
                Some(LogicalType::Integer(of)) => {
                    format!("INT({},{})", of.bit_width, of.is_signed)
                }
                other => format!("{other:?}"),
            };
            format!(
                "{} {} {annotation}",
                column.path().string(),
                column.physical_type()
            )
        })
        .collect()
}

/// The keys of the entries of a footer, in order.
fn entry_keys(footer: &ParquetMetaData) -> Vec<String> {
    let entries = footer.file_metadata().key_value_metadata().unwrap();
    let mut keys: Vec<String> = entries.iter().map(|entry| entry.key.clone()).collect();
    keys.sort();
    keys
}

/// The file each row group of a metadata file says it is in.
fn group_files(footer: &ParquetMetaData) -> Vec<String> {
    let groups = footer.row_groups().iter();
    groups
        .map(|group| {
            let mut paths = group.columns().iter().map(|column| column.file_path());
            let first = paths.next().unwrap();
            assert!(paths.all(|path| path == first));
            first.unwrap_or("").to_string()
        })
        .collect()
}

/// `types.parquet` with its columns `keys` as partitioning columns.
fn partitioned_by(table: &QueryTable, keys: &[&str]) -> TableDefinition {
    let columns = table.definition().columns().iter();
    let columns = columns.map(|column| match keys.contains(&column.name.as_str()) {
        true => ColumnDefinition::partitioning(&column.name, column.column_type.clone()),
        false => column.clone(),
    });
    TableDefinition::new(columns.collect()).unwrap()
}

fn by_keys(table: &QueryTable, keys: &[&str], base_name: Option<&str>) -> WriteOptions {
    WriteOptions {
        definition: Some(partitioned_by(table, keys)),
        base_name: base_name.map(str::to_string),
        ..WriteOptions::default()
    }
}

fn write_by(
    table: &Arc<QueryTable>,
    directory: &str,
    options: &WriteOptions,
) -> Result<(), WriteError> {
    let table: Arc<dyn Table> = table.clone();
    write_partitioned(PartitionedSource::Table(&table), directory, options)
}

/// The annotations are the ones Java 42.9 wrote for the same table, and the precision of
/// a decimal column is the one its values need: 12345.67 needs 7 digits, 255 needs 3.
#[test]
fn the_object_types_are_written_as_java_writes_them() {
    let directory = scratch("write-objects");
    let path = target(&directory, "more.parquet");
    let table = read_default("more_types.parquet").unwrap();
    write_one(&table, &path, &WriteOptions::default()).unwrap();

    let ours = footer(&path);
    assert_eq!(
        leaves(&ours),
        [
            "Dec BYTE_ARRAY DECIMAL(7,2)",
            "Wide BYTE_ARRAY DECIMAL(38,10)",
            "Whole BYTE_ARRAY DECIMAL(3,0)",
            "Date INT32 DATE",
            "TimeMs INT64 TIME(true,NANOS)",
            "TimeUs INT64 TIME(true,NANOS)",
            "TimeNs INT64 TIME(true,NANOS)",
            "LocalMs INT64 TIMESTAMP(false,NANOS)",
            "LocalUs INT64 TIMESTAMP(false,NANOS)",
            "LocalNs INT64 TIMESTAMP(false,NANOS)",
            "U64 BYTE_ARRAY DECIMAL(1,0)",
            "Bin.list.element INT32 INT(8,true)",
            "Fixed.list.element INT32 INT(8,true)",
        ]
    );
    assert_eq!(deephaven(&ours), r#"{"version":"42.9"}"#);

    let back = read_back(&path);
    assert_eq!(columns(&back), columns(&table));
    for (name, _) in columns(&table) {
        assert_eq!(cells(&back, &name), cells(&table, &name), "{name}");
    }

    // a decimal column kept in an integer has the digits of its values too
    let ints = read_default("more_decimals_int.parquet").unwrap();
    write_one(&ints, &path, &WriteOptions::default()).unwrap();
    assert_eq!(
        leaves(&footer(&path)),
        [
            "D32 BYTE_ARRAY DECIMAL(9,2)",
            "D64 BYTE_ARRAY DECIMAL(18,3)"
        ]
    );
    let back = read_back(&path);
    for (name, _) in columns(&ints) {
        assert_eq!(cells(&back, &name), cells(&ints, &name), "{name}");
    }
}

#[test]
fn a_destination_may_be_a_file_uri() {
    let directory = scratch("write-uri");
    let root = directory.to_str().unwrap();
    let table = read_default("types.parquet").unwrap();
    let default = WriteOptions::default();
    write_one(&table, &format!("file://{root}/one.parquet"), &default).unwrap();
    write_one(&table, &format!("file:{root}/two.parquet"), &default).unwrap();
    // the path of a URI is decoded, and a path that is not a URI is taken as it is
    write_one(&table, &format!("file://{root}/a%20b.parquet"), &default).unwrap();
    write_one(&table, &format!("{root}/c%20d.parquet"), &default).unwrap();
    assert_eq!(
        names(&directory),
        ["a b.parquet", "c%20d.parquet", "one.parquet", "two.parquet"]
    );
    assert_java_types(&read_back(&target(&directory, "a b.parquet")));

    let refused = write_one(
        &table,
        &format!("file://localhost{root}/x.parquet"),
        &default,
    );
    assert_eq!(invalid(refused), "URI has an authority component");
    let spaced = format!("file://{root}/x y.parquet");
    let refused = write_one(&table, &spaced, &default);
    assert_eq!(
        invalid(refused),
        format!("Failed to convert to URI: '{spaced}'")
    );
    // Java knows a scheme in the case it is registered in
    for other in [
        format!("FILE://{root}/x.parquet"),
        "nope://x/y.parquet".to_string(),
    ] {
        let java = not_ported(write_one(&table, &other, &default));
        assert!(
            java.ends_with("SeekableChannelsProviderLoader#load"),
            "{java}"
        );
    }
    assert_eq!(names(&directory).len(), 4);
}

#[test]
fn metadata_files_list_the_row_groups_of_every_file_of_the_write() {
    let directory = scratch("write-metadata");
    let types: Arc<dyn Table> = read_default("types.parquet").unwrap();
    let same: Arc<dyn Table> = read_default("types_gzip.parquet").unwrap();
    let options = WriteOptions {
        generate_metadata_files: true,
        row_groups: RowGroups::MaxRows(4),
        ..WriteOptions::default()
    };
    let one = target(&directory, "d/one.parquet");
    let two = target(&directory, "d/two.parquet");
    write(&[types.clone(), same.clone()], &[&one, &two], &options).unwrap();
    assert_eq!(
        names(&directory.join("d")),
        [
            "_common_metadata",
            "_metadata",
            "one.parquet",
            "two.parquet"
        ]
    );

    let all = footer(&target(&directory, "d/_metadata"));
    let of_one = footer(&one);
    assert_eq!(schema(&all), schema(&of_one));
    assert_eq!(row_groups(&all), [4, 3, 4, 3]);
    assert_eq!(all.file_metadata().num_rows(), 14);
    assert_eq!(
        group_files(&all),
        ["one.parquet", "one.parquet", "two.parquet", "two.parquet"]
    );
    // where the pages of a row group are is where its file has them
    for (listed, own) in all.row_groups().iter().zip(of_one.row_groups()) {
        for (listed, own) in listed.columns().iter().zip(own.columns()) {
            assert_eq!(listed.data_page_offset(), own.data_page_offset());
            assert_eq!(listed.compressed_size(), own.compressed_size());
            assert_eq!(own.file_path(), None);
        }
    }
    let keys = [
        "deephaven",
        "deephaven_per_file_one.parquet",
        "deephaven_per_file_two.parquet",
    ];
    assert_eq!(entry_keys(&all), keys);
    for entry in all.file_metadata().key_value_metadata().unwrap() {
        assert_eq!(entry.value.as_deref(), Some(r#"{"version":"42.9"}"#));
    }

    let common = footer(&target(&directory, "d/_common_metadata"));
    assert_eq!(schema(&common), schema(&of_one));
    assert!(common.row_groups().is_empty());
    assert_eq!(common.file_metadata().num_rows(), 0);
    assert_eq!(entry_keys(&common), keys);

    // the directory is read through its metadata file
    let back = read_back(&target(&directory, "d"));
    assert_eq!(back.row_set().size(), 14);

    // a later write replaces the metadata files, which then list only its files
    let three = target(&directory, "d/three.parquet");
    write(std::slice::from_ref(&types), &[&three], &options).unwrap();
    let all = footer(&target(&directory, "d/_metadata"));
    assert_eq!(group_files(&all), ["three.parquet", "three.parquet"]);
    assert_eq!(names(&directory.join("d")).len(), 5);

    // the metadata files of files of two directories have no one place to be
    let apart = [
        target(&directory, "e/one.parquet"),
        target(&directory, "e/f/two.parquet"),
    ];
    let refused = write(&[types, same], &[&apart[0], &apart[1]], &options);
    let message = invalid(refused);
    assert!(
        message.starts_with("All destination files must be in the same directory"),
        "{message}"
    );
    assert_eq!(names(&directory), ["d"]);
}

#[test]
fn a_table_is_written_under_a_directory_for_each_value_of_its_partitioning_columns() {
    let directory = scratch("write-partitioned");
    let table = read_default("types.parquet").unwrap();
    let root = target(&directory, "by");

    // B is null, false, true, null, true, false, null: a null names the directory `B=`
    write_by(&table, &root, &by_keys(&table, &["B"], Some("data"))).unwrap();
    let files = [
        "B=/data.parquet",
        "B=false/data.parquet",
        "B=true/data.parquet",
    ];
    assert_eq!(files_under(&directory.join("by")), files);
    let leaf = footer(&target(&directory, "by/B=false/data.parquet"));
    assert_eq!(leaves(&leaf).len(), 9, "every column but the key");
    assert!(leaves(&leaf).iter().all(|leaf| !leaf.starts_with("B ")));
    assert_eq!(row_groups(&leaf), [2]);
    assert_eq!(deephaven(&leaf), r#"{"version":"42.9"}"#);

    let back = read_back(&root);
    assert!(back.definition().columns()[0].partitioning);
    assert_eq!(back.definition().columns()[0].name, "B");
    assert_eq!(
        cells(&back, "B"),
        ["null", "null", "null", "false", "false", "true", "true"]
    );
    assert_eq!(
        cells(&back, "I"),
        ["-300000", "null", "300000", "-200000", "200000", "-100000", "100000"]
    );

    // the tokens of a base name; a file under another name stays, as in Java
    std::fs::write(directory.join("by/B=true/kept.txt"), "kept").unwrap();
    let named = by_keys(&table, &["B"], Some("{partitions}-{i}"));
    write_by(&table, &root, &named).unwrap();
    assert_eq!(
        files_under(&directory.join("by")),
        [
            "B=/B=-0.parquet",
            "B=/data.parquet",
            "B=false/B=false-0.parquet",
            "B=false/data.parquet",
            "B=true/B=true-0.parquet",
            "B=true/data.parquet",
            "B=true/kept.txt",
        ]
    );

    // the default name is a UUID, a different one for each file
    let uuids = target(&directory, "uuids");
    write_by(&table, &uuids, &by_keys(&table, &["B"], None)).unwrap();
    let written = files_under(&directory.join("uuids"));
    assert_eq!(written.len(), 3);
    let mut seen = std::collections::HashSet::new();
    for file in &written {
        let name = file.rsplit('/').next().unwrap();
        let uuid = name.strip_suffix(".parquet").unwrap();
        assert_eq!(uuid.len(), 36, "{file}");
        assert!(uuid.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        assert!(seen.insert(uuid.to_string()), "{file}");
    }

    // two keys are two levels, in the order of the definition
    let two = target(&directory, "two");
    write_by(&table, &two, &by_keys(&table, &["B", "Y"], Some("d"))).unwrap();
    let written = files_under(&directory.join("two"));
    assert_eq!(written.len(), 7);
    assert!(
        written.contains(&"B=false/Y=2/d.parquet".to_string()),
        "{written:?}"
    );
    assert!(
        written.contains(&"B=/Y=-3/d.parquet".to_string()),
        "{written:?}"
    );
    assert!(
        written.contains(&"B=false/Y=/d.parquet".to_string()),
        "{written:?}"
    );

    // a partitioned table is written by its keys
    let parent: Arc<dyn Table> = table.clone();
    let parts = partition_by(&parent, &["H"], false).unwrap();
    let from_parts = target(&directory, "parts");
    let options = WriteOptions {
        base_name: Some("p".to_string()),
        ..WriteOptions::default()
    };
    write_partitioned(
        PartitionedSource::Partitioned(&parts),
        &from_parts,
        &options,
    )
    .unwrap();
    let written = files_under(&directory.join("parts"));
    assert_eq!(written.len(), 7);
    assert!(
        written.contains(&"H=-3000/p.parquet".to_string()),
        "{written:?}"
    );
    assert!(written.contains(&"H=/p.parquet".to_string()), "{written:?}");
    assert_eq!(read_back(&from_parts).row_set().size(), 7);
}

#[test]
fn a_partitioned_write_has_metadata_files_with_the_partitioning_columns() {
    let directory = scratch("write-partitioned-metadata");
    let table = read_default("types.parquet").unwrap();
    let root = target(&directory, "by");
    let options = WriteOptions {
        generate_metadata_files: true,
        ..by_keys(&table, &["B"], Some("data"))
    };
    write_by(&table, &root, &options).unwrap();
    assert_eq!(
        files_under(&directory.join("by")),
        [
            "B=/data.parquet",
            "B=false/data.parquet",
            "B=true/data.parquet",
            "_common_metadata",
            "_metadata",
        ]
    );
    let all = footer(&target(&directory, "by/_metadata"));
    let leaf = footer(&target(&directory, "by/B=/data.parquet"));
    assert_eq!(schema(&all), schema(&leaf));
    assert_eq!(
        group_files(&all),
        [
            "B=/data.parquet",
            "B=false/data.parquet",
            "B=true/data.parquet"
        ]
    );
    assert_eq!(row_groups(&all), [3, 2, 2]);
    // a `/` of a path is a `_` of its key
    let keys = [
        "deephaven",
        "deephaven_per_file_B=_data.parquet",
        "deephaven_per_file_B=false_data.parquet",
        "deephaven_per_file_B=true_data.parquet",
    ];
    assert_eq!(entry_keys(&all), keys);

    let common = footer(&target(&directory, "by/_common_metadata"));
    assert!(common.row_groups().is_empty());
    assert_eq!(entry_keys(&common), keys);
    let mut expected = vec!["B BOOLEAN None".to_string()];
    expected.extend(leaves(&leaf));
    assert_eq!(leaves(&common), expected);

    // reading a directory whose partitioning columns only `_common_metadata` knows is
    // not ported: the files are read one by one, or by the layout of their directories
    let through_metadata = read(&root, &ReadOptions::default());
    match through_metadata {
        Err(crate::ReadError::NotPorted { java, .. }) => {
            assert!(java.ends_with("ParquetMetadataFileLayout#create"), "{java}")
        }
        Err(other) => panic!("{other}"),
        Ok(_) => panic!("read through the metadata file: check its rows here instead"),
    }
}

#[test]
fn a_partitioned_write_that_fails_leaves_what_was_there() {
    let directory = scratch("write-partitioned-failed");
    let table = read_default("types.parquet").unwrap();
    let root = target(&directory, "by");
    std::fs::create_dir_all(directory.join("by/B=")).unwrap();
    std::fs::write(directory.join("by/B=/data.parquet"), "old").unwrap();
    std::fs::write(directory.join("by/_metadata"), "old metadata").unwrap();
    std::fs::create_dir_all(directory.join("by/B=true/data.parquet")).unwrap();
    let options = WriteOptions {
        generate_metadata_files: true,
        ..by_keys(&table, &["B"], Some("data"))
    };
    let message = invalid(write_by(&table, &root, &options));
    assert!(
        message.ends_with("B=true/data.parquet exists and is a directory"),
        "{message}"
    );
    assert_eq!(
        files_under(&directory.join("by")),
        ["B=/data.parquet", "_metadata"]
    );
    assert!(!directory.join("by/B=false").exists());
    let old = std::fs::read_to_string(directory.join("by/B=/data.parquet")).unwrap();
    assert_eq!(old, "old");
    let old = std::fs::read_to_string(directory.join("by/_metadata")).unwrap();
    assert_eq!(old, "old metadata");
}

#[test]
fn partitioned_writes_java_refuses_are_refused_with_its_words() {
    let directory = scratch("write-partitioned-refused");
    let table = read_default("types.parquet").unwrap();
    let root = target(&directory, "by");
    let nothing_written = || assert!(files_under(&directory).is_empty());

    let refused = write_by(&table, &root, &WriteOptions::default());
    assert_eq!(
        invalid(refused),
        "Table must have partitioning columns to write partitioned data"
    );
    let every = ["B", "Y", "H", "I", "L", "C", "F", "D", "S", "T"];
    let refused = write_by(&table, &root, &by_keys(&table, &every, Some("d")));
    assert_eq!(
        invalid(refused),
        "Cannot write a partitioned parquet table without any non-partitioning columns"
    );
    let indexed = WriteOptions {
        index_columns: vec![vec!["B".to_string()]],
        ..by_keys(&table, &["B"], Some("d"))
    };
    assert_eq!(
        invalid(write_by(&table, &root, &indexed)),
        "Cannot add index on partitioning column B"
    );
    nothing_written();

    // S is null in one row and empty in another: both are the directory `S=`, and the
    // second file can not be put where the first is
    let message = invalid(write_by(&table, &root, &by_keys(&table, &["S"], Some("d"))));
    assert!(
        message.starts_with("Failed to install shadow file at"),
        "{message}"
    );
    nothing_written();

    for key in ["F", "D", "T"] {
        let java = not_ported(write_by(&table, &root, &by_keys(&table, &[key], Some("d"))));
        assert!(java.ends_with("PartitionFormatter#formatObject"), "{java}");
    }
    nothing_written();

    // Java asks the first file for its scheme, and there is none
    let empty = read_default("flat/empty.parquet").unwrap();
    assert!(empty.row_set().is_empty());
    let first = empty.definition().columns()[0].name.clone();
    let refused = write_by(&empty, &root, &by_keys(&empty, &[&first], Some("d")));
    assert_eq!(
        invalid(refused),
        "java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0"
    );
    nothing_written();
}
