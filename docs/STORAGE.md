# Tables backed by storage, and reading Parquet

How Oxhaven makes a table from data that lives outside the engine, and how
`deephaven.parquet.read` is built on it. Written by task 7.1
(`tasks/7.1-table-provider-parquet-read.md`), which also built the first slice. Tasks 7.2
(writing Parquet), 7.3 (CSV) and 7.4 (Kafka) build on it.

## Decision

1. **A stored table is read on demand, a column of a row group at a time.** Opening a file
   reads its footer only. A column's values are decoded the first time a row of that row
   group is read, into one Arrow array, which is kept for as long as the table lives.
   Nothing is loaded for a column that no operation reads.
2. **The engine knows locations, not formats.** `oxhaven-engine::source` has one trait,
   `TableLocation`: a fixed number of rows and a way to read a range of one column as an
   Arrow array. `stored_table` makes a static table from a definition and a list of
   locations. It is what Java's `impl/locations` and `sources/regioned` come to once the
   class hierarchy is taken away.
3. **Row keys are Java's**: a location is a region, and the row key of row `r` of region `n`
   is `n << 43 | r`. Regions are the locations that have rows, in the order given.
4. **A format is a crate of its own** that depends on the engine and not on the server:
   `oxhaven-parquet` here, `oxhaven-csv` and `oxhaven-kafka` later. It offers one plain
   Rust function per entry point, which takes a path and an options struct and returns a
   table or a typed error.
5. **The Parquet reader is the `parquet` crate of arrow-rs**, at the version of `arrow` the
   workspace already has. The column types come from the Parquet schema and the
   `deephaven` entry of the footer, as in Java, never from the Arrow schema a writer may
   have embedded.
6. **Python reaches a format through one native function per entry point**
   (`_oxhaven.read_parquet`), given the arguments as plain values. `deephaven.parquet`
   has the names and signatures of the reference module and no logic beyond checking and
   passing them. Java has no gRPC call that reads a file, so there is no service handler to go
   through, unlike the table operations (`docs/CONSOLE.md`, decision 3).

## The three ways a table gets its data

Java has one large abstraction for tables that are not computed from other tables
(`SourceTable`, `TableLocationProvider`, `ColumnSourceManager`, and the adapters that feed
streams into them). Looking at what the three tasks of this track need, there are three
shapes, and each is one small thing here:

| Shape | Who | What the engine offers |
| --- | --- | --- |
| Loaded whole when it is created | CSV (`CsvTools#readCsv` builds an in-memory table) | A static `QueryTable` over in-memory columns, which exists (`docs/CSV.md`) |
| Read on demand from locations that do not change | Parquet, later Iceberg | `source::stored_table` over `TableLocation`, this task |
| Pushed by a feed, cycle by cycle | Kafka (`StreamToBlinkTableAdapter`) | `QueryTable` with a `RootSource`, which exists for input tables and time tables |

So 7.3 and 7.4 do not implement `TableLocation`. What they reuse from this task is the rest
of the seam, which is the same for every format:

- the crate layout of decision 4 and the error type of "Errors" below;
- the Python binding of decision 6: a module of `deephaven` that builds options, one
  native function in `console/python/<format>.rs`, registered with one line in
  `console/python/mod.rs`;
- script scenarios that carry their input files inside the script ("Gate" below);
- for the key-value directory layout of 7.1's second slice, the type inference of 7.3:
  Java infers the types of partition values with its CSV reader
  (`LocationTableBuilderCsv`).

A directory that gains files while the table is open (`is_refreshing=True`) is the one
place where locations and a feed meet: a root source that asks a provider for new
locations on each cycle and appends them as new regions. Regions never move, so the table
is append-only by construction. It is designed for (the region numbering leaves room, and
`stored_table` is the static case of it) and not built in this task.

## `oxhaven-engine::source`

```rust
pub trait TableLocation: Send + Sync {
    /// What to call it in an error: a file's path.
    fn name(&self) -> &str;
    /// The number of rows, which never changes.
    fn size(&self) -> u64;
    /// The values of rows `rows` of the column at `column` of the table's definition, as
    /// an array of that column type's Arrow type, nulls as validity bits.
    fn read(&self, column: usize, rows: Range<u64>) -> Result<ArrayRef, TableFailure>;
}

pub fn stored_table(
    definition: TableDefinition,
    locations: Vec<Arc<dyn TableLocation>>,
) -> Result<Arc<QueryTable>, TableFailure>;
```

- The table is static. Its sources are registered as sources that do not hold their values
  (`table::reads_through`), because Java's regioned sources are not in-memory sources:
  `select` copies them and `update` does not reuse them.
- A read that fails is a `TableFailure` from `ColumnSource::chunk`, which fails the
  operation that asked, as the contract says.
- A location with no rows gets no region, as in Java
  (`RegionedColumnSourceManager#refresh`).
- The table has the attribute `AddOnly`, as Java's has. Java gives one without rows
  `EmptySourceTable` as well, which no client reads and Oxhaven does not send.
- More than 2^20 regions or 2^43 rows in a location is an error.

Partitioning columns (the second slice) are a constant per region: a location will also
say `partition_value(column)`, and the source of a partitioning column answers from that
without reading the file. They need one addition to the contracts, a flag on
`ColumnDefinition` that the schema sends as `deephaven:isPartitioning`; it is not made in
this task.

## `oxhaven-parquet`

| Module | Holds |
| --- | --- |
| `lib.rs` | `read`, `ReadOptions`, `ReadError` |
| `layout.rs` | From a path and a layout to the list of files |
| `schema.rs` | From a Parquet schema and the `deephaven` footer entry to a table definition |
| `location.rs` | `ParquetLocation`: a file as a `TableLocation` |
| `write.rs` | `write`, `WriteOptions`, `delete`: tables as files ("Writing Parquet") |

**Which files.** As `ParquetTools#readTable`: a path that ends in `.parquet` is one file; a
path that ends in `_metadata` or `_common_metadata` is a metadata file; anything else is a
directory. A directory with a `_metadata` file is read through it; otherwise every
`.parquet` file under it whose name, and whose directories below the root, do not start
with `.`, sorted by path. The definition is inferred from the last file. `file_layout`
skips the guess.

**Which types.** From the Parquet schema, by the rules of
`ParquetSchemaReader#readParquetSchema`:

| Parquet | Column type |
| --- | --- |
| `BOOLEAN` | `Boolean` |
| `INT32`; `INT(8/16/32, signed)` | `Int`; `Byte`, `Short`, `Int` |
| `INT64`; `INT(64, signed)` | `Long` |
| `INT(8, unsigned)`, `INT(16, unsigned)` | `Char` |
| `INT(32, unsigned)` | `Long` |
| `INT(64, unsigned)` with `unsigned_long_target` `LONG` or `SIGNED_LONG` | `Long` |
| `FLOAT`, `DOUBLE` | `Float`, `Double` |
| `STRING`, `ENUM` | `String` |
| `TIMESTAMP(millis/micros/nanos, adjusted to UTC)`, `INT96` | `Instant` |
| A repeated field or a `LIST` of one of the above | `Array` of it, or `Vector` when the `deephaven` entry says `Vector` |

A value equal to the Java null of its type reads as null, as it does in Java, where it is
the same bit pattern: a 32-bit `-2147483648` from another writer is a null `int`.

Types that Java reads and that have no `ColumnType` yet are refused by name, when the
table is created: `DECIMAL` (`BigDecimal`, `BigInteger`), `DATE`, `TIME`, a timestamp that
is not adjusted to UTC (`LocalDateTime`), an unsigned 64-bit integer without a target
(`BigInteger`), `BINARY` and `FIXED_LEN_BYTE_ARRAY` without a logical type (`byte[]`),
a column with a codec, a `StringSet`, and a struct with one leaf (Java reads the leaf).
They wait for the column types, not for this design.

## Dependencies

| Crate | Licence | Why |
| --- | --- | --- |
| `parquet` 60.1, features `arrow`, `lz4`, `flate2`, `flate2-rust_backend`, `snap`, `zstd` | Apache-2.0 | The reader |
| `lz4_flex` | MIT | `LZ4` and `LZ4_RAW` pages |
| `flate2`, `crc32fast` | MIT or Apache-2.0 | `GZIP` pages |
| `miniz_oxide`, `adler2`, `simd-adler32` | MIT or Zlib or Apache-2.0; 0BSD or MIT or Apache-2.0; MIT | The inflate behind `flate2` (its `rust_backend`; the `zlib-rs` backend that `parquet` takes by default is under the Zlib licence only) |
| `seq-macro`, `twox-hash` | MIT or Apache-2.0; MIT | Needed by `parquet` |
| `snap` | BSD-3-Clause | `SNAPPY` pages |
| `zstd`, `zstd-safe` | BSD-3-Clause | `ZSTD` pages |
| `zstd-sys` | BSD-3-Clause and BSD-2-Clause | The Zstandard C library, compiled when the crate is built |
| `jobserver`, `pkg-config` | MIT or Apache-2.0 | Build dependencies of `zstd-sys` |

`serde_json` (already in the workspace) reads the `deephaven` footer entry.

`snap`, `zstd`, `zstd-safe` and `zstd-sys` are under the exception to the licence rule
that `PORTING.md` records ("Dependencies"): the owner accepted BSD-3-Clause for the
Snappy, Zstandard and Brotli codec crates of `parquet` and what they depend on, on
2026-11-15, and for nothing else. Snappy is the default of Java's `write`, of pyarrow
and of Spark. `zstd-sys` 2.1.3 declares `BSD-3-Clause AND BSD-2-Clause`, because the
decoders of the legacy formats in the C library are under the 2-clause licence; the
owner accepted BSD-2-Clause for that crate alone, the same day. Building `zstd-sys`
compiles the C library, so a build needs a C compiler.

**Brotli is not enabled**, for parity and not for its licence. Its crates fit the
exception (`brotli` is BSD-3-Clause and MIT, `brotli-decompressor` BSD-3-Clause or MIT,
`alloc-no-stdlib` and `alloc-stdlib` BSD-3-Clause). But the reference server as packaged
has no Brotli codec: it makes the table and fails with `INVALID_ARGUMENT` when a column
is read, and that is what `parquet_read_codecs` recorded. A read that succeeds where
Java fails is against `PARITY.md`, "Must match", item 7, and the replay gate reports it
as a kind the baseline does not list (`failure_expected`). Reading Brotli therefore
needs an allowed difference in `PARITY.md` and that kind in the baseline, both protected
changes; with them it is one word in the workspace `Cargo.toml` and one line in
`location.rs`.

A file with a column chunk in Brotli or any other codec this build does not have
(`LZO`) is refused by name when it is opened.

## Errors

A format's entry point returns its own error enum with three cases, which the Python
binding turns into exceptions that `deephaven.parquet.read` wraps in `DHError("failed to
read parquet data.")`, the reference's text:

| Case | Python | Meaning |
| --- | --- | --- |
| `NotPorted { java, what }` | `NotImplementedError` | Java does this and Oxhaven does not yet. The text names the Java method, as `unported!` does. It is an error and not a panic because a user's file can reach it |
| `Invalid` | `ValueError` | Java refuses this too |
| `Io` | `OSError` | The file system said no |

## In and out of the first slice

In:

- `read(path)` of a single file and of a directory whose files are directly in it
  (`SINGLE_FILE`, `FLAT_PARTITIONED`, and `KV_PARTITIONED` or no layout when there are no
  partition directories), with every type of the table above, several row groups, nulls,
  files written by Java (with the `deephaven` footer entry) and by other writers.
- `col_instructions` for renaming a column and for `unsigned_long_target`; `file_layout`;
  the errors Java raises for a missing file, an empty directory, a layout that does not
  fit, and `is_refreshing` with a single file.
- Uncompressed, `SNAPPY`, `GZIP`, `ZSTD`, `LZ4` and `LZ4_RAW` pages.

Out, each refused with `NotPorted`:

| What | Waits for |
| --- | --- |
| Partition directories (`key=value`), partitioning columns | The `isPartitioning` flag in the contracts; the type inference of 7.3 |
| `_metadata` and `_common_metadata` files | Partitioning columns |
| `is_refreshing=True` on a directory | The refreshing provider above |
| `table_definition` | `deephaven.dtypes` and `TableDefinition` in the Python package (6.8) |
| `special_instructions`, and any path with a URI scheme (S3 and the other stores) | Its own task |
| The types listed under "Which types" | New column types |
| A column name that Java would change to make it legal (`a b`, `2x`, `int`) | `NameValidator#legalizeColumnName` |
| A codec named in `col_instructions` | Codecs |
| Brotli | A decision on parity: the reference fails to read it ("Dependencies") |

Not done and not refused, because nothing observable depends on them yet: data indexes
and the sorted-column marks in the `deephaven` footer entry (`impl/dataindex`), pushing
filters down to row-group statistics, and a bound on the memory the decoded arrays use
(Java holds pages through soft references).

`write`, `write_partitioned`, `batch_write` and `delete` are task 7.2: "Writing Parquet",
below.

## Gate

```bash
cargo test -p oxhaven-engine --lib source::
cargo test -p oxhaven-parquet
cargo test -p oxhaven-server --features python --lib parquet
oracle/bin/harness replay-scripts --url $URL --scripts parquet_
```

The script scenarios `parquet_*` are recorded from Java 42.9. A scenario carries its
Parquet files inside the script, base64 encoded, and writes them to a temporary directory
before it reads them, so that the same script runs on the reference and on Oxhaven with
no file shared between them. The files are a few hundred bytes to a few kilobytes: some
written by Java's own `deephaven.parquet.write`, some by pyarrow.
`crates/oxhaven-parquet/tests/data` holds the same files for the Rust tests, with a
`README.md` that says how each was made.

| Scenario | State | Gates |
| --- | --- | --- |
| `parquet_read_types`, `parquet_read_lists` | clean | Files Java wrote: every column type, nulls, vectors and arrays, `GZIP` and `LZ4_RAW` |
| `parquet_read_directory` | clean | Directories: the three layouts that read one, the order of files, hidden files, files with other columns, no rows |
| `parquet_read_foreign` | clean | Files of another writer: unsigned integers, timestamps, `INT96`, row groups, Java's nulls, column instructions |
| `parquet_read_errors` | clean | Fourteen reads that fail in Java |
| `parquet_read_codecs` | differs | Snappy and Zstandard, read; Brotli, refused |
| `parquet_read_more_types` | differs | The types without a column type |
| `parquet_read_partitioned` | differs | Partition directories, `_metadata`, refreshing directories |
| `parquet_read_names` | differs | Names Java makes legal |

The harness has no comparison for a `char` column (`unsupported_type`), so the scenarios
compare a char column through `(int)` in a `view`. The Rust tests compare the chars
themselves with what Java read.
It has none either for `BigDecimal`, `BigInteger`, `LocalDate`, `LocalTime` and `byte[]`,
so `parquet_read_more_types` compares those as text: the slice that ports them also
needs `toString` of each in a formula.

## Differences from Java

Besides what is refused:

- A file of a directory whose column has another type than the table's is refused when
  the table is made. Java makes the table and fails when that column is read ("Cannot
  convert parquet DOUBLE column to int"), and converts between some types.
- A codec named in `col_instructions` is refused; Java takes it and ignores it for a
  column that needs none.
- A file with Brotli pages is refused when it is opened. The reference as packaged
  makes the table and fails when a column is read, for want of the codec.
- An instruction that names a Parquet column and no table column is a `ValueError`
  inside the `DHError`; in Java it is a `NullPointerException` inside it.
- A table without rows does not carry the attribute `EmptySourceTable`.
- The files of a directory are ordered by path; Java orders them by URI, which differs
  for names outside ASCII.
- A timestamp in milliseconds or microseconds too large for nanoseconds is an error when
  it is read. What Java does with one was not looked at.
- Error texts are this reader's, except where Java's text is short and names no class
  (`PARITY.md` A5 does not compare them).

## Risks

1. **Memory.** A decoded column chunk is never dropped while its table lives, so a query
   that touches every column of a large file holds the file decoded. Java bounds this with
   soft references. A bounded cache fits behind `ParquetLocation` without changing the
   trait.
2. **Reads happen where the operation runs**, on a blocking file read under the update
   graph's shared lock when the operation is part of a console command. Local disks make
   that acceptable; a remote store will need the reads off that path.
3. **Types.** A good part of real files have dates and decimals, which wait for column
   types. The refusal is clear, but it is a refusal.
4. **Codecs**, as above.

## Writing Parquet

Task 7.2 (`tasks/7.2-parquet-write.md`). This section is its design; the code is
`crates/oxhaven-parquet/src/write.rs`, the binding `console/python/parquet.rs`
(`write_parquet`, `delete_parquet`) and `deephaven/parquet.py` (`write`, `batch_write`,
`write_partitioned`, `delete`, `RowGroupInfo`).

### Decision

1. **The file is Java's file.** A table written here has the Parquet schema, the
   `deephaven` footer entry, the row groups, the codec and the dictionaries that Java's
   `ParquetTableWriter` gives the same table, so that Java reads from it the table it
   reads from its own file, and so does this crate. What was compared is the footer (as
   pyarrow reads it) and the table each server reads back, never the bytes: page
   boundaries, the encodings listed for a chunk, statistics and `created_by` are the
   writing library's.
2. **The writer is `parquet`'s `ArrowWriter`**, given the Parquet schema explicitly
   (`ArrowWriterOptions::with_parquet_schema`) and told to leave the Arrow schema out of
   the footer. No dependency is added.
3. **One entry point.** `write(tables, paths, options)` is
   `ParquetTools#writeTables(Table[], String[], ParquetInstructions)`; `write` of one
   table is the same call with one table, as in Java.
4. **A table is read whole, then written.** The caller holds the update graph's shared
   lock for a table that changes (`guarded` in the binding, graph lock first and the
   interpreter released: `docs/PYTHON_UDF.md`), so the file is the table as it is at one
   moment, as Java's snapshot is.
5. **A write is taken back when it fails**, in Java's steps
   (`LocalCompletableOutputStream`): each file is written as `.NEW_<name>` beside its
   destination; when every file is written, each destination is moved to `.OLD_<name>`
   and the new file put in its place; then the old files are removed. On a failure the
   files in place are moved away again and the old ones back, the unfinished files are
   removed, and so are the directories the write made.

### What a file holds

| Column type | Parquet |
| --- | --- |
| `Boolean` | `BOOLEAN` |
| `Byte`, `Short`, `Int` | `INT32`, `INT(8/16/32, signed)` |
| `Char` | `INT32`, `INT(16, unsigned)` |
| `Long` | `INT64`, no logical type |
| `Float`, `Double` | `FLOAT`, `DOUBLE` |
| `String` | `BINARY`, `STRING` |
| `Instant` | `INT64`, `TIMESTAMP(nanos, adjusted to UTC)` |
| `Vector` or `Array` of one of the above | `optional group <name> (LIST) { repeated group list { optional <element> element } }` |

Every field is optional, the root is named `root`, and a null is a missing value, never
the Java null of the type.

The footer has one key-value entry, `deephaven`, as `TableInfo` writes it:
`{"version":"42.9"}`, then `columnTypes` with `{"columnName":<name in the
file>,"specialType":"Vector"}` for each vector column, then `sortingColumns` with the
first column the table is sorted by (its `SortedColumns` attribute), under its name in
the table. The version is the reference's (`oracle/REFERENCE.md`).

Row groups follow `RowGroupInfo`: one; `max_groups(n)`; `max_rows(n)`; `by_groups(columns,
max_rows)`, which fails when the rows of a group are not next to each other. A table
without rows has no row group.

Pages are Snappy unless `compression_codec_name` says otherwise. A name is one the
reference's Hadoop codec factory knows, in any case, with or without `Codec`:
`uncompressed`, `snappy`, `gzip`, `jdkgzip`, `zstd`, `lz4`, `lz4raw`, `lz4_raw`. Only a
string column has a dictionary, and only while its distinct strings stay within
`max_dictionary_keys` and `max_dictionary_size` (2^20 each by default); `use_dictionary`
in a column instruction lifts the limits.

`table_definition` chooses the columns of the file, orders them, and adds columns of
nulls for names the table does not have. A column instruction names the column in the
file.

### Out, each refused with `NotPorted`

| What | Java method named |
| --- | --- |
| `write_partitioned` with a definition that has partitioning columns | `ParquetTools#writeKeyValuePartitionedTable` |
| `generate_metadata_files=True` | `ParquetMetadataFileWriterImpl#writeMetadataFiles` |
| `index_columns` that names columns | `ParquetInstructions.Builder#addAllIndexColumns` |
| `special_instructions` | `ParquetInstructions.Builder#setSpecialInstructions` |
| A destination with a URI scheme, `file://` among them | `SeekableChannelsProviderLoader#load` |
| `LZO`, and a codec named by its class | `DeephavenCompressorAdapterFactory#getByName` |
| `codec_name` in a column instruction | `ParquetInstructions.Builder#addColumnCodec` |
| An empty name for the file in a column instruction | `ParquetInstructions.Builder#addColumnNameMapping` |
| A column whose cells hold vectors or arrays, which Java writes with `SerializableCodec` | `TypeInfos#getCodecAndArgs` |
| A column type without a Parquet type here | `TypeInfos#getTypeInfo` |
| A column that has another type in `table_definition` than in the table | `TransferObject#create` |
| `by_groups` on a vector or array column | `RowGroupTableIteratorVisitor.SplitByGroupsIterator#next` |

`write_partitioned` of a table without partitioning columns raises what Java raises
("Table must have partitioning columns to write partitioned data"); no table of the
engine has partitioning columns yet. The types of "Which types" that have no column type
cannot be in a table, so they cannot be written.

### Differences from Java

Besides what is refused, and none of them seen by a reader of the table:

- The chunk of a column lists `RLE` among its encodings where Java's does not, and a
  `float` or `double` chunk has its minimum and maximum under the IEEE 754 total order,
  which Java and pyarrow ignore as statistics. The null counts in the statistics of
  list columns are `parquet`'s, and larger than Java's. `created_by` is `parquet-rs`.
- A column has a dictionary in every row group or in none: when one row group goes over
  the limits, no row group of that column has one. Java decides for each row group.
- A table is held in memory whole while it is written; Java writes it page by page.
- A relative path is relative to the server's working directory, as in Java, whose
  working directory is another.
- Java makes a data index for the grouping columns a table already has an index for,
  and a write with `index_columns` leaves the index on the table. No table here has a
  data index, so no index file is written.
- A column of the table with another type in the definition is refused. Java casts: it
  fails for most pairs and writes the values under the definition's type for some (an
  `int` as a `short`).
- An instruction without a name for the file is a `ValueError` inside the `DHError`; in
  Java it is a `NullPointerException` inside it. Error texts are Java's where they are
  short and name no class.

One accident of Java is kept: a destination given twice in `batch_write` fails, and the
destination is left as the last table, because both tables are written to the one
unfinished file and the second cannot put it in place.

### Gate

```bash
cargo test -p oxhaven-parquet write
cargo test -p oxhaven-server --features python --lib parquet
oracle/bin/harness replay-scripts --url $URL --scripts parquet_write_
```

| Scenario | State | Gates |
| --- | --- | --- |
| `parquet_write_types` | clean | Every column type with nulls, vectors, arrays, no rows: read back, and the footer as pyarrow reads it (schema, `deephaven` entry, row groups, codec, dictionary) |
| `parquet_write_options` | clean | Codec names, row groups, names in the file, dictionary limits, a definition, `batch_write`, a sorted table, a replaced file, `delete`, a table that changes |
| `parquet_write_errors` | clean | Thirty-eight writes that fail in Java, and what they leave on disk |
| `parquet_write_unported` | differs | The refusals of the table above that Java does |

A scenario reads the footers with `pyarrow`, which the oracle's environment and the
reference's both have. Each server reads back its own files; that Java reads the files
written here to the same tables as its own was checked by hand, on 27 files
(types, vectors, arrays, codecs, row groups, dictionaries, renames, a definition), and
is not part of the gate, which runs one server at a time.

### Risks

1. **Memory**: a table larger than memory cannot be written. Writing row group by row
   group needs the row group sizes before the rows, which `by_groups` does not have.
2. **Partitioned writes and metadata files** wait for partitioning columns, with the
   partitioned reads.
3. **Two servers and one file.** Nothing locks a destination; two writes to one path
   race on `.NEW_<name>`, as in Java.
