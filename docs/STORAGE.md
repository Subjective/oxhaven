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
is append-only by construction. It is `source::following_table`, built by the second
slice ("The second slice", below).

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

Partitioning columns (the second slice) are a constant per region. The trait did not
need a method for them: a location answers a read of a partitioning column with its value
repeated, without reading the file. The flag on `ColumnDefinition` that the schema sends
as `deephaven:isPartitioning` is in the contracts since that slice.

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
| `DECIMAL(precision, scale)` on any storage | `BigDecimal`; `BigInteger` for precision 1 and scale 0, which is how Java writes one |
| `INT(64, unsigned)` without a target | `BigInteger` |
| `DATE` | `LocalDate` |
| `TIME(millis/micros/nanos)` | `LocalTime` |
| `TIMESTAMP(millis/micros/nanos, not adjusted to UTC)` | `LocalDateTime` |
| `BINARY`, `FIXED_LEN_BYTE_ARRAY` without a logical type | `Array` of `Byte`, Java's `byte[]` |
| A group that is not a list and has one field | The type of the field |

A value equal to the Java null of its type reads as null, as it does in Java, where it is
the same bit pattern: a 32-bit `-2147483648` from another writer is a null `int`.

The last seven rows are the second slice's ("The second slice", below). Still refused by
name when the table is created: a list of one of the types of those rows, a column with a
codec, and a `StringSet`.

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
| `table_definition` | `deephaven.dtypes` and `TableDefinition` in the Python package (6.8) |
| `special_instructions`, and any path with a URI scheme (S3 and the other stores) | Its own task |
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
| `parquet_read_more_types` | clean | Decimals, dates, times, local timestamps, unsigned 64-bit integers, bytes, a struct with one field |
| `parquet_read_partitioned` | clean | Partition directories, `_metadata`, a directory that is followed |
| `parquet_read_names` | clean | Names Java makes legal |

The harness has no comparison for a `char` column (`unsupported_type`), so the scenarios
compare a char column through `(int)` in a `view`. The Rust tests compare the chars
themselves with what Java read.
It has none either for `BigDecimal`, `BigInteger`, `LocalDate`, `LocalTime` and `byte[]`,
so `parquet_read_more_types` compares those as text, with `toString()` of each in a
formula.

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

## The second slice

Task 7.1b (`tasks/7.1b-parquet-read-more.md`) took the four rows out of the list above
that the first slice left for it.

### The column types of Java's objects

Five column types were added to the contracts, each with the Arrow type a column of it
is held as. The layout of a cell is in `oxhaven-column::java_values`, with the texts
Java prints.

| Column type | Held as | Sent to a client as (Java's `DefaultChunkWriterFactory`) |
| --- | --- | --- |
| `BigDecimal` | `Binary`: the scale, 4 bytes little-endian, then the unscaled value as `BigInteger#toByteArray` gives it | The same bytes, of the value without its trailing zeros |
| `BigInteger` | `Binary`: `BigInteger#toByteArray` | The same bytes |
| `LocalDate` | `Date32` | `Date64`, milliseconds |
| `LocalTime` | `Time64(ns)` | `Time64(ns)` |
| `LocalDateTime` | `Timestamp(ns)` without a zone | `Utf8`, the text of `toString()` |

`byte[]` needed no new type: it is `Array` of `Byte`, and a client is sent `Binary`.

A formula can test a column of these types for null and print it (`isNull(D)`,
`D.toString()`), and make a string of a `byte[]` and take its length (`new String(B)`,
`B.length`). Every other use is refused by the type checker, so the values never reach
an operator that does not know them. A `LocalDate` before the year -5877641 or a
`LocalDateTime` outside 1677 to 2262 cannot be held and is an error when it is read.

### Names

A column no instruction names gets the name `NameValidator#legalizeColumnName` gives it,
with Java's rule for which names a changed name must differ from: none for the first
name that is changed, then the names of the instructions and the changed names, and
never a name that was legal as it was. So `a_b` then `a b` is two columns `a_b` and a
refusal, and `a b` then `a_b` is `a_b` and `a_b2`, as in Java.

### Partition directories

A directory without a `_metadata` file is walked, and each directory named `key=value`
between the root and a file is a partitioning column, before the columns of the files.
The first file by path names the keys; the values get their types from the CSV reader,
given a file whose header is the keys and whose rows are the values, as Java does
(`LocationTableBuilderCsv`): `K=1` and `K=2` are an `int` column, `K=a` and `K=b` a
`char` column. The files are ordered by their partition values, then by path. A column of
the files with the name of a key is not read.

The definition says which columns are partitioning ones, and the schema and the meta
table tell a client. An operation keeps them where Java does: where its result has the
definition of its parent (`where`, `head`, `tail`, `slice`, `reverse`, `where_in`, a
`sort` that moves no row, a copy), and where a column of the result is the very source
of a partitioning column (`view`, `update_view`, `update`, `lazy_update`,
`drop_columns`, `rename_columns`, `move_columns`, `update_by`; Java's
`TableDefinition#inferFrom(Table, Map)`). Every other table is made without them
(`TableSpec::keeping_partitioning` is the exception, not the rule).

### Metadata files

`_metadata` names the files of the table and has its columns; the files are the ones it
names, in the order it first names them. The footers of the files themselves are read
for their row groups, where Java reads what the metadata file says of them, so a file
that the metadata file lists a part of is refused. Partitioning columns that only
`_common_metadata` knows are refused (`ParquetMetadataFileLayout#create`): that is the
layout `write_partitioned` makes with `generate_metadata_files`, which is not ported
either.

### A directory that is followed

`read_in(graph, path, options)` with `refreshing` makes `source::following_table`: the
files that are there, and a provider that looks at the directory again when 60 seconds
have passed since it last did (Java's `TableDataRefreshService`, by default). The files
it had not seen become regions after the ones that are there, in the order above among
themselves. The table has the attribute `AppendOnly` and not `AddOnly`, as Java's has.
The look happens in the cycle, under the graph's exclusive lock: Java looks on a thread
of its own and hands the result to the cycle.

### Differences from Java, second slice

- A column that renames a partitioning column in `view`, `update_view` or
  `lazy_update` (`Q = K`) is a partitioning column. In Java it is one when the
  operation is applied to any table derived from the read, and not when it is applied
  to the table as `read` answers it, which is not coalesced yet and answers through
  `DeferredViewTable`. Oxhaven has no such state of a table.
- The files of partition directories are taken by path; Java takes them in the order
  its walk of the directory finds them, which decides the names of the keys and which
  refusal a directory with mixed depths or mixed keys gets. A file with fewer partition
  directories than an earlier one is refused as not ported
  (`URIStreamKeyValuePartitionLayout#getPartitions`); Java refuses the deeper file when
  it meets the shallower one first.
- A file a `_metadata` file names that is not there is an error when the table is made;
  in Java when its rows are read.
- A partition value that turns up later with another type than its column fails the
  table when it is found; Java fails when the value is read.
- `new String(bytes)` of bytes that are not UTF-8 is refused; Java replaces them.
- A `TIME` in microseconds too large for nanoseconds is an error; Java lets it wrap.

## Risks

1. **Memory.** A decoded column chunk is never dropped while its table lives, so a query
   that touches every column of a large file holds the file decoded. Java bounds this with
   soft references. A bounded cache fits behind `ParquetLocation` without changing the
   trait.
2. **Reads happen where the operation runs**, on a blocking file read under the update
   graph's shared lock when the operation is part of a console command. Local disks make
   that acceptable; a remote store will need the reads off that path.
3. **Types.** Dates, times and decimals are read since the second slice, and a formula
   can only print them and test them for null: a query that computes with them is refused
   by the type checker.
4. **Codecs**, as above.

## Writing Parquet

Tasks 7.2 (`tasks/7.2-parquet-write.md`) and 7.2b (`tasks/7.2b-parquet-write-more.md`).
This section is their design; the code is `crates/oxhaven-parquet/src/write.rs` with
`write/partitioned.rs` and `write/metadata.rs`, the binding `console/python/parquet.rs`
(`write_parquet`, `write_parquet_partitioned`, `delete_parquet`) and
`deephaven/parquet.py` (`write`, `batch_write`, `write_partitioned`, `delete`,
`RowGroupInfo`).

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
3. **One place where files are written.** `write(tables, paths, options)` is
   `ParquetTools#writeTables(Table[], String[], ParquetInstructions)`; `write` of one
   table is the same call with one table, as in Java. `write_partitioned(source,
   directory, options)` is `ParquetTools#writeKeyValuePartitionedTable`: it names a
   file for each constituent and writes them with the same step, so a partitioned write
   is also all files or none.
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
| `BigDecimal` | `BINARY`, `DECIMAL(precision, scale)`: the unscaled value at the column's scale |
| `BigInteger` | `BINARY`, `DECIMAL(1, 0)` |
| `LocalDate` | `INT32`, `DATE` |
| `LocalTime` | `INT64`, `TIME(nanos, adjusted to UTC)` |
| `LocalDateTime` | `INT64`, `TIMESTAMP(nanos, not adjusted to UTC)` |
| `Vector` or `Array` of one of the above | `optional group <name> (LIST) { repeated group list { optional <element> element } }` |

Every field is optional, the root is named `root`, and a null is a missing value, never
the Java null of the type. A `byte[]` column is an array of `Byte`, so it is a list of
`INT(8)`, as in Java.

The scale of a `BigDecimal` column is the largest scale of its values, and its precision
the most digits before the point of any value plus that scale
(`BigDecimalUtils#computePrecisionAndScale`), over every table of the write. A column
without a value is `DECIMAL(1, 1)`. One accident of Java is kept: a column whose values
all have one digit and no scale is `DECIMAL(1, 0)`, which is read back as `BigInteger`.

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
`uncompressed`, `snappy`, `gzip`, `jdkgzip`, `zstd`, `lz4`, `lz4raw`, `lz4_raw`. A codec
is also known by the name of its class, in the case it is spelt in:
`org.apache.hadoop.io.compress.GzipCodec`, `io.airlift.compress.gzip.JdkGzipCodec`,
`org.apache.parquet.hadoop.codec.SnappyCodec`,
`org.apache.parquet.hadoop.codec.Lz4RawCodec`, `org.apache.hadoop.io.compress.Lz4Codec`,
`io.airlift.compress.lz4.Lz4Codec` and `io.airlift.compress.zstd.ZstdCodec`. Every other
name fails as in Java, `BROTLI` among them. Only a
string column has a dictionary, and only while its distinct strings stay within
`max_dictionary_keys` and `max_dictionary_size` (2^20 each by default); `use_dictionary`
in a column instruction lifts the limits.

`table_definition` chooses the columns of the file, orders them, and adds columns of
nulls for names the table does not have. A column instruction names the column in the
file.

A destination is a path, or a `file:` URI whose path is decoded (`a%20b` is `a b`); a
path that is not a URI is taken as it is. Java's failures for a URI are kept: an
authority (`file://host/...`), a query, a fragment, a character a URI cannot have.

### Partitioned writes

`write_partitioned` takes a table, which it splits by the partitioning columns of
`table_definition` (or of the table), or a partitioned table, whose key columns are the
partitioning columns. Each constituent is one file,
`<directory>/<key>=<value>/.../<base name>.parquet`, without the key columns.

- `base_name` may hold `{uuid}`, a random UUID and the default; `{i}`, which counts the
  files of one key from 0; and `{partitions}`, the directory names joined by `_`. A
  partitioned table whose keys are not unique needs `{i}` or `{uuid}`.
- A value is the text `PartitionFormatter` gives it: a null is the empty text, so the
  directory is `K=`. Ported: `String`, `Boolean`, `Char`, `Byte`, `Short`, `Int`,
  `Long`, `BigInteger`, `BigDecimal`, `LocalDate`, `LocalTime`.
- The directory is not emptied first, as in Java: a file under a name the write uses is
  replaced, and every other file and directory stays, so the files of an earlier write
  under other names are still read with the new ones.
- A write that fails leaves the files that were there as they were and nothing new.

Java's failures are kept with their texts, among them a table without partitioning
columns, a definition with nothing but partitioning columns, an index on a partitioning
column, and three accidents: a table without rows fails with
`java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0`; a null
and an empty string of one key are both `K=`, and the second file cannot be put where
the first is; a value with `:` in the first directory name (a `LocalTime`) fails, since
Java takes what is before it for the scheme of a URI. A `/` in a value makes a directory
below, as in Java.

### Metadata files

With `generate_metadata_files=True` a write also leaves `_metadata` and
`_common_metadata` (`write/metadata.rs`, `ParquetMetadataFileWriterImpl`): for `write`
and `batch_write` in the directory of the files, which must be one directory; for
`write_partitioned` in the destination directory. They are written and taken back with
the files, and replace the ones that were there, so they list the files of the last
write only.

- `_metadata` has the schema of the files and every row group of every file, each with
  the path of its file from the directory.
- `_common_metadata` has no row group; its schema starts with the partitioning columns
  of a partitioned write, under their names in the file.
- Both have the `deephaven` entry of each file under `deephaven_per_file_<path with _
  for />`, and a `deephaven` entry for the table: the version and the column types,
  without sorting columns.

Java reads a directory written here through these files to the table it reads from its
own. This crate does not yet read a directory whose partitioning columns only
`_common_metadata` knows (`ParquetMetadataFileLayout#create`, in the reader), so what
`write_partitioned` writes with metadata files is read here file by file, or after the
two files are removed.

### Out, each refused with `NotPorted`

| What | Java method named |
| --- | --- |
| `index_columns` that names columns | `ParquetInstructions.Builder#addAllIndexColumns` |
| `special_instructions` | `ParquetInstructions.Builder#setSpecialInstructions` |
| A destination with another URI scheme than `file` | `SeekableChannelsProviderLoader#load` |
| `LZO`, by its name or by `io.airlift.compress.lzo.LzoCodec` | `DeephavenCompressorAdapterFactory#getByName` |
| A partitioning column of type `Float`, `Double` or `Instant` | `PartitionFormatter#formatObject` |
| A directory or file name of a partitioned write that is `.` or `..`, starts with `/`, or starts like a URI scheme | `ParquetUtils#resolve` |
| `codec_name` in a column instruction | `ParquetInstructions.Builder#addColumnCodec` |
| An empty name for the file in a column instruction | `ParquetInstructions.Builder#addColumnNameMapping` |
| A column whose cells hold vectors or arrays, which Java writes with `SerializableCodec` | `TypeInfos#getCodecAndArgs` |
| A column type without a Parquet type here | `TypeInfos#getTypeInfo` |
| A column that has another type in `table_definition` than in the table | `TransferObject#create` |
| `by_groups` on a vector or array column | `RowGroupTableIteratorVisitor.SplitByGroupsIterator#next` |

The types of "Which types" that have no column type cannot be in a table, so they cannot
be written.

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
- The entries of a metadata file are in the order of the files, then the table's; Java's
  are in the order of a hash map.
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
| `parquet_write_unported` | differs | The refusals of the table above that Java does (`lzo`, `indexed`, `nested`, `narrowed`); its other steps match: a codec by its class, metadata files, a `file:` destination, a partitioned write under a base name without a UUID |

A scenario reads the footers with `pyarrow`, which the oracle's environment and the
reference's both have. Each server reads back its own files; that Java reads the files
written here to the same tables as its own was checked by hand, on 27 files
(types, vectors, arrays, codecs, row groups, dictionaries, renames, a definition), and
is not part of the gate, which runs one server at a time. For task 7.2b the same was
done in both directions on the files of the object types, of partitioned writes and of
writes with metadata files: Java read every file and every directory written here to
the table it reads from its own, and this crate read every file Java wrote; the
directories it does not read are the ones of "Metadata files" above.

### Risks

1. **Memory**: a table larger than memory cannot be written. Writing row group by row
   group needs the row group sizes before the rows, which `by_groups` does not have.
2. **A partitioned write does not clean its directory**, as in Java: a partition that
   is no longer in the table keeps its old file, which is read with the new ones.
3. **Two servers and one file.** Nothing locks a destination; two writes to one path
   race on `.NEW_<name>`, as in Java.
