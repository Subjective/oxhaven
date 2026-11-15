# Test files of the Parquet reader

Small Parquet files, read by the tests in `src/tests.rs`, by the console test in
`oxhaven-server` (`console/python/parquet.rs`) and, embedded in the scripts, by the script
scenarios `oracle/scenarios/python/parquet_*.py`. What the tests expect is what the Java
reference (Deephaven 42.9) answered when it read the same files, through those scenarios
and through `deephaven.parquet.read` in a console.

| File | Written by | Holds |
| --- | --- | --- |
| `types.parquet`, `types_gzip.parquet`, `types_lz4.parquet`, `types_snappy.parquet` | Java | Every column type Oxhaven has, a null in each column, a NaN, an empty string; uncompressed, `GZIP`, `LZ4_RAW`, and Java's default, Snappy |
| `lists.parquet` | Java | Vector columns (from `group_by`) and array columns |
| `flat/` | Java | Three files of 3, 4 and 5 rows and one without rows |
| `foreign.parquet` | pyarrow | Unsigned and narrow integers, timestamps in three units, large and dictionary strings, a list; three row groups; values equal to Java's nulls |
| `int96.parquet` | pyarrow | Timestamps as `INT96` |
| `uint64_big.parquet` | pyarrow | Unsigned 64-bit values up to 2^64 - 1 |
| `no_*.parquet` | pyarrow | One file per type that is refused: seven that Java reads and Oxhaven has no column type for, and two (`map`, `nested_list`) that Java refuses too |
| `names.parquet`, `reserved.parquet` | pyarrow | Column names Java changes: `a b`, `2x`, `int` |
| `codec_*.parquet` | pyarrow | One file per compression codec; `codec_zstd.parquet` and `codec_brotli.parquet` are refused |
| `evolve/`, `mismatch/` | pyarrow | Directories whose files do not have the same columns, or the same type for a column |
| `hidden/` | pyarrow | Hidden files and directories, and a file that is not Parquet |
| `kv/`, `meta/` | pyarrow | Partition directories; a `_metadata` file |

To make them again, with the environment of `oracle/README.md` and a Java reference
running:

```bash
oracle/.venv/bin/python crates/oxhaven-parquet/tests/data/make_arrow.py
oracle/.venv/bin/python crates/oxhaven-parquet/tests/data/make_java.py 10000
python3 crates/oxhaven-parquet/tests/data/embed.py oracle/scenarios/python/parquet_*.py
oracle/bin/harness record-scripts --url dh+psk://oxhaven@127.0.0.1:10000 --scripts parquet_
```

`make_arrow.py` writes the same bytes every time with pyarrow 26.0.1. `embed.py` copies
the files a scenario names into the scenario; a changed file means a changed scenario,
which has to be recorded again (a protected change, `oracle/README.md`).
