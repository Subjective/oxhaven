# Reading and writing CSV

How `deephaven.csv.read` and `deephaven.csv.write` are built. Written by task 7.3
(`tasks/7.3-csv.md`), which also built it. The seam it uses is that of `docs/STORAGE.md`:
a format is a crate of its own, reached from Python by one native function per entry
point.

## Decision

1. **The reader is written here, cell by cell as Java's is, and uses no CSV crate.** What a
   user sees of a CSV read is the type of each column, and Java decides it with the cells
   as its own tokenizer cut them: what a quote in the middle of a cell does, which blanks
   belong to a cell, when a short row is an error. A general CSV parser decides those
   otherwise in the corners, and the inference then runs on other cells. The cutting is
   four hundred lines; the corners are the work either way.
2. **The inference is Java's algorithm, not a table of its outcomes.** Java does not look
   at a whole column and pick a type. It runs a chain of parsers, each carrying on from
   the cell where the one before it stopped, and the column has the type of the parser
   that reached the end ("The rules" below). Which type a column gets depends on the order
   of its cells, so only the same chain gives the same answers.
3. **A CSV table is loaded whole**, as in Java: one in-memory column per column of the
   file, row keys `0..size`, static. It does not use `oxhaven-engine::source`.
4. **The crate is `oxhaven-csv`**: `read(path, &ReadOptions)`, `read_bytes(input,
   &ReadOptions)` and `write(table, path, columns, zone)`, with the error enum of
   `docs/STORAGE.md` ("Errors"). It depends on the engine and not on the server.
5. **Python reaches it through `_oxhaven.read_csv` and `_oxhaven.write_csv`**
   (`console/python/csv.rs`). `deephaven.csv` has the names and signatures of the
   reference module and no logic beyond turning its arguments into plain values;
   `deephaven.read_csv` and `deephaven.write_csv` are the same functions.
6. **Java's results are recorded twice.** `crates/oxhaven-csv/tests/data/recorded.json` is
   what the reference made of 555 small files, most of one column, written to be awkward:
   the Rust test reads each and compares names, types and values, the chars and the bits
   of the doubles included. The script scenarios `csv_*` in `oracle/` gate the same
   through the server and the Python module, on fewer and larger files.

## The rules

Behavior: `io.deephaven.csv.reading.ParseDenseStorageToColumn` of the `deephaven-csv`
library (0.19.0, Apache-2.0), with the parsers `Parsers.DEFAULT` and the reserved values
that `io.deephaven.csv.CsvTools#makeMySinkFactory` sets.

- An empty cell is null and says nothing about the type. A quoted empty cell is one too.
  A column of nulls only is a `String` column.
- The first cell with text chooses the family. If it reads as a `double` the column is
  numeric. Otherwise, if it is `true` or `false` in any case, booleans are tried first,
  and if it is a date-time, instants.
- Numeric: `int` for as long as the cells are `int`s, `long` from the first cell that is
  not, then `double`. The column has the last type that was needed. `byte`, `short` and
  `float` are never inferred.
- When a cell does not fit, and for every column that is not numeric: `char` if every
  cell with text is one character, otherwise `String`. So `1` then `x` is a `char`
  column, and `1` then `xy` a `String` column with the text of the numbers.
- The value that stands for null in a type is not a value of the type:
  `-2147483648` makes a `long` column, `-9223372036854775808` a `double` column,
  `-1.7976931348623157e308` a `String` column, and the character U+FFFF a `String`
  column.
- A type named in `header` is read with that type's parser alone, and a cell it does not
  take fails the read.

What a cell of each type looks like is in `crates/oxhaven-csv/src/tokens.rs`, each with
the Java method it follows. Three things there are Java's and not what one would write:

- A whole number of twenty digits can pass Java's overflow check and wrap:
  `25000000000000000000` is the `long` 6553255926290448384.
- A floating point number has the grammar of a Java literal: `1d`, `1.5F`, `0x1p3`, `NaN`
  and `Infinity` are numbers, `nan` and `inf` are not. A hexadecimal number of more than
  sixteen digits is cut, not rounded, and an exponent stops growing at 1024; both are
  reproduced.
- A date-time is ISO 8601 with `T` or a space, a zone that is `Z`, an offset or a space
  and a zone name, or no zone, which is UTC. A month, day or hour that does not exist
  fails the read; it does not make the column a `String` column. Java finds a zone name
  by a number computed from its letters, so `utc` is UTC, and so are a few texts that are
  no zone at all; `crates/oxhaven-csv/src/zones.rs` has the same lookup over the zone
  names of the reference's JVM.

Column names are the headers made legal as Java makes them
(`io.deephaven.csv.ColumnNameLegalizer`, which is
`io.deephaven.api.util.NameValidator#legalizeColumnNames`):
characters that no name can have are dropped or replaced, a name that is a reserved word
or starts with a digit gets a `column_` prefix, and a name that is taken gets a number.

## Writing

Behavior: `io.deephaven.csv.CsvTools#writeCsv(Table, boolean, String, String...)`. A line
of column names, then a line per row, each line ended by a line feed. A null is nothing.
A cell with a comma, a quote or a line feed is quoted, with its quotes doubled. Floating
point numbers are spelled as `Double.toString` and `Float.toString` spell them, and an
instant as `LocalDateTime.toString` in the server's time zone followed by the offset. A
refreshing table is written under the update graph's shared lock.

## Not ported

Each is a `CsvError::NotPorted` that names the Java method, a `NotImplementedError` inside
the `DHError` in Python.

| What | Java method named |
| --- | --- |
| A date-time cell in a zone whose offset changes (`ET`, `Europe/London`). Zones with one offset (`UTC`, `Etc/GMT+5`, `SystemV/EST5`) are read. The engine has no time zone rules; the Python package's are Python's | `io.deephaven.csv.DeephavenTimeZoneParser#tryParse` |
| A file that is not UTF-8. Java reads bytes, and makes U+FFFD of a malformed sequence only where it builds a string | `io.deephaven.csv.reading.cells.DelimitedCellGrabber#grabNext` |
| A path that is a URL other than a plain `file:` one | `io.deephaven.csv.CsvTools#readCsv(URL, CsvSpecs)` |
| A path that ends in `.zip`, `.bz2`, `.gz`, `.7z`, `.zst` or `.tar`, which Java unpacks | `io.deephaven.engine.util.PathUtil#open` |
| A decimal number of more than nineteen digits whose exponent has more digits than Java adds up, when the short exponent is one a `double` can have. Java then makes a wrong value by an algorithm that is not here. It takes hundreds of zeros to write one | `ch.randelshofer.fastdoubleparser.JavaDoubleParser#parseDouble` |
| Writing a column of instants when the server's time zone is not UTC throughout | `io.deephaven.time.DateTimeUtils#timeZone` |
| Writing a column of vectors or arrays. Java writes a vector as `[1,2]` and an array as the identity of the Java object | `io.deephaven.csv.CsvTools.CsvRowFormatter.ObjectColumnFormatter#write` |

## Dependencies

No crate is added to the workspace. `oxhaven-csv` uses `arrow` and, in its tests,
`serde_json`, which the workspace has.

The `deephaven-csv` library was read for behavior, with its licence checked first
(Apache-2.0, as its Maven POM says), and so was FastDoubleParser, the
number parser it uses (MIT). No code of either is here. The module `deephaven/csv.py` was
written from the reference module's names, signatures and error texts only.

## Gate

```bash
cargo test -p oxhaven-csv
cargo test -p oxhaven-server --features python --lib csv
oracle/bin/harness replay-scripts --url $URL --scripts csv_
```

`crates/oxhaven-csv/tests/data/record.py` holds the cases of the first and records them
from a running reference server; `README.md` next to it says how.

| Scenario | State | Gates |
| --- | --- | --- |
| `csv_read_types` | clean | The type of a column from its cells: every type, widening, nulls and null values, the spellings of numbers and date-times, types named in `header`, operations on the table |
| `csv_read_options` | clean | Every argument of `read`: header rows, skipped and counted rows, delimiter and quote, blanks, short and long rows, empty lines; the names made of headers; a file without columns |
| `csv_read_errors` | clean | Twenty-four reads that fail in Java |
| `csv_write` | clean | Files written from every column type, compared line by line and read back; two writes that fail |
| `csv_not_ported` | differs | Date-times in zones with rules, and a column of vectors written |

The harness has no comparison for a `char` column, so the scenarios compare one through
`(int)` in a `view`; the Rust test compares the chars themselves.

## Differences from Java

Besides what is refused:

- The zone names a date-time cell may end in are those of the reference's JVM (OpenJDK
  21, tz database 2026b) and Deephaven's default aliases, fixed in
  `crates/oxhaven-csv/src/zones.txt`. An alias added at run time with
  `time_zone_alias_add` is not known to the reader, and a JVM with another tz database
  knows other names.
- `write` takes the server's time zone from the machine (`TZ`, `/etc/localtime`), as the
  rest of the Python package does. Java's is `ZoneId.systemDefault()`, which the JVM
  option `user.timezone` sets before the machine does.
- A delimiter or quote that is no single character is a `TypeError` inside the `DHError`
  here as in the reference; one outside the Basic Multilingual Plane is refused as not
  7-bit ASCII here, and fails in jpy before it reaches Java there.
- Error texts are this reader's where Java's name a class or a stack
  (`PARITY.md` A5 does not compare them). The texts of `CsvSpecs#check`, of a row with
  too few or too many cells and of an unknown column in `write` are Java's.
- A failed `write` leaves the file empty, as Java's does, when the failure is an unknown
  column; a refused column type leaves an empty file where Java would have written one.
