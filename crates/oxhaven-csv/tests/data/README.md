# Recorded CSV reads

`recorded.json` is what the Java reference (42.9, `oracle/REFERENCE.md`) made of each case
of `record.py`: the column names, types and values of the table, or the fact that the read
failed. `src/tests.rs` reads every case with this crate and compares.

To record again, with a reference server running (`oracle/bin/start-java.sh <port>`):

```bash
oracle/.venv/bin/python crates/oxhaven-csv/tests/data/record.py <port>
```

A case is a name, the text of a file and the keyword arguments of `deephaven.csv.read`.
About 150 are written by hand, each for one rule or one corner of the reader; 400 are
single columns generated from a fixed seed, of cells drawn from the spellings that sit on
the edges between types. Recording the same cases twice gives the same file.

In the recording an `int`, `long`, `char` (its code) or instant (nanoseconds) is a decimal
string, a `float` or `double` is the bits of the `double` as a signed decimal string, so
that `-0.0` and `NaN` compare, and a null is `null`. `args` is the options once more as
data; `header` is a list of pairs because its order assigns the names to the columns.
