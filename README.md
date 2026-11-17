# Oxhaven

A Rust implementation of the Deephaven Community Core server. It aims to speak the same gRPC,
Arrow Flight and Barrage protocols as the Java server, so existing clients and the web IDE connect
unchanged, with the engine reimplemented in Rust.

Status: early. The server runs locally with the Deephaven web IDE and a Python console. It is
not ready for data anyone depends on.

## Run it with the web IDE

Needs Rust, a Python 3 with its shared library (the console embeds it, see
`docs/CONSOLE.md`), and the `deephaven-server` wheel, which the IDE's files come from. The
oracle's Python environment has the wheel (`oracle/README.md`):

```bash
python3 -m venv oracle/.venv && oracle/.venv/bin/pip install -r oracle/requirements.txt
cargo build -p oxhaven-server --features python
scripts/extract-web-root.sh ~/oxhaven-web     # the IDE's files; keep them outside the checkout
VIRTUAL_ENV=$PWD/oracle/.venv target/debug/oxhaven-server \
  --psk mykey --web-root ~/oxhaven-web --storage-root ~/oxhaven-storage
```

Then open <http://localhost:10000/ide/?psk=mykey>. `VIRTUAL_ENV` names the environment whose
packages the console imports (numpy, pandas, pyarrow), `--storage-root` is the directory the
IDE keeps notebooks and layouts in, and `oxhaven-server --help` lists the other options.
`--app-dir <directory>` runs the Python scripts that the `.app` files of the directory name
before the server accepts clients, as Java's application mode does (`docs/CONSOLE.md`,
"Applications defined by scripts").

What works in the IDE, as `oracle/ide/ide-test.js` tests it: the console, tables in the grid
that tick, scroll, sort and filter, notebooks in the File Explorer (create, run, save,
rename, move, delete), the Log panel and Command History. From Python: `empty_table`,
`new_table`, `time_table`, `input_table`, `merge`, `update`, `view`, `select`, `where`,
`sort`, the joins, `agg_by` and the dedicated aggregations, `update_by`, Python functions in
formulas, reading CSV and Parquet, and conversion to and from pandas and numpy.

What does not: plots (`deephaven.plot`), `deephaven.ui`, `partition_by`, `rollup` and `tree`,
Groovy and https. A call that is not ported answers `UNIMPLEMENTED` or raises, naming the
Java method. After the server restarts the IDE has to be loaded again, as with Java.

Contributors, human or agent, start with `AGENTS.md`.

## Licence

Oxhaven is under the Apache License 2.0 (`LICENSE`). Its Python package is named `deephaven`
so that scripts written for Deephaven run. Whether that name may be used needs a legal reading
before any release (the owner's ruling of 2026-11-17); it is an open item.
