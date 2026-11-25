# Oxhaven

A Rust implementation of the Deephaven Community Core server. It aims to speak the same gRPC,
Arrow Flight and Barrage protocols as the Java server, so existing clients and the web IDE connect
unchanged, with the engine reimplemented in Rust.

Status: early. The server runs locally with the Deephaven web IDE and a Python console. It is
not ready for data anyone depends on.

## Run it with the web IDE

The quickest way is the Docker image, which has the IDE, a Python console with numpy, pandas,
pyarrow and `deephaven-plugin-plotly-express`, and notebooks in the volume `/data`:

```bash
docker build -f docker/Dockerfile -t oxhaven .
docker run --rm -p 10000:10000 -e START_OPTS=-Dauthentication.psk=mykey oxhaven
```

Then open <http://localhost:10000/ide/?psk=mykey>. Without `START_OPTS` the server makes a
key and prints it to the log. `docker/README.md` has the options, HTTPS among them.

To build and run it without Docker: this needs Rust, a Python 3 with its shared library (the console embeds it, see
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

Build features of `oxhaven-server`: `python` (the console, off by default), `tls` (serving
over HTTPS and TLS connections of the Barrage client through the system's OpenSSL 3, off by
default, turned on by `python`; `docs/TLS.md`) and `mimalloc` (on by default), which makes
mimalloc the binary's memory allocator; build with `--no-default-features` (and `--features
python` if wanted) for the system allocator.
`docs/BENCHMARK.md`, "After task 9.13", has the measurements behind it.

What works in the IDE, as `oracle/ide/ide-test.js` tests it: the console, tables in the grid
that tick, scroll, sort and filter, notebooks in the File Explorer (create, run, save,
rename, move, delete), the Log panel and Command History. From Python: `empty_table`,
`new_table`, `time_table`, `input_table`, `merge`, `update`, `view`, `select`, `where`,
`sort`, the joins, `agg_by` and the dedicated aggregations, `update_by`, Python functions in
formulas, reading CSV and Parquet, and conversion to and from pandas and numpy.

Also served: plots, from `deephaven.plot` and from the `deephaven-plugin-plotly-express`
package (`docs/FIGURES.md`), partitioned tables (`docs/PARTITIONED.md`), rollups and trees
(`docs/HIERARCHICAL.md`), Kafka consumers and producers (`docs/KAFKA.md`), business calendars,
and tables of other servers through `deephaven.uri` and `deephaven.barrage`, over TLS too
(`docs/CONSOLE.md`).

What does not: Groovy, SQL, Iceberg, S3 and JDBC (out of scope), calls to Java classes
through `jpy` beyond the few the console carries out (`docs/CONSOLE.md`, "The jpy module"),
`deephaven.ui` (untried). The server speaks plain HTTP unless it is given a certificate
(`--tls-cert`, `--tls-key`; `docs/TLS.md`), and checks a pre-shared key. A call that is not ported answers `UNIMPLEMENTED` or raises, naming the Java
method. The differences from Deephaven that are allowed are listed in `PARITY.md`. After the
server restarts the IDE has to be loaded again, as with Java.

Contributors, human or agent, start with `AGENTS.md`.

## Licence

Oxhaven is under the Apache License 2.0 (`LICENSE`). Its Python package is named `deephaven`
so that scripts written for Deephaven run. Whether that name may be used needs a legal reading
before any release (the owner's ruling of 2026-11-17); it is an open item.
