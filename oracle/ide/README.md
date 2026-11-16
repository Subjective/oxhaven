# IDE acceptance test

The acceptance criterion of Oxhaven is: open the Deephaven web IDE that the server serves,
connect, and see a table. `ide-test.js` does that in headless Chromium, against any server URL,
and checks the values the grid paints. [REQUIREMENTS.md](REQUIREMENTS.md) lists what the IDE
asks of a server, recorded with this test against the Java reference.

| Path | Holds |
| --- | --- |
| `ide-test.js` | The test |
| `expected.json` | The tables the test expects and the text the grid paints for them |
| `lib/flows.js` | The routes around the console: `notebook`, `log`, `grid`, `errors`, `reconnect` |
| `lib/trace.js` | Records every request, websocket frame and gRPC call of a page |
| `lib/faults.js` | Answers chosen gRPC calls in the browser (`--deny`, `--empty`) |
| `decode_trace.py` | Prints a recorded trace with decoded protobuf and Barrage messages |
| `probe-faults.js` | Runs the test once per gRPC call with that call taken away |
| `bin/start-java-ide.sh` | Starts the Java reference with the expected tables |
| `app.d/` | The tables, as Python for the Java reference |

## Run

Needs Node with Playwright and its Chromium (`require('playwright')` must resolve; nothing is
installed by the test) and, for the Java side, the oracle's Python environment
(`oracle/README.md`).

```bash
oracle/ide/bin/start-java-ide.sh 10500            # prints http://127.0.0.1:10500
node oracle/ide/ide-test.js --url dh+psk://oxhaven@127.0.0.1:10500
oracle/bin/stop-java.sh 10500
```

```
ok   panels: grid cells of ide_static match; ide_ticking N went 979 -> 981
     18 gRPC calls over ws-multiplex; screenshots: oracle/ide/.out/panels-static.png, ...
ok   iframe: ...
ok   jsapi: rows of ide_static match; ide_ticking N went 983 -> 989 over 4 updates
PASSED: panels, iframe, jsapi
```

The exit status is 0 when every route passed, 1 when a route failed, 2 when the test could not
run. `--url` takes `http://host:port` or `https://host:port` with `--psk <key>` (no `--psk`:
anonymous authentication), or the oracle's `dh+psk://<key>@host:port` and `dh://host:port`,
which mean plain http. A certificate that the browser does not trust is accepted.

## Routes

Each route runs in a fresh browser profile. `--routes` selects them; the default is
`panels,iframe,jsapi`, which is what a server without a console can do.

| Route | What it does |
| --- | --- |
| `panels` | Opens `/ide/`, types the key into the login form, opens `ide_static` and then `ide_ticking` from the **Panels** list. The tables exist on the server beforehand. |
| `iframe` | Opens `/iframe/table/?name=ide_static&psk=<key>`, then the same for `ide_ticking`. |
| `jsapi` | Loads `/jsapi/dh-core.js` in a page of the server's origin, logs in with `CoreClient.login`, fetches both tables with `getObject`, sets a viewport and reads rows. |
| `console` | Opens `/ide/?psk=<key>` and types the commands of `expected.json` for the console's language into the console. Needs a Python or Groovy console. |
| `rollup` | Makes a table in the console with the command of `expected.json` and a rollup of it in the **Rollup Rows** panel of its grid (Table Options), with the constituents the panel includes by default; reads the rows the grid paints (the grand total and the first level), expands a node with a click on its marker, then a finest group under it, whose children are the rows of the table, reads the rows after each, and collapses both; sorts a column from its header, expands the two again and reads the rows of the table sorted by the column the sorted one aggregates; filters by the value of a cell. Checks that the panel called `HierarchicalTableService/Rollup` and the sort and the filter `Apply`. Needs a Python console. |
| `tree` | Makes a tree in the console with the command of `expected.json`, which the IDE opens as it opens a table; reads the rows the grid paints (the rows without a parent), expands a row with a click on its marker, reads the rows again, and collapses it; sorts a column from its header, expands the row again and filters by the value of a cell of one of its children, after which the grid shows that row and its ancestor. Checks that the sort and the filter each called `HierarchicalTableService/Apply`. Needs a Python console. |

| `notebook` | In the File Explorer: makes a folder and a notebook, runs a selected line and the whole notebook, saves it, opens it again, changes it, renames it, drags it into the folder, deletes both. Needs a console and a server that stores files. |
| `log` | Prints from the console and finds the line in the Log panel; opens a command from Command History in the console and runs it. |
| `grid` | Makes a table of 300 rows in the console, scrolls its grid to the end and back, sorts it from the header menu and filters it from a cell's menu. |
| `errors` | Runs a command that raises and reads the error the console shows; removes a variable whose panel is open and sees the panel close. |
| `reconnect` | Makes a table, restarts the server with the command of `--restart`, sees the IDE say that it reconnects and the server refuse the old token, loads the page again and makes the table again. |

`panels` logs in through the form and the others through the URL or the API, so both ways of
presenting the key are covered.

The last five routes drive the IDE as a user does and check, besides what the IDE shows, that
the gRPC calls of each step were made and ended with `OK`; the calls are listed in
REQUIREMENTS.md, "What the flows around the console add". They leave nothing in the storage
when they pass; names carry a random part, so a failed run does not break the next. On Java
the storage is `-Dstorage.path=<directory>`
(`start-java-ide.sh <port> scope --jvm-arg=-Dstorage.path=<directory>`); without it Java
stores in `storage` under its data directory. `reconnect` needs `--restart <command>`: a shell command
that stops the server and returns once a new one answers on the same port, for example

```bash
node oracle/ide/ide-test.js --url dh+psk://oxhaven@127.0.0.1:10500 --routes reconnect \
  --restart 'oracle/bin/stop-java.sh 10500 && oracle/ide/bin/start-java-ide.sh 10500'
```

## How the values are read

The IDE grid is a single `<canvas>`; its cells are not in the DOM. The test installs a hook on
`CanvasRenderingContext2D.fillText` before any script of the page runs. The hook keeps, per
canvas, the text of the frame being painted with its position; a frame starts where the grid
fills its whole canvas. Rows are the texts that share a `y`, ordered by `x`. The first row has
to be the column names, the following rows the cells. So the `panels`, `iframe` and `console`
routes compare exactly the strings the IDE painted, in the positions it painted them, and
nothing is read through a side channel.

The `jsapi` route is the side channel: it reads the same tables through the JS API and
compares the typed values (`rows` in `expected.json`) and the column types. It is there because
it needs the least of a server, so it localizes a failure: when `jsapi` passes and `iframe`
does not, the difference is what the IDE application adds to the API.

For `ide_static` the painted cells must equal `displayed` in `expected.json`. For `ide_ticking`
the test waits until it has seen two states of three rows in which `N` is consecutive, `Sq` is
`N` squared, and `N` has grown; a value that goes back, or a row where `Sq` is not `N` squared,
fails at once. The `Timestamp` column is not compared, because the IDE paints it in the
browser's time zone.

## The tables a server must offer

`expected.json` names them; `app.d/ide_tables.py` builds them on Java:

- `ide_static`: `Id` int 1..5, `Sym` String, `Price` double, five rows.
- `ide_ticking`: `Timestamp`, `N` long, `Sq` long: the last three rows of a table that gains a
  row every half second, `N` counting up and `Sq = N * N`.

Both must be listed by `ApplicationService/ListFields` under these names. `--expected` takes
another file of the same shape.

## Output

`--out` (default `oracle/ide/.out`, not committed) receives, per route, screenshots
(`<route>-static.png`, `<route>-ticking.png`, `jsapi-tables.png`, or `<route>-FAILED.png`),
`<route>.trace.json` and one `report.json`. A trace holds every static request with its
headers, the websocket handshake, and every gRPC call with its request and response messages:

```bash
oracle/.venv/bin/python oracle/ide/decode_trace.py oracle/ide/.out/panels.trace.json
oracle/.venv/bin/python oracle/ide/decode_trace.py oracle/ide/.out/panels.trace.json --static --full
```

When a route fails, the test prints the requests that failed, the gRPC calls that did not end
with `OK`, and page errors.

## Finding out what the IDE needs

```bash
# The IDE with a call answered UNIMPLEMENTED, or answered with one empty message:
node oracle/ide/ide-test.js --url $URL --deny ConsoleService/StartConsole
node oracle/ide/ide-test.js --url $URL --empty ConsoleService/GetConsoleTypes
# One run per call, with and without it:
node oracle/ide/probe-faults.js --url $URL
```

`--deny` and `--empty` take `Service/Method` or `Service/*`, separated by commas. The calls
are answered inside the browser and never reach the server.

## Java server variants

`bin/start-java-ide.sh <port> <mode>`:

- `scope` (default): the tables are variables of the Python query scope.
- `app`: the tables are fields of the application `ide_app` (`app.d/ide_app.app`), with the
  tickets `a/ide_app/f/<name>`; nothing is put in the query scope.
- `disabled`: as `scope`, with `-Ddeephaven.console.disable=true`. `ListFields` then lists no
  scope variable, so the test fails on this server: the tables have to be application fields.
- `app --jvm-arg=-Ddeephaven.console.disable=true` is the Java server nearest to an Oxhaven
  without a console: application fields only, and every console call refused.
- Further arguments go to `bin/java_ide_server.py`: `--no-tables` defines no tables (the test
  must fail), `--jvm-arg=-D...` adds a JVM argument. For https, with the development
  certificate of the reference checkout (`CN=localhost`, so the URL must say `localhost`):

  ```bash
  C=/home/user/dh-ref/server/dev-certs
  oracle/ide/bin/start-java-ide.sh 10501 scope --jvm-arg=-Dssl.identity.type=privatekey \
    --jvm-arg=-Dssl.identity.certChainPath=$C/server.chain.crt \
    --jvm-arg=-Dssl.identity.privateKeyPath=$C/server.key
  node oracle/ide/ide-test.js --url https://localhost:10501 --psk oxhaven
  ```

## Known limits

- The grid hook depends on how web-client-ui v1.33.1 paints: a full-canvas `fillRect` per
  frame and one `fillText` per cell. A cell wider than its column is painted truncated; the
  expected tables are narrow enough.
- The selectors of the `panels` and `console` routes (`Panels` button,
  `panel-list-item-<name>-button`, `#auth-psk-token-input`, `.console-input`) and those of
  `lib/flows.js` (menus, the File Explorer, modals) are those of the same IDE version.
- The routes of `lib/flows.js` were run with a Python console only.
- Only pre-shared key and anonymous authentication are driven.
