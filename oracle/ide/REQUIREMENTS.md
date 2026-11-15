# What the web IDE needs from a server

Recorded on 2026-11-14 by running `ide-test.js` against the Java reference (Deephaven 42.9 from
the `deephaven-server` wheel, which ships web-client-ui v1.33.1) in headless Chromium 153.
Everything here was observed in those runs unless a line says "source", which cites the
reference checkout, or "not determined". The decoded recordings are in [traces/](traces); each
section names the file it comes from.

| Recording | Server | Routes |
| --- | --- | --- |
| `traces/http-noconsole-*.txt` | http, `-Ddeephaven.console.disable=true`, tables as fields of an application | panels, iframe, jsapi |
| `traces/http-console-*.txt` | http, Python console, tables as query scope variables | panels, console |
| `traces/https-console-*.txt` | https (self-signed), Python console, scope variables | panels, jsapi, console |
| `traces/probe-faults.txt` | http, Python console | one run per call with that call taken away |
| `traces/http-flows-*.txt` | http, Python console, scope variables, `-Dstorage.path=<a new directory>` | notebook, grid, errors, log, reconnect |

The first server is the shape Oxhaven has at first: no console, tables that exist before a
client connects. All three routes pass against it.

## The short version

1. On plain `http://` the browser makes **no gRPC-web request at all**. Every call goes over
   one websocket of subprotocol `grpc-websockets-multiplex`, including the bidirectional
   `DoExchange`. gRPC-web over fetch and the `Open`/`Next` variants of bidirectional calls are
   used only on `https://`.
2. The IDE needs 9 methods to show a table without a console, and two of them can be trivial:
   `ConsoleService/GetConsoleTypes` answering an empty list and `TerminationNotification`
   staying open.
3. Login is `ConfigService/GetConfigurationConstants` with an `authorization` request header.
   The browser never calls Flight `Handshake`.
4. The grid does not subscribe to the table it fetched. It runs `TableService/ApplyPreviewColumns`
   on it and subscribes to the result with a Barrage viewport subscription.
5. Tables reach the IDE through `ApplicationService/ListFields`. All three routes look the
   table up there by name, including `/iframe/table/?name=` and `getObject` of the JS API.

## Static files

All of them are in one jar of the wheel, unmodified:
`deephaven_server/jars/deephaven-web-42.9.jar` (39,697,632 bytes, sha256
`23981436b4aa511728952cc08ef6f6771582afde38dafe4e691cc3c9ca5e909b`), under
`META-INF/resources/`. The jar registers itself through
`META-INF/services/io.deephaven.server.resources.ServerResource` (source:
`web/src/main/java/io/deephaven/web/WebResources.java`,
`server/src/main/java/io/deephaven/server/resources/ServerResources.java`); Jetty serves
`META-INF/resources/<path>` at `/<path>`.

| URL prefix | In the jar | Files | Used by |
| --- | --- | --- | --- |
| `/ide/` | `META-INF/resources/ide/` | `index.html`, `manifest.json`, `logo.png`, favicons, `download/serviceWorker.js`, 398 files in `assets/` (72 MB unpacked) | panels, console |
| `/iframe/table/`, `/iframe/chart/` | `META-INF/resources/iframe/{table,chart}/index.html` | one 422-byte page each, which replaces the location with `../widget/` keeping the query string | iframe |
| `/iframe/widget/` | `META-INF/resources/iframe/widget/` | `index.html` and 373 files in `assets/` (67 MB) | iframe |
| `/jsapi/` | `META-INF/resources/jsapi/` | `dh-core.js` (8.2 MB, the JS API as an ES module), `dh-internal.js`, example pages | all routes |
| `/js-plugins/manifest.json` | not in the jar: built by the server from installed JS plugins | answers `{"plugins":[]}` | panels, console, iframe |

What was requested (`traces/http-noconsole-panels.txt`, `-iframe.txt`, `-jsapi.txt`, the lines
before the websocket):

- panels, no console: `/ide/`, `/jsapi/dh-core.js`, `/js-plugins/manifest.json`,
  `/ide/logo.png` and 36 files of `/ide/assets/` (js, css, woff2). With a console: 43 files,
  among them the Monaco editor, its worker and `ruff_wasm_bg-*.wasm`.
- iframe: `/iframe/table/?name=..&psk=..`, then `/iframe/widget/?name=..&psk=..`,
  `/jsapi/dh-core.js`, `/js-plugins/manifest.json`, 24 files of `/iframe/widget/assets/` per
  page.
- jsapi: `/jsapi/dh-core.js` only (plus `/ide/manifest.json`, which the test uses as its page).
- The asset names carry a content hash and are referenced from `index.html` and from each
  other, so the set to serve is "whatever is in the jar", not a list.

How Java serves them:

- `GET /` answers `302` to `/ide/`, keeping the query string (`/?psk=abc` to `/ide/?psk=abc`);
  `GET /ide` answers `302` to `/ide/`. A directory URL serves its `index.html`. An unknown
  path answers `404`.
- Content types seen: `text/html`, `text/javascript`, `text/css`, `font/woff2`,
  `application/json`, `image/png`, `application/wasm`.
- `Cache-Control: max-age=31536000, public, immutable` under `*/assets/`;
  `no-cache, must-revalidate, pre-check=0, post-check=0` with `Pragma: no-cache` and
  `Expires: Thu, 01 Jan 1970` for everything else. Every file has a weak `ETag`, and a request
  with `If-None-Match` got `304` (the second iframe page did this for `dh-core.js`).
- `Content-Encoding: gzip` when the request accepts it (most files; small ones are sent plain).
- Every response, the websocket handshake included, carries
  `Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Embedder-Policy: require-corp`,
  `Cross-Origin-Resource-Policy: same-origin`, `Referrer-Policy: no-referrer`,
  `X-Content-Type-Options: nosniff`, `X-XSS-Protection: 1; mode=block`,
  `Vary: Accept-Encoding, Origin`.
- With `/js-plugins/manifest.json`, `/ide/manifest.json` or `/ide/logo.png` answered `404`,
  the IDE still loaded and listed the tables (one run each, done by hand with a Playwright
  route; not part of the committed tools).

## Transport

| Page | Static files | gRPC | Bidirectional calls |
| --- | --- | --- | --- |
| `http://` | HTTP/1.1 | one websocket, subprotocol `grpc-websockets-multiplex`, every call a stream on it | real streams: `FlightService/DoExchange`, `ConsoleService/AutoCompleteStream` |
| `https://` | HTTP/2 (`h2`) | gRPC-web: one `POST` per call over HTTP/2, `content-type: application/grpc-web+proto` | split: `BrowserFlightService/OpenDoExchange` + `NextDoExchange`, `ConsoleService/OpenAutoCompleteStream` + `NextAutoCompleteStream` |

The JS API chooses by the scheme of the server URL (source:
`web/client-api/.../ConnectOptions.java`: fetch for https, websockets otherwise, unless the
caller forces one). The IDE does not force one. TLS is therefore not needed, and without it
gRPC-web is not used.

### The websocket (`http://`)

`traces/http-noconsole-panels.txt`, the `WEBSOCKET` line; framing from the frames themselves
and from source (`grpc-java/grpc-servlet-websocket-jakarta/.../MultiplexedWebSocketServerStream.java`,
`web/client-api/.../grpc/MultiplexedWebsocketTransport.java`).

- The browser opens **one** websocket per page, to the path of the first call it makes:
  `ws://host:port/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants`
  (`.../GetConfigurationConstants` in the jsapi route). Java accepts the upgrade at
  `/{service}/{method}`. All later calls of the page use this socket, whatever their path.
- Request: HTTP/1.1 `Upgrade: websocket`, `Sec-WebSocket-Protocol: grpc-websockets-multiplex,
  grpc-websockets`, `Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits`,
  `Origin` of the page. No `authorization` header and no cookie that matters: authentication
  is per stream.
- Response: `101`, `Sec-WebSocket-Protocol: grpc-websockets-multiplex`. Java also accepted
  `permessage-deflate`. The client refuses to continue if the chosen subprotocol is anything
  but `grpc-websockets-multiplex` (source: `MultiplexedWebsocketTransport#onOpen`).
- Every websocket message is binary and starts with a big-endian int32 stream id. The client
  numbers streams 0, 1, 2, ... per page. The sign bit set means "last message of this stream
  from this side".
- Client to server, per stream:
  1. `id` + the request headers as text, `name: value\r\n` each, among them
     `grpc-websockets-path: <package.Service>/<Method>`, `content-type:
     application/grpc-web+proto`, `x-grpc-web: 1` and `authorization`.
  2. `id` + one byte `0x00` + a piece of the gRPC message stream (1 byte flags, 4 bytes
     big-endian length, payload). The client sends the 5-byte prefix and the payload as two
     websocket messages; an empty request message is the prefix alone.
  3. `id | 0x80000000` + one byte `0x01`: half close.
- Server to client, per stream:
  1. `id` + a frame with flag `0x80` holding the response headers as text
     (`authorization: Bearer ..`, `grpc-encoding: identity`, `grpc-accept-encoding: gzip`).
  2. `id` + a data frame (`0x00`, length, payload) per response message.
  3. `id | 0x80000000` + a frame with flag `0x80` holding the trailers (`grpc-status: 0`).
  A call that fails before any message is one message: `id | 0x80000000` + a `0x80` frame
  with `grpc-status` and `grpc-message` (seen for `UNAUTHENTICATED`).
- A stream with id `0x7fffffff` is the transport's GO_AWAY (source); not seen in a run.

### gRPC-web (`https://`)

`traces/https-console-panels.txt`.

- `POST /<package.Service>/<Method>` over HTTP/2 with `content-type:
  application/grpc-web+proto`, `x-grpc-web: 1`, `authorization`; same origin, so no CORS
  preflight. Response `200`, `content-type: application/grpc-web`, messages as gRPC frames in
  the body, the trailers as a last frame with flag `0x80`.
- A server-streaming call is one long response. `TerminationNotification` shows no response
  headers at all while it is pending.
- Bidirectional calls: the first message is `OpenDoExchange` (server streaming) with headers
  `x-deephaven-stream-ticket: <n>` and `x-deephaven-stream-sequence: 0`; each further message
  is a unary `NextDoExchange` with the same ticket and the next sequence number, answered
  `BrowserNextResponse{}`. Half close is a `Next` call with `x-deephaven-stream-halfclose`
  (source: `web/client-api/.../WorkerConnection.java#bidiStream`,
  `server/.../browserstreaming/BrowserStreamInterceptor.java`).
- Java refuses gRPC over HTTP/1.1 (`grpc-status: 13`) unless `http.requireHttp2=false`
  (source: `server/jetty/.../GrpcFilter.java`); the browser was never in that position.

## Authentication and headers

`traces/http-noconsole-panels.txt`, streams 0, 1 and 4.

1. `ConfigService/GetAuthenticationConstants`, no `authorization`. Response:
   `AuthHandlers = io.deephaven.authentication.psk.PskAuthenticationHandler`. The IDE picks
   its login screen from this (the key form; with
   `io.deephaven.auth.AnonymousAuthenticationHandler` it logs in by itself).
2. `ConfigService/GetConfigurationConstants` with
   `authorization: io.deephaven.authentication.psk.PskAuthenticationHandler <key>` (no
   `Bearer`), or `authorization: Anonymous`. The response headers carry
   `authorization: Bearer <uuid>`. A wrong key: `grpc-status: 16`,
   `grpc-message: Authentication details invalid`, and the IDE stays on the form.
3. Every later call sends `authorization: Bearer <uuid>`, and every response repeats the
   header. The token did not change within a run (18 calls).
4. The client calls `GetConfigurationConstants` again at half of `http.session.durationMs`
   (300000 in the response) to keep the session (source: `WorkerConnection#authUpdate`); no run
   lasted that long.

The key reaches the IDE as `?psk=<key>` in the URL or through the form; the IDE then keeps it
in the cookie `io.deephaven.web.client.auth.psk.token`. The server does not read the cookie.

`GetConfigurationConstants` answered: `barrage.version=1.0.0`, `deephaven.version=42.9`,
`file.separator=/`, `groovy.version`, `java.version`, `python.version`,
`http.session.durationMs=300000`, `web.flattenViewports=false`,
`web.storage.layout.directory=/layouts`, `web.storage.notebook.directory=/notebooks`,
`web.webgl=true`, `web.webgl.editable=true`. Which of them the IDE cannot do without is not
determined.

## The calls, in order

Route `panels` against the server without a console (`traces/http-noconsole-panels.txt`).
"Without it" is from `traces/probe-faults.txt`: the call answered `UNIMPLEMENTED` in the
browser, console types answered empty, three routes run.

| # | Call | Kind | Request and response | Without it |
| --- | --- | --- | --- | --- |
| 1 | `ConfigService/GetAuthenticationConstants` | unary | see above | IDE and iframe fail (no login screen). The jsapi route does not call it. |
| 2 | `ConfigService/GetConfigurationConstants` | unary | the login; called a second time right after | everything fails |
| 3 | `SessionService/TerminationNotification` | unary, never answered | empty request; stays pending for the life of the page | the client reconnects in a loop (over 300 calls in 12 s) and routes fail or pass by chance. Answered with a message, the IDE shows "Server shutdown". It has to stay open. |
| 4 | `TableService/ExportedTableUpdates` | server stream | one `{export_id, size}` per exported table, and again when a ticking table's size changes | all routes pass |
| 5 | `ConsoleService/GetConsoleTypes` | unary | `GetConsoleTypesResponse{}`: no console types | the IDE shows an error page instead of the dashboard. With the empty answer it starts no console and calls nothing else of the console service. iframe and jsapi do not call it. |
| 6 | `StorageService/ListItems` | unary | `{path: "/layouts", filter_glob: "*.json"}` answered `{canonical_path: "/layouts"}` with no items | all routes pass |
| 7 | `ApplicationService/ListFields` | server stream, stays open | first message lists every field: `created { typed_ticket { ticket: "a/ide_app/f/ide_static", type: "Table" }, field_name: "ide_static", application_id: "ide_app", application_name: ... }` | everything fails: the Panels list is empty, and iframe and `getObject` find no table of that name |
| 8 | `TableService/FetchTable` | unary | `{source_id: <ticket from 7>, result_id: export 1}` answered `ExportedTableCreationResponse{success, is_static, size, schema_header}` | everything fails |
| 9 | `TableService/ApplyPreviewColumns` | unary | `{source_id: export 1, result_id: export 2, convert_arrays: true, unpreviewed_types: [23 type names]}`, same kind of answer | everything fails (the grid stays empty, the API's viewport never arrives) |
| 10 | `FlightService/DoExchange` | bidirectional | a Barrage viewport subscription to export 2, see below | everything fails |

Steps 8 to 10 repeat for the second table with exports 3 and 4. The IDE called nothing else.
Not called in any route: Flight `Handshake`, `TableService/Batch`, `DoGet`, `DoPut`,
`ObjectService/FetchObject`, `SessionService/ExportNotifications`, `NewSession`,
`RefreshSessionToken`, and no table operation other than the two above. Answering `Batch`,
`DoGet`, `FetchObject` and `Release` with `UNIMPLEMENTED` changed nothing.

Differences in the other routes:

- **iframe** (`traces/http-noconsole-iframe.txt`): the same ten calls per page, in the same
  order, except that `ListItems` is sent with an empty path and no `GetConsoleTypes` is sent.
  Each of the two pages is a new websocket and a new login. The table is found by matching
  `name` against `field_name` of `ListFields`.
- **jsapi** (`traces/http-noconsole-jsapi.txt`): `GetConfigurationConstants` (the login),
  `TerminationNotification`, `ExportedTableUpdates`, then per `getObject` a `ListFields`,
  `FetchTable`, `ApplyPreviewColumns` and `DoExchange`, and `SessionService/Release` of the
  four exports when the tables are closed. `ApplyPreviewColumns` comes from the JS API
  itself (`setViewport`), not from the IDE.

The tickets in `ListFields` decide what `FetchTable` has to resolve. Java names an
application's field `a/<application id>/f/<field name>` and a variable of the query scope
`s/<name>` with `application_id: "scope"` and `field_description: "query scope variable"`;
the IDE lists both alike. With `-Ddeephaven.console.disable=true` Java does not list scope
variables at all (source: `ApplicationServiceGrpcImpl#onScopeChanges`), only application
fields. A Python application script's own variables are scope variables; `app.d/ide_app.py`
sets fields explicitly for that reason.

## The Barrage subscription of the grid

`traces/http-noconsole-panels.txt`, streams 10 and 13.

The request is one `FlightData` whose only field is `app_metadata`: a `BarrageMessageWrapper`
with magic `0x6E687064`, type `BarrageSubscriptionRequest` and:

| Field | Value |
| --- | --- |
| `ticket` | the export of `ApplyPreviewColumns`, `e` + int32 little-endian |
| `columns` | a bitset of all columns (`[0, 1, 2]`) |
| `viewport` | rows `0-88` in the IDE at 1400x900 (`0-50` with the console above the grid, `0-94` in the iframe); whatever was passed to `setViewport` through the API |
| `reverse_viewport` | false |
| `subscription_options` | `use_deephaven_nulls=true`, `min_update_interval_ms=0`, `batch_size=100000`, `max_message_size=10000000`, `columns_as_list=false`, `preview_list_length_limit=0` |

No flight descriptor and no `data_header` are sent. The stream stays open.

The responses, each a `FlightData`:

1. The Arrow schema message, fields with their `deephaven:type` metadata (`int`,
   `java.lang.String`, `double`, `java.time.Instant`, `long`).
2. The snapshot: a record batch whose `app_metadata` is a `BarrageUpdateMetadata` with
   `is_snapshot=true`, `effective_viewport` equal to the requested one, `effective_column_set`
   all columns, `added_rows={0-4}`, `table_size=5`, and for a static table
   `first_seq=last_seq=-1`.
3. For the ticking table, one message per update cycle that changed the viewport:
   `is_snapshot=false`, no effective viewport or column set, `added_rows={1-2}`,
   `removed_rows={0-1}`, a record batch of 2 rows, `table_size=3`, and consecutive
   `first_seq=last_seq` (this is `tail(3)` of a table that gains two rows a second, as Java
   reports it).

A viewport change is a second `BarrageSubscriptionRequest` on the same stream with the new
row range; seen once, when the second panel resized the first
(`traces/http-console-console.txt`; `NextDoExchange` over https). Scrolling, sorting and
filtering are under "Sorting, filtering and scrolling a grid" below.

## What the console route adds

`traces/http-console-console.txt`, `traces/https-console-console.txt`.

- `GetConsoleTypes` answers `python`. The IDE then calls `ConsoleService/StartConsole`
  (`{result_id: export 1, session_type: "python"}`), `SubscribeToLogs` (server stream; Java
  replays its whole log), `GetHeapInfo` (repeated on a timer), and `AutoCompleteStream`
  (bidirectional; one message per keystroke, 382 for the two commands of the test).
- A command is `ConsoleService/ExecuteCommand{console_id, code}`. The answer lists what
  changed: `changes { created { typed_ticket { ticket: "s/ide_console_static", type: "Table" },
  field_name, application_id: "scope" } }`. The IDE opens a panel for each created table by
  itself and continues with steps 8 to 10 on the `s/` ticket.
- So the console route needs everything above, the console service with a language behind
  it, scope tickets (`s/<name>`), and whatever engine operations the typed code uses. Its
  static files include the Monaco editor, its web worker and, for Python, a wasm linter.
- With `-Ddeephaven.console.disable=true` the console route fails ("no console input"), as it
  should.

## What the flows around the console add

Recorded on 2026-11-15 with the routes `notebook`, `grid`, `errors`, `log` and `reconnect`
against the Java reference on http with a Python console and a storage directory of its own
(`-Dstorage.path=<directory>`): `traces/http-flows-<route>.txt`. Each route starts with the
eleven calls of the console route's start, and `ConsoleService/GetHeapInfo` and
`AutoCompleteStream` go on underneath; they are left out below. What each step shows is what
the route checks.

### The File Explorer and a notebook (`notebook`)

The File Explorer lists `web.storage.notebook.directory` (`/notebooks`) and nothing above
it. Every change is followed by `ListItems{path: "/notebooks"}`, usually three of them.

| Step in the IDE | Calls |
| --- | --- |
| Open the File Explorer tab | `ListItems{path: "/notebooks"}` |
| New folder, a name, Create | `ListItems{path: "/notebooks/", filter_glob: "<name>"}` (does it exist?), `CreateDirectory{path: "/notebooks/<name>"}` |
| New notebook, type | none: the notebook is in the browser until it is saved |
| Run Selected (one line selected) | `ExecuteCommand{code: <the selected text>}`, then `FetchTable`, `ApplyPreviewColumns`, `DoExchange` for the table it made |
| Run | `ExecuteCommand{code: <the whole text>}`; a variable that existed is `updated` in `changes`, and the IDE releases the exports of its open panel and fetches it again |
| Save (Ctrl+S), a name, Save | `ListItems{path: "/notebooks/", filter_glob: "<file name>"}`, `SaveFile{allow_overwrite: true, path: "/notebooks/<file>", contents}` answered with an `etag` |
| Close the tab, double-click the file | `FetchFile{path}` with no etag, answered `{contents, etag}` |
| Edit, Save | `SaveFile{allow_overwrite: true, ...}` |
| Rename (context menu) | `ListItems{path: "/notebooks/", filter_glob: "<new name>"}`, `MoveItem{old_path, new_path}` without `allow_overwrite` |
| Drag the file onto a folder | `MoveItem{old_path: "/notebooks/<file>", new_path: "/notebooks/<folder>/<file>"}` |
| Open the folder | `ListItems{path: "/notebooks/<folder>"}` |
| Delete the file, confirm | `DeleteItem{path: "/notebooks/<folder>/<file>"}` |
| Delete the folder, confirm | `DeleteItem{path: "/notebooks/<folder>/"}`, with the trailing `/` |

Two things a server has to get right that the calls do not say: the name of a new file or
folder is sent as a **glob** (`filter_glob`), so a name with `*`, `?`, `[` or `{` in it is a
pattern there; and the `etag` of `ListItems`, `FetchFile` and `SaveFile` has to be the same
function of the contents in all three (Java: Murmur3 128 with seed 0, in hexadecimal). The
IDE sent no `FetchFileRequest.etag` in these runs. `/layouts` is only listed
(`filter_glob: "*.json"`); when it holds a `.json` file the IDE fetches it at start (seen
once, by hand).

What Java answers to each storage call, with the paths that try to leave the storage, is
recorded call by call in `crates/oxhaven-server/src/storage/fixtures/java.txt`.

### Sorting, filtering and scrolling a grid (`grid`)

A table of 300 rows made in the console; the IDE's first viewport is rows `0-50`.

| Step in the IDE | Calls |
| --- | --- |
| Scroll to the end, and back | no new call: a further `BarrageSubscriptionRequest` on the open `DoExchange` per viewport (`252-327`, then `0-50`), each answered with a snapshot of the new viewport |
| Header menu, Sort by, `K` Descending | `Batch{ops: [sort{source_id: <the fetched table>, sorts: [{column_name: "K", direction: DESCENDING}]}]}`, `Release` of the preview export, `ApplyPreviewColumns` of the sorted table, a new `DoExchange` |
| Cell menu (right click) | `ApplyPreviewColumns` and a `DoExchange` with a `BarrageSnapshotRequest` for that one cell (`viewport={0}`, one column), which ends with `OK` |
| Filter by Value, is equal to | `Batch{ops: [filter{source_id: <the fetched table>, filters: [compare{EQUALS, reference "G", literal double 5.0}]}, sort{source_id: <the filter's result>, ...}]}`: one answer per operation; then `Release`, `ApplyPreviewColumns`, `Release`, a new `DoExchange` |

So the grid always rebuilds from the table it fetched: filters first, then sorts, in one
`Batch`, with the operations referring to each other by export ticket. The literal of a
filter on an `int` column is sent as a double. Java's schema of a sorted table carries
`deephaven:attribute.SortedColumns`; the IDE showed the same without it.

### A command that fails, a variable that goes away (`errors`)

- `ExecuteCommand` of a command that raises ends with `OK` and `error_message` set, and
  carries no `changes`. The IDE does not show `error_message` in the console. What it shows
  is a record of the **log stream**: the server logs "Error running script: " and the error
  at level `ERROR`, and the console shows that record's first line, which opens to the rest.
  A server that does not log the failure, or whose `SubscribeToLogs` does not deliver it,
  shows nothing for a failed command. (The oracle's Java also shows every log line again as
  `STDERR`, which is its logging configuration.)
- A variable assigned before the error is in the scope afterwards.
- Removing a variable (`del name`) answers `changes { removed { ... } }`. The IDE closes the
  variable's panel, disables the variable's button in the console, and sends
  `SessionService/Release` for the panel's two exports. It makes no other call.

### The Log panel and Command History (`log`)

Neither makes a call. The Log panel shows the stream of `SubscribeToLogs` that the console
opened at start, one line per record: time, level, text; what a script prints is a record
of level `STDOUT`. Command History is kept by the browser (it lists the commands of this
browser profile, not of the server), and "Open selected commands in the console" puts the
text into the console input; running it is an ordinary `ExecuteCommand`.

### The server restarts (`reconnect`)

- When the websocket closes the IDE shows "Attempting to reconnect..." over the dashboard
  and each table panel shows "Table disconnected". It opens a new websocket (to
  `/ConfigService/GetConfigurationConstants`) with growing pauses, six attempts in three
  seconds in one run.
- Once a server answers, it calls `GetConfigurationConstants` with the **bearer token of the
  old session**. Java, restarted, does not know the token and answers `UNAUTHENTICATED`
  (seen with the message "Authentication details invalid" and with none). The IDE then
  stays on "Attempting to reconnect..." and repeats the call; it does not go back to the
  login form (watched for 20 seconds).
- Loading the page again logs in with the key in the IDE's cookie and starts a console as
  on a first visit. The layout is the browser's, so the panel of the table that no longer
  exists is opened again: `FetchTable` of `s/<name>` answers `NOT_FOUND`
  ("variable '<name>' not found"), and the panel fills once a command makes the variable
  again.
- Not determined: what the IDE does when the server it reconnects to still knows the
  session (a network interruption, not a restart). No run did that.

## What Oxhaven needs, in order

For routes (b) `panels` and (c) `iframe`, `jsapi`, on plain http, with no console. Each item
is useless without the ones before it; the test's `jsapi` route passes at item 9 and the
other two need all ten.

1. **An HTTP/1.1 listener for static files** on the server's port: the contents of
   `deephaven-web-42.9.jar` under `/ide/`, `/iframe/`, `/jsapi/`, `index.html` for directory
   URLs, `302` from `/` to `/ide/`. `/js-plugins/manifest.json` answering `{"plugins":[]}` is
   optional. No task in the ledger covers this.
2. **The multiplexed websocket transport**: upgrade at `/{service}/{method}`, subprotocol
   `grpc-websockets-multiplex`, the framing above, each stream dispatched to the same gRPC
   services as an HTTP/2 call, with long-lived server streams and true bidirectional
   streams. No task covers this; task 2.6 names gRPC-web and the `Open`/`Next` calls, which
   the browser uses only over https.
3. **`ConfigService`**: `GetAuthenticationConstants` (`AuthHandlers`) and
   `GetConfigurationConstants`, with a session created from the `authorization` header of the
   call and `authorization: Bearer <token>` in the response headers (task 2.6; the session
   store of 2.1 already accepts this header form).
4. **`SessionService/TerminationNotification`** that stays pending, and `Release` (2.1).
5. **`ConsoleService/GetConsoleTypes`** answering an empty list. Needed by the IDE only.
6. **`ApplicationService/ListFields`**: one message with a `created` entry per table, then
   the stream stays open (6.13). Any ticket form works as long as item 7 resolves it; Java's
   are `a/<app>/f/<name>` and `s/<name>`.
7. **`TableService/FetchTable`** from such a ticket into an export, answering with the size,
   `is_static` and the schema with `deephaven:type` metadata (2.2), and the tables themselves:
   a static one and one that ticks.
8. **`TableService/ApplyPreviewColumns`** (4.10). For the column types of the test tables
   Java's result has the same schema as its source.
9. **`FlightService/DoExchange`** with a Barrage viewport subscription: schema, snapshot,
   then deltas (2.3, 2.4).
10. Optional, and tolerated as `UNIMPLEMENTED`: `TableService/ExportedTableUpdates`,
    `StorageService/ListItems`, `TableService/Batch`.

Over https instead, item 2 is replaced by TLS, HTTP/2, gRPC-web and
`BrowserFlightService/OpenDoExchange` with `NextDoExchange` (2.6).

The console route adds: `ConsoleService` (`StartConsole`, `ExecuteCommand` with its field
changes, `SubscribeToLogs`, `GetHeapInfo`, `AutoCompleteStream` or its `Open`/`Next` pair), a
script language, `s/<name>` tickets for its variables, `ListFields` updates when variables
change, and the engine operations the scripts call (6.1 to 6.7).

## Not determined

- Whether the IDE needs any of the security headers (`Cross-Origin-*`, `nosniff`), the
  `ETag`/`Cache-Control` headers or gzip. Removing them through a Playwright route also turns
  the page into one that Chromium does not let connect to a loopback server, so the one
  attempt says nothing.
- Which `GetConfigurationConstants` values the IDE requires. `--empty` cannot tell: its answer
  has no `authorization` response header, so the login fails before the values matter.
- Whether a server may decline `permessage-deflate`, and frame size limits on the websocket.
  Java accepted the extension; nothing larger than a few kilobytes was sent.
- Token refresh at half the session duration and GO_AWAY on the websocket: read in source
  only. Reconnection was run only against a server that had restarted.
- What a user does in the grid beyond scrolling, one sort and one "Filter by Value": the
  other filter kinds, quick filters, column statistics, copy, downloads, roll-ups. These will
  call table operations that no run made.
- The File Explorer beyond one folder and one notebook: copying a file, moving a folder,
  names that are glob patterns, a file changed on the server while it is open.
- A Java server with no console type at all (`-Ddeephaven.console.type=none`): the wheel's
  embedded server cannot start that way. `-Ddeephaven.console.disable=true` and
  `--empty ConsoleService/GetConsoleTypes` are what was run instead.
- The Groovy console: the wheel is Python only, so the Groovy commands in `expected.json` were
  never run.
- The requests headless Chromium does not make or that the recorder does not see: favicons,
  the download service worker (`/ide/download/serviceWorker.js`), the response of the Monaco
  web worker script.
- Browsers other than Chromium.
