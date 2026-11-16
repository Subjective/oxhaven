# Objects and plugins

How Oxhaven hands a client a server-side object that is not a table, and how a Python package
adds a kind of object. Written by task 7.10 (`tasks/7.10-object-plugins.md`).

What Java does was read in `ObjectServiceGrpcImpl`, `ObjectTypes`, `TypeLookup`,
`ObjectTypeBase`, the Python bridge (`ObjectTypeAdapter`, `ExporterAdapter`, `CallbackAdapter`,
`PythonPluginsRegistration`) and the `deephaven-plugin` package (0.8.0), and observed on Java
42.9: the `deephaven-server` wheel with a plugin package of our own installed beside it,
driven with plain gRPC calls. Where this document says "Java", it means that observation. The
cases that are kept are the tests of `crates/oxhaven-server/src/object/tests.rs` (the service,
with a type written in Rust) and `crates/oxhaven-server/src/console/python/plugin_tests.rs`
(the same through a Python plugin).

## Decision

1. **An object type is a trait object in one registry of the process.**
   `object::types::ObjectType` has a name, says whether a value is of the type (`is_type`),
   and opens a value for a client (`connect`). `object::types::registry()` holds the types in
   the order they were registered; the type of a value is the first that takes it, as in Java
   (`ObjectTypes#findObjectType`). There is one registry per process because there is one
   Python interpreter per process, and Java's is a singleton too. `ObjectTypes::new()` makes
   an empty one for tests.
2. **Every type is bidirectional underneath**, as in Java (`ObjectTypeBase`): `connect` is
   given a `MessageStream` to the client and returns the `MessageStream` that takes the
   client's messages. A type whose objects are only fetched sends one message and closes
   (`send_once`). `FetchObject` is a connection that must send and close before `connect`
   returns; `MessageStream` is one that may stay open.
3. **The service owns the stream, the type owns the bytes.** `object::mod` checks the
   requests and keeps the messages of a stream in order; `object::connection` turns what a
   type sends into responses, exporting each reference as a new server-side export of the
   session with its type name. A type never sees a ticket or a session.
4. **`deephaven.plugin` is part of the server's Python package**, written against the names
   and signatures of `deephaven-plugin` 0.8.0 and none of its code. The embedded importer
   comes first, so a `deephaven-plugin` distribution in the environment is not used.
   Registration classes are found through the entry point group `deephaven.plugin`, name
   `registration_cls`, with `importlib.metadata`, as Java's Python server finds them.
5. **A Python object type is an `ObjectType` whose methods take the interpreter lock**
   (`console::python::plugin::PythonObjectType`), registered through the native function
   `_oxhaven.register_object_type`. Nothing in the plugin API touches a jpy type that a plugin
   can observe, except the payload of `on_data` (below).
6. **The plugins are registered once per process, when the first Python session starts,
   before the server accepts clients.** A registration that raises keeps the server from
   starting, as in Java.

## The registry

| Rule | Java | Here |
| --- | --- | --- |
| Reserved names: `Table`, `TreeTable`, `RollupTable`, the empty name, in any case | `Unable to register type, name is reserved: <name>` | the same |
| A Java keyword in any case (`class`, `Int`, `TRUE`, `null`, `_`; not `var`, `record`) | `Unable to register type, name is keyword: <name>` | the same, from the list of `SourceVersion#isKeyword` |
| A name that is taken, in any case | `Unable to register type, type name already registered: <name>` | the same |
| Any other name, with dots or spaces | registered | registered |

A plugin sees these as `RuntimeError: java.lang.IllegalArgumentException: <text>`.

Java registers `PartitionedTable`, `HierarchicalTable` and `Figure` before any plugin. Here
the three are registered in that order (`JAVA_TYPES_NOT_PORTED_LOWERCASE` is the list of
those that are not, and is empty).

The lower case of a name is Rust's `to_lowercase`, Java's is `toLowerCase(Locale.ENGLISH)`.
They agree on ASCII.

## What a client is told

`TypeLookup#type`: `Table` for a table, the name of the first object type that takes the
value, and nothing otherwise. That one function (`ObjectTypes::type_name`) names the type of a
console variable (`ListFields`, the changes of `ExecuteCommand`), of an application field and
of a reference in a message.

Observed of the variables of the console, and reproduced:

- A variable whose value is of a type is listed with the type and the ticket `s/<name>`.
  Assigning a value of the same type updates it; of another type removes it and creates it;
  a value of no type removes it. An object of a subclass is of the type its class is of.
- A variable set by a thread of the script is listed.
- A pandas `DataFrame` is of the type `pandas.DataFrame`, whose fetch is no bytes and a
  reference to the table made from the frame (`deephaven.pandasplugin`).

### A type that fails

A plugin's `is_type` may raise, or return `None`, which Java cannot make a `boolean` of.
Either fails whatever asked. Asked by a fetch, the call fails with `Details Logged`. Asked by
the look at the variables after a command, **the `ExecuteCommand` call fails with `Details
Logged` although the code ran**, the changes are never told to anyone, the next command
starts from the variables as they are, and removing the variable fails the same way, because
the type is asked about the old value. Oxhaven reproduces this (`Changes::failure`). A
truthy answer that is no `bool` counts as yes.

## FetchObject

| Request | Answer |
| --- | --- |
| an object of the named type that is only fetched | the type, the bytes, and the references: each a ticket of a new export with a negative id, and its type name |
| the same again | new exports |
| a type that talks, and sends one message and closes before it returns | that message |
| sends several messages and closes | the last message; the exports of the others stay with the session |
| sends and does not close | `INVALID_ARGUMENT`, `Plugin didn't close response, use MessageStream instead for this object` |
| sends nothing | `INVALID_ARGUMENT`, `Plugin didn't send a response before returning from clientConnection()` |
| the plugin raises, or `to_bytes` returns a `str`, `None` or a number | `INVALID_ARGUMENT`, `Details Logged w/ID '<id>'` |
| `to_bytes` returns a `bytearray` or a `memoryview` | its bytes |
| the object is of another type | `FAILED_PRECONDITION`, `Unexpected ObjectType, expected type 'X', actual type 'Y'` |
| the value is of no type (a table too) | `NOT_FOUND`, `No ObjectType found, expected type 'X'` |
| no type, no ticket | `INVALID_ARGUMENT`, `No type supplied`, `No ticket supplied` |

The stream a talking type returned for a fetch is never told that the fetch is over.

A reference is exported whatever it is: a table as `Table`, an object of a type under the
type's name, anything else with an empty type name.

## MessageStream

The first message must be a connect request; each later one is data with references. The
messages of a stream are handled one after the other in their order
(`SendMessageObserver`): the work of a message waits for the exports it names and for the
message before it.

How a stream ends, and whether the object's stream is told (`on_close`):

| The end | The client sees | The object is told |
| --- | --- | --- |
| the client closes its side | the end of the stream, after the answers to its messages | yes |
| the client cancels the call | | yes |
| the object closes the stream | the end of the stream; later messages of the client go nowhere | no |
| the object raises on a message, or a reference of the message fails to resolve | the status (`Details Logged`, or the resolver's) | yes |
| the object raises when it is opened | `Details Logged` | no: there is no stream |
| a message is refused: none, data before connect, a second connect | the status | no |
| the session ends | `CANCELLED`, `Session closed` | no |
| a message waits for an export that never comes, and the client leaves | | not until the session ends |

A type that is only fetched sends its message and the stream ends at once, though the client
has not closed its side; this holds for `PartitionedTable` too. (The first slice of 7.6 kept
that stream open. Its test asserted so; the assertion was changed to what Java does.)

What the object sends after the end is refused, with Java's texts: `RuntimeError:
io.deephaven.plugin.type.ObjectCommunicationException: java.lang.IllegalStateException: Stream
is already completed, no further calls are allowed` after the stream completed, and
`...ObjectCommunicationException: io.grpc.StatusRuntimeException: CANCELLED: call already
cancelled. Use ServerCallStreamObserver.setOnCancelHandler() to disable this exception` after
the client cancelled. Java's text goes on with a stack trace, which is not reproduced.

`OpenMessageStream` and `NextMessageStream` are the same stream for a browser, with the
stream headers of `console::browser`. What a stream refuses is the status of the open call.
When the server ended the stream, the client's side stays an export until the client ends it
or the session does: a later `Next` call is answered and its message dropped, as in Java.
(`AutoCompleteStream` answers such a call with `CANCELLED`; that is the console's and was
left alone.)

## The Python side

| Module | Holds |
| --- | --- |
| `deephaven.plugin` | `Plugin`, `Callback`, `Registration`, the discovery functions, `__version__` |
| `deephaven.plugin.object_type` | `ObjectType`, `FetchOnlyObjectType`, `BidirectionalObjectType`, `MessageStream`, `Exporter`, `Reference` |
| `deephaven.plugin.js` | `JsPlugin`, so that a package that has one imports |
| `deephaven.plugin_authorization` | `transform`, the identity, as in Deephaven Community |
| `deephaven.pandasplugin` | the type `pandas.DataFrame` |
| `deephaven_internal.plugin` | what the server calls: `initialize_all_and_register_into`, the adapters |

- `Callback.register` takes a plugin or a plugin class, which it creates. Anything that is no
  object type and no JS plugin raises `NotImplementedError: Unexpected type: <class ...>`.
- `Exporter.reference(obj)` returns a `Reference` whose index is the place of the object among
  the references of the message. `allow_unknown_type=False` and `force_new=False` are refused
  with Java's texts.
- The connection a plugin is handed is a `ClientResponseStreamAdapter`, as in Java. Its
  `on_data` takes `bytes`, a `bytearray`, a `memoryview` or a sequence of small integers, and
  refuses a `str` with `TypeError: 'str' object cannot be interpreted as an integer`.
- A reference of the client arrives as the package's `Table` for a table and as the object
  itself for a Python object.
- `authorization_export_behavior` may answer `transform`, `manual` or `unset` in any case, or
  `None`; anything else fails the client's call. The three behave alike: this server
  authorizes a session and nothing per object.

### Locks

A call into a plugin (`is_type`, `to_bytes`, `create_client_connection`, `on_data`,
`on_close`) holds the interpreter lock and no other. Java was observed to hold neither side
of the update graph's lock in `to_bytes`, `on_data` and `on_close`. A table operation that the
plugin calls takes the graph's lock as for any Python thread, after releasing the interpreter
lock, which keeps the order of `docs/CONSOLE.md`: graph lock first. The two directions out of
Python, a message to the client and the registration of a type, release the interpreter lock
before they touch the server. The registry's own lock is not held while a type is asked.

## Differences from Java

| What | Java | Here |
| --- | --- | --- |
| The payload a plugin's `on_data` is given | a jpy `byte[]` | `bytes` |
| A `PartitionedTable` reference of the client, as the plugin gets it | the raw Java object | the package's `PartitionedTable` |
| The pandas plugin | an entry point of the `deephaven-server` distribution; imports pandas | registered by the server itself; looks for pandas among the loaded modules, so the server runs without pandas |
| The text of an error a plugin is raised | ends with a Java stack trace | the first line |
| A second session of one process | a JVM has one | the plugins stay registered; they are not looked for again |

## Refused

| What | Java method | Here |
| --- | --- | --- |
| A JS plugin | `io.deephaven.server.plugin.python.CallbackAdapter#registerJsPlugin` | left out with a warning in the server's log that names the method; the object types of its package are registered, and `/js-plugins/manifest.json` stays empty |
| A reference to `None` | `io.deephaven.server.session.SessionState#newServerSideExport` of null, which makes an export that fails when a client uses it | the message that refers to it fails, with a `RuntimeError` that names the method |

## For the owner

Java behaviours that look like hazards, each reproduced as observed and not decided here:

- An object is not told that its stream is over when it closed the stream itself, when the
  session ends, and when the service refuses a message.
- A message that waits for an export that never comes holds its stream after the client has
  left.
- `FetchObject` of a talking type never closes the stream the type returned, and the exports
  of every message but the last stay with the session.
- A type whose `is_type` fails makes every command fail that creates, changes or removes a
  value it fails on.
- After the server ended a browser's stream, the client's side stays an export.

## The figure (7.11)

`Figure` is registered in `registry()` after `HierarchicalTable`, fetch-only, as described
here before it existed; `docs/FIGURES.md` is its design.

## Outside the task's paths

- `console/session.rs`, `console/mod.rs`, `app/fields.rs`: the type name of a value comes
  from the registry and can fail.
- `console/browser.rs`: the two checks of a browser call that the console had, as functions
  both services use.
- `console/python/mod.rs`, `console/python/plugin.rs`: the native side of the plugins.
- `docs/PARTITIONED.md`, `docs/CONSOLE.md`: the sentences this task made untrue.
