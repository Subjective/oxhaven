"""Client smoke checks: what a real client does in one area of the protocol, against one server.

No Java counterpart: these are client scripts. Each area is a list of named checks that use
pydeephaven (and pyarrow.flight or plain gRPC where the area is about the wire), written
against the behavior observed on the Java reference server. A check fails by raising; the area
runs all its checks and the command exits non-zero if any failed.

    session     authentication, configuration, token refresh, export tickets, shared tickets,
                isolation between sessions, closing
    tables      the table service through pydeephaven: creation, single operations, a batch,
                schemas, sizes, a rejected operation
    flight      Arrow Flight with a plain pyarrow.flight client: header authentication, DoPut,
                DoGet, GetSchema, GetFlightInfo, ListFlights, all column types
    barrage     a Barrage subscription (pydeephaven-ticking): the client-side replica follows
                adds, modifies, deletes and row shifts and equals a snapshot after each change
    inputtable  append-only and keyed input tables: add, replace, delete, rejected changes
    console     the Python console through pydeephaven: run_script, open_table, bind_table, a
                script error as DHError, the tables the session lists

The status codes checked are the ones the Java server returns; PARITY.md, Must match, item 7.
"""

from __future__ import annotations

import math
import threading
import time
import traceback
from typing import Any, Callable, Iterator

import grpc
import pyarrow as pa
import pyarrow.flight as paflight
from pydeephaven import DHError, Session, agg
from pydeephaven.ticket import SharedTicket

from . import rules
from .connect import PSK_AUTH_TYPE, collect_garbage, open_session, parse_url
from .runner import failure_code

AREAS = ("session", "tables", "flight", "barrage", "inputtable", "console")
# The areas that need the server's Python console (`scripts.has_python_console`).
PYTHON_AREAS = ("console",)
_CONFIG_RPC = "/io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants"


class SmokeFailure(AssertionError):
    pass


def expect(condition: Any, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def expect_equal(actual: Any, wanted: Any, what: str) -> None:
    if actual != wanted:
        raise SmokeFailure(f"{what}: expected {wanted!r}, got {actual!r}")


def expect_failure(action: Callable[[], Any], code: str | None, what: str) -> None:
    """`action` must fail; with `code`, the failure must carry that gRPC status code."""
    try:
        action()
    except Exception as error:  # noqa: BLE001 - any client exception counts as the failure
        got = failure_code(error)
        if code is not None and got != code:
            raise SmokeFailure(f"{what}: expected status {code}, got {got} ({error!r})") from error
        return
    raise SmokeFailure(f"{what}: expected a failure, but it succeeded")


def expect_table(actual: pa.Table, wanted: dict[str, list], what: str) -> None:
    """Column names in order and cell values; NaN equals NaN."""
    expect_equal(actual.column_names, list(wanted), f"{what}: columns")
    for name, cells in wanted.items():
        got = actual.column(name).to_pylist()
        same = len(got) == len(cells) and all(
            (isinstance(g, float) and isinstance(c, float) and math.isnan(g) and math.isnan(c))
            or (g == c and type(g) is type(c))
            for g, c in zip(got, cells)
        )
        if not same:
            raise SmokeFailure(f"{what}: column {name}: expected {cells!r}, got {got!r}")


def _raw_code(url: str, method: str, request: bytes, metadata: list[tuple[str, str]]) -> str:
    """Status code name of one unary call over plain gRPC, 'OK' if it succeeds."""
    server = parse_url(url)
    with grpc.insecure_channel(f"{server.host}:{server.port}") as channel:
        call = channel.unary_unary(
            method,
            request_serializer=lambda message: message,
            response_deserializer=lambda message: message,
        )
        try:
            call(request, metadata=metadata, timeout=20)
        except grpc.RpcError as error:
            return error.code().name
    return "OK"


def _do_get_code(session: Session, ticket: bytes) -> str:
    """Status code name of a DoGet for `ticket` in `session`, 'OK' if it succeeds."""
    call = session.grpc_channel.unary_stream(
        "/arrow.flight.protocol.FlightService/DoGet",
        request_serializer=lambda message: message,
        response_deserializer=lambda message: message,
    )
    try:
        for _ in call(b"\x0a" + bytes([len(ticket)]) + ticket, metadata=session.grpc_metadata,
                      timeout=20):
            pass
    except grpc.RpcError as error:
        return error.code().name
    return "OK"


def _ints(name: str, values: list[int | None]) -> pa.Table:
    return pa.table({name: pa.array(values, pa.int32())})


# ---------------------------------------------------------------- session


def session_checks(url: str) -> list[tuple[str, Callable[[], None]]]:
    server = parse_url(url)

    def connect_and_configuration() -> None:
        with open_session(url) as session:
            expect(session.is_alive, "a new session is not alive")
            constants = session.config_service.get_configuration_constants()
            expect("http.session.durationMs" in constants,
                   "the configuration lacks http.session.durationMs")
            expect(int(constants["http.session.durationMs"].string_value) > 0,
                   "http.session.durationMs is not a positive number")

    def bearer_token_and_refresh() -> None:
        with open_session(url) as session:
            header = dict(session.grpc_metadata)[b"authorization"]
            expect(header.startswith(b"Bearer "), f"no bearer token after connecting: {header!r}")
            expect(session._refresh_token(), "refreshing the token failed")  # noqa: SLF001
            again = dict(session.grpc_metadata)[b"authorization"]
            expect(again.startswith(b"Bearer "), "no bearer token after a refresh")
            expect_table(session.empty_table(1).update(["X = 1"]).to_arrow(), {"X": [1]},
                         "a table after the refresh")

    def wrong_credentials_are_rejected() -> None:
        expect_equal(
            _raw_code(url, _CONFIG_RPC, b"", [("authorization", "Bearer not-a-token")]),
            "UNAUTHENTICATED", "a call with an unknown bearer token")
        expect_equal(
            _raw_code(url, _CONFIG_RPC, b"", [("authorization", f"{PSK_AUTH_TYPE} wrong-key")]),
            "UNAUTHENTICATED", "a call with a wrong pre-shared key")
        if server.psk is not None:
            def connect_wrongly() -> None:
                Session(host=server.host, port=server.port, auth_type=PSK_AUTH_TYPE,
                        auth_token=server.psk + "x").close()

            expect_failure(connect_wrongly, None, "connecting with a wrong pre-shared key")

    def export_and_release() -> None:
        with open_session(url) as session:
            table = session.import_table(_ints("K", [1, 2, 3]))
            ticket = table.ticket.bytes
            expect_equal(_do_get_code(session, ticket), "OK", "DoGet of an exported table")
            session.release(table.ticket)
            expect_equal(_do_get_code(session, ticket), "FAILED_PRECONDITION",
                         "DoGet of a released export")
            # Ticket numbers are not reused: the next export is a new table.
            other = session.import_table(_ints("K", [9]))
            expect(other.ticket.bytes != ticket, "an export ticket was reused")
            expect_table(other.to_arrow(), {"K": [9]}, "the next export")

    def sessions_are_isolated() -> None:
        with open_session(url) as first, open_session(url) as second:
            # Export ticket numbers are per session: the same ticket names another table.
            mine = first.import_table(_ints("K", [1]))
            theirs = second.import_table(_ints("K", [2, 3]))
            expect_equal(mine.ticket.bytes, theirs.ticket.bytes, "the first ticket of a session")
            expect_table(mine.to_arrow(), {"K": [1]}, "the first session's export")
            expect_table(theirs.to_arrow(), {"K": [2, 3]}, "the second session's export")
            first.release(mine.ticket)
            expect_table(theirs.to_arrow(), {"K": [2, 3]},
                         "the second session's export after the first released its own")

    def export_used_before_it_is_defined() -> None:
        """A client may send a request that names an export it has not created yet; the
        server holds the request until the export exists (clients pipeline this way)."""
        with open_session(url) as session:
            first = session.import_table(_ints("K", [0]))
            number = int.from_bytes(first.ticket.bytes[1:], "little") + 1
            ticket = b"e" + number.to_bytes(4, "little")
            outcome: list[str] = []
            waiter = threading.Thread(
                target=lambda: outcome.append(_do_get_code(session, ticket)))
            waiter.start()
            time.sleep(1.0)
            expect(waiter.is_alive(),
                   f"DoGet of an export not yet defined returned at once: {outcome}")
            table = session.import_table(_ints("K", [7]))
            expect_equal(table.ticket.bytes, ticket, "the ticket of the next export")
            waiter.join(25)
            expect_equal(outcome, ["OK"], "the held DoGet once the export exists")

    def shared_ticket_crosses_sessions() -> None:
        with open_session(url) as first, open_session(url) as second:
            table = first.import_table(_ints("K", [4, 5]))
            shared = SharedTicket.random_ticket()
            first.publish_table(shared, table)
            fetched = second.fetch_table(shared)
            expect_table(fetched.to_arrow(), {"K": [4, 5]}, "a table fetched by shared ticket")
            expect_failure(lambda: second.fetch_table(SharedTicket.random_ticket()), None,
                           "fetching a shared ticket nobody published")

    def closed_session_token_is_dead() -> None:
        session = open_session(url)
        header = dict(session.grpc_metadata)[b"authorization"].decode()
        expect_equal(_raw_code(url, _CONFIG_RPC, b"", [("authorization", header)]), "OK",
                     "a call with the token of an open session")
        session.close()
        expect(not session.is_alive, "a closed session says it is alive")
        expect_equal(_raw_code(url, _CONFIG_RPC, b"", [("authorization", header)]),
                     "UNAUTHENTICATED", "a call with the token of a closed session")

    return [
        ("connect and read the configuration", connect_and_configuration),
        ("bearer token and refresh", bearer_token_and_refresh),
        ("wrong credentials are rejected", wrong_credentials_are_rejected),
        ("export and release a ticket", export_and_release),
        ("sessions do not see each other's exports", sessions_are_isolated),
        ("an export used before it is defined", export_used_before_it_is_defined),
        ("a shared ticket crosses sessions", shared_ticket_crosses_sessions),
        ("a closed session's token is dead", closed_session_token_is_dead),
    ]


# ---------------------------------------------------------------- tables


def tables_checks(url: str) -> list[tuple[str, Callable[[], None]]]:
    data = pa.table({
        "K": pa.array([2, 1, 2, None, 1], pa.int32()),
        "V": pa.array([1.5, 2.5, None, 4.0, 0.5], pa.float64()),
        "S": pa.array(["b", "a", None, "d", "e"], pa.string()),
    })

    def empty_table_and_update() -> None:
        with open_session(url) as session:
            table = session.empty_table(3).update(["I = (int) i", "D = I * 0.5", "S = `x`"])
            expect_equal(table.size, 3, "size of empty_table(3)")
            result = table.to_arrow()
            expect_table(result, {"I": [0, 1, 2], "D": [0.0, 0.5, 1.0], "S": ["x", "x", "x"]},
                         "empty_table.update")
            expect_equal([str(t) for t in result.schema.types], ["int32", "double", "string"],
                         "types")
            expect_equal(result.schema.field("I").metadata[b"deephaven:type"], b"int",
                         "deephaven:type of I")

    def time_table_schema() -> None:
        with open_session(url) as session:
            table = session.time_table("PT1S")
            expect_equal([(f.name, str(f.type)) for f in table.schema],
                         [("Timestamp", "timestamp[ns, tz=UTC]")], "time_table schema")
            expect(table.is_refreshing, "a time table is not marked refreshing")
            expect(not session.empty_table(1).is_refreshing, "an empty table is refreshing")

    def single_operations() -> None:
        with open_session(url) as session:
            t = session.import_table(data)
            expect_equal(t.size, 5, "size of the imported table")
            expect_table(t.where(["K == 2"]).to_arrow(),
                         {"K": [2, 2], "V": [1.5, None], "S": ["b", None]}, "where")
            expect_table(t.sort(["K", "S"]).view(["K", "S"]).to_arrow(),
                         {"K": [None, 1, 1, 2, 2], "S": ["d", "a", "e", None, "b"]}, "sort")
            expect_table(t.head(2).drop_columns(["V"]).to_arrow(),
                         {"K": [2, 1], "S": ["b", "a"]}, "head")
            expect_table(t.tail(1).to_arrow(), {"K": [1], "V": [0.5], "S": ["e"]}, "tail")
            expect_table(
                t.agg_by([agg.sum_(["V"]), agg.count_("N")], ["K"]).to_arrow(),
                {"K": [2, 1, None], "V": [1.5, 3.0, 4.0], "N": [2, 2, 1]}, "agg_by")
            names = session.import_table(pa.table({
                "K": pa.array([1, 2], pa.int32()), "Name": pa.array(["one", "two"])}))
            expect_table(t.natural_join(names, on=["K"], joins=["Name"]).view(["Name"]).to_arrow(),
                         {"Name": ["two", "one", "two", None, "one"]}, "natural_join")
            expect_table(session.merge_tables([t.head(1), t.tail(1)]).view(["S"]).to_arrow(),
                         {"S": ["b", "e"]}, "merge")

    def batch_query() -> None:
        with open_session(url) as session:
            t = session.import_table(data)
            result = (session.query(t).update(["W = V * 2"]).where(["K != null"])
                      .sort(["K"]).tail(3).exec())
            expect_table(result.to_arrow(),
                         {"K": [1, 2, 2], "V": [0.5, 1.5, None], "S": ["e", "b", None],
                          "W": [1.0, 3.0, None]}, "a batch of four operations")

    def rejected_operation() -> None:
        with open_session(url) as session:
            t = session.import_table(data)
            expect_failure(lambda: t.where(["Nope > 1"]), "INVALID_ARGUMENT",
                           "a filter on a column the table lacks")
            expect_failure(lambda: t.update(["X = K +"]), "INVALID_ARGUMENT",
                           "a formula with a syntax error")
            # The session and the source table survive a rejected request.
            expect_table(t.head(1).view(["K"]).to_arrow(), {"K": [2]}, "an operation afterwards")

    def released_table() -> None:
        with open_session(url) as session:
            t = session.import_table(data)
            derived = t.where(["K == 1"])
            session.release(t.ticket)
            # A derived table does not depend on its source's export ticket.
            expect_table(derived.view(["S"]).to_arrow(), {"S": ["a", "e"]},
                         "a table derived before its source was released")
            expect_failure(lambda: t.head(1), None, "an operation on a released table")

    return [
        ("empty_table and update", empty_table_and_update),
        ("time_table schema and refreshing flag", time_table_schema),
        ("single operations", single_operations),
        ("a batch query", batch_query),
        ("a rejected operation", rejected_operation),
        ("a released table", released_table),
    ]


# ---------------------------------------------------------------- flight


class _AuthFactory(paflight.ClientMiddlewareFactory):
    """Sends the current `authorization` header and adopts the bearer token the server returns,
    which is how a Flight client without Deephaven's library authenticates."""

    def __init__(self, header: str):
        super().__init__()
        self.header = header
        self.calls = 0

    def start_call(self, info: Any) -> "_AuthMiddleware":
        self.calls += 1
        return _AuthMiddleware(self)


class _AuthMiddleware(paflight.ClientMiddleware):
    def __init__(self, factory: _AuthFactory):
        super().__init__()
        self.factory = factory

    def sending_headers(self) -> dict[str, str]:
        return {"authorization": self.factory.header}

    def received_headers(self, headers: dict) -> None:
        values = headers.get("authorization")
        if values:
            self.factory.header = values[0]


def _all_types() -> pa.Table:
    return pa.table({
        "B": pa.array([True, None, False], pa.bool_()),
        "I8": pa.array([1, None, -127], pa.int8()),
        "I16": pa.array([1, None, 32767], pa.int16()),
        "I32": pa.array([1, None, 2147483647], pa.int32()),
        "I64": pa.array([1, None, 9223372036854775807], pa.int64()),
        "F32": pa.array([1.5, None, math.nan], pa.float32()),
        "F64": pa.array([1.5, None, math.inf], pa.float64()),
        "S": pa.array(["a", None, "é𝄞"], pa.string()),
        "T": pa.array([0, None, 1700000000000000001], pa.timestamp("ns", tz="UTC")),
    })


def flight_checks(url: str) -> list[tuple[str, Callable[[], None]]]:
    server = parse_url(url)
    first_header = "Anonymous" if server.psk is None else f"{PSK_AUTH_TYPE} {server.psk}"

    def client() -> tuple[paflight.FlightClient, _AuthFactory]:
        factory = _AuthFactory(first_header)
        return paflight.FlightClient(f"grpc://{server.host}:{server.port}",
                                     middleware=[factory]), factory

    def put(flight: paflight.FlightClient, number: int, data: pa.Table) -> paflight.Ticket:
        writer, reader = flight.do_put(
            paflight.FlightDescriptor.for_path("export", str(number)), data.schema)
        writer.write_table(data)
        writer.done_writing()
        reader.read()
        writer.close()
        return paflight.Ticket(b"e" + number.to_bytes(4, "little", signed=True))

    def authenticate_by_header() -> None:
        flight, factory = client()
        with flight:
            list(flight.list_flights())
            expect(factory.header.startswith("Bearer "),
                   f"the server returned no bearer token: {factory.header!r}")
            token = factory.header
            list(flight.list_flights())
            expect(factory.header.startswith("Bearer "), "the bearer token was not accepted")
            expect(token.startswith("Bearer "), "unreachable")
        bad = _AuthFactory("Bearer not-a-token")
        with paflight.FlightClient(f"grpc://{server.host}:{server.port}",
                                   middleware=[bad]) as rejected:
            try:
                list(rejected.list_flights())
            except paflight.FlightUnauthenticatedError:
                pass
            else:
                raise SmokeFailure("a call with an unknown bearer token was accepted")

    def put_and_get_all_types() -> None:
        flight, _ = client()
        with flight:
            data = _all_types()
            ticket = put(flight, 1, data)
            result = flight.do_get(ticket).read_all()
            found = rules.compare_tables("flight", "round_trip", 0, data, result,
                                         rules=rules.Rules(compared_field_metadata=(),
                                                           compared_schema_metadata=()))
            expect(not found, f"the table changed on the way: {[str(m) for m in found]}")
            expect_equal(
                [result.schema.field(n).metadata[b"deephaven:type"].decode()
                 for n in result.column_names],
                ["java.lang.Boolean", "byte", "short", "int", "long", "float", "double",
                 "java.lang.String", "java.time.Instant"],
                "deephaven:type of each column")

    def schema_and_flight_info() -> None:
        flight, _ = client()
        with flight:
            data = _all_types()
            put(flight, 1, data)
            descriptor = paflight.FlightDescriptor.for_path("export", "1")
            schema = flight.get_schema(descriptor).schema
            expect_equal([(f.name, str(f.type)) for f in schema],
                         [(f.name, str(f.type)) for f in data.schema], "GetSchema")
            info = flight.get_flight_info(descriptor)
            expect_equal(info.total_records, 3, "GetFlightInfo total_records")
            expect_equal([(f.name, str(f.type)) for f in info.schema],
                         [(f.name, str(f.type)) for f in data.schema], "GetFlightInfo schema")
            expect(len(info.endpoints) == 1, "GetFlightInfo: expected one endpoint")
            again = flight.do_get(info.endpoints[0].ticket).read_all()
            expect_equal(again.num_rows, 3, "DoGet with the ticket from GetFlightInfo")

    def several_batches() -> None:
        flight, _ = client()
        with flight:
            schema = pa.schema([("K", pa.int32())])
            writer, reader = flight.do_put(
                paflight.FlightDescriptor.for_path("export", "1"), schema)
            for start in (0, 1000, 2000):
                writer.write_batch(pa.record_batch(
                    [pa.array(range(start, start + 1000), pa.int32())], schema=schema))
            writer.done_writing()
            reader.read()
            writer.close()
            result = flight.do_get(
                paflight.Ticket(b"e" + (1).to_bytes(4, "little"))).read_all()
            expect_equal(result.column("K").to_pylist(), list(range(3000)),
                         "three record batches in one DoPut")

    def unknown_ticket() -> None:
        flight, _ = client()
        with flight:
            list(flight.list_flights())
            options = paflight.FlightCallOptions(timeout=20)
            for what, ticket in (("a ticket of no known kind", b"zzz"),
                                 ("a scope ticket for a variable that does not exist",
                                  b"s/no_such_variable")):
                try:
                    flight.do_get(paflight.Ticket(ticket), options).read_all()
                except paflight.FlightTimedOutError as error:
                    raise SmokeFailure(f"DoGet of {what} did not return") from error
                except (paflight.FlightError, pa.ArrowException):
                    pass
                else:
                    raise SmokeFailure(f"DoGet of {what} succeeded")
            try:
                flight.get_schema(
                    paflight.FlightDescriptor.for_path("scope", "no_such_variable"), options)
            except paflight.FlightTimedOutError as error:
                raise SmokeFailure("GetSchema of an unknown variable did not return") from error
            except (paflight.FlightError, pa.ArrowException):
                pass
            else:
                raise SmokeFailure("GetSchema of a variable that does not exist succeeded")

    return [
        ("authenticate by header", authenticate_by_header),
        ("DoPut and DoGet of every column type", put_and_get_all_types),
        ("GetSchema and GetFlightInfo", schema_and_flight_info),
        ("a DoPut of several record batches", several_batches),
        ("an unknown ticket", unknown_ticket),
    ]


# ---------------------------------------------------------------- barrage


class _Replica:
    """Follows one table through a Barrage subscription and keeps the latest replica."""

    def __init__(self, table: Any):
        from pydeephaven_ticking import table_listener  # the area's only user of this package

        self._schema = table.schema
        self._lock = threading.Lock()
        self.table: pa.Table | None = None
        self.updates = 0
        self.added = self.removed = self.modified = 0
        self.error: Exception | None = None
        self._handle = table_listener.listen(table, self._on_update, self._on_error)
        self._handle.start()

    def _on_update(self, update: Any) -> None:
        from pydeephaven_ticking.table_listener import _make_generator

        ticking = update.update
        current = ticking.current
        names = [field.name for field in self._schema]
        arrays: dict[str, list] = {name: [] for name in names}
        for chunk in _make_generator(current, current.get_row_sequence(), names, 4096):
            for name in names:
                arrays[name].append(chunk[name])
        replica = pa.table(
            {name: pa.chunked_array(arrays[name], self._schema.field(name).type)
             for name in names})
        with self._lock:
            self.table = replica
            self.updates += 1
            self.added += ticking.added_rows.size
            self.removed += ticking.removed_rows.size
            self.modified += ticking.all_modified_rows.size

    def _on_error(self, error: Exception) -> None:
        self.error = error

    def counts(self) -> tuple[int, int, int]:
        with self._lock:
            return self.added, self.removed, self.modified

    def await_equal(self, wanted: pa.Table, what: str, seconds: float = 20.0) -> None:
        """Waits until the replica equals `wanted` (values, order and types)."""
        deadline = time.monotonic() + seconds
        found: list[Any] = ["no update received"]
        while time.monotonic() < deadline:
            if self.error is not None:
                raise SmokeFailure(f"{what}: the subscription failed: {self.error!r}")
            with self._lock:
                replica = self.table
            if replica is not None:
                found = rules.compare_tables(
                    "barrage", "replica", 0, wanted, replica,
                    rules=rules.Rules(compared_field_metadata=(), compared_schema_metadata=()))
                if not found:
                    return
            time.sleep(0.05)
        raise SmokeFailure(f"{what}: the replica never matched the snapshot; last difference: "
                           f"{found[0]}")

    def stop(self) -> None:
        self._handle.stop()


def barrage_checks(url: str) -> list[tuple[str, Callable[[], None]]]:
    schema = pa.schema([("Id", pa.int32()), ("G", pa.string()), ("V", pa.float64())])

    def rows(*cells: tuple[int, str | None, float | None]) -> pa.Table:
        return pa.Table.from_arrays(
            [pa.array([c[0] for c in cells], pa.int32()),
             pa.array([c[1] for c in cells], pa.string()),
             pa.array([c[2] for c in cells], pa.float64())], schema=schema)

    def keys(*ids: int) -> pa.Table:
        return pa.table({"Id": pa.array(ids, pa.int32())})

    def static_table() -> None:
        with open_session(url) as session:
            table = session.import_table(rows((1, "a", 1.5), (2, None, math.nan)))
            replica = _Replica(table)
            try:
                replica.await_equal(table.to_arrow(), "the initial snapshot of a static table")
                expect_equal(replica.counts(), (2, 0, 0), "rows (added, removed, modified)")
            finally:
                replica.stop()

    def follows_an_input_table() -> None:
        with open_session(url) as session:
            source = session.input_table(schema=schema, key_cols=["Id"])
            source.add(session.import_table(rows((1, "a", 1.0), (2, "b", 2.0), (3, "a", 3.0))))
            derived = {
                "the input table": source,
                "a filter": source.where(["V >= 2"]),
                "a sort": source.sort_descending(["V"]),
                "an aggregation": source.agg_by([agg.sum_(["V"]), agg.count_("N")], ["G"]),
            }
            replicas = {name: _Replica(table) for name, table in derived.items()}

            def settle(change: str) -> None:
                for name, table in derived.items():
                    # The snapshot is taken after the change; the replica must reach it.
                    last = None
                    for _ in range(3):
                        snapshot = table.to_arrow()
                        try:
                            replicas[name].await_equal(snapshot, f"{name} after {change}", 5.0)
                            last = None
                            break
                        except SmokeFailure as error:
                            last = error
                    if last is not None:
                        raise last

            try:
                settle("the initial rows")
                before = replicas["the input table"].counts()
                expect_equal(before, (3, 0, 0), "initial rows (added, removed, modified)")

                source.add(session.import_table(rows((4, "c", 4.0), (5, None, None))))
                settle("adding two rows")
                expect_equal(replicas["the input table"].counts(), (5, 0, 0),
                             "after adding two rows (added, removed, modified)")

                source.add(session.import_table(rows((2, "c", 0.5))))
                settle("replacing a row")
                expect_equal(replicas["the input table"].counts(), (5, 0, 1),
                             "after replacing one row (added, removed, modified)")

                source.delete(session.import_table(keys(1, 4)))
                settle("deleting two rows")
                expect_equal(replicas["the input table"].counts(), (5, 2, 1),
                             "after deleting two rows (added, removed, modified)")

                source.delete(session.import_table(keys(2, 3, 5)))
                settle("deleting every row")
                expect_equal(replicas["the input table"].table.num_rows, 0, "rows left")

                source.add(session.import_table(rows((7, "z", 7.0))))
                settle("adding to the emptied table")
            finally:
                for replica in replicas.values():
                    replica.stop()

    def unsubscribe_and_resubscribe() -> None:
        with open_session(url) as session:
            source = session.input_table(schema=schema, key_cols=["Id"])
            source.add(session.import_table(rows((1, "a", 1.0))))
            first = _Replica(source)
            first.await_equal(source.to_arrow(), "the first subscription")
            first.stop()
            updates = first.updates
            source.add(session.import_table(rows((2, "b", 2.0))))
            second = _Replica(source)
            try:
                second.await_equal(source.to_arrow(), "a subscription made after a change")
                expect_equal(second.table.num_rows, 2, "rows in the second subscription")
                expect_equal(first.updates, updates, "updates delivered after unsubscribing")
            finally:
                second.stop()

    return [
        ("subscribe to a static table", static_table),
        ("a replica follows an input table and tables derived from it", follows_an_input_table),
        ("unsubscribe and subscribe again", unsubscribe_and_resubscribe),
    ]


# ---------------------------------------------------------------- inputtable


def inputtable_checks(url: str) -> list[tuple[str, Callable[[], None]]]:
    schema = pa.schema([("K", pa.int32()), ("S", pa.string()), ("V", pa.float64())])

    def rows(*cells: tuple[int | None, str | None, float | None]) -> pa.Table:
        return pa.Table.from_arrays(
            [pa.array([c[0] for c in cells], pa.int32()),
             pa.array([c[1] for c in cells], pa.string()),
             pa.array([c[2] for c in cells], pa.float64())], schema=schema)

    def append_only() -> None:
        with open_session(url) as session:
            table = session.input_table(schema=schema)
            expect_equal(table.to_arrow().num_rows, 0, "rows of a new input table")
            expect(table.is_refreshing, "an input table is not marked refreshing")
            table.add(session.import_table(rows((1, "a", 1.0), (1, "a", 1.0))))
            table.add(session.import_table(rows((None, None, None))))
            expect_table(table.to_arrow(),
                         {"K": [1, 1, None], "S": ["a", "a", None], "V": [1.0, 1.0, None]},
                         "an append-only input table keeps duplicates in order")
            expect_failure(
                lambda: table.delete(session.import_table(rows((1, "a", 1.0)))), None,
                "deleting from an append-only input table")
            expect_equal(table.to_arrow().num_rows, 3, "rows after the rejected delete")

    def keyed() -> None:
        with open_session(url) as session:
            table = session.input_table(schema=schema, key_cols=["K"])
            fields = table.to_arrow().schema
            expect_equal(fields.field("K").metadata.get(b"deephaven:inputtable.isKey"), b"true",
                         "inputtable.isKey of the key column")
            expect_equal(fields.field("V").metadata.get(b"deephaven:inputtable.isValue"),
                         b"true", "inputtable.isValue of a value column")
            expect_equal(fields.field("V").metadata.get(b"deephaven:inputtable.isKey"), None,
                         "inputtable.isKey of a value column")
            table.add(session.import_table(rows((1, "a", 1.0), (2, "b", 2.0))))
            table.add(session.import_table(rows((1, "c", 3.0), (3, None, None))))
            expect_table(table.to_arrow(),
                         {"K": [1, 2, 3], "S": ["c", "b", None], "V": [3.0, 2.0, None]},
                         "a keyed add replaces the row with the same key in place")
            table.delete(session.import_table(pa.table({"K": pa.array([2, 9], pa.int32())})))
            expect_table(table.to_arrow(), {"K": [1, 3], "S": ["c", None], "V": [3.0, None]},
                         "a delete by key, with a key that is absent")
            table.add(session.import_table(rows((2, "again", 0.0))))
            # io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable keeps the row
            # number of a deleted key in keyToRowMap and gives it back to the same key.
            expect_table(table.view(["K", "S"]).to_arrow(),
                         {"K": [1, 2, 3], "S": ["c", "again", None]},
                         "a deleted key added again returns to its old position")

    def derived_tables_follow() -> None:
        with open_session(url) as session:
            table = session.input_table(schema=schema, key_cols=["K"])
            total = table.agg_by([agg.sum_(["V"]), agg.count_("N")], [])
            table.add(session.import_table(rows((1, "a", 1.0), (2, "b", 2.0))))
            table.add(session.import_table(rows((2, "b", 5.0))))
            wanted = {"V": [6.0], "N": [2]}
            deadline = time.monotonic() + 20
            while True:
                try:
                    expect_table(total.to_arrow(), wanted, "an aggregation of the input table")
                    return
                except SmokeFailure:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)

    def rejected_changes() -> None:
        with open_session(url) as session:
            table = session.input_table(schema=schema, key_cols=["K"])
            table.add(session.import_table(rows((1, "a", 1.0))))
            wrong_type = pa.table({"K": pa.array(["x"]), "S": pa.array(["a"]),
                                   "V": pa.array([1.0])})
            missing = pa.table({"K": pa.array([5], pa.int32())})
            extra = rows((6, "e", 6.0)).append_column("Extra", pa.array([1], pa.int32()))
            for what, data in (("a column of another type", wrong_type),
                               ("a missing column", missing), ("an extra column", extra)):
                expect_failure(lambda data=data: table.add(session.import_table(data)),
                               "INVALID_ARGUMENT", f"adding rows with {what}")
            expect_failure(lambda: session.input_table(schema=schema, key_cols=["Nope"]), None,
                           "an input table keyed on a column it lacks")
            expect_table(table.to_arrow(), {"K": [1], "S": ["a"], "V": [1.0]},
                         "the input table after the rejected changes")
            static = session.import_table(rows((1, "a", 1.0)))
            add_to_static = session.input_table_service.add
            expect_failure(lambda: add_to_static(static, static), None,
                           "adding to a table that is not an input table")

    def from_initial_table() -> None:
        """The server takes only the columns of the source table, not its rows
        (io.deephaven.server.table.ops.CreateInputTableGrpcImpl#create)."""
        with open_session(url) as session:
            initial = session.import_table(rows((1, "a", 1.0), (2, "b", 2.0)))
            table = session.input_table(init_table=initial, key_cols=["K"])
            expect_table(table.to_arrow(), {"K": [], "S": [], "V": []},
                         "an input table made from a table")
            expect_equal([str(t) for t in table.to_arrow().schema.types],
                         ["int32", "string", "double"], "its column types")
            table.add(initial)
            expect_table(table.to_arrow(), {"K": [1, 2], "S": ["a", "b"], "V": [1.0, 2.0]},
                         "after adding the source table itself")
            expect_equal(initial.to_arrow().num_rows, 2, "rows of the source table")

    return [
        ("append-only input table", append_only),
        ("keyed input table", keyed),
        ("derived tables follow an input table", derived_tables_follow),
        ("rejected changes", rejected_changes),
        ("input table from an initial table", from_initial_table),
    ]


# ---------------------------------------------------------------- console


def console_checks(url: str) -> list[tuple[str, Callable[[], None]]]:
    """The Python console as pydeephaven uses it. The server has one Python session, so
    every check removes the variables it made, whether it passed or not."""

    def forget(session: Session, *names: str) -> None:
        session.run_script(f"for _smoke_name in {names!r}:\n"
                           "    globals().pop(_smoke_name, None)\n"
                           "del _smoke_name")

    def run_script_and_open_table() -> None:
        with open_session(url) as session, open_session(url) as other:
            try:
                session.run_script("from deephaven import empty_table\n"
                                   "smoke_made = empty_table(3).update(['X = i * 2'])\n")
                table = session.open_table("smoke_made")
                expect_table(table.to_arrow(), {"X": [0, 2, 4]}, "a table made by a script")
                expect_table(table.update(["Y = X + 1"]).to_arrow(),
                             {"X": [0, 2, 4], "Y": [1, 3, 5]},
                             "a client operation on a table made by a script")
                expect_table(other.open_table("smoke_made").to_arrow(), {"X": [0, 2, 4]},
                             "the table opened from another session")
                session.run_script("smoke_made = smoke_made.where('X > 0')")
                expect_table(session.open_table("smoke_made").to_arrow(), {"X": [2, 4]},
                             "the variable after a second script replaced it")
                expect_table(table.to_arrow(), {"X": [0, 2, 4]},
                             "the table opened before the variable was replaced")
                expect_failure(lambda: session.open_table("smoke_never_assigned"), None,
                               "opening a variable that was never assigned")
            finally:
                forget(session, "smoke_made")
            expect_failure(lambda: session.open_table("smoke_made"), None,
                           "opening a variable that a script deleted")

    def bind_table() -> None:
        with open_session(url) as session:
            try:
                session.bind_table("smoke_bound", session.empty_table(4).update(["X = i"]))
                expect_table(session.open_table("smoke_bound").to_arrow(),
                             {"X": [0, 1, 2, 3]}, "a table bound to a variable")
                session.run_script("smoke_from_bound = smoke_bound.where('X > 1')")
                expect_table(session.open_table("smoke_from_bound").to_arrow(), {"X": [2, 3]},
                             "a table a script made from the bound variable")
            finally:
                forget(session, "smoke_bound", "smoke_from_bound")

    def script_error() -> None:
        def error_of(script: str, what: str) -> str:
            try:
                session.run_script(script)
            except DHError as error:
                return str(error)
            raise SmokeFailure(f"{what}: expected a DHError, but the script ran")

        with open_session(url) as session:
            try:
                text = error_of("from deephaven import empty_table\n"
                                "smoke_kept = empty_table(2)\n"
                                "raise ValueError('smoke says no')\n", "a script that raises")
                expect("ValueError: smoke says no" in text,
                       f"the error of a script that raises does not name the exception: {text!r}")
                expect_equal(session.open_table("smoke_kept").to_arrow().num_rows, 2,
                             "rows of the table assigned before the script raised")
                text = error_of("smoke_kept.update('Y = NoSuchColumn')",
                                "a script whose table operation fails")
                expect("table update operation failed." in text,
                       f"the error of a failed update does not say what failed: {text!r}")
                text = error_of("smoke_kept = = 1", "a script that is not Python")
                expect("SyntaxError" in text, f"a syntax error is not named: {text!r}")
                session.run_script("smoke_kept = smoke_kept.head(1)")
                expect_equal(session.open_table("smoke_kept").to_arrow().num_rows, 1,
                             "rows after a script that ran after the failed ones")
            finally:
                forget(session, "smoke_kept")

    def tables_of_the_scope() -> None:
        with open_session(url) as session:
            try:
                session.run_script("from deephaven import empty_table\n"
                                   "smoke_listed = empty_table(1)\n"
                                   "smoke_not_a_table = 5\n")
                session.bind_table("smoke_bound_listed", session.empty_table(1))
                names = session.tables
                for name in ("smoke_listed", "smoke_bound_listed"):
                    expect(name in names, f"the session's tables lack {name}: {names}")
                expect("smoke_not_a_table" not in names,
                       f"the session's tables list a variable that is not a table: {names}")
                expect("empty_table" not in names,
                       f"the session's tables list an imported function: {names}")
            finally:
                forget(session, "smoke_listed", "smoke_not_a_table", "smoke_bound_listed")
            names = session.tables
            expect("smoke_listed" not in names and "smoke_bound_listed" not in names,
                   f"the session's tables list deleted variables: {names}")

    return [
        ("run_script and open_table", run_script_and_open_table),
        ("bind_table", bind_table),
        ("a script error is a DHError", script_error),
        ("the session's tables are the tables of the scope", tables_of_the_scope),
    ]


_AREAS: dict[str, Callable[[str], list[tuple[str, Callable[[], None]]]]] = {
    "session": session_checks,
    "tables": tables_checks,
    "flight": flight_checks,
    "barrage": barrage_checks,
    "inputtable": inputtable_checks,
    "console": console_checks,
}


def describe(error: BaseException) -> str:
    """One line for a failed check: the expectation that did not hold, or the client error."""
    if isinstance(error, SmokeFailure):
        return str(error)
    last = traceback.extract_tb(error.__traceback__)[-1]
    return f"{type(error).__name__}: {error} ({last.name}, line {last.lineno})"


def run_checks(area: str, url: str) -> Iterator[tuple[str, Exception | None]]:
    """Runs every check of `area` against `url`; yields each name with what it failed with."""
    for name, check in _AREAS[area](url):
        # a check may open its sessions itself, and the last check has left garbage
        collect_garbage()
        try:
            check()
        except Exception as error:  # noqa: BLE001 - a client error is a failed check too
            yield name, error
        else:
            yield name, None


def run(area: str, url: str) -> int:
    """Runs every check of `area` against `url`. 0 when all pass, 1 otherwise."""
    total = failed = 0
    for name, error in run_checks(area, url):
        total += 1
        if error is None:
            print(f"ok    {area}: {name}")
        else:
            failed += 1
            print(f"FAIL  {area}: {name}: {describe(error)}")
    print(f"client-smoke {area} {url}: {total} checks, {total - failed} passed, "
          f"{failed} failed")
    return 1 if failed else 0
