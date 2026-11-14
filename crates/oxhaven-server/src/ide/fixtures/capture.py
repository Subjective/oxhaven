#!/usr/bin/env python3
"""Records what the Java reference answers to the calls of the web IDE path.

No Java counterpart: test tooling. The output is `java.txt` next to this script, which the
tests of `oxhaven-server` replay against this server (`src/ide/mod.rs`). Run it with the
oracle's Python environment against two Java servers started by the IDE acceptance tooling:

    oracle/ide/bin/start-java-ide.sh 10840 app
    oracle/ide/bin/start-java-ide.sh 10841 app --jvm-arg=-Ddeephaven.console.disable=true
    oracle/.venv/bin/python crates/oxhaven-server/src/ide/fixtures/capture.py \
        --app 127.0.0.1:10840 --disabled 127.0.0.1:10841 --psk oxhaven \
        > crates/oxhaven-server/src/ide/fixtures/java.txt
    oracle/bin/stop-java.sh 10840; oracle/bin/stop-java.sh 10841

Record from servers that were just started: a run leaves the variable `ide_published` in the
query scope of both.

The first server has a Python console and the application `ide_app`; the second is the same
with the console disabled, which is the Java server nearest to an Oxhaven without a script
session. Each server is recorded in one session, so the cases of a section depend on the
exports of the cases before them and have to be replayed in order.

Format of the output:

    ## <section name>                         one session on one server
    == <case name>
    call: <Service>/<Method>
    auth: none                                only for a call sent without a session
    request: <hex of the request message>
    # <the request, protobuf text format>
    status: <gRPC code name>[: <message>]     OPEN for a stream this script cancelled
    message: <hex of one response message>
    # <the same message, protobuf text format>

A line break in a status message is written `\\n`. The message of `data_static` is not a
protobuf message: it is the Arrow IPC stream of the table that Flight `DoGet` returned.

Error ids (`Details Logged w/ID '...'`) and the memory figures of `GetHeapInfo` differ from
run to run; nothing else should.
"""

import argparse
import sys
import time

import grpc
import pyarrow as pa
import pyarrow.flight as flight
from google.protobuf import text_format

from deephaven_core.proto import (
    application_pb2,
    application_pb2_grpc,
    config_pb2,
    config_pb2_grpc,
    console_pb2,
    console_pb2_grpc,
    session_pb2,
    session_pb2_grpc,
    storage_pb2,
    storage_pb2_grpc,
    table_pb2,
    table_pb2_grpc,
    ticket_pb2,
)

# Seconds after which a call is given up: `DEADLINE_EXCEEDED` in the output means Java had
# not answered by then.
CALL_TIMEOUT = 10

PSK_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"

# What the JS API sends with every ApplyPreviewColumns
# (web/client-api/.../subscription/AbstractTableSubscription.java#createPreview).
IDE_UNPREVIEWED_TYPES = [
    "byte", "java.lang.Byte", "char", "java.lang.Character", "short", "java.lang.Short",
    "int", "java.lang.Integer", "long", "java.lang.Long", "float", "java.lang.Float",
    "double", "java.lang.Double", "boolean", "java.lang.Boolean", "java.lang.String",
    "java.math.BigDecimal", "java.math.BigInteger", "java.time.Instant",
    "java.time.LocalDate", "java.time.LocalTime", "java.time.ZonedDateTime",
]


def one_line(message) -> str:
    return text_format.MessageToString(message, as_one_line=True)


def status_line(error: grpc.RpcError) -> str:
    details = (error.details() or "").replace("\\", "\\\\").replace("\n", "\\n")
    return f"{error.code().name}: {details}"


class Server:
    def __init__(self, address: str, psk: str):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        call = config_pb2_grpc.ConfigServiceStub(self.channel).GetConfigurationConstants.with_call
        _, info = call(
            config_pb2.ConfigurationConstantsRequest(),
            metadata=[("authorization", f"{PSK_AUTH_TYPE} {psk}")],
        )
        self.bearer = dict(info.initial_metadata())["authorization"]
        self.metadata = [("authorization", self.bearer)]
        self.next_export = 0
        self.stubs = {
            "ApplicationService": application_pb2_grpc.ApplicationServiceStub(self.channel),
            "ConsoleService": console_pb2_grpc.ConsoleServiceStub(self.channel),
            "SessionService": session_pb2_grpc.SessionServiceStub(self.channel),
            "StorageService": storage_pb2_grpc.StorageServiceStub(self.channel),
            "TableService": table_pb2_grpc.TableServiceStub(self.channel),
        }

    def export(self) -> ticket_pb2.Ticket:
        self.next_export += 1
        return export_ticket(self.next_export)

    def begin(self, name: str, call: str, request, authenticated: bool):
        """Prints the head of a case and returns the method to call."""
        service, method = call.split("/")
        print(f"== {name}")
        print(f"call: {call}")
        if not authenticated:
            print("auth: none")
        print(f"request: {request.SerializeToString().hex()}")
        print(f"# {one_line(request)}")
        return getattr(self.stubs[service], method)

    def unary(self, name: str, call: str, request, authenticated: bool = True):
        """Records a unary call; returns the response, or None if the call failed."""
        method = self.begin(name, call, request, authenticated)
        try:
            response = method(
                request, metadata=self.metadata if authenticated else None, timeout=CALL_TIMEOUT
            )
        except grpc.RpcError as error:
            print(f"status: {status_line(error)}")
            print()
            return None
        print("status: OK")
        print(f"message: {response.SerializeToString().hex()}")
        print(f"# {one_line(response)}")
        print()
        return response

    def stream(
        self, name: str, call: str, request, authenticated: bool = True, after_first=None
    ):
        """Records the first message of a server stream and then cancels it; a stream that
        ends by itself is recorded with its status. With `after_first`, that runs once the
        first message has arrived and the stream is read to its end."""
        method = self.begin(name, call, request, authenticated)
        responses = method(
            request, metadata=self.metadata if authenticated else None, timeout=CALL_TIMEOUT
        )
        received = []
        status = "OK"
        try:
            for response in responses:
                received.append(response)
                if len(received) > 1:
                    continue
                if after_first is None:
                    status = "OPEN"
                    responses.cancel()
                    break
                after_first()
        except grpc.RpcError as error:
            status = status_line(error)
        print(f"status: {status}")
        for response in received:
            print(f"message: {response.SerializeToString().hex()}")
            print(f"# {one_line(response)}")
        print()


def export_ticket(export_id: int) -> ticket_pb2.Ticket:
    return ticket_pb2.Ticket(ticket=b"e" + export_id.to_bytes(4, "little", signed=True))


def reference(ticket: ticket_pb2.Ticket) -> table_pb2.TableReference:
    return table_pb2.TableReference(ticket=ticket)


def fetch(server: Server, name: str, source: bytes):
    result = server.export()
    request = table_pb2.FetchTableRequest(
        source_id=reference(ticket_pb2.Ticket(ticket=source)), result_id=result
    )
    return result, server.unary(name, "TableService/FetchTable", request)


def preview(server: Server, name: str, source: ticket_pb2.Ticket, convert_arrays: bool, types):
    request = table_pb2.ApplyPreviewColumnsRequest(
        source_id=reference(source),
        result_id=server.export(),
        convert_arrays=convert_arrays,
        unpreviewed_types=types,
    )
    return server.unary(name, "TableService/ApplyPreviewColumns", request)


def record_application(server: Server):
    server.stream("list_fields", "ApplicationService/ListFields", application_pb2.ListFieldsRequest())
    server.stream(
        "list_fields_unauthenticated", "ApplicationService/ListFields",
        application_pb2.ListFieldsRequest(), authenticated=False,
    )


def record_tickets(server: Server):
    static, _ = fetch(server, "fetch_static", b"a/ide_app/f/ide_static")
    # ide_ticking is the last three rows of a table that gains two rows a second
    time.sleep(3)
    ticking, _ = fetch(server, "fetch_ticking", b"a/ide_app/f/ide_ticking")
    for name, source in [
        # the segment between the application and the field is not checked
        ("fetch_other_segment", b"a/ide_app/x/ide_static"),
        ("fetch_other_route_text", b"abc/ide_app/f/ide_static"),
        ("fetch_field_with_slash", b"a/ide_app/f/ide/static"),
        ("fetch_unknown_application", b"a/nope/f/ide_static"),
        ("fetch_unknown_field", b"a/ide_app/f/nope"),
        ("fetch_empty_field", b"a/ide_app/f/"),
        ("fetch_no_field_segment", b"a/ide_app/f"),
        ("fetch_no_application", b"a/"),
        ("fetch_route_only", b"a"),
        ("fetch_no_first_slash", b"aide_app/f/ide_static"),
        ("fetch_not_utf8", b"a/\xff/f/ide_static"),
        ("fetch_scope_application", b"a/scope/f/ide_static"),
        ("fetch_scope_unknown", b"s/nope"),
        ("fetch_scope_empty_name", b"s/"),
        ("fetch_scope_route_only", b"s"),
        ("fetch_scope_no_slash", b"sxy"),
        ("fetch_scope_not_utf8", b"s/\xff"),
    ]:
        fetch(server, name, source)

    server.unary(
        "export_from_application", "SessionService/ExportFromTicket",
        session_pb2.ExportRequest(
            source_id=ticket_pb2.Ticket(ticket=b"a/ide_app/f/ide_static"),
            result_id=server.export(),
        ),
    )
    server.unary(
        "export_from_unknown_field", "SessionService/ExportFromTicket",
        session_pb2.ExportRequest(
            source_id=ticket_pb2.Ticket(ticket=b"a/ide_app/f/nope"), result_id=server.export()
        ),
    )
    # an application ticket cannot be published to
    server.unary(
        "publish_to_application", "SessionService/PublishFromTicket",
        session_pb2.PublishRequest(
            source_id=static, result_id=ticket_pb2.Ticket(ticket=b"a/ide_app/f/published")
        ),
    )
    server.unary(
        "publish_to_unknown_application", "SessionService/PublishFromTicket",
        session_pb2.PublishRequest(
            source_id=static, result_id=ticket_pb2.Ticket(ticket=b"a/nope/f/published")
        ),
    )
    return static, ticking


def record_scope(server: Server, static: ticket_pb2.Ticket):
    # a scope ticket can be published to: the value becomes a variable of the query scope
    server.unary(
        "publish_to_scope", "SessionService/PublishFromTicket",
        session_pb2.PublishRequest(
            source_id=static, result_id=ticket_pb2.Ticket(ticket=b"s/ide_published")
        ),
    )
    fetch(server, "fetch_scope_published", b"s/ide_published")
    # whether the new variable is listed depends on the console: see ApplicationServiceGrpcImpl#onScopeChanges
    time.sleep(1)
    server.stream(
        "list_fields_after_publish", "ApplicationService/ListFields",
        application_pb2.ListFieldsRequest(),
    )
    server.unary(
        "publish_to_scope_empty_name", "SessionService/PublishFromTicket",
        session_pb2.PublishRequest(source_id=static, result_id=ticket_pb2.Ticket(ticket=b"s/")),
    )


def record_preview(server: Server, static: ticket_pb2.Ticket, ticking: ticket_pb2.Ticket):
    preview(server, "preview_static_ide", static, True, IDE_UNPREVIEWED_TYPES)
    preview(server, "preview_ticking_ide", ticking, True, IDE_UNPREVIEWED_TYPES)
    preview(server, "preview_static_no_types", static, False, [])
    preview(server, "preview_ticking_no_types", ticking, False, [])
    preview(server, "preview_convert_arrays_without_types", static, True, [])
    without_int = [name for name in IDE_UNPREVIEWED_TYPES if name != "int"]
    preview(server, "preview_static_int_not_listed", static, False, without_int)
    preview(server, "preview_static_one_other_type", static, False, ["java.lang.Object"])
    request = table_pb2.ApplyPreviewColumnsRequest(
        source_id=reference(static), unpreviewed_types=IDE_UNPREVIEWED_TYPES
    )
    server.unary("preview_no_result_ticket", "TableService/ApplyPreviewColumns", request)
    request = table_pb2.ApplyPreviewColumnsRequest(
        result_id=server.export(), unpreviewed_types=IDE_UNPREVIEWED_TYPES
    )
    server.unary("preview_no_source", "TableService/ApplyPreviewColumns", request)
    # in a batch, reading the fetch of the same batch
    batch = table_pb2.BatchTableRequest(
        ops=[
            table_pb2.BatchTableRequest.Operation(
                fetch_table=table_pb2.FetchTableRequest(
                    source_id=reference(ticket_pb2.Ticket(ticket=b"a/ide_app/f/ide_static")),
                )
            ),
            table_pb2.BatchTableRequest.Operation(
                apply_preview_columns=table_pb2.ApplyPreviewColumnsRequest(
                    source_id=table_pb2.TableReference(batch_offset=0),
                    result_id=server.export(),
                    convert_arrays=True,
                    unpreviewed_types=IDE_UNPREVIEWED_TYPES,
                )
            ),
        ]
    )
    server.stream("preview_in_batch", "TableService/Batch", batch, after_first=lambda: None)


def record_storage(server: Server):
    def items(name: str, **fields):
        server.unary(name, "StorageService/ListItems", storage_pb2.ListItemsRequest(**fields))

    items("storage_layouts_json", path="/layouts", filter_glob="*.json")
    items("storage_root_json", filter_glob="*.json")
    items("storage_root", path="")
    items("storage_root_slash", path="/")
    items("storage_root_star", path="/", filter_glob="*")
    items("storage_root_prefix_glob", path="/", filter_glob="lay*")
    items("storage_root_infix_glob", path="/", filter_glob="*o*o*")
    items("storage_root_question_glob", path="/", filter_glob="layout?")
    items("storage_root_question_too_long", path="/", filter_glob="layouts?")
    items("storage_root_literal_glob", path="/", filter_glob="notebooks")
    items("storage_root_literal_prefix_only", path="/", filter_glob="notebook")
    items("storage_root_regex_characters", path="/", filter_glob="l.youts")
    items("storage_root_empty_glob", path="/", filter_glob="")
    items("storage_notebooks", path="/notebooks")
    items("storage_layouts_no_slash", path="layouts")
    items("storage_layouts_trailing_slash", path="/layouts/")
    items("storage_layouts_inner_slashes", path="/layouts//")
    items("storage_layouts_dot", path="/layouts/./")
    items("storage_layouts_through_parent", path="/notebooks/../layouts")
    items("storage_root_through_parent", path="/layouts/..")
    items("storage_double_slash", path="//layouts")
    items("storage_missing", path="/nope")
    items("storage_missing_below", path="/layouts/nope")
    items("storage_missing_then_parent", path="/nope/../layouts")
    items("storage_outside", path="/..")
    items("storage_outside_deeper", path="/layouts/../../x")
    items("storage_outside_and_back", path="/../storage/layouts")
    items("storage_nul_character", path="/lay\0outs")
    items("storage_double_star_glob", path="/layouts", filter_glob="**")
    items("storage_separator_glob", path="/layouts", filter_glob="a/b")
    items("storage_bad_glob", path="/layouts", filter_glob="[")
    items("storage_class_glob", path="/", filter_glob="[ln]*")
    items("storage_group_glob", path="/", filter_glob="{layouts,x}")
    items("storage_escape_glob", path="/", filter_glob="\\layouts")
    items("storage_bad_glob_missing_directory", path="/nope", filter_glob="**")
    server.unary(
        "storage_unauthenticated", "StorageService/ListItems",
        storage_pb2.ListItemsRequest(path="/layouts"), authenticated=False,
    )


def record_console_types(server: Server):
    server.unary(
        "console_types", "ConsoleService/GetConsoleTypes", console_pb2.GetConsoleTypesRequest()
    )
    server.unary(
        "console_types_unauthenticated", "ConsoleService/GetConsoleTypes",
        console_pb2.GetConsoleTypesRequest(), authenticated=False,
    )


def record_disabled_console(server: Server):
    result = server.export()
    server.unary(
        "console_start", "ConsoleService/StartConsole",
        console_pb2.StartConsoleRequest(result_id=result, session_type="python"),
    )
    server.unary("console_heap", "ConsoleService/GetHeapInfo", console_pb2.GetHeapInfoRequest())
    server.stream(
        "console_logs", "ConsoleService/SubscribeToLogs", console_pb2.LogSubscriptionRequest()
    )
    server.unary(
        "console_execute", "ConsoleService/ExecuteCommand",
        console_pb2.ExecuteCommandRequest(console_id=result, code="x = 1"),
    )
    server.unary(
        "console_cancel", "ConsoleService/CancelCommand",
        console_pb2.CancelCommandRequest(console_id=result, command_id=result),
    )
    server.unary(
        "console_bind", "ConsoleService/BindTableToVariable",
        console_pb2.BindTableToVariableRequest(
            console_id=result, variable_name="x", table_id=result
        ),
    )
    server.unary(
        "console_cancel_autocomplete", "ConsoleService/CancelAutoComplete",
        console_pb2.CancelAutoCompleteRequest(console_id=result, request_id=1),
    )
    server.stream(
        "console_open_autocomplete", "ConsoleService/OpenAutoCompleteStream",
        console_pb2.AutoCompleteRequest(console_id=result, request_id=1),
    )
    server.unary(
        "console_next_autocomplete", "ConsoleService/NextAutoCompleteStream",
        console_pb2.AutoCompleteRequest(console_id=result, request_id=1),
    )


def record_data(server: Server, name: str, ticket: bytes):
    """Records the rows of a table as the Arrow IPC stream of what Flight DoGet returns."""
    client = flight.FlightClient(f"grpc://{server.address}")
    options = flight.FlightCallOptions(headers=[(b"authorization", server.bearer.encode())])
    table = client.do_get(flight.Ticket(ticket), options).read_all()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    print(f"== {name}")
    print("call: FlightService/DoGet")
    print(f"request: {ticket.hex()}")
    print(f"# ticket {ticket!r}")
    print("status: OK")
    print(f"message: {sink.getvalue().to_pybytes().hex()}")
    for column in table.column_names:
        print(f"# {column}: {table.column(column).to_pylist()}")
    print()
    client.close()


def record_session_end(server: Server):
    """What an open ListFields stream gets when its session is closed. Ends the session."""
    close = lambda: server.stubs["SessionService"].CloseSession(
        session_pb2.HandshakeRequest(), metadata=server.metadata, timeout=CALL_TIMEOUT
    )
    server.stream(
        "list_fields_session_closed", "ApplicationService/ListFields",
        application_pb2.ListFieldsRequest(), after_first=close,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--app", required=True, help="host:port of the server in mode `app`")
    parser.add_argument(
        "--disabled", required=True,
        help="host:port of the server in mode `app` with -Ddeephaven.console.disable=true",
    )
    parser.add_argument("--psk", default="oxhaven")
    args = parser.parse_args()

    print("# Recorded from the Java reference (Deephaven 42.9) by capture.py; do not edit.")
    print()
    print("## console_disabled")
    print()
    disabled = Server(args.disabled, args.psk)
    record_application(disabled)
    static, ticking = record_tickets(disabled)
    record_preview(disabled, static, ticking)
    record_storage(disabled)
    record_console_types(disabled)
    record_disabled_console(disabled)
    record_scope(disabled, static)
    record_data(disabled, "data_static", b"a/ide_app/f/ide_static")
    record_session_end(disabled)

    print("## with_console")
    print()
    app = Server(args.app, args.psk)
    record_application(app)
    record_console_types(app)
    static, _ = fetch(app, "fetch_static", b"a/ide_app/f/ide_static")
    record_scope(app, static)
    return 0


if __name__ == "__main__":
    sys.exit(main())
