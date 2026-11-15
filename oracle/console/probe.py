#!/usr/bin/env python3
"""Sends one fixed sequence of console calls to a server and prints what it answers.

No Java counterpart: test tooling. Run it against the Java reference and against Oxhaven and
compare the two outputs; `oracle/console/README.md` has the commands and the differences
that are expected.

    oracle/.venv/bin/python oracle/console/probe.py 127.0.0.1:10916 --psk oxhaven

Each case prints the call, the request and the status, then one line per response message in
protobuf text format. What differs from run to run is replaced: timestamps by `<ts>`, memory
figures by `<n>`, error ids by `<id>`; the lines of a Java stack trace in an error message
are left out. A server stream is cancelled at the end (`OPEN`). The two streams that run
beside the commands are printed after them: of the log, the records of level `STDOUT` and
the `STDERR` records that start with `[probe]` (Java's `STDERR` also carries its own log);
of the field changes, one line per change. The script waits after each command so that every
change is sent by itself. The sequence leaves variables in the query scope, so use a server
that was just started.
"""

import argparse
import re
import sys
import threading
import time

import grpc
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
    table_pb2,
    table_pb2_grpc,
    ticket_pb2,
)

CALL_TIMEOUT = 20
SETTLE = 0.4
PSK_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"

VOLATILE = [
    (re.compile(r"(start_timestamp|end_timestamp|micros): \d+"), r"\1: <ts>"),
    (re.compile(r"(max_memory|total_memory|free_memory): \d+"), r"\1: <n>"),
    (re.compile(r"Details Logged w/ID '[0-9a-f-]+'"), "Details Logged w/ID '<id>'"),
    (re.compile(r"0x[0-9a-f]{6,}"), "0x<addr>"),
    (re.compile(r'File \\"[^"\\]*/site-packages/'), 'File \\"<site-packages>/'),
]


def scrub(text: str) -> str:
    for pattern, replacement in VOLATILE:
        text = pattern.sub(replacement, text)
    return text


def one_line(message) -> str:
    if hasattr(message, "error_message") and message.error_message:
        copy = type(message)()
        copy.CopyFrom(message)
        lines = message.error_message.split("\n")
        copy.error_message = "\n".join(line for line in lines if not line.startswith("\tat "))
        message = copy
    if getattr(message, "schema_header", b""):
        # compared by the table service's own tests; here only that there is one
        copy = type(message)()
        copy.CopyFrom(message)
        copy.schema_header = b"<%d bytes>" % len(message.schema_header)
        message = copy
    return scrub(text_format.MessageToString(message, as_one_line=True))


def export_ticket(export_id: int) -> ticket_pb2.Ticket:
    return ticket_pb2.Ticket(ticket=b"e" + export_id.to_bytes(4, "little", signed=True))


class Server:
    def __init__(self, address: str, psk: str, listen: float):
        self.listen = listen
        self.channel = grpc.insecure_channel(address)
        call = config_pb2_grpc.ConfigServiceStub(self.channel).GetConfigurationConstants.with_call
        _, info = call(
            config_pb2.ConfigurationConstantsRequest(),
            metadata=[("authorization", f"{PSK_AUTH_TYPE} {psk}")],
        )
        self.metadata = [("authorization", dict(info.initial_metadata())["authorization"])]
        self.stubs = {
            "ApplicationService": application_pb2_grpc.ApplicationServiceStub(self.channel),
            "ConsoleService": console_pb2_grpc.ConsoleServiceStub(self.channel),
            "SessionService": session_pb2_grpc.SessionServiceStub(self.channel),
            "TableService": table_pb2_grpc.TableServiceStub(self.channel),
        }

    def head(self, name: str, call: str, request, authenticated: bool = True):
        service, method = call.split("/")
        print(f"== {name}")
        print(f"call: {call}{'' if authenticated else ' (no session)'}")
        print(f"request: {one_line(request)}")
        return getattr(self.stubs[service], method)

    def unary(self, name: str, call: str, request, authenticated: bool = True):
        method = self.head(name, call, request, authenticated)
        try:
            response = method(
                request, metadata=self.metadata if authenticated else None, timeout=CALL_TIMEOUT
            )
        except grpc.RpcError as error:
            print(f"status: {error.code().name}: {scrub(error.details() or '')!r}")
            print()
            return None
        print("status: OK")
        print(f"message: {one_line(response)}")
        print()
        return response

    def open_stream(self, call: str, request):
        """Starts a server stream in the background; returns (messages, status, cancel)."""
        service, method = call.split("/")
        responses = getattr(self.stubs[service], method)(request, metadata=self.metadata)
        messages = []
        status = ["OPEN"]

        def read():
            try:
                for response in responses:
                    messages.append(response)
                status[0] = "OK"
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.CANCELLED:
                    status[0] = f"{error.code().name}: {scrub(error.details() or '')!r}"

        thread = threading.Thread(target=read, daemon=True)
        thread.start()

        def cancel():
            time.sleep(self.listen)
            responses.cancel()
            thread.join(5)

        return messages, status, cancel


def execute(server: Server, name: str, console, code: str):
    request = console_pb2.ExecuteCommandRequest(console_id=console, code=code)
    response = server.unary(name, "ConsoleService/ExecuteCommand", request)
    # longer than the interval in which a server gathers field changes into one message
    time.sleep(SETTLE)
    return response


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--psk", default="oxhaven")
    parser.add_argument("--listen", type=float, default=1.5)
    args = parser.parse_args()
    server = Server(args.address, args.psk, args.listen)
    console = export_ticket(1)

    server.unary(
        "console types, no session",
        "ConsoleService/GetConsoleTypes",
        console_pb2.GetConsoleTypesRequest(),
        authenticated=False,
    )
    server.unary(
        "start console, no session",
        "ConsoleService/StartConsole",
        console_pb2.StartConsoleRequest(result_id=console, session_type="python"),
        authenticated=False,
    )
    server.unary(
        "start console of an unknown type",
        "ConsoleService/StartConsole",
        console_pb2.StartConsoleRequest(result_id=export_ticket(9), session_type="groovy"),
    )
    server.unary(
        "start console",
        "ConsoleService/StartConsole",
        console_pb2.StartConsoleRequest(result_id=console, session_type="python"),
    )
    server.unary(
        "start console, type in capitals",
        "ConsoleService/StartConsole",
        console_pb2.StartConsoleRequest(result_id=export_ticket(2), session_type="Python"),
    )
    server.unary(
        "start console on an export id in use",
        "ConsoleService/StartConsole",
        console_pb2.StartConsoleRequest(result_id=console, session_type="python"),
    )
    server.unary("heap info", "ConsoleService/GetHeapInfo", console_pb2.GetHeapInfoRequest())

    # Log lines and field changes that the commands below cause.
    since = int(time.time() * 1_000_000)
    logs, logs_status, logs_cancel = server.open_stream(
        "ConsoleService/SubscribeToLogs",
        console_pb2.LogSubscriptionRequest(
            last_seen_log_timestamp=since, levels=["STDOUT", "STDERR"]
        ),
    )
    fields, fields_status, fields_cancel = server.open_stream(
        "ApplicationService/ListFields", application_pb2.ListFieldsRequest()
    )
    time.sleep(0.5)

    execute(server, "execute without a console id", ticket_pb2.Ticket(), "x = 1")
    execute(server, "execute on an export that does not exist", export_ticket(77), "x = 1")
    execute(server, "import", console, "from deephaven import empty_table")
    execute(server, "create a table", console, 't = empty_table(5).update(["X = i"])')
    execute(server, "replace a table", console, 't = empty_table(3).update(["X = i"])')
    execute(server, "a variable that is no table", console, "n = 41 + 1")
    execute(server, "print", console, 'print("hello from the console")\nprint(n)')
    execute(server, "expression statement", console, "n")
    execute(server, "write to stderr", console, 'import sys\nsys.stderr.write("[probe] to stderr\\n")')
    execute(server, "python exception", console, "1 / 0")
    execute(server, "syntax error", console, "t = = 1")
    execute(server, "undefined name", console, "u = no_such_name")
    execute(
        server,
        "error after an assignment",
        console,
        'v = empty_table(1)\nraise ValueError("after v")',
    )
    execute(server, "bad formula", console, 'w = empty_table(1).update(["X = nosuchcolumn"])')
    execute(server, "two tables and a removal", console, "a = t\nb = t.view(['X'])\ndel v")
    execute(server, "table set to None", console, "a = None")
    execute(server, "empty code", console, "")

    server.unary(
        "fetch the scope variable",
        "TableService/FetchTable",
        table_pb2.FetchTableRequest(
            source_id=table_pb2.TableReference(ticket=ticket_pb2.Ticket(ticket=b"s/t")),
            result_id=export_ticket(3),
        ),
    )
    server.unary(
        "fetch a variable that is no table",
        "TableService/FetchTable",
        table_pb2.FetchTableRequest(
            source_id=table_pb2.TableReference(ticket=ticket_pb2.Ticket(ticket=b"s/n")),
            result_id=export_ticket(4),
        ),
    )
    server.unary(
        "bind a table to a variable",
        "ConsoleService/BindTableToVariable",
        console_pb2.BindTableToVariableRequest(
            console_id=console, variable_name="bound", table_id=export_ticket(3)
        ),
    )
    time.sleep(SETTLE)
    server.unary(
        "bind without a console id",
        "ConsoleService/BindTableToVariable",
        console_pb2.BindTableToVariableRequest(
            variable_name="bound2", table_id=export_ticket(3)
        ),
    )
    time.sleep(SETTLE)
    server.unary(
        "bind without a table id",
        "ConsoleService/BindTableToVariable",
        console_pb2.BindTableToVariableRequest(console_id=console, variable_name="bound3"),
    )
    execute(server, "use the bound variable", console, "c = bound.head(1)\nprint(bound.size)")
    server.unary(
        "cancel command",
        "ConsoleService/CancelCommand",
        console_pb2.CancelCommandRequest(console_id=console, command_id=export_ticket(50)),
    )
    server.unary(
        "cancel autocomplete",
        "ConsoleService/CancelAutoComplete",
        console_pb2.CancelAutoCompleteRequest(console_id=console, request_id=1),
    )

    print("== autocomplete stream")
    print("call: ConsoleService/AutoCompleteStream")
    uri = "inmemory://probe/1"
    requests = [
        console_pb2.AutoCompleteRequest(
            console_id=console,
            request_id=1,
            open_document=console_pb2.OpenDocumentRequest(
                text_document=console_pb2.TextDocumentItem(
                    uri=uri, language_id="python", version=1, text="emp"
                )
            ),
        ),
        console_pb2.AutoCompleteRequest(
            console_id=console,
            request_id=2,
            get_completion_items=console_pb2.GetCompletionItemsRequest(
                context=console_pb2.CompletionContext(trigger_kind=1),
                text_document=console_pb2.VersionedTextDocumentIdentifier(uri=uri, version=1),
                position=console_pb2.Position(line=0, character=3),
            ),
        ),
    ]
    for request in requests:
        print(f"request: {one_line(request)}")
    sent = threading.Event()

    def send():
        yield from requests
        sent.wait(args.listen + 2)

    responses = server.stubs["ConsoleService"].AutoCompleteStream(
        send(), metadata=server.metadata, timeout=CALL_TIMEOUT
    )
    status = "OPEN"
    answers = []

    def stop():
        time.sleep(args.listen + 1)
        responses.cancel()

    threading.Thread(target=stop, daemon=True).start()
    try:
        for response in responses:
            answers.append(response)
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.CANCELLED:
            status = f"{error.code().name}: {scrub(error.details() or '')!r}"
    sent.set()
    print(f"status: {status}")
    for response in answers:
        text = one_line(response)
        print(f"message: {text if len(text) < 600 else text[:600] + ' ...'}")
    print()

    logs_cancel()
    fields_cancel()
    print("== what the commands printed")
    print("call: ConsoleService/SubscribeToLogs")
    print(f"status: {logs_status[0]}")
    for message in logs:
        if message.log_level == "STDOUT" or message.message.startswith("[probe]"):
            print(f"message: {one_line(message)}")
    print()
    print("== field changes seen by a listener during the commands")
    print("call: ApplicationService/ListFields")
    print(f"status: {fields_status[0]}")
    for message in fields:
        for kind in ("created", "updated", "removed"):
            for field in getattr(message, kind):
                print(f"{kind}: {one_line(field)}")
    print()

    server.unary(
        "release the console",
        "SessionService/Release",
        session_pb2.ReleaseRequest(id=console),
    )
    execute(server, "execute on the released console", console, "x = 1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
