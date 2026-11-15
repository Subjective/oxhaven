#!/usr/bin/env python3
"""Sends one fixed sequence of console calls to a server and prints what it answers.

No Java counterpart: test tooling. Run it against the Java reference and against Oxhaven and
compare the two outputs; `oracle/console/README.md` has the commands and the differences
that are expected.

    oracle/.venv/bin/python oracle/console/probe.py 127.0.0.1:10916 --psk oxhaven

Each case prints the call, the request and the status, then one line per response message in
protobuf text format. What differs from run to run is replaced: timestamps by `<ts>`, memory
figures by `<n>`, error ids by `<id>`, line numbers in the files of a package by `<n>`; the lines of a Java stack trace in an error message
are left out. A server stream is cancelled at the end (`OPEN`). The two streams that run
beside the commands are printed after them: of the log, the records of level `STDOUT` and
the `STDERR` records that start with `[probe]` (Java's `STDERR` also carries its own log);
of the server's own log, the records that hold `[probe-log]`, without the names of the
thread and the logger; of the field changes, one line per change. The script waits after each command so that every
change is sent by itself. The sequence leaves variables in the query scope, so use a server
that was just started.
"""

import argparse
import re
import sys
import threading
import time

import grpc
import pyarrow
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
    (
        re.compile(r"(start_timestamp|end_timestamp|micros|last_seen_log_timestamp): \d+"),
        r"\1: <ts>",
    ),
    (re.compile(r"(max_memory|total_memory|free_memory): \d+"), r"\1: <n>"),
    (re.compile(r"Details Logged w/ID '[0-9a-f-]+'"), "Details Logged w/ID '<id>'"),
    (re.compile(r"0x[0-9a-f]{6,}"), "0x<addr>"),
    (re.compile(r'File \\"[^"\\]*/site-packages/'), 'File \\"<site-packages>/'),
    # a line of a package's source moves with every change to the package
    (re.compile(r'(File \\"<(?:site-packages|oxhaven)>/[^"\\]*\\", line )\d+'), r"\1<n>"),
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


def print_columns(name: str, response):
    """The names and Deephaven types of the columns of a table creation response."""
    print(f"== {name}")
    if response is None or not response.schema_header:
        print("columns: none")
    else:
        schema = pyarrow.ipc.open_stream(response.schema_header).schema
        columns = [
            f"{field.name} {(field.metadata or {}).get(b'deephaven:type', b'?').decode()}"
            for field in schema
        ]
        print(f"columns: {', '.join(columns)}")
    print()


def execute(server: Server, name: str, console, code: str):
    request = console_pb2.ExecuteCommandRequest(console_id=console, code=code)
    response = server.unary(name, "ConsoleService/ExecuteCommand", request)
    # longer than the interval in which a server gathers field changes into one message
    time.sleep(SETTLE)
    return response


def open_document(console, uri: str, text: str, version: int = 1):
    return console_pb2.AutoCompleteRequest(
        console_id=console,
        open_document=console_pb2.OpenDocumentRequest(
            text_document=console_pb2.TextDocumentItem(
                uri=uri, language_id="python", version=version, text=text
            )
        ),
    )


def change_document(console, uri: str, version: int, line: int, character: int, length: int, text: str):
    """An edit that puts `text` in place of `length` code units at a position."""
    position = console_pb2.Position(line=line, character=character)
    change = console_pb2.ChangeDocumentRequest.TextDocumentContentChangeEvent(
        range=console_pb2.DocumentRange(start=position, end=position), range_length=length, text=text
    )
    return console_pb2.AutoCompleteRequest(
        console_id=console,
        change_document=console_pb2.ChangeDocumentRequest(
            text_document=console_pb2.VersionedTextDocumentIdentifier(uri=uri, version=version),
            content_changes=[change],
        ),
    )


def close_document(console, uri: str):
    return console_pb2.AutoCompleteRequest(
        console_id=console,
        close_document=console_pb2.CloseDocumentRequest(
            text_document=console_pb2.VersionedTextDocumentIdentifier(uri=uri)
        ),
    )


def completion_items(console, request_id: int, uri: str, version: int, line: int, character: int):
    return console_pb2.AutoCompleteRequest(
        console_id=console,
        request_id=request_id,
        get_completion_items=console_pb2.GetCompletionItemsRequest(
            context=console_pb2.CompletionContext(trigger_kind=1),
            text_document=console_pb2.VersionedTextDocumentIdentifier(uri=uri, version=version),
            position=console_pb2.Position(line=line, character=character),
        ),
    )


def position_request(console, request_id: int, kind: str, uri: str, version: int, line: int, character: int):
    """A `get_signature_help` or `get_hover` request."""
    request = console_pb2.AutoCompleteRequest(console_id=console, request_id=request_id)
    inner = getattr(request, kind)
    inner.text_document.uri = uri
    inner.text_document.version = version
    inner.position.line = line
    inner.position.character = character
    return request


def print_answers(status: str, answers, limit: int, labels_only: bool):
    """The answers of a completion stream. Java answers requests in no fixed order, so they
    are printed by request id; of `labels_only` answers, the labels and details of the items."""
    print(f"status: {status}")
    for response in sorted(answers, key=lambda answer: answer.request_id):
        if labels_only and response.HasField("completion_items"):
            print(f"message: request_id: {response.request_id} success: {str(response.success).lower()}")
            for item in response.completion_items.items:
                edit = item.text_edit.range
                print(
                    f"item: {item.label!r} detail: {item.detail!r} sort_text: {item.sort_text!r} "
                    f"start: {item.start} length: {item.length} replaces: "
                    f"{edit.start.line}:{edit.start.character}-{edit.end.line}:{edit.end.character}"
                )
            continue
        text = one_line(response)
        print(f"message: {text if len(text) < limit else text[:limit] + ' ...'}")
    print()


def autocomplete(server: Server, name: str, requests, limit: int = 600, labels_only: bool = False):
    """One completion stream: sends the requests, listens, and cancels the stream (`OPEN`)
    unless the server ended it."""
    print(f"== {name}")
    print("call: ConsoleService/AutoCompleteStream")
    for request in requests:
        print(f"request: {one_line(request)}")
    sent = threading.Event()

    def send():
        for request in requests:
            yield request
            time.sleep(0.1)
        sent.wait(server.listen + 2)

    responses = server.stubs["ConsoleService"].AutoCompleteStream(
        send(), metadata=server.metadata, timeout=CALL_TIMEOUT
    )
    status = "OPEN"
    answers = []

    def stop():
        time.sleep(server.listen + 1 + 0.1 * len(requests))
        responses.cancel()

    threading.Thread(target=stop, daemon=True).start()
    try:
        for response in responses:
            answers.append(response)
        status = "OK"
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.CANCELLED:
            status = f"{error.code().name}: {scrub(error.details() or '')!r}"
    sent.set()
    print_answers(status, answers, limit, labels_only)


def browser_autocomplete(server: Server, name: str, export_id: int, requests):
    """The pair a browser uses in place of the stream: the first request opens a server
    stream, the others are one call each, the last with the half close; all carry the
    stream's export id and their place in the sequence as headers."""
    print(f"== {name}")
    stub = server.stubs["ConsoleService"]

    def headers(sequence: int, half_close: bool):
        stream = [
            ("x-deephaven-stream-ticket", str(export_id)),
            ("x-deephaven-stream-sequence", str(sequence)),
        ]
        return server.metadata + stream + ([("x-deephaven-stream-halfclose", "1")] if half_close else [])

    print("call: ConsoleService/OpenAutoCompleteStream")
    print(f"request: {one_line(requests[0])}")
    responses = stub.OpenAutoCompleteStream(requests[0], metadata=headers(0, False), timeout=CALL_TIMEOUT)
    answers = []
    status = ["OPEN"]

    def read():
        try:
            for response in responses:
                answers.append(response)
            status[0] = "OK"
        except grpc.RpcError as error:
            status[0] = f"{error.code().name}: {scrub(error.details() or '')!r}"

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    time.sleep(0.3)
    for sequence, request in enumerate(requests[1:], start=1):
        last = sequence == len(requests) - 1
        print(f"call: ConsoleService/NextAutoCompleteStream{' (half close)' if last else ''}")
        print(f"request: {one_line(request)}")
        try:
            answer = stub.NextAutoCompleteStream(request, metadata=headers(sequence, last), timeout=CALL_TIMEOUT)
            print("status: OK")
            print(f"message: {one_line(answer)}")
        except grpc.RpcError as error:
            print(f"status: {error.code().name}: {scrub(error.details() or '')!r}")
        # the answer to a request comes on the open call; the half close must not overtake it
        time.sleep(server.listen)
    reader.join(server.listen + 5)
    print("the open call:")
    print_answers(status[0], answers, 600, False)


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
    server_log, server_log_status, server_log_cancel = server.open_stream(
        "ConsoleService/SubscribeToLogs",
        console_pb2.LogSubscriptionRequest(last_seen_log_timestamp=since, levels=["INFO"]),
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

    # The query scope is one set of variables for Python and for clients.
    server.unary(
        "export a variable that is no table",
        "SessionService/ExportFromTicket",
        session_pb2.ExportRequest(
            source_id=ticket_pb2.Ticket(ticket=b"s/n"), result_id=export_ticket(5)
        ),
    )
    server.unary(
        "publish the object to a scope ticket",
        "SessionService/PublishFromTicket",
        session_pb2.PublishRequest(
            source_id=export_ticket(5), result_id=ticket_pb2.Ticket(ticket=b"s/m")
        ),
    )
    time.sleep(SETTLE)
    server.unary(
        "publish a table to a scope ticket",
        "SessionService/PublishFromTicket",
        session_pb2.PublishRequest(
            source_id=export_ticket(3), result_id=ticket_pb2.Ticket(ticket=b"s/published")
        ),
    )
    time.sleep(SETTLE)
    execute(
        server,
        "read what was published in Python",
        console,
        "print(m, m is n)\n"
        "print(type(published).__module__, type(published).__name__, published.size)\n"
        "d = published.head(2)",
    )
    server.unary(
        "export a variable that does not exist",
        "SessionService/ExportFromTicket",
        session_pb2.ExportRequest(
            source_id=ticket_pb2.Ticket(ticket=b"s/nothing"), result_id=export_ticket(6)
        ),
    )
    execute(
        server,
        "a variable set by a thread",
        console,
        "import threading\n"
        "def _later():\n    global threaded\n    threaded = empty_table(2)\n"
        "threading.Timer(0.3, _later).start()",
    )
    time.sleep(1.0)
    server.unary(
        "fetch the variable the thread set",
        "TableService/FetchTable",
        table_pb2.FetchTableRequest(
            source_id=table_pb2.TableReference(ticket=ticket_pb2.Ticket(ticket=b"s/threaded")),
            result_id=export_ticket(7),
        ),
    )
    execute(
        server,
        "scope variables in formulas",
        console,
        "xi = 41\nxl = 2**40\nxf = 1.5\nxs = 'abc'\nxb = True\n"
        "r = empty_table(3).update(['A = xi', 'B = xi + i', 'C = xl', 'D = xf * 2', "
        "'E = xs + `d`', 'F = xb'])\n"
        "print(r.where('B > xi').size, r.where('E = `abcd` && xb').size)",
    )
    fetched = server.unary(
        "fetch the table the formulas made",
        "TableService/FetchTable",
        table_pb2.FetchTableRequest(
            source_id=table_pb2.TableReference(ticket=ticket_pb2.Ticket(ticket=b"s/r")),
            result_id=export_ticket(8),
        ),
    )
    print_columns("the columns of that table", fetched)
    updated = server.unary(
        "a scope variable in the formula of a client",
        "TableService/Update",
        table_pb2.SelectOrUpdateRequest(
            source_id=table_pb2.TableReference(ticket=ticket_pb2.Ticket(ticket=b"s/t")),
            result_id=export_ticket(10),
            column_specs=["Z = xi + X", "S = xs"],
        ),
    )
    print_columns("the columns of the client's table", updated)
    execute(
        server,
        "a formula in a function names its locals",
        console,
        "def _local():\n    xi = 'local'\n    zz = 2\n"
        "    return empty_table(1).update(['A = xi', 'B = zz'])\n"
        "local = _local()",
    )
    fetched = server.unary(
        "fetch the table the function made",
        "TableService/FetchTable",
        table_pb2.FetchTableRequest(
            source_id=table_pb2.TableReference(ticket=ticket_pb2.Ticket(ticket=b"s/local")),
            result_id=export_ticket(11),
        ),
    )
    print_columns("the columns of the function's table", fetched)
    execute(server, "a formula that negates a variable", console, "neg = t.update('Y = -xi')")
    execute(server, "a formula that names a list", console, "lst = [1, 2]\nbad = t.update('Y = lst')")
    execute(server, "a command the server logs", console, "logged = 1  # [probe-log]")

    # A command whose call the client cancels while it runs, and the command after it.
    code = "import time\nran_on = False\ntime.sleep(3)\nran_on = True"
    request = console_pb2.ExecuteCommandRequest(console_id=console, code=code)
    method = server.head("a command whose call is cancelled", "ConsoleService/ExecuteCommand", request)
    call = method.future(request, metadata=server.metadata)
    time.sleep(0.5)
    call.cancel()
    try:
        call.result()
        print("status: OK")
    except grpc.FutureCancelledError:
        print("status: cancelled by the client after 0.5 seconds")
    except grpc.RpcError as error:
        print(f"status: {error.code().name}: {scrub(error.details() or '')!r}")
    print()
    execute(
        server,
        "the command after the cancelled one",
        console,
        "print('the cancelled command ran to its end:', ran_on)",
    )

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

    execute(
        server,
        "define a function to complete",
        console,
        "def scaled(table, factor: int = 2) -> int:\n"
        '    """Multiplies the size of a table.\n\n'
        "    Args:\n"
        "        table (Table): the table\n"
        "        factor (int): what to multiply by\n\n"
        "    Returns:\n"
        "        the product\n"
        '    """\n'
        "    return table.size * factor",
    )
    uri = "inmemory://probe/1"
    autocomplete(
        server,
        "autocomplete stream",
        [open_document(console, uri, "emp"), completion_items(console, 2, uri, 1, 0, 3)],
    )
    autocomplete(
        server,
        "autocomplete: a signature and a hover of a function of the script",
        [
            open_document(console, uri, "scaled(t, "),
            position_request(console, 3, "get_signature_help", uri, 1, 0, 10),
            position_request(console, 4, "get_hover", uri, 1, 0, 3),
        ],
        limit=2000,
    )
    autocomplete(
        server,
        "autocomplete: an attribute of a table variable, typed in two edits",
        [
            open_document(console, uri, "x = 1\n"),
            change_document(console, uri, 2, 1, 0, 0, "t."),
            change_document(console, uri, 3, 1, 2, 0, "he"),
            completion_items(console, 5, uri, 3, 1, 4),
        ],
        labels_only=True,
    )
    autocomplete(
        server,
        "autocomplete: requests that find nothing, and diagnostics",
        [
            open_document(console, uri, "no_such_name_at_all"),
            completion_items(console, 6, uri, 1, 0, 19),
            position_request(console, 7, "get_signature_help", uri, 1, 0, 19),
            position_request(console, 8, "get_hover", uri, 1, 0, 5),
            console_pb2.AutoCompleteRequest(
                console_id=console,
                request_id=9,
                get_diagnostic=console_pb2.GetDiagnosticRequest(
                    text_document=console_pb2.VersionedTextDocumentIdentifier(uri=uri, version=1)
                ),
            ),
            # a version the document does not have
            completion_items(console, 10, uri, 7, 0, 19),
            close_document(console, uri),
        ],
    )
    autocomplete(
        server,
        "autocomplete: a document that was never opened",
        [change_document(console, "inmemory://probe/none", 2, 0, 0, 0, "x")],
    )
    browser_autocomplete(
        server,
        "autocomplete as a browser sends it",
        export_id=60,
        requests=[
            open_document(console, uri, "empty_t"),
            completion_items(console, 11, uri, 1, 0, 7),
            console_pb2.AutoCompleteRequest(),
        ],
    )

    logs_cancel()
    server_log_cancel()
    fields_cancel()
    print("== what the commands printed")
    print("call: ConsoleService/SubscribeToLogs")
    print(f"status: {logs_status[0]}")
    for message in logs:
        if message.log_level == "STDOUT" or message.message.startswith("[probe]"):
            print(f"message: {one_line(message)}")
    print()
    print("== what the server logged of the marked command")
    print("call: ConsoleService/SubscribeToLogs")
    print(f"status: {server_log_status[0]}")
    for message in server_log:
        if "[probe-log]" in message.message:
            # thread | logger | text: the names differ, the widths and the text do not
            thread, logger, text = message.message.split(" | ", 2)
            print(
                f"record: log_level: {message.log_level!r} thread: {len(thread)} characters "
                f"logger: {len(logger)} characters text: {text!r}"
            )
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

    # The log stream of a session ends when the session does.
    other = Server(args.address, args.psk, args.listen)
    request = console_pb2.LogSubscriptionRequest(
        last_seen_log_timestamp=int(time.time() * 1_000_000), levels=["STDOUT"]
    )
    method = other.head("the log stream of a session that is closed", "ConsoleService/SubscribeToLogs", request)
    stream = method(request, metadata=other.metadata)
    ended = []

    def read_to_the_end():
        try:
            for _ in stream:
                pass
            ended.append("OK")
        except grpc.RpcError as error:
            ended.append(f"{error.code().name}: {scrub(error.details() or '')!r}")

    reader = threading.Thread(target=read_to_the_end, daemon=True)
    reader.start()
    time.sleep(0.5)
    other.stubs["SessionService"].CloseSession(
        session_pb2.HandshakeRequest(), metadata=other.metadata, timeout=CALL_TIMEOUT
    )
    reader.join(5)
    print(f"status: {ended[0] if ended else 'OPEN'}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
