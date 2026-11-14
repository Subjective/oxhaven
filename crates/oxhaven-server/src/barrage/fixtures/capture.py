"""Captures the Barrage fixtures in this directory from the Java reference server.

    oracle/bin/start-java.sh 10850
    oracle/.venv/bin/python crates/oxhaven-server/src/barrage/fixtures/capture.py \
        dh+psk://oxhaven@127.0.0.1:10850
    oracle/bin/stop-java.sh 10850

Every `DoExchange` call goes over plain gRPC with the message bytes untouched. A case is one
call, in `<case>.exchange`: records of one kind byte, a length as u32 LE, and that many
bytes. The kinds:

    0   a FlightData message the client sent
    1   a FlightData message the server sent
    2   the name of a step: what the script did next, before waiting for the server
    3   how the call ended: the gRPC status code name, a colon, a space, and the message

`statuses.txt` has one line per malformed call: what was sent, and how the call ended.

The steps of a ticking case change a keyed input table; `tests.rs` replays the same steps on
this server's input table. `main` says what each case is.
"""

from __future__ import annotations

import os
import queue
import re
import struct
import sys
import threading

import grpc
import pyarrow as pa
from pydeephaven import Session

HERE = os.path.dirname(os.path.abspath(__file__))
PSK_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"
EXCHANGE = "/arrow.flight.protocol.FlightService/DoExchange"
MAGIC = 0x6E687064
SUBSCRIPTION_REQUEST, SNAPSHOT_REQUEST = 5, 7
# how long the server is given to send what a step causes
QUIET_SECONDS = 2.0


def identity(data: bytes) -> bytes:
    return data


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        low = value & 0x7F
        value >>= 7
        if value:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def flight_data(app_metadata: bytes | None) -> bytes:
    """A FlightData with nothing but `app_metadata` (field 3)."""
    if app_metadata is None:
        return b""
    return varint(3 << 3 | 2) + varint(len(app_metadata)) + app_metadata


# A flatbuffer writer for the three request tables. A field is (kind, value): "bool", "i8",
# "i32", "u32" and "i64" scalars, "bytes" for a vector of bytes, "table" for a table built
# by `table`; None leaves the slot out.

SCALARS = {"bool": "<?", "i8": "<b", "i32": "<i", "u32": "<I", "i64": "<q"}


def pad(data: bytes, alignment: int = 8) -> bytes:
    return data + b"\0" * (-len(data) % alignment)


def table(fields: list) -> tuple[bytes, int]:
    """A table and what it refers to, as bytes that may be put at any offset that is a
    multiple of 8, and the offset of the table within them."""
    vtable_size = 4 + 2 * len(fields)
    start = len(pad(b"\0" * vtable_size))
    inline = bytearray(struct.pack("<i", start))  # back to the vtable, which is at 0
    slots, references = [], []
    for item in fields:
        if item is None:
            slots.append(0)
            continue
        kind, value = item
        size = struct.calcsize(SCALARS.get(kind, "<I"))
        inline += b"\0" * (-len(inline) % size)
        slots.append(len(inline))
        if kind in SCALARS:
            inline += struct.pack(SCALARS[kind], value)
        else:
            references.append((len(inline), kind, value))
            inline += b"\0\0\0\0"
    inline = bytearray(pad(bytes(inline)))
    tail = b""
    for position, kind, value in references:
        target = start + len(inline) + len(tail)
        if kind == "bytes":
            tail += pad(struct.pack("<I", len(value)) + value)
        else:
            child, child_start = value
            target += child_start
            tail += pad(child)
        struct.pack_into("<I", inline, position, target - (start + position))
    vtable = struct.pack("<HH", vtable_size, len(inline)) + b"".join(
        struct.pack("<H", slot) for slot in slots)
    return pad(vtable) + bytes(inline) + tail, start


def finished(root: tuple[bytes, int]) -> bytes:
    data, start = root
    return struct.pack("<II", 8 + start, 0) + data


def wrapper(msg_type: int, payload: bytes | None, magic: int = MAGIC) -> bytes:
    """A BarrageMessageWrapper."""
    return finished(table([
        ("u32", magic), ("i8", msg_type), None if payload is None else ("bytes", payload)]))


def row_set(*ranges: tuple[int, int]) -> bytes:
    """Ranges of row keys as Barrage sends a row set, every value as a long."""
    out, offset = b"", 0
    for first, last in ranges:
        out += b"\x0b" + struct.pack("<q", first - offset)
        if last != first:
            out += b"\x0b" + struct.pack("<q", -(last - first))
        offset = last
    return out + b"\x20"


def bit_set(*columns: int) -> bytes:
    out = bytearray(max(columns) // 8 + 1)
    for column in columns:
        out[column // 8] |= 1 << (column % 8)
    return bytes(out)


def subscription(ticket: bytes | None, columns: bytes | None = None,
                 viewport: bytes | None = None, reverse: bool = False,
                 nulls: bool = False, options: bool = True, as_list: bool = False) -> bytes:
    """A BarrageSubscriptionRequest in its wrapper. The update interval is 100 ms."""
    subscription_options = table([
        None, ("bool", nulls), ("i32", 100), ("i32", 0), ("i32", 0), ("bool", as_list), ("i64", 0)])
    return wrapper(SUBSCRIPTION_REQUEST, finished(table([
        None if ticket is None else ("bytes", ticket),
        None if columns is None else ("bytes", columns),
        None if viewport is None else ("bytes", viewport),
        ("table", subscription_options) if options else None,
        ("bool", reverse)])))


def snapshot(ticket: bytes, columns: bytes | None = None, viewport: bytes | None = None,
             reverse: bool = False, nulls: bool = False) -> bytes:
    """A BarrageSnapshotRequest in its wrapper."""
    snapshot_options = table([None, ("bool", nulls), ("i32", 0), ("i32", 0), ("i64", 0)])
    return wrapper(SNAPSHOT_REQUEST, finished(table([
        ("bytes", ticket),
        None if columns is None else ("bytes", columns),
        None if viewport is None else ("bytes", viewport),
        ("table", snapshot_options),
        ("bool", reverse)])))


class Exchange:
    """One DoExchange call, and the record of it."""

    def __init__(self, channel, metadata):
        self.records: list[tuple[int, bytes]] = []
        self.requests: queue.Queue = queue.Queue()
        self.received: queue.Queue = queue.Queue()
        self.ended: str | None = None
        call = channel.stream_stream(
            EXCHANGE, request_serializer=identity, response_deserializer=identity)
        self.responses = call(iter(self.requests.get, None), metadata=metadata)
        threading.Thread(target=self.read, daemon=True).start()

    def read(self) -> None:
        try:
            for message in self.responses:
                self.received.put((1, message))
            self.received.put((3, b"OK: "))
        except grpc.RpcError as error:
            self.received.put((3, f"{error.code().name}: {error.details()}".encode()))

    def step(self, name: str) -> None:
        self.records.append((2, name.encode()))

    def send(self, app_metadata: bytes | None) -> None:
        message = flight_data(app_metadata)
        self.records.append((0, message))
        self.requests.put(message)

    def collect(self) -> None:
        """Waits until the server has sent nothing for a while, or ended the call."""
        while self.ended is None:
            try:
                kind, data = self.received.get(timeout=QUIET_SECONDS)
            except queue.Empty:
                return
            self.records.append((kind, data))
            if kind == 3:
                self.ended = data.decode()

    def close(self) -> None:
        """Closes the client's side and waits for the server to end the call."""
        self.step("close")
        self.requests.put(None)
        self.collect()
        if self.ended is None:
            self.responses.cancel()
            self.records.append((3, b"never ended"))
            self.ended = "never ended"

    def framed(self) -> bytes:
        return b"".join(
            bytes([kind]) + struct.pack("<I", len(data)) + data for kind, data in self.records)


class Capture:
    def __init__(self, url: str):
        key, address = url.removeprefix("dh+psk://").split("@")
        host, port = address.split(":")
        self.session = Session(host=host, port=int(port), auth_type=PSK_AUTH_TYPE, auth_token=key)
        self.channel = grpc.insecure_channel(address)
        self.statuses: list[str] = []

    def exchange(self) -> Exchange:
        return Exchange(self.channel, self.session.grpc_metadata)

    def write(self, name: str, data: bytes) -> None:
        with open(os.path.join(HERE, name), "wb") as out:
            out.write(data)

    def one_request(self, case: str, request: bytes) -> None:
        """A call of one request, closed once the server has answered it."""
        exchange = self.exchange()
        exchange.step("request")
        exchange.send(request)
        exchange.collect()
        exchange.close()
        self.write(f"{case}.exchange", exchange.framed())

    def status(self, what: str, *requests: bytes | None) -> None:
        """How a call ends that sends `requests` and then waits."""
        exchange = self.exchange()
        for request in requests:
            exchange.send(request)
            exchange.collect()
        if exchange.ended is None:
            exchange.close()
            ended = "open until the client closes, then " + exchange.ended
        else:
            ended = exchange.ended
        # the marshaller names itself by its identity hash
        ended = re.sub(r"DoExchangeMarshaller\{[0-9a-f]+\}", "DoExchangeMarshaller{#}", ended)
        ended = re.sub(r"Details Logged w/ID '[0-9a-f-]+'", "Details Logged w/ID '#'", ended)
        self.statuses.append(f"{what}: {ended}")


INPUT_SCHEMA = pa.schema([("K", pa.int32()), ("S", pa.string()), ("V", pa.float64())])


def ticket_of(table) -> bytes:
    return table.ticket.pb_ticket.ticket


def main(url: str) -> None:
    capture = Capture(url)
    session = capture.session

    def rows(keys: list[int], changed: bool = False):
        """Row `k` is (k, "s<k>", k + 0.5), or (k, "m<k>", 10 k) once changed; the row of
        key 4 has no `S` and no `V` until it is changed."""
        return session.import_table(pa.table({
            "K": pa.array(keys, pa.int32()),
            "S": [(f"m{k}" if changed else None if k == 4 else f"s{k}") for k in keys],
            "V": [(10.0 * k if changed else None if k == 4 else k + 0.5) for k in keys],
        }, schema=INPUT_SCHEMA))

    def keys_of(keys: list[int]):
        return session.import_table(pa.table({"K": pa.array(keys, pa.int32())}))

    def ticking(case: str, columns: bytes | None, viewport: bytes | None, reverse: bool,
                changed_viewport: bytes | None, nulls: bool = False,
                as_list: bool = False) -> None:
        """A subscription to a keyed input table of the keys 1 to 6, through these steps.
        A viewport subscription then changes its viewport and sees one more step."""
        keyed = session.input_table(schema=INPUT_SCHEMA, key_cols=["K"])
        keyed.add(rows([1, 2, 3, 4, 5, 6]))
        exchange = capture.exchange()
        ticket = ticket_of(keyed)

        exchange.step("subscribe")
        exchange.send(subscription(ticket, columns, viewport, reverse, nulls, as_list=as_list))
        exchange.collect()
        exchange.step("add 7 8")
        keyed.add(rows([7, 8]))
        exchange.collect()
        exchange.step("change 2")
        keyed.add(rows([2], changed=True))
        exchange.collect()
        exchange.step("delete 1")
        keyed.delete(keys_of([1]))
        exchange.collect()
        exchange.step("add 9, change 3 4, delete 5")
        keyed.add(rows([9]))
        keyed.add(rows([3, 4], changed=True))
        keyed.delete(keys_of([5]))
        exchange.collect()
        if changed_viewport is not None:
            exchange.step("viewport")
            exchange.send(subscription(
                ticket, columns, changed_viewport, reverse, nulls, as_list=as_list))
            exchange.collect()
            exchange.step("add 10, delete 2")
            keyed.add(rows([10]))
            keyed.delete(keys_of([2]))
            exchange.collect()
        exchange.close()
        capture.write(f"{case}.exchange", exchange.framed())

    # Subscriptions to a ticking table: the whole table; a viewport of the positions 1 to
    # 3, changed to 0 to 1; the last two rows, changed to the last four; the first and
    # third column of a viewport larger than the table, with Deephaven nulls.
    ticking("sub_full", None, None, False, None)
    ticking("sub_viewport", None, row_set((1, 3)), False, row_set((0, 1)))
    ticking("sub_reverse", None, row_set((0, 1)), True, row_set((0, 3)))
    ticking("sub_columns", bit_set(0, 2), row_set((0, 99)), False, row_set((2, 4)), nulls=True)
    # What the C++ client asks for: each column of a record batch as one list.
    ticking("sub_as_list", None, None, False, None, as_list=True)
    ticking("sub_as_list_viewport", None, row_set((1, 3)), False, row_set((0, 1)), as_list=True)

    # The static tables of the Flight fixtures: every column type, with a null row.
    formulas = [
        "Bo = i==1 ? (Boolean)null : (Boolean)(i==0)",
        "By = i==1 ? NULL_BYTE : (byte)(i+1)",
        "Ch = i==1 ? NULL_CHAR : (char)('a'+i)",
        "Sh = i==1 ? NULL_SHORT : (short)(i+1)",
        "In = i==1 ? NULL_INT : i+1",
        "Lo = i==1 ? NULL_LONG : (long)(i+1)",
        "Fl = i==1 ? NULL_FLOAT : (float)(i+1.5)",
        "Do = i==1 ? NULL_DOUBLE : (double)(i+1.5)",
        "St = i==1 ? (String)null : `s`+i",
        "Ti = i==1 ? (Instant)null : epochNanosToInstant(1700000000000000001L+i)",
    ]
    every_type = session.empty_table(3).update(formulas)
    many_rows = session.empty_table(10000).update(["In = i"])

    # What the IDE sends: a viewport larger than the table, with Deephaven nulls; then a
    # viewport of the second row alone.
    exchange = capture.exchange()
    exchange.step("subscribe")
    exchange.send(subscription(ticket_of(every_type), None, row_set((0, 88)), nulls=True))
    exchange.collect()
    exchange.step("viewport")
    exchange.send(subscription(ticket_of(every_type), None, row_set((1, 1)), nulls=True))
    exchange.collect()
    exchange.close()
    capture.write("sub_static_viewport.exchange", exchange.framed())

    # The whole of static tables: with Arrow nulls and no options at all; and one too
    # large for a single snapshot.
    capture.one_request("sub_static_full", subscription(ticket_of(every_type), options=False))
    capture.one_request("sub_static_many", subscription(ticket_of(many_rows)))

    # Snapshot requests: a whole static table with Arrow and with Deephaven nulls; one
    # sent in parts; viewports from either end, with a column subset; a ticking table.
    capture.one_request("snap_static", snapshot(ticket_of(every_type)))
    capture.one_request("snap_static_nulls", snapshot(ticket_of(every_type), nulls=True))
    capture.one_request("snap_static_many", snapshot(ticket_of(many_rows)))
    capture.one_request(
        "snap_static_viewport",
        snapshot(ticket_of(every_type), bit_set(4, 8), row_set((1, 5))))
    capture.one_request(
        "snap_static_reverse",
        snapshot(ticket_of(many_rows), None, row_set((0, 1), (5, 5)), reverse=True))
    keyed = session.input_table(schema=INPUT_SCHEMA, key_cols=["K"])
    keyed.add(rows([1, 2, 3, 4, 5, 6]))
    capture.one_request("snap_ticking", snapshot(ticket_of(keyed)))
    capture.one_request(
        "snap_ticking_viewport", snapshot(ticket_of(keyed), bit_set(1), row_set((1, 2))))
    capture.one_request(
        "snap_ticking_reverse",
        snapshot(ticket_of(keyed), None, row_set((0, 0), (9, 9)), reverse=True))
    empty = session.input_table(schema=INPUT_SCHEMA, key_cols=["K"])
    capture.one_request("snap_ticking_empty", snapshot(ticket_of(empty)))

    # Calls that are malformed, or ask for what there is not.
    table_ticket = ticket_of(every_type)
    whole = subscription(table_ticket)
    missing = b"e" + struct.pack("<i", 999999)
    capture.status("a message type that is not a request", wrapper(4, b""))
    capture.status("no metadata, then a subscription", None, subscription(table_ticket, None, row_set((0, 0))))
    capture.status("no metadata twice", None, None)
    capture.status("metadata with another magic number", wrapper(SUBSCRIPTION_REQUEST, b"", magic=1), None)
    capture.status("a subscription without a payload", wrapper(SUBSCRIPTION_REQUEST, None))
    capture.status("a subscription without a ticket", subscription(None))
    capture.status("a subscription to an export that does not exist", subscription(missing))
    capture.status("a subscription with a ticket of no known kind", subscription(b"?abc"))
    capture.status("a subscription, then a snapshot request", whole, snapshot(table_ticket))
    capture.status("a subscription, then no metadata", whole, None)
    capture.status("a subscription to the whole table, then a viewport",
                   whole, subscription(table_ticket, None, row_set((0, 0))))
    capture.status("a viewport, then the whole table",
                   subscription(table_ticket, None, row_set((0, 0))), whole)
    capture.status("a snapshot, then a subscription", snapshot(table_ticket), whole)
    capture.status("a snapshot of an export that does not exist", snapshot(missing))
    capture.status("two snapshots", snapshot(table_ticket), snapshot(table_ticket))
    capture.write("statuses.txt", ("\n".join(capture.statuses) + "\n").encode())


if __name__ == "__main__":
    main(sys.argv[1])
