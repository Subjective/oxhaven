"""Reading a hierarchical table (a rollup or a tree) as a client does: through a view.

No Java counterpart: this is the harness. A hierarchical table has no rows a client can
fetch. What can be compared between two servers is a snapshot of a view of it: the rows the
server shows for the expansions of a key table (`docs/HIERARCHICAL.md`). `read_view` makes a
view with `HierarchicalTableService/View`, subscribes to it with a Barrage viewport
subscription on `FlightService/DoExchange`, as the web client does, and returns the first
snapshot as an Arrow table.

The client library has neither the service nor Barrage subscriptions, so the subscription
request (a flatbuffer) and the Flight messages (protobuf) are written and read here by hand.
`describe` fetches the object as `ObjectService/FetchObject` gives it.
"""

from __future__ import annotations

import struct
import threading

import pyarrow as pa
from deephaven_core.proto import hierarchicaltable_pb2, hierarchicaltable_pb2_grpc
from deephaven_core.proto import object_pb2, object_pb2_grpc, ticket_pb2
from pydeephaven import Session

MAGIC = 0x6E687064
SUBSCRIPTION_REQUEST = 5
DO_EXCHANGE = "/arrow.flight.protocol.FlightService/DoExchange"
# The most rows of a view that are read: a viewport of this many rows from the first.
MAX_ROWS = 2000
TIMEOUT = 30


def _vector(data: bytes) -> bytes:
    return struct.pack("<I", len(data)) + data + b"\0" * (-len(data) % 4)


def _table(fields: list[bytes | int | None]) -> bytes:
    """A flatbuffer holding one table whose fields are byte vectors, unsigned 32-bit
    numbers, or absent."""
    vtable_size = 4 + 2 * len(fields)
    padding = -(4 + vtable_size) % 4
    table_size = 4 + 4 * len(fields)
    table_at = 4 + vtable_size + padding
    offsets, body, tail = [], b"", b""
    for index, field in enumerate(fields):
        at = 4 + 4 * index
        if field is None:
            offsets.append(0)
            body += struct.pack("<I", 0)
        elif isinstance(field, int):
            offsets.append(at)
            body += struct.pack("<I", field)
        else:
            offsets.append(at)
            body += struct.pack("<I", table_size - at + len(tail))
            tail += _vector(field)
    vtable = struct.pack(f"<HH{len(fields)}H", vtable_size, table_size, *offsets)
    return (struct.pack("<I", table_at) + vtable + b"\0" * padding
            + struct.pack("<i", table_at - 4) + body + tail)


def _first_rows(count: int) -> bytes:
    """The row set `0 .. count - 1` as Barrage writes one
    (`ExternalizableRowSetUtils#writeExternalCompressedDeltas`)."""
    if not 129 < count < 32000:
        raise ValueError("a viewport of 130 to 31999 rows")
    return b"\x14\x02" + struct.pack("<hh", 0, -(count - 1)) + b"\x20"


def subscription_request(ticket: bytes, columns: int, rows: int) -> bytes:
    """The `app_metadata` of a `BarrageSubscriptionRequest` for the first `rows` rows and
    the first `columns` columns of the view behind `ticket`, with default options."""
    column_bits = bytearray((columns + 7) // 8)
    for column in range(columns):
        column_bits[column // 8] |= 1 << (column % 8)
    request = _table([ticket, bytes(column_bits), _first_rows(rows)])
    # the message type is one byte, read from the low byte of its slot
    return _table([MAGIC, SUBSCRIPTION_REQUEST, request])


def _flight_data(app_metadata: bytes) -> bytes:
    """A `FlightData` message with only `app_metadata` (field 3)."""
    return b"\x1a" + _varint(len(app_metadata)) + app_metadata


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        low = value & 0x7F
        value >>= 7
        out.append(low | (0x80 if value else 0))
        if not value:
            return bytes(out)


def _fields(message: bytes) -> dict[int, bytes]:
    """The length-delimited fields of a protobuf message, by number."""
    out: dict[int, bytes] = {}
    at = 0

    def varint() -> int:
        nonlocal at
        value, shift = 0, 0
        while True:
            byte = message[at]
            at += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return value

    while at < len(message):
        tag = varint()
        if tag & 7 != 2:
            raise ValueError(f"FlightData field {tag >> 3} is not length-delimited")
        length = varint()
        out[tag >> 3] = message[at:at + length]
        at += length
    return out


def _ipc(header: bytes, body: bytes = b"") -> bytes:
    padded = header + b"\0" * (-len(header) % 8)
    return struct.pack("<Ii", 0xFFFFFFFF, len(padded)) + padded + body



def _scope(name: str) -> ticket_pb2.Ticket:
    return ticket_pb2.Ticket(ticket=f"s/{name}".encode())


def read_view(session: Session, name: str, keys: str | None = None,
              action_column: str | None = None) -> pa.Table:
    """The first `MAX_ROWS` rows of a view of the hierarchical table in the scope variable
    `name`, expanded as the table in the scope variable `keys` says (`None`: the default
    expansions), with every column."""
    stub = hierarchicaltable_pb2_grpc.HierarchicalTableServiceStub(session.grpc_channel)
    columns = len(describe(session, name)[0])
    export = session.make_export_ticket()
    view = export.pb_ticket
    request = hierarchicaltable_pb2.HierarchicalTableViewRequest(
        result_view_id=view, hierarchical_table_id=_scope(name))
    if keys is not None:
        request.expansions.key_table_id.CopyFrom(_scope(keys))
        if action_column is not None:
            request.expansions.key_table_action_column = action_column
    stub.View(request, metadata=session.grpc_metadata, timeout=TIMEOUT)
    try:
        exchange = session.grpc_channel.stream_stream(DO_EXCHANGE)
        first = _flight_data(subscription_request(view.ticket, columns, MAX_ROWS))
        # The subscription lives while the client's side of the call is open.
        done = threading.Event()

        def requests():
            yield first
            done.wait(TIMEOUT)

        call = exchange(requests(), metadata=session.grpc_metadata, timeout=TIMEOUT)
        try:
            schema = _fields(next(call))
            batch = _fields(next(call))
        finally:
            done.set()
            call.cancel()
    finally:
        session.release(export)
    stream = _ipc(schema[2]) + _ipc(batch[2], batch.get(1000, b""))
    return _distinct_names(pa.ipc.open_stream(stream).read_all())


def _distinct_names(table: pa.Table) -> pa.Table:
    """`table` with every column name once: a later column of a name that an earlier one
    has is named `<name>#2`, `<name>#3`. A rollup with constituents sends the group-by
    columns of its levels and the columns of its source under their own names, and the
    comparison and the recordings name a column by its name."""
    seen: dict[str, int] = {}
    names = []
    for name in table.schema.names:
        seen[name] = seen.get(name, 0) + 1
        names.append(name if seen[name] == 1 else f"{name}#{seen[name]}")
    return table if names == table.schema.names else table.rename_columns(names)


def describe(session: Session, name: str) -> tuple[pa.Schema, bool]:
    """The snapshot schema and `is_static` of the hierarchical table in the scope variable
    `name`, as `ObjectService/FetchObject` answers."""
    stub = object_pb2_grpc.ObjectServiceStub(session.grpc_channel)
    answer = stub.FetchObject(
        object_pb2.FetchObjectRequest(source_id=ticket_pb2.TypedTicket(
            ticket=_scope(name), type="HierarchicalTable")),
        metadata=session.grpc_metadata, timeout=TIMEOUT)
    descriptor = hierarchicaltable_pb2.HierarchicalTableDescriptor.FromString(answer.data)
    schema = pa.ipc.read_schema(pa.py_buffer(descriptor.snapshot_schema))
    return schema, descriptor.is_static
