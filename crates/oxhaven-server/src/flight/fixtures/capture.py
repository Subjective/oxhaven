"""Captures the Flight fixtures in this directory from the Java reference server.

    oracle/bin/start-java.sh 10400
    oracle/.venv/bin/python crates/oxhaven-server/src/flight/fixtures/capture.py \
        dh+psk://oxhaven@127.0.0.1:10400
    oracle/bin/stop-java.sh 10400

With `lists` after the address it captures only the cases of `lists`.

Every call goes over plain gRPC with the message bytes untouched, so the files hold exactly
what the server sent (and, for `.put`, what the client sent). Files per case:

    <case>.put      the DoPut request: FlightData messages, each after its length as u32 LE
    <case>.schema   the GetSchema response (a SchemaResult)
    <case>.info     the GetFlightInfo response (a FlightInfo)
    <case>.doget    the DoGet response: FlightData messages, each after its length as u32 LE
    <case>.status   for a case that fails: the gRPC status code name of each call

The comments in `main` say what each case is.
"""

from __future__ import annotations

import math
import os
import struct
import sys

import grpc
import pyarrow as pa
from pydeephaven import Session

HERE = os.path.dirname(os.path.abspath(__file__))
PSK_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"
FLIGHT = "/arrow.flight.protocol.FlightService/"
INPUT_TABLE = "/io.deephaven.proto.backplane.grpc.InputTableService/"
TABLE = "/io.deephaven.proto.backplane.grpc.TableService/"
# The types the web IDE asks to be left as they are.
UNPREVIEWED_TYPES = [
    "byte", "java.lang.Byte", "char", "java.lang.Character", "short", "java.lang.Short",
    "int", "java.lang.Integer", "long", "java.lang.Long", "float", "java.lang.Float",
    "double", "java.lang.Double", "boolean", "java.lang.Boolean", "java.lang.String",
    "java.math.BigDecimal", "java.math.BigInteger", "java.time.Instant",
    "java.time.ZonedDateTime", "java.time.LocalDate", "java.time.LocalTime",
]


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


def field(number: int, payload: bytes) -> bytes:
    return varint(number << 3 | 2) + varint(len(payload)) + payload


def path_descriptor(*path: str) -> bytes:
    """A FlightDescriptor of type PATH."""
    return b"\x08\x01" + b"".join(field(3, part.encode()) for part in path)


def export_ticket(number: int) -> bytes:
    return b"e" + struct.pack("<i", number)


def flight_data(descriptor: bytes | None, header: bytes, body: bytes = b"") -> bytes:
    out = b""
    if descriptor is not None:
        out += field(1, descriptor)
    out += field(2, header)
    if body:
        out += field(1000, body)
    return out


def split_ipc(message: bytes) -> tuple[bytes, bytes]:
    """The flatbuffer header and the body of one encapsulated IPC message."""
    assert message[:4] == b"\xff\xff\xff\xff"
    size = struct.unpack("<i", message[4:8])[0]
    return message[8:8 + size], message[8 + size:]


def put_messages(number: int, schema: pa.Schema, batches: list[pa.RecordBatch]) -> list[bytes]:
    header, _ = split_ipc(schema.serialize().to_pybytes())
    messages = [flight_data(path_descriptor("export", str(number)), header)]
    for batch in batches:
        header, body = split_ipc(batch.serialize().to_pybytes())
        messages.append(flight_data(None, header, body))
    return messages


def framed(messages: list[bytes]) -> bytes:
    return b"".join(struct.pack("<I", len(m)) + m for m in messages)


class Capture:
    def __init__(self, url: str):
        rest = url.removeprefix("dh+psk://")
        key, address = rest.split("@")
        host, port = address.split(":")
        self.session = Session(host=host, port=int(port), auth_type=PSK_AUTH_TYPE, auth_token=key)
        self.channel = grpc.insecure_channel(address)
        self.next_export = 1000

    @property
    def metadata(self):
        return self.session.grpc_metadata

    def unary(self, method: str, request: bytes, service: str = FLIGHT) -> bytes:
        call = self.channel.unary_unary(
            service + method, request_serializer=identity, response_deserializer=identity)
        return call(request, metadata=self.metadata, timeout=15)

    def server_stream(self, method: str, request: bytes) -> list[bytes]:
        call = self.channel.unary_stream(
            FLIGHT + method, request_serializer=identity, response_deserializer=identity)
        return list(call(request, metadata=self.metadata, timeout=15))

    def do_put(self, messages: list[bytes]) -> list[bytes]:
        call = self.channel.stream_stream(
            FLIGHT + "DoPut", request_serializer=identity, response_deserializer=identity)
        return list(call(iter(messages), metadata=self.metadata, timeout=15))

    def write(self, name: str, data: bytes) -> None:
        with open(os.path.join(HERE, name), "wb") as out:
            out.write(data)

    def code(self, action) -> str:
        try:
            action()
        except grpc.RpcError as error:
            return error.code().name
        return "OK"

    def capture_export(self, case: str, number: int) -> None:
        """GetSchema, GetFlightInfo and DoGet of export `number`."""
        descriptor = path_descriptor("export", str(number))
        self.write(f"{case}.schema", self.unary("GetSchema", descriptor))
        self.write(f"{case}.info", self.unary("GetFlightInfo", descriptor))
        self.write(f"{case}.doget",
                   framed(self.server_stream("DoGet", field(1, export_ticket(number)))))

    def capture_table(self, case: str, table) -> None:
        self.capture_export(case, struct.unpack("<i", table.ticket.pb_ticket.ticket[1:])[0])

    def capture_put(self, case: str, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
        number = self.next_export
        self.next_export += 1
        messages = put_messages(number, schema, batches)
        self.write(f"{case}.put", framed(messages))
        self.do_put(messages)
        self.capture_export(case, number)

    def capture_preview(self, case: str, source: int, number: int) -> None:
        """`ApplyPreviewColumns` on export `source` as the web IDE calls it, into export
        `number`."""
        request = (field(1, field(1, field(1, export_ticket(source))))
                   + field(2, field(1, export_ticket(number))) + b"\x18\x01"
                   + b"".join(field(4, name.encode()) for name in UNPREVIEWED_TYPES))
        self.unary("ApplyPreviewColumns", request, TABLE)
        self.capture_export(case, number)

    def status_of_put(self, case: str, messages: list[bytes], number: int) -> None:
        self.write(f"{case}.put", framed(messages))
        descriptor = path_descriptor("export", str(number))
        codes = [
            "DoPut " + self.code(lambda: self.do_put(messages)),
            "GetSchema " + self.code(lambda: self.unary("GetSchema", descriptor)),
            "DoGet " + self.code(
                lambda: self.server_stream("DoGet", field(1, export_ticket(number)))),
        ]
        self.write(f"{case}.status", ("\n".join(codes) + "\n").encode())


ALL_TYPES = [
    ("Bo", pa.bool_(), [True, None, False]),
    ("By", pa.int8(), [1, None, -127]),
    ("Ch", pa.uint16(), [97, None, 0x1F0]),
    ("Sh", pa.int16(), [1, None, 32767]),
    ("In", pa.int32(), [1, None, 2147483647]),
    ("Lo", pa.int64(), [1, None, 9223372036854775807]),
    ("Fl", pa.float32(), [1.5, None, math.nan]),
    ("Do", pa.float64(), [1.5, None, math.inf]),
    ("St", pa.string(), ["a", None, "é𝄞"]),
    ("Ti", pa.timestamp("ns", tz="UTC"), [0, None, 1700000000000000001]),
]

DEEPHAVEN_TYPE = {
    "Bo": "java.lang.Boolean", "By": "byte", "Ch": "char", "Sh": "short", "In": "int",
    "Lo": "long", "Fl": "float", "Do": "double", "St": "java.lang.String",
    "Ti": "java.time.Instant",
}


def arrow_batch(with_nulls: bool, typed: bool) -> tuple[pa.Schema, pa.RecordBatch]:
    fields = []
    arrays = []
    for name, arrow_type, cells in ALL_TYPES:
        if not with_nulls:
            cells = [cells[0], cells[2], cells[0]]
        metadata = {"deephaven:type": DEEPHAVEN_TYPE[name]} if typed else None
        fields.append(pa.field(name, arrow_type, metadata=metadata))
        arrays.append(pa.array(cells, arrow_type))
    schema = pa.schema(fields)
    return schema, pa.record_batch(arrays, schema=schema)


LIST_ELEMENTS = [
    ("Bo", pa.bool_(), [True, None, False], "java.lang.Boolean", "ObjectVector"),
    ("By", pa.int8(), [1, None, -3], "byte", "ByteVector"),
    ("Ch", pa.uint16(), [97, None, 0x1F0], "char", "CharVector"),
    ("Sh", pa.int16(), [1, None, -3], "short", "ShortVector"),
    ("In", pa.int32(), [1, None, -3], "int", "IntVector"),
    ("Lo", pa.int64(), [1, None, -3], "long", "LongVector"),
    ("Fl", pa.float32(), [1.5, None, math.nan], "float", "FloatVector"),
    ("Do", pa.float64(), [1.5, None, math.inf], "double", "DoubleVector"),
    ("St", pa.string(), ["a", None, "é𝄞"], "java.lang.String", "ObjectVector"),
    ("Ti", pa.timestamp("ns", tz="UTC"), [0, None, 1700000000000000001], "java.time.Instant",
     "ObjectVector"),
]
VECTOR = "io.deephaven.vector."


def list_batch(nested: bool, metadata) -> tuple[pa.Schema, pa.RecordBatch]:
    """A list column of every element type. The rows: three elements with a null among them,
    a null cell, an empty cell, a cell of one null, and two elements; nested, each of those
    is in turn an element."""
    fields = []
    arrays = []
    for name, element, values, java, vector in LIST_ELEMENTS:
        if nested:
            arrow_type = pa.list_(pa.list_(element))
            cells = [[values, [], None], None, [], [None], [values[::2]]]
        else:
            arrow_type = pa.list_(element)
            cells = [values, None, [], [None], values[::2]]
        entries = metadata(java, vector) if metadata else None
        fields.append(pa.field(name, arrow_type, metadata=entries))
        arrays.append(pa.array(cells, arrow_type))
    schema = pa.schema(fields)
    return schema, pa.record_batch(arrays, schema=schema)


def lists(capture: Capture) -> None:
    """Uploads with array and vector columns."""
    capture.next_export = 1100
    # Without `deephaven:type` a list is an array of its elements' default type,
    schema, batch = list_batch(False, None)
    capture.capture_put("put_list_arrays", schema, [batch, batch.slice(1, 3)])
    # What the web IDE shows of it: every cell as text, of at most five elements.
    capture.capture_preview("preview_list_arrays", 1100, 1150)
    long = pa.array([list(range(n)) for n in (5, 6, 12)], pa.list_(pa.int32()))
    long = pa.record_batch([long], schema=pa.schema([pa.field("In", long.type)]))
    capture.capture_put("put_list_long_arrays", long.schema, [long])
    capture.capture_preview("preview_list_long_arrays", 1101, 1151)
    schema, batch = list_batch(True, None)
    capture.capture_put("put_list_nested_arrays", schema, [batch])
    # with the name of a vector class it is that vector,
    schema, batch = list_batch(False, lambda java, vector: {"deephaven:type": VECTOR + vector})
    capture.capture_put("put_list_vectors", schema, [batch])
    capture.capture_preview("preview_list_vectors", 1103, 1152)
    # and a vector of objects holds what `deephaven:componentType` names.
    schema, batch = list_batch(True, lambda java, vector: {
        "deephaven:type": VECTOR + "ObjectVector", "deephaven:componentType": java + "[]"})
    capture.capture_put("put_list_vectors_of_arrays", schema, [batch])

    ints = pa.array([[1, None], None, []], pa.list_(pa.int32()))

    def refused(case: str, number: int, arrow_type, array, metadata) -> None:
        schema = pa.schema([pa.field("X", arrow_type, metadata=metadata)])
        batch = pa.record_batch([array], schema=schema)
        capture.status_of_put(case, put_messages(number, schema, [batch]), number)

    # A component type that is not the one of the type.
    refused("refused_list_component", 2101, ints.type, ints,
            {"deephaven:type": "int[]", "deephaven:componentType": "long"})
    # A vector of objects whose elements are primitives.
    refused("refused_list_object_vector_of_int", 2102, ints.type, ints,
            {"deephaven:type": VECTOR + "ObjectVector"})
    # A vector of objects whose elements are vectors of objects.
    nested = pa.array([[["a"]]], pa.list_(pa.list_(pa.string())))
    refused("refused_list_object_vector_of_object_vector", 2103, nested.type, nested,
            {"deephaven:type": VECTOR + "ObjectVector",
             "deephaven:componentType": VECTOR + "ObjectVector"})
    # A list that is told to be a scalar, and a scalar that is told to be an array.
    refused("refused_list_as_int", 2104, ints.type, ints, {"deephaven:type": "int"})
    refused("refused_int_as_array", 2105, pa.int32(), pa.array([1, None], pa.int32()),
            {"deephaven:type": "int[]"})
    large = pa.array([[1, None], None, []], pa.large_list(pa.int32()))
    refused("refused_large_list", 2106, large.type, large, None)


def main(url: str, only: str | None = None) -> None:
    capture = Capture(url)
    session = capture.session
    if only == "lists":
        lists(capture)
        session.close()
        return

    # Tables made by the engine: the schema is derived from the table definition.
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
    capture.capture_table("engine_nulls", session.empty_table(3).update(formulas))
    capture.capture_table(
        "engine_no_nulls",
        session.empty_table(3).update([f.replace("i==1 ?", "i==7 ?") for f in formulas]))
    capture.capture_table("engine_empty", session.empty_table(0).update(formulas))
    # Not flat: the row set of a filtered table is a subset of its parent's.
    capture.capture_table(
        "engine_filtered", session.empty_table(6).update(["In = i", "St = `s`+i"])
        .where("In % 2 == 1"))
    capture.capture_table("engine_many_rows", session.empty_table(10000).update(["In = i"]))

    # Uploaded tables: the schema the client sent is kept as the table's wire format.
    schema, batch = arrow_batch(with_nulls=True, typed=False)
    capture.capture_put("put_plain", schema, [batch])
    schema, batch = arrow_batch(with_nulls=True, typed=True)
    capture.capture_put("put_typed", schema, [batch])
    schema, batch = arrow_batch(with_nulls=False, typed=True)
    capture.capture_put("put_no_nulls", schema, [batch])
    capture.capture_put("put_three_batches", schema, [batch, batch.slice(1, 2), batch.slice(0, 1)])
    capture.capture_put("put_schema_only", schema, [])
    capture.capture_put("put_zero_row_batch", schema, [batch.slice(0, 0)])
    capture.capture_put("put_zero_then_rows", schema, [batch.slice(0, 0), batch])
    capture.capture_put("put_rows_then_zero", schema, [batch, batch.slice(0, 0), batch.slice(0, 1)])
    no_zone = pa.schema([pa.field("Ti", pa.timestamp("ns"))])
    capture.capture_put("put_timestamp_no_zone", no_zone, [
        pa.record_batch([pa.array([1, None], pa.timestamp("ns"))], schema=no_zone)])
    not_nullable = pa.schema([pa.field("In", pa.int32(), nullable=False),
                              pa.field("Bo", pa.bool_(), nullable=False)])
    capture.capture_put("put_not_nullable", not_nullable, [
        pa.record_batch([pa.array([1, 2], pa.int32()), pa.array([True, False])],
                        schema=not_nullable)])
    # Deephaven's null values sent as plain values, without validity bits.
    sentinels = pa.schema([pa.field("In", pa.int32()), pa.field("Do", pa.float64())])
    capture.capture_put("put_sentinels", sentinels, [pa.record_batch(
        [pa.array([1, -2147483648], pa.int32()),
         pa.array([-sys.float_info.max, 2.0], pa.float64())], schema=sentinels)])
    illegal = pa.schema([pa.field("a b", pa.int32()), pa.field("1x", pa.int32())])
    capture.status_of_put("refused_illegal_names", put_messages(1999, illegal, [
        pa.record_batch([pa.array([1], pa.int32()), pa.array([2], pa.int32())], schema=illegal)]),
        1999)

    # Input tables.
    input_schema = pa.schema([("K", pa.int32()), ("S", pa.string()), ("V", pa.float64())])
    rows = session.import_table(pa.table(
        {"K": pa.array([1, 2], pa.int32()), "S": ["a", None], "V": [1.5, None]},
        schema=input_schema))
    keyed = session.input_table(schema=input_schema, key_cols=["K"])
    capture.capture_table("input_keyed_empty", keyed)
    keyed.add(rows)
    capture.capture_table("input_keyed", keyed)
    append_only = session.input_table(schema=input_schema)
    append_only.add(rows)
    capture.capture_table("input_append_only", append_only)

    # The input table service.
    def ticket(table) -> bytes:
        return field(1, table.ticket.pb_ticket.ticket)

    def change(method: str, target: bytes | None, other: bytes) -> bytes:
        request = (field(1, target) if target is not None else b"") + field(2, other)
        return capture.unary(method, request, INPUT_TABLE)

    only_key = session.import_table(pa.table({"K": pa.array([2], pa.int32())}))
    add, delete = "AddTableToInputTable", "DeleteTableFromInputTable"
    codes = []
    for what, call in [
        ("Add to a keyed input table", lambda: change(add, ticket(keyed), ticket(rows))),
        ("Add to a table that is not an input table",
         lambda: change(add, ticket(rows), ticket(rows))),
        ("Add a table of other columns", lambda: change(add, ticket(keyed), ticket(only_key))),
        ("Add without an input table", lambda: change(add, None, ticket(rows))),
        ("Add an export that does not exist",
         lambda: change(add, ticket(keyed), field(1, export_ticket(999999)))),
        ("Delete from a keyed input table",
         lambda: change(delete, ticket(keyed), ticket(only_key))),
        ("Delete from an append-only input table",
         lambda: change(delete, ticket(append_only), ticket(only_key))),
        ("Delete with a column that is not a key",
         lambda: change(delete, ticket(keyed), ticket(rows))),
        ("Delete from a table that is not an input table",
         lambda: change(delete, ticket(rows), ticket(only_key))),
    ]:
        codes.append(f"{what}: {capture.code(call)}")
    capture.write("input_calls.status", ("\n".join(codes) + "\n").encode())
    capture.capture_table("input_keyed_after_delete", keyed)

    # Requests the server refuses.
    schema, batch = arrow_batch(with_nulls=False, typed=True)
    good = put_messages(2000, schema, [batch])
    header, body = split_ipc(batch.serialize().to_pybytes())
    schema_header, _ = split_ipc(schema.serialize().to_pybytes())
    capture.status_of_put("refused_no_descriptor", [
        flight_data(None, schema_header), flight_data(None, header, body)], 2001)
    capture.status_of_put("refused_batch_before_schema", [
        flight_data(path_descriptor("export", "2002"), header, body)], 2002)
    capture.status_of_put("refused_no_schema", [
        field(1, path_descriptor("export", "2003"))], 2003)
    capture.status_of_put("refused_second_descriptor", [
        good[0].replace(b"2000", b"2004"),
        flight_data(path_descriptor("export", "2005"), header, body)], 2004)
    capture.status_of_put("refused_second_schema", [
        good[0].replace(b"2000", b"2006"), flight_data(None, header, body),
        flight_data(None, schema_header), flight_data(None, header, body)], 2006)
    capture.do_put(put_messages(2007, schema, [batch]))
    capture.status_of_put("refused_export_exists", put_messages(2007, schema, [batch]), 2007)
    capture.status_of_put("refused_not_an_export", [
        flight_data(path_descriptor("scope", "x"), schema_header)], 2008)
    capture.status_of_put("refused_garbage_batch", [
        good[0].replace(b"2000", b"2009"), flight_data(None, header[: len(header) // 2], body)],
        2009)
    short = pa.record_batch([batch.column(0)], schema=pa.schema([schema.field(0)]))
    short_header, short_body = split_ipc(short.serialize().to_pybytes())
    capture.status_of_put("refused_too_few_columns", [
        good[0].replace(b"2000", b"2010"), flight_data(None, short_header, short_body)], 2010)

    codes = []
    for what, call in [
        ("DoGet unknown export", lambda: capture.server_stream(
            "DoGet", field(1, export_ticket(999999)))),
        ("DoGet empty ticket", lambda: capture.server_stream("DoGet", b"")),
        ("DoGet unknown route", lambda: capture.server_stream("DoGet", field(1, b"zzz"))),
        ("DoGet malformed export ticket", lambda: capture.server_stream(
            "DoGet", field(1, b"e12"))),
        ("GetSchema unknown export", lambda: capture.unary(
            "GetSchema", path_descriptor("export", "999999"))),
        ("GetSchema unknown route", lambda: capture.unary(
            "GetSchema", path_descriptor("nowhere", "1"))),
        ("GetSchema empty path", lambda: capture.unary("GetSchema", b"\x08\x01")),
        ("GetSchema command", lambda: capture.unary("GetSchema", b"\x08\x02" + field(2, b"x"))),
        ("GetSchema unknown type", lambda: capture.unary("GetSchema", b"")),
        ("GetSchema export id not a number", lambda: capture.unary(
            "GetSchema", path_descriptor("export", "x"))),
        ("GetSchema export path too long", lambda: capture.unary(
            "GetSchema", path_descriptor("export", "1", "2"))),
        ("GetFlightInfo unknown export", lambda: capture.unary(
            "GetFlightInfo", path_descriptor("export", "999999"))),
        ("GetFlightInfo unknown route", lambda: capture.unary(
            "GetFlightInfo", path_descriptor("nowhere", "1"))),
        ("ListFlights", lambda: capture.server_stream("ListFlights", b"")),
        ("ListFlights with an expression", lambda: capture.server_stream(
            "ListFlights", field(1, b"x"))),
    ]:
        codes.append(f"{what}: {capture.code(call)}")
    capture.write("calls.status", ("\n".join(codes) + "\n").encode())

    lists(capture)
    session.close()


if __name__ == "__main__":
    main(*sys.argv[1:3])
    sys.stdout.flush()
    os._exit(0)
