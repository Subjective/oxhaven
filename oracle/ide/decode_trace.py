"""Prints a trace written by ide-test.js as a readable list of what the browser asked for.

    oracle/.venv/bin/python oracle/ide/decode_trace.py oracle/ide/.out/panels.trace.json [--full]

No Java counterpart: test tooling. Messages of the Deephaven services are decoded with the
protobuf classes of the pinned `deephaven-core` wheel. Arrow Flight messages (FlightData and
friends) and the Barrage flatbuffers inside them are decoded here by hand, from Flight.proto,
Message.fbs and Barrage.fbs; only the fields a reader needs to follow a subscription.
"""

from __future__ import annotations

import argparse
import base64
import importlib
import json
import pkgutil
import struct
import sys

from google.protobuf import message_factory, text_format

STATUS = [
    "OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND",
    "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION",
    "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS",
    "UNAUTHENTICATED",
]


def method_types() -> dict[str, tuple[type, type]]:
    """Maps `<package>.<Service>/<Method>` to its request and response classes."""
    import deephaven_core.proto as protos

    out = {}
    for info in pkgutil.iter_modules(protos.__path__):
        if not info.name.endswith("_pb2"):
            continue
        module = importlib.import_module(f"deephaven_core.proto.{info.name}")
        for service in module.DESCRIPTOR.services_by_name.values():
            for method in service.methods:
                out[f"{service.full_name}/{method.name}"] = (
                    message_factory.GetMessageClass(method.input_type),
                    message_factory.GetMessageClass(method.output_type),
                )
    return out


# ---- protobuf wire format, for the Flight messages -------------------------------------


def wire_fields(data: bytes) -> list[tuple[int, int, object]]:
    out, pos = [], 0
    while pos < len(data):
        key, pos = varint(data, pos)
        number, kind = key >> 3, key & 7
        if kind == 0:
            value, pos = varint(data, pos)
        elif kind == 1:
            value, pos = data[pos:pos + 8], pos + 8
        elif kind == 2:
            size, pos = varint(data, pos)
            value, pos = data[pos:pos + size], pos + size
        elif kind == 5:
            value, pos = data[pos:pos + 4], pos + 4
        else:
            raise ValueError(f"wire type {kind}")
        out.append((number, kind, value))
    return out


def varint(data: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7


# ---- flatbuffers, read only -------------------------------------------------------------


class FlatTable:
    def __init__(self, buf: bytes, pos: int):
        self.buf, self.pos = buf, pos
        self.vtable = pos - struct.unpack_from("<i", buf, pos)[0]
        self.vtable_size = struct.unpack_from("<H", buf, self.vtable)[0]

    @classmethod
    def root(cls, buf: bytes) -> "FlatTable":
        return cls(buf, struct.unpack_from("<I", buf, 0)[0])

    def _at(self, index: int) -> int:
        slot = 4 + 2 * index
        if slot >= self.vtable_size:
            return 0
        offset = struct.unpack_from("<H", self.buf, self.vtable + slot)[0]
        return self.pos + offset if offset else 0

    def scalar(self, index: int, fmt: str, default=0):
        at = self._at(index)
        return struct.unpack_from("<" + fmt, self.buf, at)[0] if at else default

    def table(self, index: int) -> "FlatTable | None":
        at = self._at(index)
        return FlatTable(self.buf, at + struct.unpack_from("<I", self.buf, at)[0]) if at else None

    def _vector(self, index: int) -> tuple[int, int] | None:
        at = self._at(index)
        if not at:
            return None
        start = at + struct.unpack_from("<I", self.buf, at)[0]
        return start + 4, struct.unpack_from("<I", self.buf, start)[0]

    def bytes(self, index: int) -> bytes | None:
        v = self._vector(index)
        return None if v is None else self.buf[v[0]:v[0] + v[1]]

    def string(self, index: int) -> str | None:
        b = self.bytes(index)
        return None if b is None else b.decode("utf-8", "replace")

    def tables(self, index: int) -> list["FlatTable"]:
        v = self._vector(index)
        if v is None:
            return []
        out = []
        for i in range(v[1]):
            at = v[0] + 4 * i
            out.append(FlatTable(self.buf, at + struct.unpack_from("<I", self.buf, at)[0]))
        return out

    def vector_length(self, index: int) -> int | None:
        v = self._vector(index)
        return None if v is None else v[1]


def rowset(data: bytes | None) -> str:
    """The row set encoding of Barrage (ExternalizableRowSetUtils), as `a-b,c`."""
    if data is None:
        return "absent"
    if not data:
        return "empty bytes"
    sizes = {1: ("<h", 2), 2: ("<i", 4), 3: ("<q", 8), 4: ("<b", 1)}
    pos, offset, pending, ranges = 0, 0, -1, []

    def value(command: int) -> int:
        nonlocal pos
        fmt, size = sizes[command & 7]
        v = struct.unpack_from(fmt, data, pos)[0]
        pos += size
        return v

    def append(v: int) -> None:
        nonlocal pending
        if pending == -1:
            pending = v
        elif v < 0:
            ranges.append((pending, -v))
            pending = -1
        else:
            ranges.append((pending, pending))
            pending = v

    def step(v: int) -> None:
        nonlocal offset
        actual = offset + abs(v)
        append(-actual if v < 0 else actual)
        offset = actual

    try:
        while True:
            command = data[pos]
            pos += 1
            kind = command & 0x38
            if kind == 0x08:
                step(value(command))
            elif kind == 0x10:
                for _ in range(value(command)):
                    step(struct.unpack_from("<h", data, pos)[0])
                    pos += 2
            elif kind == 0x18:
                for _ in range(value(command)):
                    step(struct.unpack_from("<b", data, pos)[0])
                    pos += 1
            elif kind == 0x20:
                if pending >= 0:
                    ranges.append((pending, pending))
                break
            else:
                return f"undecodable {data.hex()}"
    except (IndexError, struct.error, KeyError):
        return f"undecodable {data.hex()}"
    if not ranges:
        return "{}"
    return "{" + ",".join(str(a) if a == b else f"{a}-{b}" for a, b in ranges) + "}"


def bitset(data: bytes | None) -> str:
    if data is None:
        return "absent (all columns)"
    bits = [i for i in range(len(data) * 8) if data[i // 8] >> (i % 8) & 1]
    return f"{bits}"


BARRAGE_TYPES = {
    4: "BarrageSerializationOptions", 5: "BarrageSubscriptionRequest", 6: "BarrageUpdateMetadata",
    7: "BarrageSnapshotRequest", 8: "BarragePublicationRequest",
}


def barrage(app_metadata: bytes) -> str:
    wrapper = FlatTable.root(app_metadata)
    magic = wrapper.scalar(0, "I")
    kind = wrapper.scalar(1, "b")
    payload = wrapper.bytes(2) or b""
    name = BARRAGE_TYPES.get(kind, f"type {kind}")
    head = f"BarrageMessageWrapper magic=0x{magic:08X} {name}"
    if kind == 5 or kind == 7:
        t = FlatTable.root(payload)
        options = t.table(3)
        text = (
            f" ticket={t.bytes(0)!r} columns={bitset(t.bytes(1))} viewport={rowset(t.bytes(2))}"
            f" reverse_viewport={bool(t.scalar(4, '?', False))}"
        )
        if options is None:
            text += " options=absent"
        elif kind == 5:
            text += (
                " options{"
                f"use_deephaven_nulls={bool(options.scalar(1, '?', False))}"
                f" min_update_interval_ms={options.scalar(2, 'i')}"
                f" batch_size={options.scalar(3, 'i')}"
                f" max_message_size={options.scalar(4, 'i')}"
                f" columns_as_list={bool(options.scalar(5, '?', False))}"
                f" preview_list_length_limit={options.scalar(6, 'q')}"
                "}"
            )
        else:
            text += (
                " options{"
                f"use_deephaven_nulls={bool(options.scalar(1, '?', False))}"
                f" batch_size={options.scalar(2, 'i')}"
                f" max_message_size={options.scalar(3, 'i')}"
                f" preview_list_length_limit={options.scalar(4, 'q')}"
                "}"
            )
        if kind == 5 and t.bytes(5) is not None:
            text += f" subscription_token={t.bytes(5)!r}"
        return head + text
    if kind == 6:
        t = FlatTable.root(payload)
        return head + (
            f" first_seq={t.scalar(0, 'q')} last_seq={t.scalar(1, 'q')}"
            f" is_snapshot={bool(t.scalar(2, '?', False))}"
            f" effective_viewport={rowset(t.bytes(3))}"
            f" effective_reverse_viewport={bool(t.scalar(4, '?', False))}"
            f" effective_column_set={bitset(t.bytes(5))}"
            f" added_rows={rowset(t.bytes(6))} removed_rows={rowset(t.bytes(7))}"
            f" shift_data={(t.bytes(8) or b'').hex() or 'empty'}"
            f" added_rows_included={rowset(t.bytes(9))}"
            f" mod_column_nodes={[rowset(m.bytes(0)) for m in t.tables(10)]}"
            f" table_size={t.scalar(11, 'q')}"
        )
    return head + f" payload={len(payload)} bytes"


ARROW_HEADERS = {0: "NONE", 1: "Schema", 2: "DictionaryBatch", 3: "RecordBatch", 4: "Tensor", 5: "SparseTensor"}


def arrow_message(data_header: bytes) -> str:
    m = FlatTable.root(data_header)
    kind = m.scalar(1, "B")
    body = m.scalar(3, "q")
    text = f"arrow {ARROW_HEADERS.get(kind, kind)} bodyLength={body}"
    header = m.table(2)
    if header is not None and kind == 3:
        text += f" rows={header.scalar(0, 'q')} nodes={header.vector_length(1)} buffers={header.vector_length(2)}"
    if header is not None and kind == 1:
        fields = []
        for f in header.tables(1):
            meta = {kv.string(0): kv.string(1) for kv in f.tables(6)}
            fields.append(f"{f.string(0)}:{meta.get('deephaven:type', '?')}")
        attributes = {kv.string(0): kv.string(1) for kv in header.tables(2)}
        text += f" fields=[{', '.join(fields)}] metadata={attributes}"
    return text


def flight_data(data: bytes) -> str:
    parts = []
    for number, _kind, value in wire_fields(data):
        if number == 1:
            descriptor = {n: v for n, _k, v in wire_fields(value)}
            parts.append(f"flight_descriptor type={descriptor.get(1)} cmd={descriptor.get(2)!r} path={descriptor.get(3)!r}")
        elif number == 2:
            parts.append(arrow_message(value))
        elif number == 3:
            parts.append(barrage(value))
        elif number == 1000:
            parts.append(f"data_body={len(value)} bytes")
        else:
            parts.append(f"field {number}: {value!r}")
    return "FlightData{" + "; ".join(parts) + "}" if parts else "FlightData{}"


def generic(data: bytes) -> str:
    try:
        return "{" + ", ".join(f"{n}: {v!r}" for n, _k, v in wire_fields(data)) + "}"
    except (IndexError, ValueError):
        return data.hex()


FLIGHT = {"FlightService", "BrowserFlightService"}
FLIGHT_DATA = {
    ("DoGet", "response"), ("DoPut", "request"), ("DoExchange", "request"), ("DoExchange", "response"),
    ("OpenDoPut", "request"), ("NextDoPut", "request"), ("OpenDoExchange", "request"),
    ("OpenDoExchange", "response"), ("NextDoExchange", "request"),
}


def schema_bytes(data: bytes) -> str:
    """An Arrow IPC stream message (continuation, length, flatbuffer), as in schema_header."""
    if data[:4] == b"\xff\xff\xff\xff":
        data = data[8:]
    try:
        return arrow_message(data)
    except (struct.error, IndexError):
        return f"{len(data)} bytes"


def render(call: dict, side: str, raw: bytes, types: dict) -> str:
    short = call["service"].rsplit(".", 1)[-1]
    if short in FLIGHT:
        if (call["method"], side) in FLIGHT_DATA:
            return flight_data(raw)
        return generic(raw)
    classes = types.get(f"{call['service']}/{call['method']}")
    if classes is None:
        return generic(raw)
    message = classes[0 if side == "request" else 1]()
    message.ParseFromString(raw)
    name = type(message).__name__
    schema = getattr(message, "schema_header", None)
    if schema:
        return f"{name}{{{shorten_schema(message)}}} schema_header: {schema_bytes(schema)}"
    return f"{name}{{{text_format.MessageToString(message, as_one_line=True)}}}"


def shorten_schema(message) -> str:
    copy = type(message)()
    copy.CopyFrom(message)
    copy.ClearField("schema_header")
    return text_format.MessageToString(copy, as_one_line=True)


def clip(text: str, limit: int) -> str:
    return text if limit <= 0 or len(text) <= limit else text[:limit] + f" ... ({len(text)} chars)"


HEADERS_OF_INTEREST = (
    "authorization", "x-deephaven-stream-ticket", "x-deephaven-stream-sequence",
    "x-deephaven-stream-halfclose", "x-grpc-web", "content-type", "grpc-timeout",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("trace")
    parser.add_argument("--full", action="store_true", help="do not shorten messages or streams")
    parser.add_argument("--static", action="store_true", help="also list the static requests")
    args = parser.parse_args()
    with open(args.trace, encoding="utf-8") as f:
        trace = json.load(f)
    types = method_types()
    limit = 0 if args.full else 600
    keep = 10**9 if args.full else 4

    if args.static:
        for r in trace["static"]:
            headers = {k.lower(): v for k, v in (r.get("responseHeaders") or {}).items()}
            print(
                f"{r['t']:>6}ms {r['method']} {r['url']} -> {r.get('status')} {r.get('protocol')}"
                f" {headers.get('content-type')} cache-control={headers.get('cache-control')!r}"
                f" etag={headers.get('etag')!r} encoding={headers.get('content-encoding')!r}"
            )
        print()
    for s in trace["sockets"]:
        request = {k.lower(): v for k, v in (s.get("requestHeaders") or {}).items()}
        response = {k.lower(): v for k, v in (s.get("responseHeaders") or {}).items()}
        print(
            f"{s['t']:>6}ms WEBSOCKET {s['url']} -> {s.get('status')}"
            f" offered={request.get('sec-websocket-protocol')!r} chosen={response.get('sec-websocket-protocol')!r}"
            f" frames={s['frameCount']} closed={s.get('closed')} error={s.get('error')}"
        )
    print()

    events = [(m["t"], "mark", m) for m in trace["marks"]] + [(c["t"], "call", c) for c in trace["calls"]]
    events.sort(key=lambda e: e[0])
    # A long run of the same unary call (a keystroke each, in the console) is shown by its
    # first two and its last.
    hidden: dict[int, int] = {}
    if not args.full:
        def key(e):
            return (e[1], e[2].get("service"), e[2].get("method"), e[2].get("status"), len(e[2].get("requests", [])))
        start = 0
        while start < len(events):
            stop = start
            while stop + 1 < len(events) and events[stop + 1][1] == "call" and key(events[stop + 1]) == key(events[start]):
                stop += 1
            if events[start][1] == "call" and stop - start >= 4:
                hidden[start + 2] = stop - start - 2
                for i in range(start + 3, stop):
                    hidden[i] = 0
            start = stop + 1
    for index, (_t, kind, item) in enumerate(events):
        if index in hidden:
            if hidden[index]:
                print(f"         ... {hidden[index]} more calls of {item['service']}/{item['method']} like the last ...")
            continue
        if kind == "mark":
            print(f"\n==== {item['name']} (at {item['t']}ms) ====\n")
            continue
        c = item
        status = "still open" if c.get("status") is None else STATUS[c["status"]]
        extra = f" [{c['httpProtocol']}, HTTP {c['httpStatus']}]" if c.get("httpProtocol") else f" [stream {c.get('streamId')}]"
        print(f"{c['t']:>6}ms {c['transport']}{extra} {c['service']}/{c['method']} -> {status}"
              + (f" \"{c['message']}\"" if c.get("message") else ""))
        headers = c.get("requestHeaders") or {}
        shown = {k: headers[k] for k in HEADERS_OF_INTEREST if k in headers}
        if "authorization" in shown:
            scheme = shown["authorization"].split(" ")[0]
            shown["authorization"] = scheme + " <...>"
        print(f"         request headers: {shown}")
        response_headers = {k: v for k, v in (c.get("responseHeaders") or {}).items()
                            if k in ("authorization", "grpc-encoding", "grpc-accept-encoding", "content-type")}
        if "authorization" in response_headers:
            response_headers["authorization"] = response_headers["authorization"].split(" ")[0] + " <...>"
        print(f"         response headers: {response_headers}")
        for side, key in (("request", "requests"), ("response", "responses")):
            messages = c[key]
            for i, b64 in enumerate(messages):
                if i == keep and len(messages) > keep + 1:
                    print(f"         {side} ... {len(messages) - keep - 1} more ...")
                if keep <= i < len(messages) - 1:
                    continue
                try:
                    text = render(c, side, base64.b64decode(b64), types)
                except Exception as e:  # a decoder gap must not hide the rest of the trace
                    text = f"<undecoded: {e}> {b64[:80]}"
                print(f"         {side} {i}: {clip(text, limit)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
