"""The topics of the Kafka corpus: what is published to the broker before a Kafka script
scenario runs, and what the schema registry answers.

No Java counterpart: test data of the harness. `oracle/corpus/kafka/topics.json` is the output
of `fixture()`, committed: the recordings were made from it and a replay publishes it. This
module is where it is written and explained; `tests/test_kafka.py` checks that the two are
the same, so a change here needs a new recording (`bin/kafka-record.sh`).

Everything is fixed: each record names its partition, and has a timestamp given by the
producer, a second after the record before it in its topic. Nothing here depends on the time
of day or on what the broker assigns. Offsets are deterministic too, because a fresh broker
gets the records of each topic in this order, one at a time.

The binary records are encoded here, by hand, from the formats' specifications: Kafka's
serializers for numbers (big-endian), Avro's binary encoding, Confluent's wire format (a zero
byte and the schema's id before the value), and Protobuf's.
"""

from __future__ import annotations

import json
import struct
from typing import Any

FORMAT = 1
# 2023-11-14T22:13:20Z, in milliseconds. Records must not have the timestamp 0: librdkafka's
# producer takes it as "now" (docs/KAFKA.md), and a recording must not depend on which client
# published.
T0 = 1_700_000_000_000

Record = dict[str, Any]


def record(key: bytes | str | None, value: bytes | str | None, partition: int = 0,
           headers: list[tuple[str, bytes]] | None = None) -> Record:
    """One record. Text is sent as UTF-8; `None` is a record without that key or value.
    A record with neither is not in the corpus: the Python client that publishes refuses
    to send one."""
    assert key is not None or value is not None
    def raw(data: bytes | str | None) -> bytes | None:
        return data.encode("utf-8") if isinstance(data, str) else data

    return {"partition": partition, "key": raw(key), "value": raw(value),
            "headers": headers or []}


# ---------------------------------------------------------------- encodings


def be(fmt: str, value: int | float) -> bytes:
    """A number as Kafka's serializers write it: big-endian (`>h`, `>i`, `>q`, `>f`, `>d`)."""
    return struct.pack(fmt, value)


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        low = n & 0x7F
        n >>= 7
        if n:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def avro_long(n: int) -> bytes:
    """Avro's int and long: zig-zag, then a variable-length integer."""
    return _varint((n << 1) ^ (n >> 63))


def avro_string(text: str) -> bytes:
    data = text.encode("utf-8")
    return avro_long(len(data)) + data


def avro_bytes(data: bytes) -> bytes:
    return avro_long(len(data)) + data


def avro_double(x: float) -> bytes:
    return struct.pack("<d", x)


def avro_float(x: float) -> bytes:
    return struct.pack("<f", x)


def avro_bool(flag: bool) -> bytes:
    return b"\x01" if flag else b"\x00"


def avro_array(items: list[bytes]) -> bytes:
    """One block with every item, then the empty block that ends the array."""
    if not items:
        return avro_long(0)
    return avro_long(len(items)) + b"".join(items) + avro_long(0)


def confluent(schema_id: int, body: bytes) -> bytes:
    """Confluent's wire format: magic byte 0, the schema's id, the value."""
    return b"\x00" + struct.pack(">I", schema_id) + body


def proto_field(number: int, wire: int, payload: bytes) -> bytes:
    return _varint(number << 3 | wire) + payload


def proto_varint(number: int, value: int) -> bytes:
    return proto_field(number, 0, _varint(value & 0xFFFF_FFFF_FFFF_FFFF))


def proto_bytes(number: int, data: bytes) -> bytes:
    return proto_field(number, 2, _varint(len(data)) + data)


def proto_double(number: int, x: float) -> bytes:
    return proto_field(number, 1, struct.pack("<d", x))


def proto_serdes(schema_id: int, message: bytes) -> bytes:
    """Confluent's Protobuf wire format: magic byte 0, the schema's id, the message indexes
    (one zero byte stands for "the first message of the schema"), the message."""
    return b"\x00" + struct.pack(">I", schema_id) + b"\x00" + message


# ---------------------------------------------------------------- schemas

# The value schema of kc_avro as its consumers read it (version 2 of the subject).
TRADE = {
    "type": "record", "name": "Trade", "namespace": "kc",
    "fields": [
        {"name": "sym", "type": "string"},
        {"name": "qty", "type": "int"},
        {"name": "px", "type": "double"},
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}},
        {"name": "note", "type": ["null", "string"], "default": None},
        {"name": "venue", "type": {"type": "record", "name": "Venue", "fields": [
            {"name": "code", "type": "string"},
            {"name": "lot", "type": "long"},
        ]}},
        {"name": "flag", "type": "boolean"},
        {"name": "f", "type": "float"},
    ],
}
TRADE_ID = 11

# An older writer's schema (version 1): no `note`, `px` a float and `venue.lot` an int, which
# Avro's schema resolution promotes when the record is read as TRADE.
TRADE_OLD = {
    "type": "record", "name": "Trade", "namespace": "kc",
    "fields": [
        {"name": "sym", "type": "string"},
        {"name": "qty", "type": "int"},
        {"name": "px", "type": "float"},
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}},
        {"name": "venue", "type": {"type": "record", "name": "Venue", "fields": [
            {"name": "code", "type": "string"},
            {"name": "lot", "type": "int"},
        ]}},
        {"name": "flag", "type": "boolean"},
        {"name": "f", "type": "float"},
    ],
}
TRADE_OLD_ID = 12

MORE = {
    "type": "record", "name": "More", "namespace": "kc",
    "fields": [
        {"name": "sizes", "type": {"type": "array", "items": "int"}},
        {"name": "names", "type": {"type": "array", "items": "string"}},
        {"name": "amount",
         "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
    ],
}
MORE_ID = 13

TICK_PROTO = """\
syntax = "proto3";
package kc;

import "google/protobuf/timestamp.proto";

message Tick {
  string sym = 1;
  int32 qty = 2;
  double px = 3;
  bool live = 4;
  optional int64 seq = 5;
  Inner inner = 6;
  repeated int32 sizes = 7;
  google.protobuf.Timestamp at = 8;
}

message Inner {
  string code = 1;
  uint32 n = 2;
}
"""
TICK_ID = 21

PAINT_PROTO = """\
syntax = "proto3";
package kc;

enum Color {
  RED = 0;
  GREEN = 1;
}

message Paint {
  string name = 1;
  Color color = 2;
}
"""
PAINT_ID = 22

# Debezium's shapes for a table `people` of the database `inv` on the server `kc`.
PEOPLE_KEY = {
    "type": "record", "name": "Key", "namespace": "kc.inv.people",
    "fields": [{"name": "id", "type": "int"}],
}
PEOPLE_KEY_ID = 31
PEOPLE_VALUE = {
    "type": "record", "name": "Envelope", "namespace": "kc.inv.people",
    "fields": [
        {"name": "before", "default": None, "type": ["null", {
            "type": "record", "name": "Value", "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "score", "type": ["null", "double"], "default": None},
            ]}]},
        {"name": "after", "type": ["null", "Value"], "default": None},
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": None},
    ],
}
PEOPLE_VALUE_ID = 32


def _schema(doc: dict[str, Any]) -> str:
    return json.dumps(doc, separators=(",", ":"))


def subjects() -> list[dict[str, Any]]:
    """What the registry holds: each subject with its versions, oldest first."""
    def avro(version: int, schema_id: int, doc: dict[str, Any]) -> dict[str, Any]:
        return {"version": version, "id": schema_id, "type": "AVRO", "schema": _schema(doc)}

    return [
        {"subject": "kc_avro-value",
         "versions": [avro(1, TRADE_OLD_ID, TRADE_OLD), avro(2, TRADE_ID, TRADE)]},
        {"subject": "kc_avro_more-value", "versions": [avro(1, MORE_ID, MORE)]},
        {"subject": "kc_proto-value",
         "versions": [{"version": 1, "id": TICK_ID, "type": "PROTOBUF", "schema": TICK_PROTO}]},
        {"subject": "kc_proto_enum-value",
         "versions": [{"version": 1, "id": PAINT_ID, "type": "PROTOBUF",
                       "schema": PAINT_PROTO}]},
        {"subject": "kc.inv.people-key", "versions": [avro(1, PEOPLE_KEY_ID, PEOPLE_KEY)]},
        {"subject": "kc.inv.people-value",
         "versions": [avro(1, PEOPLE_VALUE_ID, PEOPLE_VALUE)]},
    ]


# ---------------------------------------------------------------- records


def _trade(sym: str, qty: int, px: float, ts: int, side: int, note: str | None,
           code: str, lot: int, flag: bool, f: float) -> bytes:
    union = avro_long(0) if note is None else avro_long(1) + avro_string(note)
    return confluent(TRADE_ID, avro_string(sym) + avro_long(qty) + avro_double(px)
                     + avro_long(ts) + avro_long(side) + union + avro_string(code)
                     + avro_long(lot) + avro_bool(flag) + avro_float(f))


def _trade_old(sym: str, qty: int, px: float, ts: int, side: int, code: str, lot: int,
               flag: bool, f: float) -> bytes:
    return confluent(TRADE_OLD_ID, avro_string(sym) + avro_long(qty) + avro_float(px)
                     + avro_long(ts) + avro_long(side) + avro_string(code) + avro_long(lot)
                     + avro_bool(flag) + avro_float(f))


def _more(sizes: list[int], names: list[str], unscaled: int) -> bytes:
    width = max(1, (unscaled.bit_length() + 8) // 8)
    amount = unscaled.to_bytes(width, "big", signed=True)
    return confluent(MORE_ID, avro_array([avro_long(n) for n in sizes])
                     + avro_array([avro_string(n) for n in names]) + avro_bytes(amount))


def _tick(sym: str, qty: int, px: float, live: bool, seq: int | None,
          inner: tuple[str, int] | None, sizes: list[int],
          at: tuple[int, int] | None) -> bytes:
    """A proto3 message as protobuf-java writes it: fields at their default left out."""
    out = b""
    if sym:
        out += proto_bytes(1, sym.encode("utf-8"))
    if qty:
        out += proto_varint(2, qty)
    if px:
        out += proto_double(3, px)
    if live:
        out += proto_varint(4, 1)
    if seq is not None:
        out += proto_varint(5, seq)
    if inner is not None:
        code, n = inner
        body = (proto_bytes(1, code.encode("utf-8")) if code else b"") \
            + (proto_varint(2, n) if n else b"")
        out += proto_bytes(6, body)
    if sizes:
        out += proto_bytes(7, b"".join(_varint(n & 0xFFFF_FFFF_FFFF_FFFF) for n in sizes))
    if at is not None:
        seconds, nanos = at
        body = (proto_varint(1, seconds) if seconds else b"") \
            + (proto_varint(2, nanos) if nanos else b"")
        out += proto_bytes(8, body)
    return out


_TICKS = [
    _tick("AAA", 10, 1.5, True, 7, ("x", 3), [1, 2, 3], (1_700_000_000, 123_456_789)),
    # Every field at its default: an empty message. `seq` (optional) and `inner` and `at`
    # (messages) are absent, the others are their zero values.
    _tick("", 0, 0.0, False, None, None, [], None),
    _tick("BBB", -5, -0.25, False, 0, ("", 4_000_000_000), [-1], (0, 1)),
]


def _person(person_id: int, name: str, score: float | None) -> bytes:
    union = avro_long(0) if score is None else avro_long(1) + avro_double(score)
    return avro_long(person_id) + avro_string(name) + union


def _change(before: bytes | None, after: bytes | None, op: str, ts_ms: int | None) -> bytes:
    def branch(value: bytes | None) -> bytes:
        return avro_long(0) if value is None else avro_long(1) + value

    time = avro_long(0) if ts_ms is None else avro_long(1) + avro_long(ts_ms)
    return confluent(PEOPLE_VALUE_ID, branch(before) + branch(after) + avro_string(op) + time)


def _person_key(person_id: int) -> bytes:
    return confluent(PEOPLE_KEY_ID, avro_long(person_id))


INT_MIN, LONG_MIN = -(2 ** 31), -(2 ** 63)
FLOAT_MAX = struct.unpack(">f", bytes.fromhex("7f7fffff"))[0]
DOUBLE_MAX = struct.unpack(">d", bytes.fromhex("7fefffffffffffff"))[0]


def topics() -> list[dict[str, Any]]:
    """Every topic with its number of partitions, its compression and its records."""
    def topic(name: str, records: list[Record], partitions: int = 1,
              compression: str | None = None) -> dict[str, Any]:
        return {"name": name, "partitions": partitions, "compression": compression,
                "records": records}

    return [
        # Text keys and values: an empty value, no key, no value (a tombstone), text beyond
        # ASCII, and a record with headers, which no column shows.
        topic("kc_text", [
            record("k1", "alpha"),
            record("k2", ""),
            record(None, "no key"),
            record("k4", None),
            record("k5", "héllo ✓", headers=[("trace", b"abc"), ("n", b"\x00\x01")]),
        ]),
        # An int key and a double value. Integer.MIN_VALUE and -Double.MAX_VALUE are Java's
        # nulls of these column types.
        topic("kc_int_double", [
            record(be(">i", 1), be(">d", 1.5)),
            record(be(">i", -2), be(">d", float("nan"))),
            record(be(">i", 2 ** 31 - 1), be(">d", float("inf"))),
            record(be(">i", INT_MIN), be(">d", -0.0)),
            record(None, be(">d", 2.5)),
            record(be(">i", 9), None),
            record(be(">i", 5), be(">d", -DOUBLE_MAX)),
        ]),
        # A long key and a float value.
        topic("kc_long_float", [
            record(be(">q", 1), be(">f", 1.25)),
            record(be(">q", LONG_MIN), be(">f", float("nan"))),
            record(be(">q", 2 ** 40), be(">f", -FLOAT_MAX)),
        ]),
        # A short key and a long value.
        topic("kc_short_long", [
            record(be(">h", 7), be(">q", 70)),
            record(be(">h", -(2 ** 15)), be(">q", 2 ** 63 - 1)),
        ]),
        # Values that are not UTF-8: bytes no encoding of a character starts with, and a
        # character cut off at the end of the value.
        topic("kc_text_not_utf8", [
            record("n1", "fine"),
            record("n2", b"\xff\xfeabc"),
            record("n3", b"caf\xc3"),
        ]),
        # The third value has three bytes: not an int.
        topic("kc_bad_int", [
            record("a", be(">i", 1)),
            record("b", be(">i", 2)),
            record("c", b"\x00\x00\x03"),
            record("d", be(">i", 4)),
        ]),
        # JSON values: plain fields, values that are coerced to the column's type, nulls and
        # missing fields, an object in a text column, an integer too large for its column,
        # and a record that is an array.
        topic("kc_json", [
            record("j1", '{"id": 1, "name": "ann", "price": 1.5, "ok": true, '
                         '"when": "2024-01-02T03:04:05Z", "tags": ["a", "b"], '
                         '"nested": {"deep": {"n": 7}}, "c": "x"}'),
            record("j2", '{"id": "12.9", "name": 15, "price": "1e3", "ok": "T", '
                         '"when": 1700000000, "nested": {"deep": {"n": 2.9}}, "c": 65}'),
            record("j3", '{"id": null, "name": {"a": [1, 2.50, "q"]}, "ok": null}'),
            record("j4", '{"id": 1e19, "name": true, "price": true, "ok": 1, '
                         '"when": 1700000000123, "c": ""}'),
            record("j5", '[5, "arr"]'),
        ]),
        # The second value is not JSON.
        topic("kc_json_bad", [
            record("a", '{"n": 1}'),
            record("b", '{"n": '),
            record("c", '{"n": 3}'),
        ]),
        # The second record has no value, which a JSON spec refuses.
        topic("kc_json_tombstone", [
            record("a", '{"n": 1}'),
            record("a", None),
        ]),
        # Two partitions, each record sent to the partition it names.
        topic("kc_two", [
            record("a0", "p0-0", 0),
            record("b0", "p1-0", 1),
            record("a1", "p0-1", 0),
            record("a2", "p0-2", 0),
            record("b1", "p1-1", 1),
            record("a3", "p0-3", 0),
            record("b2", "p1-2", 1),
        ], partitions=2),
        # Batches compressed with gzip.
        topic("kc_gzip", [
            record("g1", "one"),
            record("g2", "two"),
            record("g3", "three"),
        ], compression="gzip"),
        # Avro in Confluent's wire format: three records in the schema the consumers read,
        # one from an older writer, and a tombstone.
        topic("kc_avro", [
            record("t1", _trade("AAA", 10, 1.5, T0, 0, None, "X", 100, True, 0.5)),
            record("t2", _trade_old("BBB", -3, 2.25, T0 + 1, 1, "Y", 7, False, 1.5)),
            record("t3", _trade("CCC", INT_MIN, float("nan"), LONG_MIN, 1, "nöte", "",
                                2 ** 40, False, -FLOAT_MAX)),
            record("t4", None),
            record("t5", _trade("DDD", 0, -0.0, 0, 0, "", "Z", -1, True, float("inf"))),
        ]),
        # Avro records that cannot be read: after a good one, a record whose schema id the
        # registry does not have; and in the second topic one that starts with another
        # magic byte than 0.
        topic("kc_avro_unknown_id", [
            record("u1", _trade("AAA", 10, 1.5, T0, 0, None, "X", 100, True, 0.5)),
            record("u2", confluent(99, avro_string("?"))),
        ]),
        topic("kc_avro_bad_magic", [
            record("m1", _trade("AAA", 10, 1.5, T0, 0, None, "X", 100, True, 0.5)),
            record("m2", b"\x07" + _trade("BBB", 1, 1.0, T0, 0, None, "X", 1, True, 0.5)[1:]),
        ]),
        # Avro arrays and a decimal.
        topic("kc_avro_more", [
            record(None, _more([1, 2, 3], ["a", "b"], 12345)),
            record(None, _more([], [], -1)),
            record(None, _more([INT_MIN], [""], 0)),
        ]),
        # Protobuf in Confluent's wire format, and the same messages alone.
        topic("kc_proto", [record(None, proto_serdes(TICK_ID, tick)) for tick in _TICKS]),
        topic("kc_proto_raw", [record(None, tick) for tick in _TICKS]),
        # A record without a value, which a Protobuf spec refuses.
        topic("kc_proto_tombstone", [
            record("p1", proto_serdes(TICK_ID, _TICKS[0])),
            record("p1", None),
        ]),
        # A Protobuf message with an enum field: GREEN, and RED, which is the default and
        # so not written.
        topic("kc_proto_enum", [
            record(None, proto_serdes(PAINT_ID, proto_bytes(1, b"leaf") + proto_varint(2, 1))),
            record(None, proto_serdes(PAINT_ID, proto_bytes(1, b"rose"))),
        ]),
        # Change data capture as Debezium writes it: rows created, one updated, one deleted
        # and followed by its tombstone, one created after that.
        topic("kc.inv.people", [
            record(_person_key(1), _change(None, _person(1, "ann", None), "c", T0)),
            record(_person_key(2), _change(None, _person(2, "bob", 2.5), "c", T0 + 1)),
            record(_person_key(1), _change(_person(1, "ann", None), _person(1, "anne", 9.0),
                                           "u", T0 + 2)),
            record(_person_key(2), _change(_person(2, "bob", 2.5), None, "d", None)),
            record(_person_key(2), None),
            record(_person_key(3), _change(None, _person(3, "cy", -1.0), "r", T0 + 3)),
        ]),
    ]


def _shown(data: bytes | None) -> dict[str, str] | None:
    """Bytes as the fixture file shows them: as text where they are printable text."""
    if data is None:
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return {"hex": data.hex()}
    if text.isprintable() or text == "":
        return {"text": text}
    return {"hex": data.hex()}


def fixture() -> dict[str, Any]:
    """The topics and subjects in the form of `corpus/kafka/topics.json`."""
    out_topics = []
    for topic in topics():
        records = []
        for index, item in enumerate(topic["records"]):
            records.append({
                "partition": item["partition"],
                "timestamp": T0 + 1000 * index,
                "key": _shown(item["key"]),
                "value": _shown(item["value"]),
                "headers": [[name, _shown(value)] for name, value in item["headers"]],
            })
        out_topics.append({"name": topic["name"], "partitions": topic["partitions"],
                           "compression": topic["compression"], "records": records})
    return {"format": FORMAT, "topics": out_topics, "subjects": subjects()}


def dump(doc: dict[str, Any]) -> str:
    """The file form: one record per line, so that a changed record is one changed line."""
    lines = ["{", f' "format": {doc["format"]},', ' "topics": [']
    for t, topic in enumerate(doc["topics"]):
        head = {key: topic[key] for key in ("name", "partitions", "compression")}
        lines.append("  {" + json.dumps(head, ensure_ascii=False)[1:-1] + ', "records": [')
        for r, item in enumerate(topic["records"]):
            comma = "," if r + 1 < len(topic["records"]) else ""
            lines.append("   " + json.dumps(item, ensure_ascii=False) + comma)
        lines.append("  ]}" + ("," if t + 1 < len(doc["topics"]) else ""))
    lines.append(" ],")
    lines.append(' "subjects": [')
    for s, subject in enumerate(doc["subjects"]):
        comma = "," if s + 1 < len(doc["subjects"]) else ""
        lines.append("  " + json.dumps(subject, ensure_ascii=False) + comma)
    lines.append(" ]")
    lines.append("}")
    return "\n".join(lines) + "\n"
