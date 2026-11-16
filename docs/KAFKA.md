# Kafka

How Oxhaven consumes Kafka topics into tables and, later, publishes tables to topics.
Written by task 7.4.1 (`tasks/7.4.1-kafka-first-slice.md`), which also built the first
slice. It replaces the placeholders 7.4 and 7.5 with the sized tasks at the end.

## Decision

1. **A crate of its own, `crates/oxhaven-kafka`**, with a plain Rust API
   (`oxhaven_kafka::consume`) and no gRPC surface: Java has no gRPC call that consumes or
   produces, only `KafkaTools`, which scripts reach through the Python package.
2. **The Python modules are Oxhaven's own**, with the reference's names and signatures:
   `deephaven.stream.kafka.consumer` now, `producer` and `cdc` later. They call one native
   function per entry point (`_oxhaven.kafka_consume`), as `deephaven.csv` does.
3. **The client is a small one of our own over the `kafka-protocol` crate**, which holds
   the wire messages and the record batch format and nothing else. Everything it brings is
   under MIT or Apache-2.0. librdkafka, which the owner allowed on 2026-11-16, is **not
   added**: it cannot be built without code under licences that approval does not cover.
   "The client library" has both lists; the owner decides whether to widen the approval.
4. **A record source is a trait** (`RecordSource`: `poll` returns the records that
   arrived). The table, the specs and the decoding are written against it, so tests script
   one, and a librdkafka source can replace the wire one without touching the rest.
5. **It is part of the `python` feature of the server**, as CSV and Parquet are, not a
   feature of its own: the crate is pure Rust and adds about half a minute to a build from
   nothing. A default build of the server does not link it.

## What Java offers

`io.deephaven.kafka.KafkaTools`, reached from `deephaven.stream.kafka`:

| Piece | Java | Python |
| --- | --- | --- |
| Consume to one table | `consumeToTable` | `consumer.consume` |
| Consume to a table per partition | `consumeToPartitionedTable` | `consumer.consume_to_partitioned_table` |
| Table types | `TableType.blink()`, `append()`, `ring(capacity)` | `TableType` |
| Partitions | an `IntPredicate`; `ALL_PARTITIONS` | `partitions=[...]` or `None` |
| Initial offsets | an `IntToLongFunction`; `SEEK_TO_BEGINNING` (-1), `DONT_SEEK` (-2), `SEEK_TO_END` (-3) or an offset | `offsets={partition: offset}`, `ALL_PARTITIONS_*` |
| Key and value specs | `Consume.simpleSpec`, `jsonSpec`, `avroSpec`, `protobufSpec`, `rawSpec`, `objectProcessorSpec`, `IGNORE`, `FROM_PROPERTIES` | `simple_spec`, `json_spec`, `avro_spec`, `protobuf_spec`, `object_processor_spec`, `KeyValueSpec.IGNORE`, `.FROM_PROPERTIES` |
| Columns every table has | `KafkaPartition` (int), `KafkaOffset` (long), `KafkaTimestamp` (Instant); on request the receive time and the key and value sizes; renamed or removed by `deephaven.*.column.name` properties | the same properties in `kafka_config` |
| Topics of a broker | `listTopics` | `kafka.topics` |
| Produce from a table | `produceFromTable(KafkaPublishOptions)`: specs `simpleSpec`, `jsonSpec`, `avroSpec`, `rawSpec`, `IGNORE`; `lastBy` on the key columns; whether to publish the rows the table has; topic, partition and timestamp from columns | `producer.produce` |
| Change data capture | `CdcTools` | `kafka.cdc` |

How Java consumes (`KafkaIngester`): one thread per call, with a `KafkaConsumer` that is
**assigned** the partitions the filter accepts (`partitionsFor`, `assign`); it never joins
a consumer group, so there is no rebalancing. Each partition is then sought as the offsets
say. `DONT_SEEK`, the Python default, leaves the position to the consumer: the offset the
group in `group.id` committed, or `auto.offset.reset`. The thread polls with a timeout of
half a second and hands the records to a `KafkaStreamPublisher`, which fills chunks for a
`StreamToBlinkTableAdapter`: its blink table shows on each update cycle the rows that
arrived since the last. `append()` is `BlinkTableTools.blinkToAppendOnly` of that table
and `ring(n)` is `RingTableTools.of` of it.

## What exists after task 7.4.1

`deephaven.stream.kafka.consumer.consume` with:

- `TableType.blink()`, `append()` and `ring(n)`;
- `partitions`, and `offsets` that are `ALL_PARTITIONS_SEEK_TO_BEGINNING`,
  `ALL_PARTITIONS_SEEK_TO_END` or a mapping to offsets, `SEEK_TO_BEGINNING` or
  `SEEK_TO_END`;
- `simple_spec` and `KeyValueSpec.FROM_PROPERTIES` for `String`, `short`, `int`, `long`,
  `float` and `double` (Kafka's own deserializers: big-endian, of exactly the type's size),
  with the name and type taken from the argument, `deephaven.{key,value}.column.name`,
  `deephaven.{key,value}.column.type` or `{key,value}.deserializer`, in Java's order;
- `json_spec` with columns of type `bool_`, `byte`, `short`, `int32`, `long`, `float32`,
  `double` and `string`, and a `mapping` of top-level fields;
- `KeyValueSpec.IGNORE`, and the six common columns with their properties;
- plaintext brokers, uncompressed record batches.

Everything else is refused with the name of what Java does; "Refused, by Java method"
has the list.

### How a JSON field becomes a cell

Java parses each record with Jackson, set to read a number with a fraction or exponent as
an exact `BigDecimal`, and converts a field with the node's `asInt`, `asLong`, `asDouble`
or `asText`. These coerce, and the port keeps that (`src/json.rs`):

| Column | Field is a number | `true` / `false` | text | object or array | `null` or missing |
| --- | --- | --- | --- | --- | --- |
| `int`, `short`, `byte`, `long` | the integer, cut to the low bits of the type; with a fraction or exponent: **refused** | 1 / 0 | **refused** | 0 | null |
| `double`, `float` | the nearest double, then cast for `float` | 1.0 / 0.0 | **refused** | 0.0 | null |
| `String` | its text: an integer in plain digits, another number as `BigDecimal#toString` writes it (`1.50`, `1E+3`) | `true` / `false` | itself | **refused** | null |
| `Boolean` | by its text, as below | itself | trimmed: `TRUE`, `True`, `true`, `T`, `t`, `1` are true; `FALSE`, `False`, `false`, `F`, `f` are false; empty is null; anything else fails the table | **refused** | null |

A value equal to Java's null of the column's type (`Integer.MIN_VALUE` for an `int`) is a
null cell. A record that is not an object (a number, text, `null`, nothing at all) is a
row of nulls. Jackson reads one value and ignores what follows it. A record without a
value (a tombstone) fails the table, as Jackson refuses a null string.

"Refused" here means the table fails with a message that names the Jackson method, because
a record is not known until it arrives. Java gives a value in each of these cases, seen
in the probes (`2.9` in an `int` column is 2, `"12"` is 12, an object in a `String`
column is its compact JSON); they are task 7.4.3.

### Failures

A record that cannot become a row fails the table, with Java's exception text: invalid
JSON (`Failed to parse JSON string.`), a Boolean field Java does not recognize, a number
of the wrong size for a simple column (`RecordDeserializationException: Error
deserializing VALUE for partition <topic>-<n> at offset <o>...`). So does a broker that
stops answering. A topic the broker does not have gives a table that never gets rows, in
Java too; like Java's consumer, the request asks the broker to create it unless
`allow.auto.create.topics` is `false`.

## The engine side

The stream-to-table adapter is on `main` already, in pieces:

- `oxhaven_engine::table::blink_input_table` is `StreamToBlinkTableAdapter` behind an input
  table (written for `TablePublisher`'s input-table half);
- `QueryTable::root` with an `UpdateSource`, and `tracked_source` columns, are what any
  root table is built from;
- `ring_table` is `RingTableTools.of`;
- `blink_to_append_only` exists, but private to `ops/sort/slice.rs`.

The first slice does not use `blink_input_table`: the tables it makes carry the
`InputTable` attribute, which lets any client add rows, and a Kafka table in Java has no
such attribute. It builds its own root table in `crates/oxhaven-kafka/src/stream.rs`:
the polling thread leaves decoded batches in a buffer, and the update
source drains it once per cycle, replacing the rows of a blink table or appending to an
append-only one. A failure is delivered in place of the rows of its cycle.

What is missing in the engine, and belongs there and not in this crate (task 7.4.2):

1. **A public stream table**: `stream_table(graph, definition) -> (table, publisher)` with
   `publisher.add(columns)` and `publisher.fail(failure)`, which `blink_input_table`, this
   crate and `deephaven.stream.table_publisher` all use. Today there are two copies of the
   blink refresh.
2. **`blink_to_append_only` as a public operation** (also `deephaven.stream.
   blink_to_append_only`, which today raises). The slice appends in its root table
   instead; the result is the same table, but Java's is a second node, which shows in
   update performance tables.
3. **A callback when a table is no longer used.** The slice stops its thread when the
   table is dropped (`Drop` of the update source). Java stops the ingester when the
   table's liveness count reaches zero, and `TablePublisher` has `on_shutdown_callback`.

## The client library

### What librdkafka would bring, and why it is not added

`rdkafka` 0.39.0 and `rdkafka-sys` 4.10.0 are MIT. `rdkafka-sys` compiles the librdkafka
2.12.1 sources it carries. With every optional feature off (no `libz`, `zstd`, `ssl`,
`sasl`, `gssapi`, `curl`, external `lz4`), librdkafka's `src/CMakeLists.txt` still
compiles these files into the library unconditionally, and its `LICENSES.txt` gives them
these licences:

| Files in librdkafka | Licence | Within BSD-2-Clause, MIT, Apache-2.0? |
| --- | --- | --- |
| librdkafka itself | BSD-2-Clause | yes |
| `lz4*.c`, `rdxxhash.c` (LZ4, bundled) | BSD-2-Clause | yes |
| `cJSON.c` | MIT | yes |
| `rdcrc32.c` (pycrc) | MIT | yes |
| `rdhdrhistogram.c` | MIT | yes |
| `opentelemetry/*.pb.c` | Apache-2.0 | yes |
| `rdmurmur2.c` | MIT, parts public domain | yes, with a public domain part |
| `rdfnv1a.c`, `regexp.c` | public domain | not a licence in the list |
| **`snappy.c`** (Snappy, bundled, cannot be switched off through `rdkafka-sys`) | **BSD-3-Clause** | **no** |
| **`rdsysqueue.h`** (BSD `sys/queue.h`, used throughout) | **BSD-3-Clause** | **no** |
| **`crc32c.c`** (Mark Adler) | **zlib licence** | **no** |
| **`tinycthread.c`, `tinycthread_extra.c`** | **zlib licence** | **no** |
| **`nanopb/*.c`** (for the telemetry messages) | **zlib licence** | **no** |
| `wingetopt.c` (Windows only, not compiled on Linux) | ISC and BSD-3-Clause | not built |

So the smallest librdkafka needs BSD-3-Clause for two files and the zlib licence for five,
which the approval of 2026-11-16 (BSD-2-Clause, MIT, Apache-2.0) does not cover. Nothing
of it is in the repository. The C build itself is not an obstacle here: `cmake`, `make`,
`gcc` and `g++` are installed, and GitHub's `ubuntu-latest` has them.

Also considered: `rskafka` 0.6.0 (MIT OR Apache-2.0, pure Rust, no consumer groups). It
always depends on `rsasl`, which brings `subtle` (BSD-3-Clause) and three `unicode-*`
crates under `(MIT OR Apache-2.0) AND Unicode-3.0`. Not within the rule either.

### What is added

| Crate | Version | Licence | Why |
| --- | --- | --- | --- |
| `kafka-protocol` | 0.18.0 | MIT OR Apache-2.0 | Wire messages and record batches; features `client` only, so no compression crates |
| `crc` | 3.4.0 | MIT OR Apache-2.0 | of `kafka-protocol` |
| `crc-catalog` | 2.5.0 | MIT OR Apache-2.0 | of `crc` |
| `crc32c` | 0.6.9 | MIT OR Apache-2.0 | of `kafka-protocol`: the checksum of a record batch |

Its other dependencies were in `Cargo.lock` already (`anyhow`, `bytes`, `indexmap`,
`uuid`), as were `serde` and `serde_json`, whose `raw_value` feature is now on. No C code.

### What the choice costs

`src/wire.rs` does what the slice needs of a consumer: `Metadata` (v8)
for the partitions and their leaders, `ListOffsets` (v5) for the first and last offset,
`Fetch` (v11) in a loop, one blocking connection per broker, used by the one polling
thread. Brokers from 2.4 on answer these versions; it was run against 3.9.1. A librdkafka
build would give the following at once; here each is work, listed as tasks below:

| | Wire client today | With librdkafka |
| --- | --- | --- |
| TLS, SASL | refused | built in (OpenSSL and SASL are further licences to check) |
| Compressed batches (gzip, snappy, lz4, zstd) | refused | built in |
| Committed offsets (`DONT_SEEK`), `auto.offset.reset` | refused | built in |
| A partition that changes leader, a broker that restarts | the table fails | retried |
| Producing, idempotence, transactions | not built | built in |
| The consumer properties of Java's client | accepted and without effect, except those above | librdkafka's own property names, which differ from Java's in places |

Neither client runs Java's `KafkaConsumer`, so either way the consumer properties are a
translation. If the owner widens the approval, the swap is a second `RecordSource`.

## Tests and the broker

| Layer | Test | Needs | Where it runs |
| --- | --- | --- | --- |
| JSON conversion, record batches, refusals | unit tests in `src/` | nothing | `cargo test --workspace`: every pull request |
| Tables from a scripted `RecordSource`, with the consistency checker | `tests/consume.rs` | nothing | every pull request |
| The Python module: specs, checks, refusals, an unreachable broker | `console::python::kafka` | Python | the `python-console-on` job |
| The wire client against a broker | `tests/broker.rs`, ignored unless asked for | Docker | `tests/broker.sh`, by hand until CI has the job below |
| The same rows as Java | probes of both servers on one broker | Docker, the Java reference | by hand, in task 7.4.1; a recorded corpus is task 7.4.6 |

**A broker on this machine.** Docker is installed but no daemon runs. One started by hand
(`dockerd` with its own socket and data root, no bridge and no iptables rules, the
container on the host network) pulled `apache/kafka:3.9.1` (204 MB) from Docker Hub and
ran it in KRaft mode on `127.0.0.1:19092`. `tests/broker.sh` does the same for the tests:
it starts the container, creates and fills two topics with the image's own tools, runs
`cargo test -p oxhaven-kafka --test broker -- --ignored` and removes the container. The
tests refuse a broker address that is not `127.0.0.1` or `localhost`.

**A broker in CI.** GitHub-hosted `ubuntu-latest` runners have a running Docker daemon and
may pull public images, so the script should run there unchanged; this was not tried. The
job to add (a change to `.github/workflows/ci.yml`, which task 7.4.1 does not make):

```yaml
  kafka-broker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # the toolchain and cache steps of the `rust` job
      - run: crates/oxhaven-kafka/tests/broker.sh
```

No secret and no other runner are needed. Until it exists, a pull request exercises
everything but `src/wire.rs`'s connections: its record decoding has unit tests, its
requests do not.

**Not verified anywhere:** brokers other than 3.9.1; a topic with more than two
partitions or more than one broker; batches larger than one fetch; what Java does on a
leader change.

## Differences from Java seen in the probes

Both servers consumed the same topics of one broker (Java on port 10986, Oxhaven on
10987), 21 cases and a run with records arriving while the tables were open.

- Same: the schemas; the rows of simple and JSON tables, sorted by partition and offset;
  the flags (blink, refreshing, flat, failed); offsets from the beginning, the end and a
  number; one partition of two; a ring table of one partition; a blink table and an
  aggregation of it, and append-only tables, as records arrive; the four failures above.
- Different, by design for now: the three refused conversions.
- Different in text only: the Python traceback around an error (`PARITY.md` A5).
- **Row order across partitions.** Within a partition both keep offset order. Where two
  partitions have records in one poll, Java delivers them in the order of a `HashMap` of
  partitions and Oxhaven by partition number, so the last rows of an append-only table,
  and so `tail` and a ring table over two partitions, differ. Java defines no order
  here; see below.

## For the owner's ruling

1. **The licence of the client.** Either widen the approval to the zlib licence and
   BSD-3-Clause for the librdkafka files in the table above (and then decide TLS: OpenSSL
   is Apache-2.0 from 3.0), or keep the wire client and schedule tasks 7.4.4 and 7.4.5.
2. **Row order across partitions within one poll** is the iteration order of a Java
   `HashMap` keyed by topic and partition. It is stable for one topic name and partition
   count, but nothing documents it. Oxhaven orders by partition number. Is this
   `PARITY.md` A3 (row order where Java defines none), or should the hash order be
   reproduced?
3. **Records before a bad one.** Java's client returns the records before a record it
   cannot deserialize and throws on the next poll, so those rows may show for a cycle
   before the table fails. Oxhaven decodes a fetch at once and fails without them. The
   failed table is the same.
4. **A consumer property Oxhaven does not act on is accepted in silence**, as Java's
   client accepts (and logs) a property it does not know. For properties Java does act on
   (`max.poll.records`, `fetch.max.bytes`, `session.timeout.ms`, ...) this means a setting
   without effect. The alternative is to refuse every property outside a known list.
5. **Text that is not UTF-8 in a `String` column** is refused. Java replaces the bad bytes
   (`new String(bytes, UTF_8)`); Rust's lossy conversion is not known to replace them the
   same way in every case.

## Refused, by Java method

| What | Named in the refusal |
| --- | --- |
| `offsets` left at its default, `DONT_SEEK` | `org.apache.kafka.clients.consumer.KafkaConsumer#position` |
| `security.protocol` other than `PLAINTEXT` | `org.apache.kafka.common.network.ChannelBuilders#clientChannelBuilder` |
| `isolation.level=read_committed` | `org.apache.kafka.clients.consumer.internals.FetchCollector` |
| A compressed record batch | `org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator` |
| A record batch older than format 2 | `org.apache.kafka.common.record.AbstractLegacyRecordBatch` |
| A simple column of `byte[]`, `UUID`, `ByteBuffer`, `Bytes` or any other type | `io.deephaven.kafka.SimpleImpl.SerDeserVisitor` |
| Bytes that are not UTF-8 in a `String` column | `org.apache.kafka.common.serialization.StringDeserializer#deserialize` |
| A JSON column of `char`, `Instant`, or another type | `io.deephaven.kafka.ingest.JsonNodeCharFieldCopier`, `JsonNodeInstantFieldCopier`, `JsonNodeChunkAdapter#makeFieldCopier` |
| A JSON pointer to a nested field | `com.fasterxml.jackson.databind.JsonNode#at(JsonPointer)` |
| Two fields mapped to one column | `io.deephaven.kafka.ingest.MultiFieldChunkAdapter` |
| A JSON record that is an array | `com.fasterxml.jackson.databind.node.ArrayNode#_at` |
| A number with a fraction or exponent in an integer column | `com.fasterxml.jackson.databind.node.DecimalNode#asInt`, `#asLong` |
| Text in a numeric column | `com.fasterxml.jackson.databind.node.TextNode#asInt`, `#asLong`, `#asDouble` |
| An object or array in a `String` or `Boolean` column | `com.fasterxml.jackson.databind.node.ContainerNode#toString` |
| An integer outside 128 bits | `com.fasterxml.jackson.databind.node.BigIntegerNode` |
| `consume_to_partitioned_table` | `io.deephaven.kafka.KafkaTools#consumeToPartitionedTable` |
| `avro_spec`, `protobuf_spec`, `object_processor_spec`, `ProtobufProtocol` | `io.deephaven.kafka.KafkaTools.Consume#avroSpec`, `#protobufSpec`, `#objectProcessorSpec`, `io.deephaven.kafka.protobuf.Protocol` |
| `deephaven.stream.kafka.topics` | `io.deephaven.kafka.KafkaTools#listTopics` |
| `deephaven.stream.blink_to_append_only`, `add_only_to_blink` | `io.deephaven.engine.table.impl.BlinkTableTools#blinkToAppendOnly`, `AddOnlyToBlinkTableAdapter#toBlink` |

`deephaven.stream.kafka.producer`, `cdc` and `deephaven.stream.table_publisher` do not
exist yet: importing them fails.

## Tasks

These replace 7.4 and 7.5. Sizes: S is a day of one implementer, M two to three, L a week.

| Task | What | Size | Needs |
| --- | --- | --- | --- |
| 7.4.1 | This design and the first slice | done here | |
| 7.4.2 | Engine: public `stream_table` with a publisher, public `blink_to_append_only`, a callback when a table is released; `deephaven.stream.table_publisher`, `blink_to_append_only`; move this crate onto them | M | 7.4.1 |
| 7.4.3 | JSON completely: the refused conversions (`NumberInput#parseAsInt` and friends, `BigDecimal#intValue`, Jackson's compact output), nested pointers and arrays, `char` and `Instant` columns; simple `byte[]`; raw spec | M | 7.4.1 |
| 7.4.4 | Client: committed offsets and `auto.offset.reset` (`DONT_SEEK`), compressed batches (gzip through `flate2`, which is in the tree; the others need a licence check each), leader changes and reconnects, `kafka.topics`. Or, if the owner allows librdkafka, a second `RecordSource` and the property translation | L (M with librdkafka) | owner's ruling 1 |
| 7.4.5 | TLS and SASL (`rustls` if within the rule; PLAIN and SCRAM by hand) | M | 7.4.4 |
| 7.4.6 | `consume_to_partitioned_table`; a recorded Java corpus for consumption in `oracle/corpus` and a replay of it through a scripted source; the `kafka-broker` CI job | M | 7.4.2 |
| 7.4.7 | Avro specs and the schema registry client (a licence check of the Avro crate first) | L | 7.4.3 |
| 7.4.8 | Protobuf specs (`prost-reflect` is in the tree), both protocols | M | 7.4.7 for the registry |
| 7.5.1 | Produce: the `Produce` request in the wire client, simple and JSON specs, `publish_initial`, `last_by_key_columns`, how removes and modifies are published; `deephaven.stream.kafka.producer` | L | 7.4.4 |
| 7.5.2 | Produce: Avro and raw specs, topic, partition and timestamp columns | M | 7.5.1, 7.4.7 |
| 7.5.3 | `deephaven.stream.kafka.cdc` | M | 7.4.7 |
