# Kafka

How Oxhaven consumes Kafka topics into tables and, later, publishes tables to topics.
Written by task 7.4.1 (`tasks/7.4.1-kafka-first-slice.md`), which also built the first
slice, and brought up to date by task 7.4.4 (`tasks/7.4.4-kafka-librdkafka.md`), which
moved the consumer onto librdkafka. It replaces the placeholders 7.4 and 7.5 with the
sized tasks at the end.

## Decision

1. **A crate of its own, `crates/oxhaven-kafka`**, with a plain Rust API
   (`oxhaven_kafka::consume`) and no gRPC surface: Java has no gRPC call that consumes or
   produces, only `KafkaTools`, which scripts reach through the Python package.
2. **The Python modules are Oxhaven's own**, with the reference's names and signatures:
   `deephaven.stream.kafka.consumer` now, `producer` and `cdc` later. They call one native
   function per entry point (`_oxhaven.kafka_consume`), as `deephaven.csv` does.
3. **The client is librdkafka**, through the `rdkafka` crate, compiled from the sources
   `rdkafka-sys` carries, in its smallest form plus Zstandard. The owner allowed its
   licences on 2026-11-16 (`PORTING.md`, "Dependencies"). TLS, SASL and gzip are not
   built: each needs a library that ruling does not cover. "The client library" has the
   table. The wire client of the first slice is removed.
4. **A record source is a trait** (`RecordSource`: `poll` returns the records that
   arrived). The table, the specs and the decoding are written against it: tests script
   one, and `client::ClientSource` is the one over librdkafka.
5. **It is part of the `python` feature of the server**, as CSV and Parquet are, not a
   feature of its own. A default build of the server does not link it; a build of the
   workspace compiles it, and with it librdkafka: "Build cost" has the numbers.

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

## What exists after tasks 7.4.1, 7.4.3 and 7.4.4

`deephaven.stream.kafka.topics`, and `deephaven.stream.kafka.consumer.consume` with:

- `TableType.blink()`, `append()` and `ring(n)`;
- `partitions`, and `offsets` that are `ALL_PARTITIONS_SEEK_TO_BEGINNING`,
  `ALL_PARTITIONS_SEEK_TO_END`, a mapping to offsets, `SEEK_TO_BEGINNING` or
  `SEEK_TO_END`, or left out (`ALL_PARTITIONS_DONT_SEEK`): "Offsets and consumer groups";
- `simple_spec` and `KeyValueSpec.FROM_PROPERTIES` for `String`, `short`, `int`, `long`,
  `float` and `double` (Kafka's own deserializers: big-endian, of exactly the type's size)
  and `byte[]` (the bytes as they are),
  with the name and type taken from the argument, `deephaven.{key,value}.column.name`,
  `deephaven.{key,value}.column.type` or `{key,value}.deserializer`, in Java's order;
- `json_spec` with columns of type `bool_`, `byte`, `char`, `short`, `int32`, `long`,
  `float32`, `double`, `string` and `Instant`, and a `mapping` of fields and JSON pointers
  into nested objects and arrays, with every coercion of Java's (below);
- `KeyValueSpec.IGNORE`, and the six common columns with their properties;
- the consumer properties of "Consumer properties";
- plaintext brokers; record batches that are uncompressed or compressed with snappy, lz4
  or zstd; transactional topics at either `isolation.level`; a broker that restarts.

Everything else is refused with the name of what Java does; "Refused, by Java method"
has the list.

### How a JSON field becomes a cell

Since task 7.4.3 (`tasks/7.4.3-kafka-json.md`). Java parses each record with Jackson into
a tree, set to read a number with a fraction or exponent as an exact `BigDecimal`, finds
each column's node with a JSON pointer, and converts it with the node's `asInt`,
`asLong`, `asDouble`, `asText` or `toString`. These coerce, and the port keeps that
(`src/json.rs`, which reads the record into the same tree):

| Column | Integer | Number with a fraction or exponent | `true` / `false` | Text | Object or array | `null` or missing |
| --- | --- | --- | --- | --- | --- | --- |
| `int`, `short`, `byte`, `long` | cut to the low bits of the type (`byte` and `short` through `int`) | truncated toward zero, then cut (`BigDecimal#intValue`, `#longValue`): `2.9` is 2, `1e19` in an `int` is -1981284352 | 1 / 0 | `NumberInput#parseAsInt`, `#parseAsLong`: trimmed, an optional sign and digits are read as the type and are 0 when out of range; any other text goes through `Double#parseDouble` and is cast (`"12.9"` is 12, `"1e3"` 1000, `"Infinity"` the largest value); text neither reads is 0 | 0 | null |
| `double`, `float` | the nearest double, then cast for `float` | the nearest double (`1e400` is infinity) | 1.0 / 0.0 | `Double#parseDouble` of the trimmed text, with `NaN`, `Infinity`, hexadecimal numbers and an `f` or `d` suffix; 0.0 where it fails | 0.0 | null |
| `char` | the low sixteen bits | the low sixteen bits of `intValue` | `t` / `f` | its first UTF-16 unit; empty is null | null | null |
| `String` | its digits (`-0` is `0`) | `BigDecimal#toString`: `1.50`, `1E+3`, `0.00000` | `true` / `false` | itself | its compact JSON, as Jackson writes it | null |
| `Boolean` | by its text, as for text | by its text (`1e0` is `1`, so true) | itself | trimmed: `TRUE`, `True`, `true`, `T`, `t`, `1` are true; `FALSE`, `False`, `false`, `F`, `f` are false; empty is null; anything else fails the table | its compact JSON, which fails the table | null |
| `Instant` | an `int` or `long` counts seconds, milliseconds, microseconds or nanoseconds by its size (`epochAutoToInstant`, whose multiplication wraps); a larger integer is read as text | its text, as for text (`12.5e1` is `125`) | its text, so it fails | `DateTimeUtils#parseInstant`: up to nineteen digits by size as above, else a date and time with a zone; anything else fails the table | empty text, so it fails | null |

A value equal to Java's null of the column's type (`Integer.MIN_VALUE` for an `int`,
`￿` for a `char`, `Long.MIN_VALUE` nanoseconds for an `Instant`) is a null cell. An
`Instant` from second `Long.MAX_VALUE / 10^9` on fails the table
(`DateTimeOverflowException`), as Java's `DateTimeUtils#epochNanos` does.

**Pointers.** A column is filled from the node that a JSON pointer finds: `/` and the
field's name, with `~` written `~0` and `/` written `~1`, unless the `mapping` gives a
field for it. A field of the mapping that starts with `/` is a pointer as it is and may go
deeper (`/a/b/0`): a segment is an object's field by name (also when the name is a
number) or an array's element by index (digits without a leading zero, at most
`Integer.MAX_VALUE`). `~` before anything but `0` or `1` is kept as written, as Jackson
keeps it. A pointer that finds nothing, or goes through a number or text, gives a null
cell. A record that is not an object (an array, a number, text, `null`, nothing at all)
is read the same way: `/0` of an array is its first element, and a field name finds
nothing. Jackson reads one value and ignores what follows it.

**The mapping and the columns.** Java turns the mapping into a map from pointer to column
and then adds the pointer of each column the mapping does not name; a pointer already in
the map is then given to that column. Each entry fills its column once per record, so a
column that two fields are mapped to, or that loses its only pointer that way, makes
Java's chunks of different sizes, and the table fails at the first record with
`IllegalStateException: Publisher chunks have size mismatch: 1, 1, 1, 2, 1` (probed;
reproduced, with the sizes). A mapping to a column that is not defined fails the call
(`Column not found in Deephaven table: -1`). A column type other than those above fails
the call with Java's `Type <name> not supported for JSON` (`byte[]`, `int[]`,
`BigDecimal`, `LocalDate`, `Duration`, `ZonedDateTime`, ...; probed).

**What Jackson refuses.** Invalid JSON fails the table with `Failed to parse JSON string.`
Jackson's limits are kept: nesting deeper than 1000 objects and arrays, a number of more
than 1000 digits (counting those of the fraction and exponent), and a decimal whose scale
does not fit an `int` (`1e-2147483648`; Java's `BigDecimal` text). White space between
tokens is space, tab, line feed and carriage return only. A record without a value (a
tombstone) fails the table, as Jackson refuses a null string.

**Compact JSON** is Jackson's: no spaces, the fields in the order they first appeared (a
repeated field keeps its place and takes the last value), `"` and `\` escaped, the
control characters below U+0020 as `\b`, `\t`, `\n`, `\f`, `\r` or `\u00XX` in upper
case, every other character as it is, numbers as in a `String` column.

**A simple `byte[]` column** (`simple_spec(name, dtypes.byte_array)`,
`deephaven.value.column.type=byte[]` or `ByteArrayDeserializer`) holds the bytes of the
key or value as they are; an empty value is an empty array, a tombstone null. The server
does not send such a column to a client yet (Java sends Arrow `Binary`, see
`ColumnType::Array` in the contracts); operations on the server can use it.

### Failures

A record that cannot become a row fails the table, with Java's exception text: invalid
JSON (`Failed to parse JSON string.`), a Boolean field Java does not recognize, an
`Instant` Java cannot read or store, a column filled twice or never, a number of the
wrong size for a simple column (`RecordDeserializationException: Error
deserializing VALUE for partition <topic>-<n> at offset <o>...`).

A broker that goes away does not fail the table: librdkafka reconnects and goes on, as
Java's consumer does. What ends the consumption is what `KafkaConsumer#poll` throws in
Java: no position and no policy (`auto.offset.reset=none`), a missing authorization, a
corrupt batch. `client::ends_consumption` has the list of librdkafka's error codes that
are taken as such; every other code it reports is one it retries.

A topic the broker does not have gives a table that never gets rows, also once the topic
exists and has records; in Java too (probed). Like Java's consumer, the request asks the
broker to create the topic unless `allow.auto.create.topics` is `false`.

### Offsets and consumer groups

Java's ingester assigns partitions by hand; `group.id` only names where positions are
committed to and read from. The same here (`client::ClientSource::connect`):

- A partition whose offset is `DONT_SEEK` starts at the offset the group of `group.id`
  committed for it; without one, or without a group, where `auto.offset.reset` says:
  `latest` (the default), `earliest`, or `none`, which fails the table with Java's
  `NoOffsetForPartitionException: Undefined offset with no reset policy for partitions:
  [...]`.
- With a group and `enable.auto.commit` not `false`, the position of every assigned
  partition is committed every `auto.commit.interval.ms` (five seconds) and when the
  consumer closes: the offset after the last record a poll returned, and for a partition
  that has delivered nothing the offset it started at (the end of the topic after
  `SEEK_TO_END`, for one). The commit does not wait for the row to be in the table, in
  Java neither.
- Without a group nothing is committed. `enable.auto.commit=true` without a group is
  Java's `InvalidConfigurationException`.

How it is done over librdkafka, which differs from Java's client in three ways:

1. librdkafka assigns partitions only to a consumer that has a group. Without `group.id`
   the consumer is given the group `oxhaven-no-group`, commits are off, and no offset is
   asked of it. No such group appeared on the broker in the probes.
2. The start of each partition is worked out before the assignment (the committed offset,
   or the first or last offset of the partition) and the partition is assigned at that
   number, so that it can be recorded for the first commit.
3. librdkafka commits what the application stores; the source stores the start of each
   partition, then the offset after the last record of each poll
   (`enable.auto.offset.store` is off).

### Consumer properties

Java hands every property to `KafkaConsumer`, which acts on the names it knows and logs
the others. librdkafka refuses a name it does not know and has its own names and defaults,
so the properties are translated (`src/config.rs`):

| Property | What happens |
| --- | --- |
| `bootstrap.servers`, `client.id`, `client.rack`, `group.instance.id`, `auto.commit.interval.ms`, `fetch.min.bytes`, `fetch.max.bytes`, `max.partition.fetch.bytes`, `check.crcs`, `isolation.level`, `allow.auto.create.topics`, `metadata.max.age.ms`, `reconnect.backoff.ms`, `reconnect.backoff.max.ms`, `retry.backoff.ms`, `retry.backoff.max.ms`, `connections.max.idle.ms`, `socket.connection.setup.timeout.ms` | passed to librdkafka under the same name |
| `fetch.max.wait.ms`, `request.timeout.ms`, `receive.buffer.bytes`, `send.buffer.bytes` | passed as `fetch.wait.max.ms`, `socket.timeout.ms`, `socket.receive.buffer.bytes`, `socket.send.buffer.bytes` (-1 becomes 0, librdkafka's "system default") |
| `group.id`, `enable.auto.commit`, `auto.offset.reset` | read here: "Offsets and consumer groups". Checked as Java checks them: a blank group, a value that is not `true` or `false`, a policy that is not `earliest`, `latest` or `none` in lower case are Java's exceptions |
| `max.poll.records` | the most records one poll returns (500); checked as Java checks it |
| `default.api.timeout.ms` | how long finding the partitions and the offsets may take (60 seconds); an unreachable broker fails the call after it, as in Java |
| `isolation.level`, `allow.auto.create.topics`, `check.crcs`, `request.timeout.ms` left out | set to Java's defaults (`read_uncommitted`, `true`, `true`, 30 seconds), which are not librdkafka's |
| `security.protocol` other than `PLAINTEXT`; `auto.offset.reset=by_duration:...` | refused by name |
| `key.deserializer`, `value.deserializer`, `deephaven.*` | read by the specs and the table, as before |
| every other name | not passed, and without effect: Java's own (`session.timeout.ms`, `heartbeat.interval.ms`, `max.poll.interval.ms`, `partition.assignment.strategy`, which matter only to a consumer that joins a group; `interceptor.classes`, `metric.reporters`, `exclude.internal.topics`, `client.dns.lookup`, `sasl.*`, `ssl.*`, ...) and names nobody knows, which Java logs and ignores too |

A value Java accepts and librdkafka does not (its ranges are narrower in places:
`fetch.max.bytes` up to 2147483135, `max.partition.fetch.bytes` up to 1000000000) fails
the call with librdkafka's message. A value of a passed property that neither accepts
(`fetch.min.bytes=few`) fails in both.

In Python a property whose name or value is not a `str` is refused before any of this,
as the reference refuses it: `deephaven.jcompat.j_properties` calls
`Properties#setProperty(String, String)`, and jpy finds no such method for a number
(`RuntimeError: no matching Java method overloads found`) and passes `None` as null
(`java.lang.NullPointerException`). The first slice turned such values into text.

### The topics of a broker

`deephaven.stream.kafka.topics(config)` is `KafkaTools#listTopics`: the names in the
broker's metadata without Kafka's internal topics (`__consumer_offsets`,
`__transaction_state`, `__share_group_state`), with the properties translated as above.
Java returns the key set of a `HashMap`, so its order is that of the names' hash codes;
`client::java_hash_map_order` reproduces it (the more than thirty topics of the probe
came in the same order from both), except for a bucket of more than eight names, which Java reorders into
a tree.

## The engine side

Since task 7.4.2 (`tasks/7.4.2-stream-adapter.md`) the engine has Java's stream adapter,
and this crate, the blink input table and `deephaven.stream.table_publisher` are built on
it:

- `oxhaven_engine::table::stream_table(graph, definition, callbacks)` returns a blink
  table and its `StreamPublisher` (`StreamToBlinkTableAdapter` with a
  `TablePublisher`). Any thread calls `publisher.add(columns)`, `add_table(table)` or
  `fail(failure)`; each cycle the table calls the flush callback, then shows the rows
  that arrived since the last cycle, or fails with the failures, which go before the rows
  that came with them. The stream ends when the table fails or is dropped: the publisher
  is no longer alive, later rows are dropped, and the shutdown callback runs once.
- `blink_input_table` is a stream table with the `InputTable` attribute.
- `oxhaven_engine::table::blink_to_append_only(table, limit)` is
  `BlinkTableTools#blinkToAppendOnly`, a node of its own, and gives the same table when
  asked again of the same blink table while the first answer is alive, as Java's
  memoized one does. `head` of a blink table is the same operation with a limit.
- `ring_table` is `RingTableTools.of`.

This crate makes a stream table per `consume`: the polling thread publishes each decoded
poll, and a record it cannot decode fails the table and ends the thread. The shutdown
callback stops the thread, so it ends when the table is dropped, as Java's ingester ends
when the table's liveness count reaches zero. An append-only table is
`blink_to_append_only` of the blink table, and a ring table `ring_table` of it, as in
Java; each holds the blink table, which ends when they are dropped.

## The client library

### What is built

`rdkafka` 0.39.0 and `rdkafka-sys` 4.10.0 (MIT) with the features `zstd` and nothing else:
no default features (`libz`, `tokio`), and librdkafka's own `configure` build, not the
`cmake-build` feature, which would add the `cmake` crate for nothing. `rdkafka-sys`
compiles the librdkafka 2.12.1 sources it carries and links the result statically. Its
build script passes `--disable-ssl --disable-gssapi --disable-zlib --disable-curl
--disable-lz4-ext --enable-zstd`, so nothing is taken from the machine it is built on
even where the machine has it (this one and GitHub's runners have the zlib and OpenSSL
headers). The server binary links no library it did not link before (`ldd`: libc, libm,
libgcc, libpython).

The owner's ruling of 2026-11-16 06:18 UTC: BSD-3-Clause and the zlib licence are allowed
for the files bundled in librdkafka, next to BSD-2-Clause, MIT and Apache-2.0; `zstd-sys`
was allowed already (`PORTING.md`, "Dependencies").

Crates added to `Cargo.lock`:

| Crate | Version | Licence | Why |
| --- | --- | --- | --- |
| `rdkafka` | 0.39.0 | MIT | the client's Rust interface |
| `rdkafka-sys` | 4.10.0+2.12.1 | MIT | the librdkafka sources and their build |
| `num_enum`, `num_enum_derive` | 0.7.6 | BSD-3-Clause OR MIT OR Apache-2.0 | of `rdkafka-sys`: error codes as an enum |
| `proc-macro-crate` | 3.5.0 | MIT OR Apache-2.0 | of `num_enum_derive`, at build time |
| `toml_edit`, `toml_datetime`, `toml_parser` | 0.25.19, 1.1.3, 1.1.5 | MIT OR Apache-2.0 | of `proc-macro-crate`, at build time |
| `winnow` | 1.0.5 | MIT | of `toml_edit`, at build time |

Its other dependencies were in `Cargo.lock` already (`futures-channel`, `futures-util`,
`libc`, `log`, `serde`, `serde_json`, `slab`, `pkg-config`, and `zstd-sys` 2.1.3,
BSD-3-Clause AND BSD-2-Clause, for Parquet). Removed with the wire client:
`kafka-protocol`, `crc`, `crc-catalog`, `crc32c`.

C files compiled into the library, by the licence librdkafka's `LICENSES.txt` gives them
(the object files of the build were listed to see which are compiled):

| Files in librdkafka | Licence | Covered by |
| --- | --- | --- |
| librdkafka itself (`rd*.c`, `rdkafka*.c`, among them its own SASL PLAIN and its mock broker) | BSD-2-Clause | the ruling |
| `lz4.c`, `lz4frame.c`, `lz4hc.c`, `rdxxhash.c` (LZ4, bundled) | BSD-2-Clause | the ruling |
| `cJSON.c` | MIT | the ruling |
| `rdcrc32.c` (pycrc) | MIT | the ruling |
| `rdhdrhistogram.c` | MIT | the ruling |
| `opentelemetry/*.pb.c` | Apache-2.0 | the ruling |
| `rdmurmur2.c` | MIT, parts public domain | the ruling |
| `rdfnv1a.c` | public domain | the ruling ("public-domain files") |
| `snappy.c` (Snappy, bundled) | BSD-3-Clause | the ruling |
| `rdsysqueue.h` (BSD `sys/queue.h`) | BSD-3-Clause | the ruling |
| `crc32c.c` (Mark Adler) | zlib licence | the ruling |
| `tinycthread.c`, `tinycthread_extra.c` | zlib licence | the ruling |
| `nanopb/pb_common.c`, `pb_encode.c`, `pb_decode.c` | zlib licence | the ruling |
| Zstandard, from `zstd-sys` (not bundled in librdkafka) | BSD-3-Clause AND BSD-2-Clause | the earlier ruling on `zstd-sys` |
| `regexp.c` (public domain), `wingetopt.c` (ISC and BSD-3-Clause) | | not compiled: the C library's `regex` is used, and the second is for Windows |

### What is left off, and what each would add

| Feature of `rdkafka` | Gives | Adds | Licence of what it adds | Why it is off |
| --- | --- | --- | --- | --- |
| `libz` | gzip batches | the crate `libz-sys` (MIT OR Apache-2.0), which links the machine's `libz.so` | zlib licence | an external zlib, which the ruling excludes |
| `libz-static` | gzip batches | `libz-sys` compiling the zlib sources it carries | zlib licence | the same licence the ruling allows, but for files bundled in librdkafka; these are bundled in `libz-sys`. One word from the owner would cover it |
| `ssl` | TLS; SASL SCRAM and OAUTHBEARER | `openssl-sys` (MIT) with `openssl-macros`, `vcpkg` and others, and the machine's OpenSSL | Apache-2.0 (OpenSSL 3) | excluded by the ruling |
| `ssl-vendored` | the same | `openssl-src` (MIT OR Apache-2.0), which compiles OpenSSL from source (minutes of build, needs `perl`) | Apache-2.0 | excluded by the ruling |
| `gssapi` (alias `sasl`) | SASL GSSAPI (Kerberos) | `ssl`, and `sasl2-sys` (Apache-2.0) with Cyrus SASL | BSD-4-Clause-like (Cyrus SASL, with an advertising clause) | excluded by the ruling |
| `gssapi-vendored` | the same | Cyrus SASL and MIT Kerberos (`krb5-src`) compiled from source | Cyrus SASL's licence; MIT (Kerberos) | excluded by the ruling |
| `curl`, `curl-static` | OAUTHBEARER over OIDC | `curl-sys` (MIT) and libcurl, which needs TLS itself | curl licence (MIT-like) | excluded by the ruling |
| `external-lz4` | nothing new | `lz4-sys` (MIT) | BSD-2-Clause | lz4 is bundled |
| `cmake-build` | the same library | `cmake` (MIT OR Apache-2.0) | | the `configure` build works |
| `tokio`, `naive-runtime` | async consumers | `tokio` is in the tree; `futures-executor` | MIT | the source polls from its own thread |

The licences in the third and fourth columns are from the crates' manifests and from
what is generally known of the C libraries; none of these crates was downloaded, so
their own dependency lists were not checked. Check them again before turning one on.

**gzip, answered.** librdkafka has no zlib of its own: gzip needs the `libz` feature,
which is an external zlib either way (the machine's, or `libz-sys`'s copy). So gzip is
off, and a gzip batch fails the table by the name
`org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator`. Java reads it
(probed). gzip is Kafka's oldest codec and common in old topics; see "For the owner's
ruling".

**SASL PLAIN** is in librdkafka's own BSD-2-Clause code and is compiled, so
`SASL_PLAINTEXT` with `sasl.mechanism=PLAIN` needs no further licence. It is still
refused with the rest of `security.protocol`: it was not asked for, no broker with SASL
was at hand to probe it, and a password over a connection without TLS is seldom wanted.
It is the cheap first half of task 7.4.5.

### The wire client is removed

`src/wire.rs` of the first slice, and the `kafka-protocol` crate under it, are deleted.
It read uncompressed batches from a broker that never moved, and nothing else: no
committed offsets, no reconnect. Keeping it as a second way to a broker would mean two
consumers with different behaviour and one of them untested against Java from now on. Its
one advantage, a build without a C compiler, is not a need of this project: `zstd-sys`
needs one already. Tests do not need it either: tables are tested over a scripted
`RecordSource`, and the client against a broker. Its three unit tests of record batch
decoding went with it; librdkafka does that decoding now, and `tests/broker.rs` covers it.

### Build cost

Measured here (4 cores shared with three other builds, `-j 2`, line tables only):

| | Time |
| --- | --- |
| `rdkafka-sys`: `configure` and `make libs` of librdkafka, once per target directory | 105 s |
| `rdkafka`, `num_enum` and the build-time crates | under 10 s together |
| `kafka-protocol`, no longer built | 12 s saved |
| `cargo build -p oxhaven-kafka` from nothing, with both clients still in it | 3 min 28 s |

`make libs` also builds librdkafka's C++ wrapper, which nothing links; `rdkafka-sys`
offers no way to leave it out. The build directory of `rdkafka-sys` takes 200 MB.

Kafka stays in the `python` feature. The cost does not depend on that choice anyway:
`oxhaven-kafka` is a member of the workspace, so `cargo test --workspace` and
`cargo clippy --workspace` compile librdkafka whatever the server's features are.

**CI.** Every job that compiles (`rust`, `python-console-on` twice, `oracle-replay`,
`kafka-broker`, and `nightly`) builds librdkafka once and then has it in its
`Swatinem/rust-cache`. The build needs `gcc`, `g++`, `make`, `bash` and the C library's
headers, all of which GitHub's `ubuntu-latest` image has; no step installs a package and
none is needed. This was not run on GitHub by this task: it is read off the image's
documented contents and off this machine, which has the same tools.

## Tests and the broker

| Layer | Test | Needs | Where it runs |
| --- | --- | --- | --- |
| JSON conversion, refusals, the translation of properties, Java's hash order, which errors end a consumption | unit tests in `src/` | nothing | `cargo test --workspace`: every pull request |
| Tables from a scripted `RecordSource`, with the consistency checker | `tests/consume.rs` | nothing | every pull request |
| The Python modules: specs, checks, refusals, an unreachable broker | `console::python::kafka` | Python | the `python-console-on` job |
| The client against a broker: offsets, groups and what is committed, snappy, lz4, zstd, the gzip refusal, the listing of topics, a broker that restarts | `tests/broker.rs`, ignored unless asked for | Docker | `tests/broker.sh`: the `kafka-broker` job |
| The same rows as Java | probes of both servers on one broker | Docker, the Java reference | by hand, in tasks 7.4.1 and 7.4.4; a recorded corpus is task 7.4.6 |

**A broker on this machine.** Docker is installed but no daemon runs. One started by hand
(`dockerd` with its own socket and data root, no bridge and no iptables rules, the
container on the host network) pulled `apache/kafka:3.9.1` (204 MB) from Docker Hub and
ran it in KRaft mode on `127.0.0.1:19092`. `tests/broker.sh` does the same for the tests:
it starts the container, creates and fills three topics with the image's own tools (one
of them compressed with gzip, which the tests' own producer cannot write), runs the tests
and removes the container. The other records the tests produce themselves, with
librdkafka's producer, into topics named after the time. The test of a restart runs
alone, after the others, with `docker restart` as the command it is given. The tests
refuse a broker address that is not `127.0.0.1` or `localhost`.

**A broker in CI.** The job `kafka-broker` in `.github/workflows/ci.yml` runs the script
on `ubuntu-latest`, whose Docker daemon may pull public images. It uses no secret and no
other runner. It was added by task 7.4.4 without having been run on GitHub: the first
pull request shows whether the runner's Docker takes `--network host` with a broker on
`127.0.0.1` as this machine's does.

**Not verified anywhere:** brokers other than 3.9.1; more than one broker, so a
partition that changes leader while its old leader stays up (a single broker that
restarts is tested and probed); a topic with more than two partitions; aborted
transactions under `read_committed`; `SEEK_TO_END` under `read_committed` while a
transaction is open (librdkafka's last offset is the high watermark, Java's the last
stable offset); SASL PLAIN; `group.instance.id`.

## Differences from Java seen in the probes

Both servers consumed the same topics of one broker. Task 7.4.1: Java on port 10986,
Oxhaven on 10987, 21 cases and a run with records arriving while the tables were open.
Task 7.4.4, over librdkafka: Java on 10992, Oxhaven on 10993, 33 tables in four rounds
with records arriving between them, 25 calls expected to raise or to list, the offsets
each consumer group had committed (`kafka-consumer-groups.sh`), and a restart of the
broker under the open tables of both.

- Same: the schemas; the rows of simple and JSON tables, sorted by partition and offset;
  the flags (blink, refreshing, flat, failed); offsets from the beginning, the end and a
  number; one partition of two; a ring table of one partition; a blink table and an
  aggregation of it, and append-only tables, as records arrive; the four failures above.
- Same, new in 7.4.4: no `offsets` without a group (`latest`, `earliest`, `none`, which
  fails the table in both); no `offsets` with a group, before and after it has committed;
  an offset for one partition of two; an offset past the end, with each policy; what
  eight groups had committed, among them one that only sought to the end, one with a
  given offset, one of a blink table, and one with `enable.auto.commit=false` (nothing);
  topics compressed with snappy, lz4 and zstd; a transactional topic at both isolation
  levels; a topic that did not exist when the table was made (no rows, also after
  records arrived); every table after the broker restarted and more records arrived;
  the list of topics and its order; which of the 25 calls raise.
- Different, by design for now: a gzip topic (Java reads
  it, Oxhaven fails the table by name); `auto.offset.reset=by_duration:PT1H` (Java
  accepts, Oxhaven refuses by name); `security.protocol` (Java fails for want of a TLS
  broker or a JAAS entry, Oxhaven refuses by name).
- Different in text only (`PARITY.md` A5): the Python traceback around an error; an
  unreachable broker (Java: `TimeoutException: Timeout expired while fetching topic
  metadata`, here librdkafka's words, both after `default.api.timeout.ms`); a malformed
  or unresolvable `bootstrap.servers`, which Java refuses at once and Oxhaven after that
  time; `topics` without `bootstrap.servers`.
- **Row order across partitions.** Within a partition both keep offset order. Where two
  partitions have records in one poll, Java delivers them in the order of a `HashMap` of
  partitions and Oxhaven by partition number, so the last rows of an append-only table,
  and so `tail` and a ring table over two partitions, differ. Seen again in 7.4.4 (an
  offset for one partition of two). Java defines no order here; see below.

## For the owner's ruling

1. ~~The licence of the client.~~ Ruled on 2026-11-16: librdkafka is in, as "The client
   library" lists it.
2. **gzip.** Reading gzip batches needs zlib, which librdkafka does not carry. The
   ruling allows the zlib licence for files bundled in librdkafka and excludes an
   external zlib. The `libz-static` feature would compile the zlib sources bundled in
   the crate `libz-sys` (itself MIT OR Apache-2.0): the same licence, another bundle.
   Allow that, or leave gzip refused?
3. **TLS and SASL** (task 7.4.5) each need a library outside the ruling: OpenSSL
   (Apache-2.0 from version 3; the machine's, or compiled from `openssl-src`) for TLS,
   SCRAM and OAUTHBEARER; Cyrus SASL (its own BSD-style licence with an advertising
   clause) for Kerberos; libcurl (curl licence) for OIDC. SASL PLAIN alone needs
   nothing, but is of little use without TLS. Which of these may be added?
4. **Row order across partitions within one poll** is the iteration order of a Java
   `HashMap` keyed by topic and partition. It is stable for one topic name and partition
   count, but nothing documents it. Oxhaven orders by partition number. Is this
   `PARITY.md` A3 (row order where Java defines none), or should the hash order be
   reproduced? librdkafka changes one thing here: what "one poll" holds. Java's poll
   returns what one round of fetches brought, up to `max.poll.records`; the source
   returns what librdkafka has queued, up to the same number. Which records share a poll
   is a matter of timing in both.
5. **Records before a bad one.** Java's client returns the records before a record it
   cannot deserialize and throws on the next poll, so those rows may show for a cycle
   before the table fails. Oxhaven decodes a poll at once and fails without them. The
   failed table is the same. Unchanged by librdkafka.
6. **A consumer property Oxhaven does not act on is accepted in silence**, as Java's
   client accepts (and logs) a property it does not know. With librdkafka the list of
   properties that do act is longer ("Consumer properties"), and what remains without
   effect matters only to a consumer that joins a group, which neither Java's ingester
   nor this one does, or to TLS and SASL, which are refused. The alternative is still to
   refuse every property outside a known list.
7. **Text that is not UTF-8 in a `String` column** is refused. Java replaces the bad bytes
   (`new String(bytes, UTF_8)`); Rust's lossy conversion is not known to replace them the
   same way in every case. Unchanged by librdkafka.
8. **The order of `kafka.topics`** is the iteration order of a Java `HashMap` of topic
   names, undocumented like item 4. Here it is reproduced, because it could be: the
   order depends only on the names. A bucket of more than eight names in a listing of 49
   or more topics is the one case not reproduced (Java turns it into a tree). Say if
   sorted, or any order, is preferred; then the reproduction can go.
9. **A property value that is not text** (`{"max.poll.records": 5}`) is an error in the
   reference, by accident of jpy (`no matching Java method overloads found`), and so
   here now. The first slice accepted it. Most users would call the reference's
   behaviour a defect; it is reproduced until ruled otherwise.
10. **A value Java accepts and librdkafka does not** fails the call (the ranges under
    "Consumer properties"). The alternative is to clamp, which would be a silent change
    of what was asked.

## Refused, by Java method

| What | Named in the refusal |
| --- | --- |
| `security.protocol` other than `PLAINTEXT` | `org.apache.kafka.common.network.ChannelBuilders#clientChannelBuilder` |
| A record batch compressed with gzip (fails the table) | `org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator` |
| `auto.offset.reset=by_duration:<duration>` | `org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy#timestamp` |
| A simple column of `UUID`, `ByteBuffer`, `Bytes` or any other type not listed above | `io.deephaven.kafka.SimpleImpl.SerDeserVisitor` |
| Bytes that are not UTF-8 in a `String` column | `org.apache.kafka.common.serialization.StringDeserializer#deserialize` |
| A JSON column of `JObject`, which Java fills with Jackson's nodes (refused by `json_spec`, as the engine has no such column type) | `io.deephaven.engine.table.ColumnDefinition#fromGenericType` (Java: `io.deephaven.kafka.ingest.JsonNodeJsonNodeFieldCopier`) |
| Two fields of a JSON mapping that are one pointer (`a` and `/a`) for two columns: which one Java keeps depends on the order of a `HashMap` | `io.deephaven.kafka.JsonImpl.JsonConsume#mapNonPointers` |
| JSON text with a UTF-16 surrogate without its pair (`"\ud83d"`) where it would become a `String` cell, compact JSON or an `Instant`'s text (fails the table); Java keeps the lone surrogate, which a Rust string cannot hold | `java.lang.String` |
| Text in an `Instant` column in a spelling the expression crate's `parseInstant` does not read yet (fails the table) | `io.deephaven.time.DateTimeUtils#parseInstant` |
| `consume_to_partitioned_table` | `io.deephaven.kafka.KafkaTools#consumeToPartitionedTable` |
| `avro_spec`, `protobuf_spec`, `object_processor_spec`, `ProtobufProtocol` | `io.deephaven.kafka.KafkaTools.Consume#avroSpec`, `#protobufSpec`, `#objectProcessorSpec`, `io.deephaven.kafka.protobuf.Protocol` |
| `deephaven.stream.add_only_to_blink` | `io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter#toBlink` |

No longer refused since task 7.4.3: JSON columns of `char` and `Instant`, nested
pointers and records that are arrays, fractions and text in numeric columns, objects and
arrays in `String` and `Boolean` columns, integers of any size, several fields mapped to
one column (now failing the table as in Java), a simple `byte[]` column.

`KafkaTools.Consume#rawSpec` takes a Java `Deserializer` class, which no Python API of the
reference reaches (`deephaven.stream.kafka.consumer` has no `raw_spec`); there is nothing
to port it to here until Avro or Protobuf bring their own deserializers (7.4.7, 7.4.8).

No longer refused since task 7.4.4: `offsets` left at its default, compressed batches
other than gzip, `isolation.level=read_committed`, record batches older than format 2
(librdkafka reads them; not probed, the broker writes none), `deephaven.stream.kafka.
topics`.

`deephaven.stream.kafka.producer` and `cdc` do not exist yet: importing them fails.
`deephaven.stream.blink_to_append_only` and `deephaven.stream.table_publisher` exist since
task 7.4.2.

## Tasks

These replace 7.4 and 7.5. Sizes: S is a day of one implementer, M two to three, L a week.

| Task | What | Size | Needs |
| --- | --- | --- | --- |
| 7.4.1 | This design and the first slice | done here | |
| 7.4.2 | Engine: public `stream_table` with a publisher, public `blink_to_append_only`, a callback when a table is released; `deephaven.stream.table_publisher`, `blink_to_append_only`; move this crate onto them | done (`tasks/7.4.2-stream-adapter.md`) | 7.4.1 |
| 7.4.3 | JSON completely: the refused conversions (`NumberInput#parseAsInt` and friends, `BigDecimal#intValue`, Jackson's compact output), nested pointers and arrays, `char` and `Instant` columns; simple `byte[]`; raw spec | done (`tasks/7.4.3-kafka-json.md`); raw spec not reachable from Python | 7.4.1 |
| 7.4.4 | Client: librdkafka as the record source and the translation of properties; committed offsets and `auto.offset.reset` (`DONT_SEEK`), snappy, lz4 and zstd, reconnects, `kafka.topics`; the `kafka-broker` CI job | done (`tasks/7.4.4-kafka-librdkafka.md`) | |
| 7.4.5 | TLS and SASL: librdkafka's `ssl` and `gssapi` features and the translation of Java's `ssl.*` and `sasl.*` properties (keystores and JAAS have no counterpart in librdkafka); SASL PLAIN first, which needs no library; gzip if allowed | M | owner's rulings 2 and 3 |
| 7.4.6 | `consume_to_partitioned_table`; a recorded Java corpus for consumption in `oracle/corpus` and a replay of it through a scripted source | M | 7.4.2 |
| 7.4.7 | Avro specs and the schema registry client (a licence check of the Avro crate first) | L | 7.4.3 |
| 7.4.8 | Protobuf specs (`prost-reflect` is in the tree), both protocols | M | 7.4.7 for the registry |
| 7.5.1 | Produce: librdkafka's producer, simple and JSON specs, `publish_initial`, `last_by_key_columns`, how removes and modifies are published; `deephaven.stream.kafka.producer` | M | 7.4.4 |
| 7.5.2 | Produce: Avro and raw specs, topic, partition and timestamp columns | M | 7.5.1, 7.4.7 |
| 7.5.3 | `deephaven.stream.kafka.cdc` | M | 7.4.7 |
