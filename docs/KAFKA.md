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

## What exists after tasks 7.4.1, 7.4.3, 7.4.4 and 7.4.7

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
- `avro_spec` for keys and values, with the schema given as JSON text or as a subject
  and version of a Confluent schema registry (`schema.registry.url`, over plain HTTP),
  `mapping` and `mapped_only`, records in Confluent's wire format, and writer schemas
  that differ from the spec's (below);
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

**The mapping and the columns.** Java turns the mapping into a map from pointer to column,
in the order it iterates the `HashMap` that the Python `dict` became (`j_hashmap`, made
with the number of fields as its capacity), so of two fields that are one pointer (`a`
and `/a`) the later in that order keeps it; the order is that of the fields' hash codes
and is reproduced (`plan.rs`, `java_sized_hash_map_order`). Java then adds the pointer of
each column the map does not name; a pointer already in the map is then given to that
column. A column whose field lost its pointer to a colliding field is not named, so it is
filled from the field of its own name (probed: `{'a': 'X', '/a': 'Y'}` fills `Y` from `a`
and `X` from `X`). Each entry fills its column once per record, so a column that two
fields are mapped to, or that loses its only pointer to a column's own, makes Java's
chunks of different sizes, and the table fails at the first record with
`IllegalStateException: Publisher chunks have size mismatch: 1, 1, 1, 2, 1` (probed;
reproduced, with the sizes). A mapping to a column that is not defined fails the call
(`Column not found in Deephaven table: -1`). A column type other than those above fails
the call with Java's `Type <name> not supported for JSON` (`byte[]`, `int[]`,
`String[]`; probed). `json_spec` refuses the types the engine has no column type for
(`BigDecimal`, `BigInteger`, `LocalDate`, `LocalTime`, and for now `Duration`, `Period`
and `ZonedDateTime`, which Python's `dtypes` does not yet map) before Java's check is
reached; Java fails those at `consume` with the same `Type <name> not supported for JSON`.

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

### How an Avro field becomes a column

Since task 7.4.7 (`tasks/7.4.7-kafka-avro.md`). Avro is read by code of this crate
(`avro.rs`), not by a library: the Rust crate's schema resolution is lenient where Java's
fails (a long read as an int, a map read as a record), and Java's texts are needed for
every failure. `registry.rs` is the schema registry client, HTTP/1.1 over `std::net`.

**The spec.** `avro_spec(schema)` reads a schema that starts with `{` at once, with Avro
1.12's checks and texts (`SchemaParseException: No name in schema: ...`); any other text
is the subject of a registry, read at `consume` at `schema_version` (`latest`, or a
number as `Integer#parseInt` reads it). `consume` needs `schema.registry.url` whenever a
spec is Avro, also for a schema given as text, as Java does. It fetches the key's schema,
then the value's, before it defines a column: `GET /subjects/<subject>/versions/latest`
or `.../versions/<n>?deleted=false`, then `GET /schemas/ids/<id>?fetchMaxId=false&subject=`.
Several URLs, separated by commas, are tried in turn.

**Columns.** Each field of the top-level record is a column, in order, with a record
flattened into its fields (`a.b`). With no `mapping` (or an empty one) the name is the
path with `__` for `.`; with a `mapping`, the fields it names take its names, a field
mapped to `None` is left out, and every other field keeps its path as it is (`a.b`);
with `mapped_only`, only the fields it names, where a nested field needs its parent
named too. Types: `boolean` Boolean, `int` int, `long` long or `Instant` (with
`timestamp-millis` or `timestamp-micros`; any other logical type stays long), `float`,
`double`, `string` and enums String, `bytes` with a valid `decimal` BigDecimal, a union of
null and one type that type, and arrays of int, long, timestamps, float, double, string
and enums as `int[]`, `long[]`, `Instant[]`, `float[]`, `double[]` and `String[]`. A
top-level union and a top-level type other than a record fail as in Java.

**Records.** A record's key or value is magic byte 0, a four-byte schema id, and the
value in the writer's schema, which is fetched by id (`GET /schemas/ids/<id>?fetchMaxId=
false&subject=<topic>-value`) and cached. The value is read into the spec's schema with
Avro's resolution: promotions (int to long, float or double; long to float or double;
float to double; string and bytes either way), fields the writer lacks from their
defaults, fields the reader lacks skipped, enum symbols by name or the reader's default,
a writer's union by its branch. Trailing bytes are ignored. A tombstone is a row of null
cells. Java's quirks are kept: a value equal to Java's null of the column (`Integer.
MIN_VALUE`, `Long.MIN_VALUE`, `-Float.MAX_VALUE`, `-Double.MAX_VALUE`) is a null cell; a
timestamp is multiplied to nanoseconds with a product that wraps; after the first null
of a BigDecimal column in a batch, the rest of that batch's cells in the column are null
(Java's copier returns at the null); invalid UTF-8 is replaced as `new String(bytes,
UTF_8)` replaces it. A record that cannot be read fails the table with Java's text, for
example `RecordDeserializationException: Error deserializing VALUE for partition t-0 at
offset 4. ... (caused by org.apache.kafka.common.errors.SerializationException: Error
deserializing Avro message for id SchemaId{...}: org.apache.avro.AvroTypeException:
Field "a" content mismatch: Found long, expecting int)`; the part in parentheses is
Java's cause.

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

## Producing

Tasks 7.5.1 and 7.5.2. `deephaven.stream.kafka.producer.produce(table, kafka_config, topic,
key_spec, value_spec, last_by_key_columns, publish_initial, partition, topic_col,
partition_col, timestamp_col)` over `oxhaven_kafka::produce`, with `simple_spec`,
`json_spec`, `avro_spec` and `KeyValueSpec.IGNORE`.
`produce_to` takes any `RecordSink` in place of librdkafka's producer, so that the tests
can script what is sent. Each behaviour was probed on the reference with a real broker
first; the probes and their comparison are under "Differences from Java seen in the
probes".

**What is published** (`PublishToKafka`):

- Unless `publish_initial` is false, a record is sent for each row the table has, in row
  order. That happens in the call, under the graph's shared lock.
- Then, each cycle, a listener on the table acts on the cycle's update:
  - It first checks the key columns. A modified key column fails the publication with
    Java's `AssertionFailure` text, and nothing more is published.
  - For a blink table, only the added rows are published.
  - Otherwise each removed row is published first. Its record has the row's previous
    key and no value. Then the added and modified rows are published together, in row
    order, if a column of the value was modified; otherwise only the added rows.
- Rows are sent in chunks of 2048. After each batch the producer is flushed, and the
  first failed send fails the call (for the rows the table had) or the publication. Its
  text is `KafkaPublisherException: KafkaProducer reported send failure: <cause>`.
- `last_by_key_columns` publishes `last_by` of the table by the key's columns, through
  the engine's aggregation. A key that leaves the table is a record with no value.
- `partition` sets the partition of every record. A negative partition fails at the first
  record, as `ProducerRecord` does. Without one, a key's partition is librdkafka's
  `murmur2_random`, which is Java's `murmur2` of the key's bytes.
- The callback that `produce` returns stops publishing, and a second call fails as in
  Java. Dropping the callback also stops publishing; Java stops when it is garbage
  collected.
- A static table is published once and its producer is closed at once.

The checks that Java makes in Python and in `produceFromTable` come first, in its order,
with its texts:

- both specs `IGNORE`;
- `publish_initial=False` on a static table;
- no topic (`None` or `''`) and no `topic_col`; `partition=0` is ignored too, since the
  Python tests it as a boolean;
- `topic_col`, `partition_col` and `timestamp_col`: a column the table lacks fails with
  Java's `NoSuchColumnException` text, and a column of another type than `String`, `int`
  and `Instant` with its `ClassCastException: Cannot convert [L] of type long to type int`;
- a column the table lacks (Java's NullPointerException text);
- `include_columns` names the table lacks.

**The record columns** (task 7.5.2, `PublishToKafka`). Each record's topic, partition and
timestamp come from its row's `topic_col`, `partition_col` and `timestamp_col`, read with
the key and the value (the previous values for a removed row). A null cell takes `topic`,
`partition` or none. A timestamp is the `Instant` in milliseconds, truncated as
`TimeUnit#toMillis` truncates; without one the record gets the time it is sent. A record
with no topic fails with `Topic cannot be null.`, and a negative timestamp or partition
fails as `ProducerRecord` fails. A topic that Kafka does not take as a name (`''`,
`a{b}`) fails the send with `InvalidTopicException: Invalid topics: [name]`. As in Java,
the producer waits for a record's topic before it serializes the record, so a topic is
created by a broker that creates topics even when the record then fails, and a record to
a topic that fails is never registered or written. A timestamp of 0 ms (an `Instant`
less than a millisecond from the epoch, on either side) is refused by name: librdkafka
takes 0 as "now", where Java sends 0.

### How a row becomes a record

**Simple specs** use Kafka's serializers. `short`, `int`, `long`, `float` and `double` are
written big-endian; floats and doubles as their raw bits, NaN as Java's canonical NaN.
`String` columns are encoded in the charset that `key.serializer.encoding` or
`value.serializer.encoding` names, else `serializer.encoding`, else UTF-8. The charsets
are UTF-8, UTF-16 (big-endian, with a BOM), UTF-16BE, UTF-16LE, ISO-8859-1 and US-ASCII,
written in any case. A character the charset cannot hold becomes `?`. `byte[]` columns
are sent as they are. A null cell is a record without that key or value. Any other
column type fails the call with Java's `Serializer not found for column X, type
<qst type>`.

**JSON specs** (`JsonKeyOrValueSerializer`) write one object per row:

- The fields are the columns of `include_columns` in that order, or every column. A
  repeated field keeps its first place and takes the last value.
- `mapping` renames fields. With `nested_delim`, a field name is split with Java's
  `String.split` and becomes nested objects, which stay when empty. The rules are
  Java's: trailing empty strings are dropped, and a name that is only delimiters fails
  with Java's `Unknown column name X for table`, caused by an
  `ArrayIndexOutOfBoundsException`. A field under a name that already holds a value
  fails with Jackson's `ClassCastException` text.
- A null cell leaves its field out, or is written as `null` with `output_nulls`.
- `timestamp_field` adds the time the record was made, in nanoseconds since the epoch,
  as text. There is one value per chunk of 2048 rows, as in Java.
- Values are written as Jackson writes its nodes:
  - integers as numbers;
  - `float` and `double` in Java's shortest `toString`, NaN and the infinities as
    quoted text;
  - `char` as a one-character string, where half of a surrogate pair is `?`;
  - `BigDecimal` with its scale (`1E+2`), `BigInteger` as it is;
  - `Instant` in `ISO_INSTANT`, `LocalDate` and `LocalTime` in Java's `toString`, and a
    time zone as its id;
  - text with Jackson's escapes.
- Other column types fail by name; see "Refused, by Java method". Java writes their
  `toString`, which for an array is an identity hash.

**Avro specs** (task 7.5.2; `KafkaTools.Produce#avroSpec`, `AvroImpl.AvroProduce`,
`GenericRecordKeyOrValueSerializer`, Confluent's `KafkaAvroSerializer`) write one record
per row in Confluent's wire format: a zero byte, the schema's id in four big-endian
bytes, then Avro's binary encoding. Records were compared byte for byte with Java's.

- The schema is fetched from `schema.registry.url` by subject and `schema_version`, as
  the consumer fetches one. It is read as Confluent reads a schema it fetched: names are
  checked and defaults are not, so an `int` field with the default `1.5` is accepted. A
  schema that is not a record fails with Java's `Not a record` text.
- Each record registers the schema under `<topic>-key` or `<topic>-value` with a POST of
  `{"schemaType":"AVRO","schema":...}` (`auto.register.schemas`), once per subject and
  publication, and is prefixed with the id the registry gives. The schema is sent as Java
  writes it (`Schema#toString`: its keys in Java's order, a namespace only where it
  changes, short names within one), so the registry sees the same text from both.
- Fields are paired with the table's columns by name, or by `field_to_col_mapping`;
  `include_only_columns` and `exclude_columns` filter the columns. Java pairs them by
  position after it filters, which shifts the pairs or fails with
  `ArrayIndexOutOfBoundsException`; that is reproduced (owner's item 18).
- A value is written as Avro's `GenericDatumWriter` writes the boxed cell:
  - a number into any of `int`, `long`, `float` and `double`, converted as Java's
    `Number#intValue` and friends convert, whatever the field's logical type;
  - a union's branch is chosen by the value's class (null, boolean, string, int, long,
    float, double); a `byte`, `short` or `char` column in a union fails with `Unknown
    datum type java.lang.Byte: 7`, and a value of no branch with `Not in union`;
  - the `null` type writes nothing;
  - a null cell (Java's null sentinels too) in a field without null, a value of the
    wrong class, and any value of an enum, fail with Java's texts, among them the
    JVM's helpful NullPointerException and ClassCastException messages;
  - all under `SerializationException: Error serializing Avro message`.
- An `Instant` column needs a field of logical type `timestamp-millis` (floored, as
  `toEpochMilli` floors) or `timestamp-micros` (truncated); without one it fails with
  Java's `has no logical type.` or `has unrecognized logical type`.
- `timestamp_field` is given the time the record is made, one value per chunk of 2048
  rows; it must be a field of type `long` with one of those two logical types, or the
  call fails with Java's texts. A `timestamp-nanos` field is refused by name: Java's
  message prints the identity of an object.
- `publish_schema` makes the schema from the columns and registers it under the subject
  `schema`, then under the topic's subject. Each field is `[<type>, "null"]`: `int`,
  `long`, `float`, `double`, `string`; `byte`, `char` and `short` are an `int` with
  `dhType`; an `Instant` a `long` of `timestamp-micros`; a `Boolean` a `bytes` with
  `dhType` `java.lang.Boolean`, which then fails at the first record with `Not in
  union`, as in Java (owner's item 19). A dotted `schema` is a name in a namespace,
  which wins over `schema_namespace`; only the last part is checked. A `schema_version`
  other than `None` or `latest` fails in `avro_spec` with Java's text.
  `column_properties` matter only to `BigDecimal` columns, which are refused.

### Producer properties

The properties are read as `KafkaProducer` reads them. Those librdkafka has go to it,
under its name where the name differs:

- under the same name: `bootstrap.servers`, `client.id`, `linger.ms`, `retries`,
  `retry.backoff.ms`, `retry.backoff.max.ms`, `max.in.flight.requests.per.connection`,
  `metadata.max.age.ms`, `reconnect.backoff.ms`, `reconnect.backoff.max.ms`,
  `connections.max.idle.ms`, `socket.connection.setup.timeout.ms`, `request.timeout.ms`
  and `batch.size` (where 0, which turns batching off in Java, becomes librdkafka's 1);
- renamed: `delivery.timeout.ms` becomes `message.timeout.ms`, `max.request.size`
  becomes `message.max.bytes`, and `receive.buffer.bytes` and `send.buffer.bytes` become
  `socket.receive.buffer.bytes` and `socket.send.buffer.bytes`.

Java's defaults are set where librdkafka's differ: `message.timeout.ms` 120000,
`message.max.bytes` 1048576, `batch.size` 16384, and the partitioner `murmur2_random`.

- `acks` takes `all`, `-1`, `0` and `1`. Anything else fails with Java's
  `ConfigException`.
- `compression.type` takes `none`, `snappy`, `lz4` and `zstd`. `gzip` is refused, for
  want of zlib (see "The client library").
- `enable.idempotence` follows `ProducerConfig`:
  - left unset, it is on unless `acks` is not `all`, `retries` is 0, or more than five
    requests may be in flight;
  - set to true with `acks` other than `all`, it fails as in Java.
- `max.block.ms` (default 60000) bounds the wait for the topic's metadata. When the
  metadata does not come in time, the records fail with Java's `TimeoutException` text.
  That happens when the broker does not answer, or when the topic has no partition
  `partition`.
- Refused by name: `partitioner.class` and `interceptor.classes`, which load Java
  classes; `transactional.id`, with which every send in the reference fails, since it
  never calls `initTransactions`; and any `security.protocol` other than `PLAINTEXT`.
- `key.serializer` and `value.serializer` are ignored, as in Java, where the specs choose
  the serializers. Other properties are ignored in silence, as for the consumer.

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
| Avro: columns, mappings, records in another writer's schema, the wire format's failures, the registry's requests and errors | unit tests in `src/avro.rs`, `src/registry.rs`; `tests/avro.rs`, with a registry the test serves | nothing | every pull request |
| Publishing to a scripted `RecordSink`: the records of each kind of change, `last_by`, blink tables, JSON and its failures, the checks | `tests/produce.rs` | nothing | every pull request |
| Publishing Avro and the record columns to a scripted `RecordSink`, with a registry the test serves: the wire format, the registered schemas and their requests, published schemas, the writer's failures, removed rows, a topic without metadata | `tests/produce_avro.rs`; Java's `Schema#toString` in `src/avro_text.rs` | nothing | every pull request |
| Publishing to a broker: a round trip through `consume`, the partitions of keys on a topic of two, `partition`, the metadata timeouts, an Avro round trip through a registry the test serves, records whose topic, partition and timestamp come from columns | `tests/broker.rs`, ignored unless asked for | Docker | `tests/broker.sh` |
| The Python modules: specs, checks, refusals, an unreachable broker | `console::python::kafka` | Python | the `python-console-on` job |
| The client against a broker: offsets, groups and what is committed, snappy, lz4, zstd, the gzip refusal, the listing of topics, a broker that restarts, an Avro round trip (a registry served by the test, no registry image) | `tests/broker.rs`, ignored unless asked for | Docker | `tests/broker.sh`: the `kafka-broker` job |
| The same rows as Java | probes of both servers on one broker | Docker, the Java reference | by hand, in tasks 7.4.1, 7.4.4, 7.4.7, 7.5.1 and 7.5.2; a recorded corpus is task 7.4.6 |

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
- Task 7.4.3, JSON: Java on 10986, Oxhaven on 10987, 146 tables over the same topics
  (`json_spec` of every column type against 108 values, 30 pointers, records that are
  arrays and scalars, Jackson's limits, mappings that collide, the refused types; simple
  `byte[]`). Same: the schemas, every row and every failure of a table, except as
  follows. Different in text only (`PARITY.md` A5): invalid JSON fails with `Failed to
  parse JSON string.` in both, followed here by what the parser found and where (in
  Java the cause is a chained exception); the traceback around a failed call. Different,
  by design for now: `json_spec` with `BigDecimal`, `BigInteger`, `LocalDate`,
  `LocalTime`, `Duration`, `Period` or `ZonedDateTime` is refused by `json_spec`, where
  Java fails at `consume` (both before a table exists); a `JObject` column, which Java
  fills with Jackson's nodes, is refused; text with a lone UTF-16 surrogate in a
  `String` column (Java: `?` in the probe's client) fails the table by name; `Instant`
  text that is a date or a date and time without a zone (`2024-01-02`,
  `2024-01-02T03:04:05`) fails the table in both, here by naming
  `DateTimeUtils#parseInstant` as not ported, in Java with `Cannot parse instant`.
- Task 7.4.7, Avro: Java on 10968, Oxhaven on 10969, one broker and a stub registry,
  145 cases in six rounds (`tasks/7.4.7-kafka-avro.md`). Same in 130: the schemas, rows
  and failures of tables, and the errors of calls. Different, by design for now, in 14:
  the columns refused below, an `https://` registry (Java: `SSLException` against a
  plain server) and an `ftp://` one. Different in text only (`PARITY.md` A5): a schema that is not JSON
  (Jackson's text in Java). Not probed: a real Confluent registry; Java's deserializer
  first asks the registry for `/associations/...` of the topic, gets a 404 from a registry
  without associations and goes on, which Oxhaven does not ask.
- Task 7.5.2, producing Avro and the record columns: Java on 10948, Oxhaven on 10949,
  one broker and a stub registry that logs each request with its body, 484 cases in four
  rounds (`tasks/7.5.2-kafka-produce-avro.md`), among them every column type against
  every Avro field type (420; its 30 `char` cases failed in the probe's own table on both
  servers, so `char` was probed apart). Same: every record, byte for byte (partition, key, value,
  and the timestamp where it comes from a column), every registry request and body, and
  every error text, except as follows. Different by design: the timestamp
  field and the records' own timestamps, which are the time of making or sending; the
  ids a registry gives, which the stub hands out in the order of requests. Refused here:
  a `timestamp-nanos` timestamp field and a timestamp of 0 ms. Different in text only
  (`PARITY.md` A5): a registry schema whose names Java's parser refuses (Java wraps the
  same `SchemaParseException` in Guava's `UncheckedExecutionException` and Confluent's
  `Invalid schema {...}`, which prints a GUID); a send failure, whose cause Oxhaven
  writes after a colon (as since 7.5.1) where Java chains it. Not reproduced: Java's
  serializer first asks the registry for `/associations/resources/<cluster>/<topic>`
  and goes on after a 404; Oxhaven does not ask.
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
11. **Producing, `ModifiedColumnSet.ALL`.** Java's publish listener calls
    `containsAny(ALL)` and throws a NullPointerException. The publication then stops, after
    the cycle's removes when the key is `IGNORE`. Oxhaven refuses by name at the same
    point, so what is published is the same; only the text differs. No engine operation
    probed here reports `ALL` for a table one would publish. Should this be reproduced as
    the NullPointerException, or kept as a refusal?
12. **Producing, metadata that never comes.** Java waits `max.block.ms` for each record and
    fails each one. Oxhaven waits once per batch and fails the rest of the batch at once.
    The call fails with the same text, sooner. Reproducing Java's wait would hold the
    update graph for `max.block.ms` times the number of rows.
13. **Producing, records without a key** (a key spec of `IGNORE`, or a null key): Java's
    sticky partitioner and librdkafka's `murmur2_random` both choose a partition at
    random and stay on it for a while, but not the same one. Records with a key go to the
    same partition in both.
14. **Producing, the callback.** Dropping the Python callback stops publishing, as its
    garbage collection does in Java. CPython drops it at once; Java collects it whenever
    it next collects.

15. **Avro, BigDecimal columns after a null.** Java's copier returns at the first null of
    a batch, so the rest of the batch stays null in that column: values are lost without
    an error, and which ones depends on how records fall into batches. It is reproduced,
    batch by batch (a poll here, a chunk there, so the batches themselves may differ).
    Reproduce, or fill every value?
16. **Avro, columns Java's clients cannot read.** `bytes` and `fixed` (ByteVector filled
    with ByteBuffer or GenericFixed) and arrays of booleans (Boolean[] filled with
    Object[]) give tables that fail a client's fetch in Java; here they are refused at
    `consume`. Unions, maps, the null type and arrays of records are Java objects with no
    column type here, also refused. An array of enums is declared String[] and fails the
    table at its first non-empty array; an array of a nullable timestamp fails at
    `consume`: both reproduced.
17. **Avro needs `schema.registry.url` even for a schema given as text**, because Java
    makes the registry's client first. Reproduced; a spec that never needs the registry
    could do without it.
18. **Producing Avro, columns paired by position.** `GenericRecordKeyOrValueSerializer`
    pairs the schema's fields with the filtered columns by position, not by name. With
    `exclude_columns` or `include_only_columns` a field is filled from another column (a
    `Boolean` into an `Instant` field fails with `field B for column T has no logical
    type.`), or the call fails with `ArrayIndexOutOfBoundsException`. Reproduced. It
    looks like a defect in the reference: reproduce, or pair by name?
19. **Producing Avro, a published schema with a `Boolean` column** declares the field as
    `bytes` and then cannot write a Boolean into it: every such publication fails at its
    first record, after registering the schema. Reproduced.
20. **Producing Avro, the registry's other properties.** `auto.register.schemas`,
    `use.latest.version`, `normalize.schemas` and `latest.compatibility.strict` are
    accepted at their defaults and refused by name otherwise; the subject name strategies
    and credentials are refused as for the consumer.
21. **A `NumberFormatException` for a null schema version.** On the produce side Java
    calls `Integer#parseInt(null)`; its text (`Cannot parse null string`) is reproduced.
    The consumer's `avro_spec(..., schema_version=None)` fails earlier in Java, on an
    ambiguous overload (`ambiguous Java method call, too many matching method overloads
    found`), where Oxhaven's consumer gives the same `NumberFormatException`. The consumer
    is not changed by this task.
22. **Consuming Avro, schemas from the registry.** Confluent reads a schema it fetched
    without checking defaults (`AvroSchema#getParser`, `validateAsNew` false); the
    producer here now does the same. The consumer still checks defaults of a registry
    schema, as it did since 7.4.7, so a registry schema with a bad default fails at
    `consume` here and not in Java. Not probed on the consume side.

## Refused, by Java method

| What | Named in the refusal |
| --- | --- |
| `security.protocol` other than `PLAINTEXT` | `org.apache.kafka.common.network.ChannelBuilders#clientChannelBuilder` |
| A record batch compressed with gzip (fails the table) | `org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator` |
| `auto.offset.reset=by_duration:<duration>` | `org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy#timestamp` |
| A simple column of `UUID`, `ByteBuffer`, `Bytes` or any other type not listed above | `io.deephaven.kafka.SimpleImpl.SerDeserVisitor` |
| Bytes that are not UTF-8 in a `String` column | `org.apache.kafka.common.serialization.StringDeserializer#deserialize` |
| A JSON column of `JObject`, which Java fills with Jackson's nodes (refused by `json_spec`, as the engine has no such column type) | `io.deephaven.engine.table.ColumnDefinition#fromGenericType` (Java: `io.deephaven.kafka.ingest.JsonNodeJsonNodeFieldCopier`) |
| Two fields of a JSON mapping that are one pointer, among fields of which eight or more fall in one bucket of Java's `HashMap` (Java makes the bucket a tree or resizes early, which is not reproduced) | `java.util.HashMap#treeifyBin` |
| JSON text with a UTF-16 surrogate without its pair (`"\ud83d"`) where it would become a `String` cell, compact JSON or an `Instant`'s text (fails the table); Java keeps the lone surrogate, which a Rust string cannot hold | `java.lang.String` |
| Text in an `Instant` column in a spelling the expression crate's `parseInstant` does not read yet (fails the table) | `io.deephaven.time.DateTimeUtils#parseInstant` |
| `consume_to_partitioned_table` | `io.deephaven.kafka.KafkaTools#consumeToPartitionedTable` |
| `protobuf_spec`, `object_processor_spec`, `ProtobufProtocol` | `io.deephaven.kafka.KafkaTools.Consume#protobufSpec`, `#objectProcessorSpec`, `io.deephaven.kafka.protobuf.Protocol` |
| Avro: a field of `bytes` without a valid decimal, or of `fixed` | `io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier` |
| Avro: a decimal in a `fixed` (Java fails the table at its first value) | `io.deephaven.kafka.ingest.GenericRecordBigDecimalFieldCopier#copyField` |
| Avro: a union other than of null and one type, a map, the `null` type | `io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier` |
| Avro: an array of booleans, records, arrays, maps, bytes or fixed | `io.deephaven.kafka.ingest.GenericRecordArrayFieldCopier` |
| Avro: an `https://` registry | `io.confluent.kafka.schemaregistry.client.rest.RestService#setSslSocketFactory` |
| Avro: a registry URL of another scheme (`ftp://`; Java fails with a ClassCastException or an unknown protocol) | `java.net.URL#openConnection` |
| Avro: registry credentials (`basic.auth.*`, `bearer.auth.*`), `key.subject.name.strategy` and `value.subject.name.strategy`, `specific.avro.reader` and `avro.use.logical.type.converters` other than `false` | the class or property Java reads, named in the refusal |
| Avro: a registry schema with references, or of a type other than `AVRO` | `io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider#parseSchemaOrElseThrow` |
| Avro: a record with magic byte 1 (a schema GUID) | `io.confluent.kafka.serializers.schema.id.SchemaId` |
| `deephaven.stream.add_only_to_blink` | `io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter#toBlink` |
| Producing: an update whose modified columns are `ModifiedColumnSet.ALL` (Java's listener throws a NullPointerException and stops; here the publication fails at the same point) | `io.deephaven.engine.table.ModifiedColumnSet#containsAny` |
| Producing: `compression.type=gzip` | `org.apache.kafka.common.compress.GzipCompression` |
| Producing: `transactional.id` | `org.apache.kafka.clients.producer.KafkaProducer#initTransactions` |
| Producing: `partitioner.class`, `interceptor.classes` | `org.apache.kafka.clients.producer.Partitioner`, `ProducerInterceptor` |
| Producing: a `nested_delim` with a regular-expression character other than an escaped one | `java.lang.String#split` |
| Producing: a `*.serializer.encoding` other than the six charsets above | `java.nio.charset.Charset#forName` |
| Producing: a JSON `char` that is half of a surrogate pair, under a charset other than UTF-8 | `java.lang.String#getBytes` |
| Producing: JSON columns of `Duration`, `Period`, `LocalDateTime`, `ZonedDateTime`, arrays, vectors and other objects | `java.util.Objects#toString` |
| Producing: a record timestamp of 0 ms from `timestamp_col`, which librdkafka takes as the time of sending | `org.apache.kafka.clients.producer.ProducerRecord#timestamp` |
| Producing Avro: a `BigDecimal` column | `io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer#makeBigDecimalFieldProcessor` |
| Producing Avro: a column of another type than the primitives, `String`, `Boolean` and `Instant` | `io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer#makeObjectFieldProcessor` |
| Producing Avro: a published schema with a `BigDecimal` column or a column of another type than those | `io.deephaven.kafka.AvroImpl#addFieldForColDef` |
| Producing Avro: a `timestamp_field` of a logical type other than `timestamp-millis` and `timestamp-micros` (Java's text prints an object's identity) | `io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer#checkTimestampFieldAndGetUnit` |
| Producing Avro: `auto.register.schemas`, `use.latest.version`, `normalize.schemas`, `latest.compatibility.strict` other than their defaults | `io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig` |
| Producing Avro: a registry that does not register the schema (answers the POST with an error) | `io.confluent.kafka.serializers.AbstractKafkaAvroSerializer#serializeImpl` |
| Producing: a `topic_col`, `partition_col` or `timestamp_col` of a type the engine names without a Java class here | `io.deephaven.engine.table.TableDefinition#checkHasColumn` |

No longer refused since task 7.4.3: JSON columns of `char` and `Instant`, nested
pointers and records that are arrays, fractions and text in numeric columns, objects and
arrays in `String` and `Boolean` columns, integers of any size, several fields mapped to
one column (now failing the table as in Java), two fields that are one pointer (in Java's
hash order), a simple `byte[]` column.

`KafkaTools.Consume#rawSpec` takes a Java `Deserializer` class, which no Python API of the
reference reaches (`deephaven.stream.kafka.consumer` has no `raw_spec`); there is nothing
to port it to here until Avro or Protobuf bring their own deserializers (7.4.7, 7.4.8).

`KafkaTools.Produce#rawSpec` takes a Java `Serializer` class, and
`deephaven.stream.kafka.producer` has no `raw_spec` either: no Python API of the reference
reaches it, so there is nothing to port.

No longer refused since task 7.4.7: `avro_spec`, for the key and the value.

No longer refused since task 7.5.2: `producer.avro_spec` for the key and the value, and
`produce` with `topic_col`, `partition_col` and `timestamp_col`.

No longer refused since task 7.4.4: `offsets` left at its default, compressed batches
other than gzip, `isolation.level=read_committed`, record batches older than format 2
(librdkafka reads them; not probed, the broker writes none), `deephaven.stream.kafka.
topics`.

`deephaven.stream.kafka.producer` exists since task 7.5.1. `cdc` does not exist yet:
importing it fails.
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
| 7.4.7 | Avro specs and the schema registry client | done (`tasks/7.4.7-kafka-avro.md`); no new crate | 7.4.3 |
| 7.4.8 | Protobuf specs (`prost-reflect` is in the tree), both protocols | M | 7.4.7 for the registry |
| 7.5.1 | Produce: librdkafka's producer, simple and JSON specs, `publish_initial`, `last_by_key_columns`, how removes and modifies are published; `deephaven.stream.kafka.producer` | done (`tasks/7.5.1-kafka-produce.md`) | 7.4.4 |
| 7.5.2 | Produce: Avro and raw specs, topic, partition and timestamp columns | done (`tasks/7.5.2-kafka-produce-avro.md`); raw spec not reachable from Python | 7.5.1, 7.4.7 |
| 7.5.3 | `deephaven.stream.kafka.cdc` | M | 7.4.7 |
