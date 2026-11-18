# Protobuf values with the schema from the registry: in Confluent's wire format (`serdes`,
# the default), as the messages alone (`raw`), and with `include` choosing fields. The
# message has scalars, an optional field, a nested message, a repeated field and a
# `Timestamp`; one record is the empty message, where every field has its default or is
# absent. A record without a value fails the table.
# oracle: results serdes, raw, included, tombstone
import os

from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
          "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]}
common = dict(offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
              key_spec=kc.KeyValueSpec.IGNORE,
              table_type=kc.TableType.append())

serdes = kc.consume(
    config, "kc_proto", value_spec=kc.protobuf_spec(schema="kc_proto-value"), **common)
raw = kc.consume(
    config, "kc_proto_raw",
    value_spec=kc.protobuf_spec(schema="kc_proto-value", schema_version=1,
                                schema_message_name="kc.Tick",
                                protocol=kc.ProtobufProtocol.raw()),
    **common)
included = kc.consume(
    config, "kc_proto",
    value_spec=kc.protobuf_spec(schema="kc_proto-value", include=["/sym", "/inner/*"]),
    **common)
tombstone = kc.consume(
    config, "kc_proto_tombstone",
    value_spec=kc.protobuf_spec(schema="kc_proto-value"), **common)
# oracle: await serdes 3, raw 3, included 3, tombstone fails
