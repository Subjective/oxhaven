# A Protobuf message with an enum field. Java makes a column of its own
# `EnumValueDescriptor` objects, which a client reads as the values' names; Oxhaven has no
# column type for them and refuses the call by name (docs/KAFKA.md, "For the owner's
# ruling", item 23), so this scenario is listed as a difference in kafka-baseline.json.
# oracle: results paint
import os

from deephaven.stream.kafka import consumer as kc

paint = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
     "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]},
    "kc_proto_enum",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.protobuf_spec(schema="kc_proto_enum-value"),
    table_type=kc.TableType.append())
# oracle: await paint 2
