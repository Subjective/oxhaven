# Avro arrays of ints and of strings, among them empty ones, and a decimal (`bytes` with the
# logical type `decimal`), which is a `BigDecimal` column.
# oracle: results more
import os

from deephaven.stream.kafka import consumer as kc

more = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
     "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]},
    "kc_avro_more",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.avro_spec("kc_avro_more-value"),
    table_type=kc.TableType.append())
# oracle: await more 3
