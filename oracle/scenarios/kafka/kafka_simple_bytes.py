# The values of a topic as the bytes they are (a `byte[]` column): text, an empty value,
# which is an empty array, and a tombstone, which is null.
# oracle: results raw
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

raw = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}, "kc_text",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Bytes", dtypes.byte_array),
    table_type=kc.TableType.append())
# oracle: await raw 5
