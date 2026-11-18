# The values of a topic as the bytes they are (a `byte[]` column): text, an empty value,
# which is an empty array, and a tombstone, which is null.
#
# The comparison rules do not know Arrow's binary type (`rules.py` reports it and never
# passes it), so the column itself is not a result: its type is compared through the meta
# table, and its cells as their length and as the text they are.
# oracle: results types, cells
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

raw = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}, "kc_text",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Bytes", dtypes.byte_array),
    table_type=kc.TableType.append())
types = raw.meta_table.view(["Name", "DataType"])
cells = raw.update_view(
    ["Size = isNull(Bytes) ? NULL_INT : Bytes.length",
     "Text = isNull(Bytes) ? null : new String(Bytes)"]).drop_columns("Bytes")
# oracle: await cells 5
