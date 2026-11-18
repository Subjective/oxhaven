# Text keys and values of one partition, into append-only tables: with simple specs, with no
# specs at all (the columns have Java's names and the types the properties give), and with
# the key ignored. The topic has an empty
# value, a record without a key, a tombstone, text beyond ASCII and a record with headers,
# which no column shows.
# oracle: results named, defaults, value_only
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
named = kc.consume(
    config, "kc_text",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("Key", dtypes.string),
    value_spec=kc.simple_spec("Value", dtypes.string),
    table_type=kc.TableType.append())
defaults = kc.consume(
    {**config, "deephaven.key.column.type": "String", "deephaven.value.column.type": "String"},
    "kc_text",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    table_type=kc.TableType.append())
value_only = kc.consume(
    config, "kc_text",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Text", dtypes.string),
    table_type=kc.TableType.append())
# oracle: await named 5, defaults 5, value_only 5
