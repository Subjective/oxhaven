# Values that are not UTF-8 in a `String` column. Java's deserializer replaces what it
# cannot decode (`new String(bytes, UTF_8)`); Oxhaven refuses such a value by the owner's
# ruling (PARITY.md A15; docs/KAFKA.md, "For the owner's ruling", item 7), so this scenario
# is listed as a difference in kafka-baseline.json.
# oracle: results text
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

text = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}, "kc_text_not_utf8",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("Key", dtypes.string),
    value_spec=kc.simple_spec("Value", dtypes.string),
    table_type=kc.TableType.append())
# oracle: await text 3
