# The columns every Kafka table has, by the `deephaven.*.column.name` properties: the
# partition and timestamp columns renamed, the offset column left out (an empty name), and
# the three columns that exist only on request: the sizes of the key and of the value, and
# the time the record was received.
#
# The receive time is the server's clock when the record arrived, so its value is not
# compared: the table that is compared says only whether it is there, and drops it.
# oracle: results columns
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

received = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
     "deephaven.partition.column.name": "P",
     "deephaven.offset.column.name": "",
     "deephaven.timestamp.column.name": "Sent",
     "deephaven.receivetime.column.name": "Received",
     "deephaven.keybytes.column.name": "KeySize",
     "deephaven.valuebytes.column.name": "ValueSize"},
    "kc_text",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("Key", dtypes.string),
    value_spec=kc.simple_spec("Value", dtypes.string),
    table_type=kc.TableType.append())
columns = received.update_view("HasReceived = !isNull(Received)").drop_columns("Received")
# oracle: await columns 5
