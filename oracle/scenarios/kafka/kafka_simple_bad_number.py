# A value of three bytes in an `int` column: Kafka's deserializer refuses it, and the table
# fails. The records before it and after it do not make the failed table readable.
# oracle: results numbers
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

numbers = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}, "kc_bad_int",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("N", dtypes.int32),
    table_type=kc.TableType.append())
# oracle: await numbers fails
