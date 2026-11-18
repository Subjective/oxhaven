# Keys and values that are numbers, as Kafka's serializers write them: short, int, long,
# float and double, among them NaN, an infinity, and Java's null value of each type, which
# is a null cell. The names and types of the third table come from the properties.
# oracle: results int_double, long_float, short_long
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
int_double = kc.consume(
    config, "kc_int_double",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("K", dtypes.int32),
    value_spec=kc.simple_spec("V", dtypes.double),
    table_type=kc.TableType.append())
long_float = kc.consume(
    config, "kc_long_float",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("K", dtypes.long),
    value_spec=kc.simple_spec("V", dtypes.float32),
    table_type=kc.TableType.append())
short_long = kc.consume(
    {**config,
     "deephaven.key.column.name": "K",
     "deephaven.key.column.type": "short",
     "deephaven.value.column.name": "V",
     "value.deserializer": "org.apache.kafka.common.serialization.LongDeserializer"},
    "kc_short_long",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    table_type=kc.TableType.append())
# oracle: await int_double 7, long_float 3, short_long 2
