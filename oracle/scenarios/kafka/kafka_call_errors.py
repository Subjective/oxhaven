# Calls that fail before a table exists, one a step: both specs ignored; an Avro spec
# without `schema.registry.url`; a subject the registry does not have; a deserializer in
# the properties that Java's `FROM_PROPERTIES` does not take (the one for text, of all);
# a simple column whose type nothing gives; a ring table of no rows; and a partitioned
# table of a partition the topic does not have.
# `ok` is made first, so that every step has a result to read.
# oracle: results ok
import os

from deephaven import dtypes, empty_table
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
registry = {**config, "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]}
start = kc.ALL_PARTITIONS_SEEK_TO_BEGINNING
ok = empty_table(1).view("Step = 0")
# oracle: step
# oracle: fails
kc.consume(config, "kc_text", offsets=start, key_spec=kc.KeyValueSpec.IGNORE,
           value_spec=kc.KeyValueSpec.IGNORE)
# oracle: step
# oracle: fails
kc.consume(config, "kc_avro", offsets=start, key_spec=kc.KeyValueSpec.IGNORE,
           value_spec=kc.avro_spec("kc_avro-value"))
# oracle: step
# oracle: fails
kc.consume(registry, "kc_avro", offsets=start, key_spec=kc.KeyValueSpec.IGNORE,
           value_spec=kc.avro_spec("no_such_subject"))
# oracle: step
# oracle: fails
kc.consume({**config,
            "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
           "kc_text", offsets=start)
# oracle: step
# oracle: fails
kc.consume(config, "kc_text", offsets=start, key_spec=kc.KeyValueSpec.IGNORE,
           value_spec=kc.simple_spec("Value"))
# oracle: step
# oracle: fails
kc.consume(config, "kc_text", offsets=start, key_spec=kc.KeyValueSpec.IGNORE,
           value_spec=kc.simple_spec("Value", dtypes.string),
           table_type=kc.TableType.ring(0))
# oracle: step
# oracle: fails
kc.consume_to_partitioned_table(
    config, "kc_two", partitions=[7], offsets=start, key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Value", dtypes.string), table_type=kc.TableType.append())
