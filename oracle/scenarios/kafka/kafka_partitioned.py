# A topic of two partitions as a partitioned table: a constituent per partition.
#
# A partitioned table is not a result the harness reads, so what is compared is made from
# it: its merge, a constituent, and its keys. The merge and the keys are sorted, because
# the order of the constituents is the order of the broker's metadata in Java and of the
# partition numbers in Oxhaven (docs/KAFKA.md, "For the owner's ruling", item 26). The
# constituents show from the update cycle after the call on, so one is asked for in a
# second step.
# oracle: results merged, keys, partition_1
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

by_partition = kc.consume_to_partitioned_table(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}, "kc_two",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("Key", dtypes.string),
    value_spec=kc.simple_spec("Value", dtypes.string),
    table_type=kc.TableType.append())
merged = by_partition.merge().sort(["KafkaPartition", "KafkaOffset"])
# oracle: await merged 7
# oracle: step
keys = by_partition.keys().sort("Partition")
partition_1 = by_partition.get_constituent(1)
# oracle: await partition_1 3
