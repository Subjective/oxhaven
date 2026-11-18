# Which partitions are read, and from where: a topic of two partitions from the beginning,
# one of its partitions, an offset per partition (a number, the end, the beginning), and no
# `offsets` at all with a consumer group that has committed nothing and the policy
# `earliest`.
#
# A table of both partitions is sorted by partition and offset: the order in which the
# records of two partitions in one poll become rows is Java's `HashMap` order there and the
# order of the partition numbers in Oxhaven, which PARITY.md allows (A3).
#
# The group is one that never commits (`enable.auto.commit` is false), so that every run
# finds it without an offset; `at_end` gets no row, because nothing is published after the
# topics are filled, and is read once the other tables have their rows.
# oracle: results everything, partition_1, per_partition, mixed, group, at_end
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
specs = dict(key_spec=kc.simple_spec("Key", dtypes.string),
             value_spec=kc.simple_spec("Value", dtypes.string),
             table_type=kc.TableType.append())
order = ["KafkaPartition", "KafkaOffset"]

everything = kc.consume(
    config, "kc_two", offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING, **specs).sort(order)
partition_1 = kc.consume(
    config, "kc_two", partitions=[1], offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING, **specs)
per_partition = kc.consume(
    config, "kc_two", offsets={0: 2, 1: kc.SEEK_TO_END}, **specs)
mixed = kc.consume(
    config, "kc_two", offsets={0: kc.SEEK_TO_BEGINNING, 1: 1}, **specs).sort(order)
group = kc.consume(
    {**config, "group.id": "kc_group_without_offsets", "auto.offset.reset": "earliest",
     "enable.auto.commit": "false"},
    "kc_two", **specs).sort(order)
at_end = kc.consume(
    config, "kc_two", offsets=kc.ALL_PARTITIONS_SEEK_TO_END, **specs)
# oracle: await everything 7, partition_1 3, per_partition 2, mixed 6, group 7
