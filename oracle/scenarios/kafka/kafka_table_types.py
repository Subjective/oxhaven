# The table types, on one partition of four records: a ring table that keeps the last two,
# and a blink table, which shows each record for one update cycle.
#
# A blink table is empty once its records have passed, so what is compared of it is its
# columns and attributes, and two tables that are made from it and keep what it showed:
# `blink_to_append_only` of it, and its sum by partition. The wait is for those, and for
# the row of the ring table that holds the last record: when it is there, the ring has
# seen every record, however the records fell into polls.
# oracle: results ring, ring_last, blink, blink_seen, blink_sums
import os

from deephaven import dtypes
from deephaven.stream import blink_to_append_only
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
specs = dict(partitions=[0], offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
             key_spec=kc.simple_spec("Key", dtypes.string),
             value_spec=kc.simple_spec("Value", dtypes.string))

ring = kc.consume(config, "kc_two", table_type=kc.TableType.ring(2), **specs)
ring_last = ring.where("KafkaOffset == 3")
blink = kc.consume(config, "kc_two", table_type=kc.TableType.blink(), **specs)
blink_seen = blink_to_append_only(blink)
blink_sums = blink.view(["KafkaPartition", "KafkaOffset"]).sum_by("KafkaPartition")
# oracle: await ring_last 1, blink_seen 4
