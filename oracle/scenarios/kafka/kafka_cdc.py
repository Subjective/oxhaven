# Change data capture: a topic as Debezium writes it, with rows created, one updated, one
# deleted and followed by its tombstone, and one created after that. `current` is the
# database table as it is now, `raw` every record as `avro_spec` reads its key and value,
# and `events` the blink table of the events, of which `events_seen` keeps what it showed.
#
# In `current` the deleted key shows again, with null columns: the tombstone is its last
# record. That is what Java's table shows (docs/KAFKA.md, "Change data capture").
# oracle: results current, raw, events, events_seen
import os

from deephaven.stream import blink_to_append_only
from deephaven.stream.kafka import cdc
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
          "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]}
spec = cdc.cdc_short_spec("kc", "inv", "people")

current = cdc.consume(config, spec)
raw = cdc.consume_raw(config, spec, table_type=kc.TableType.append().j_object)
events = cdc.consume(config, spec, stream_table=True)
events_seen = blink_to_append_only(events)
# oracle: await current 3, raw 6, events_seen 6
