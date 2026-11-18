# Records a JSON spec cannot read fail the table: a value that is not JSON, and a record
# without a value (a tombstone).
# oracle: results not_json, tombstone
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
not_json = kc.consume(
    config, "kc_json_bad",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.json_spec({"n": dtypes.int32}),
    table_type=kc.TableType.append())
tombstone = kc.consume(
    config, "kc_json_tombstone",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.json_spec({"n": dtypes.int32}),
    table_type=kc.TableType.append())
# oracle: await not_json fails, tombstone fails
