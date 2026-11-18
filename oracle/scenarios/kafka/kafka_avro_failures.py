# Avro records that cannot be read fail the table: a schema id the registry does not have,
# and a record that does not start with the magic byte 0.
# oracle: results unknown_id, bad_magic
import os

from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
          "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]}
common = dict(offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
              key_spec=kc.KeyValueSpec.IGNORE,
              value_spec=kc.avro_spec("kc_avro-value"),
              table_type=kc.TableType.append())

unknown_id = kc.consume(config, "kc_avro_unknown_id", **common)
bad_magic = kc.consume(config, "kc_avro_bad_magic", **common)
# oracle: await unknown_id fails, bad_magic fails
