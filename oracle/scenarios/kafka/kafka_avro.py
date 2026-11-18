# Avro values in Confluent's wire format, with the schema from the registry by subject, at
# its latest version; from the registry with a mapping and `mapped_only`; and given as JSON
# text, for fewer fields than the records have, with a mapping that names one of them.
#
# The records: three in the subject's latest schema, one written with its older version
# (no `note`, a float `px` and an int `venue.lot`, which Avro's resolution promotes), and a
# tombstone, which is a row of nulls. Among the values are Java's null value of an `int`
# and of a `float`, NaN, text beyond ASCII, an empty string, and a `timestamp-millis` of
# `Long.MIN_VALUE`, whose multiplication to nanoseconds wraps.
# oracle: results by_subject, mapped, from_text
import json
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
          "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]}
common = dict(offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
              key_spec=kc.simple_spec("Key", dtypes.string),
              table_type=kc.TableType.append())

by_subject = kc.consume(
    config, "kc_avro", value_spec=kc.avro_spec("kc_avro-value"), **common)
mapped = kc.consume(
    config, "kc_avro",
    value_spec=kc.avro_spec("kc_avro-value", schema_version="2",
                            mapping={"sym": "Symbol", "venue.lot": "Lot", "ts": "At"},
                            mapped_only=True),
    **common)
schema = {"type": "record", "name": "Trade", "namespace": "kc", "fields": [
    {"name": "sym", "type": "string"},
    {"name": "px", "type": "double"},
    {"name": "note", "type": ["null", "string"], "default": None},
]}
from_text = kc.consume(
    config, "kc_avro",
    value_spec=kc.avro_spec(json.dumps(schema), mapping={"sym": "Symbol"}),
    **common)
# oracle: await by_subject 5, mapped 5, from_text 5
