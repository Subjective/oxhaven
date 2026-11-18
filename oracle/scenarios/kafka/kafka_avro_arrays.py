# Avro arrays of ints and of strings, among them empty ones and one that holds Java's null
# value of an `int`, and a decimal (`bytes` with the logical type `decimal`), which is a
# `BigDecimal` column.
#
# A `BigDecimal` is sent as Arrow binary, which the comparison rules do not know (`rules.py`
# reports it and never passes it), so that column itself is not a result: its type is
# compared through the meta table, and its cells as the text `BigDecimal#toString` gives,
# which has the scale.
# oracle: results types, cells
import os

from deephaven.stream.kafka import consumer as kc

more = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"],
     "schema.registry.url": os.environ["ORACLE_KAFKA_REGISTRY"]},
    "kc_avro_more",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.avro_spec("kc_avro_more-value"),
    table_type=kc.TableType.append())
types = more.meta_table.view(["Name", "DataType"])
cells = more.update_view(
    "AmountText = isNull(amount) ? null : amount.toString()").drop_columns("amount")
# oracle: await cells 3
