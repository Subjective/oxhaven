# JSON values into typed columns: fields by name, by a mapping, and by JSON pointers into
# nested objects and arrays. The records hold values of the column's type, values Jackson
# coerces to it (text and fractions in numeric columns, numbers in text columns, an integer
# as an `Instant`, a number too large for an `int`), nulls, missing fields, an object in a
# text column, and a record that is an array. `narrow` reads three of the fields as `short`,
# `float` and `byte`, and `as_text` reads the same records with every
# column a `String`.
# oracle: results typed, narrow, as_text
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

config = {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}
typed = kc.consume(
    config, "kc_json",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("Key", dtypes.string),
    value_spec=kc.json_spec(
        {"Id": dtypes.int32, "Name": dtypes.string, "Price": dtypes.double,
         "Ok": dtypes.bool_, "When": dtypes.Instant, "C": dtypes.char,
         "DeepN": dtypes.long, "Tag0": dtypes.string, "First": dtypes.int32},
        mapping={"id": "Id", "name": "Name", "price": "Price", "ok": "Ok", "when": "When",
                 "c": "C", "/nested/deep/n": "DeepN", "/tags/0": "Tag0", "/0": "First"}),
    table_type=kc.TableType.append())
narrow = kc.consume(
    config, "kc_json",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.json_spec({"id": dtypes.short, "price": dtypes.float32, "c": dtypes.byte}),
    table_type=kc.TableType.append())
as_text = kc.consume(
    config, "kc_json",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.json_spec(
        {"id": dtypes.string, "name": dtypes.string, "price": dtypes.string,
         "ok": dtypes.string, "when": dtypes.string, "tags": dtypes.string,
         "nested": dtypes.string}),
    table_type=kc.TableType.append())
# oracle: await typed 5, narrow 5, as_text 5
