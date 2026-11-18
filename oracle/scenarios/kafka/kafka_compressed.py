# A topic whose batches are compressed with gzip. (Snappy, lz4 and zstd are read in the
# broker tests of crates/oxhaven-kafka; the Python client that fills these topics writes
# only gzip without further packages.)
# oracle: results gzip
import os

from deephaven import dtypes
from deephaven.stream.kafka import consumer as kc

gzip = kc.consume(
    {"bootstrap.servers": os.environ["ORACLE_KAFKA_BROKER"]}, "kc_gzip",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=kc.simple_spec("Key", dtypes.string),
    value_spec=kc.simple_spec("Value", dtypes.string),
    table_type=kc.TableType.append())
# oracle: await gzip 3
