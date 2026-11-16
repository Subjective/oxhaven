"""Consuming Kafka topics into tables.

Behavior: `deephaven.stream.kafka.consumer` (the names, the signatures and the checks made
in Python); `io.deephaven.kafka.KafkaTools` for the rest. `docs/KAFKA.md` lists what is
built: `consume` with simple and JSON specs. What is not raises `NotImplementedError` with
the name of the Java method, inside the `DHError` the reference raises.
"""

import json
from collections.abc import Sequence
from typing import Optional, Union
from warnings import warn

from ... import _engine, dtypes
from . import _properties
from ..._java_time import NotPorted
from ...column import col_def
from ...dherror import DHError
from ...dtypes import DType
from ...table import PartitionedTable, Table, TableDefinition

# The partitions of a topic, all of them. Java's is a predicate; here it is only compared.
ALL_PARTITIONS = None

# Behavior: `io.deephaven.kafka.ingest.KafkaIngester#SEEK_TO_BEGINNING`, `#DONT_SEEK`,
# `#SEEK_TO_END`
SEEK_TO_BEGINNING = -1
DONT_SEEK = -2
SEEK_TO_END = -3

ALL_PARTITIONS_SEEK_TO_BEGINNING = {-1: SEEK_TO_BEGINNING}
ALL_PARTITIONS_DONT_SEEK = {-1: DONT_SEEK}
ALL_PARTITIONS_SEEK_TO_END = {-1: SEEK_TO_END}


class KeyValueSpec:
    """How the key or the value of a record becomes columns. Made by `simple_spec` and
    `json_spec`, or one of `KeyValueSpec.IGNORE` and `KeyValueSpec.FROM_PROPERTIES`."""

    def __init__(self, j_spec):
        # what `_oxhaven.kafka_consume` reads; the reference holds a Java object here
        self._j_spec = j_spec

    @property
    def j_object(self):
        return self._j_spec


KeyValueSpec.IGNORE = KeyValueSpec({"kind": "ignore"})
KeyValueSpec.FROM_PROPERTIES = KeyValueSpec({"kind": "simple", "column": None, "type": None})


class TableType:
    """The kind of table a topic is consumed into."""

    @staticmethod
    def blink() -> "TableType":
        """Each row is in the table for one update cycle."""
        return TableType(("blink", 0))

    @staticmethod
    def stream() -> "TableType":
        """Deprecated synonym for `blink()`."""
        warn("This function is deprecated, prefer blink", DeprecationWarning, stacklevel=2)
        return TableType.blink()

    @staticmethod
    def append() -> "TableType":
        """Every row stays in the table."""
        return TableType(("append", 0))

    @staticmethod
    def ring(capacity: int) -> "TableType":
        """The last `capacity` rows stay in the table."""
        return TableType(("ring", capacity))

    def __init__(self, j_table_type):
        self._j_table_type = j_table_type

    @property
    def j_object(self):
        return self._j_table_type


TableType.Stream = TableType.blink()
TableType.Append = TableType.append()
TableType.Blink = TableType.blink()


def consume(
    kafka_config: dict,
    topic: str,
    partitions: Optional[Sequence[int]] = None,
    offsets: Optional[dict[int, int]] = None,
    key_spec: Optional[KeyValueSpec] = None,
    value_spec: Optional[KeyValueSpec] = None,
    table_type: Optional[TableType] = None,
) -> Table:
    """Consumes a topic into a table that changes as records arrive.

    `kafka_config` holds the properties of the consumer and of the table.
    `partitions` lists the partitions to read, all of them without it. `offsets` maps a
    partition to the offset to start at, or is one of `ALL_PARTITIONS_SEEK_TO_BEGINNING`,
    `ALL_PARTITIONS_SEEK_TO_END` and `ALL_PARTITIONS_DONT_SEEK`, the default: each
    partition starts where the consumer group of `group.id` left off, or where
    `auto.offset.reset` says. `key_spec` and `value_spec` default to
    `KeyValueSpec.FROM_PROPERTIES`; `table_type` to `TableType.blink()`.
    """
    if table_type is None:
        table_type = TableType.blink()
    return _consume(kafka_config, topic, partitions, offsets, key_spec, value_spec, table_type)


def consume_to_partitioned_table(
    kafka_config: dict,
    topic: str,
    partitions: Optional[Sequence[int]] = None,
    offsets: Optional[dict[int, int]] = None,
    key_spec: Optional[KeyValueSpec] = None,
    value_spec: Optional[KeyValueSpec] = None,
    table_type: Optional[TableType] = None,
) -> PartitionedTable:
    """Consumes a topic into a partitioned table with a table per partition."""
    try:
        raise NotPorted("io.deephaven.kafka.KafkaTools#consumeToPartitionedTable")
    except Exception as e:
        raise DHError(e, "failed to consume a Kafka stream.") from e


def _consume(kafka_config, topic, partitions, offsets, key_spec, value_spec, table_type) -> Table:
    try:
        if partitions is not None:
            partitions = [int(partition) for partition in partitions]

        # The reference compares with the three dictionaries, so one equal to them counts.
        all_offsets = None
        pairs = []
        if offsets is None or offsets == ALL_PARTITIONS_DONT_SEEK:
            all_offsets = DONT_SEEK
        elif offsets == ALL_PARTITIONS_SEEK_TO_BEGINNING:
            all_offsets = SEEK_TO_BEGINNING
        elif offsets == ALL_PARTITIONS_SEEK_TO_END:
            all_offsets = SEEK_TO_END
        else:
            pairs = [(int(partition), int(offset)) for partition, offset in offsets.items()]

        key_spec = KeyValueSpec.FROM_PROPERTIES if key_spec is None else key_spec
        value_spec = KeyValueSpec.FROM_PROPERTIES if value_spec is None else value_spec

        if key_spec is KeyValueSpec.IGNORE and value_spec is KeyValueSpec.IGNORE:
            raise ValueError(
                "at least one argument for 'key' or 'value' must be different from KeyValueSpec.IGNORE"
            )

        config = _properties(kafka_config)
        kind, capacity = table_type.j_object
        return Table(
            _engine.native.kafka_consume(
                config,
                topic,
                partitions,
                all_offsets,
                pairs,
                json.dumps(key_spec.j_object),
                json.dumps(value_spec.j_object),
                kind,
                capacity,
            )
        )
    except Exception as e:
        raise DHError(e, "failed to consume a Kafka stream.") from e


class ProtobufProtocol:
    """The wire format of Protobuf records."""

    @staticmethod
    def serdes() -> "ProtobufProtocol":
        raise NotPorted("io.deephaven.kafka.protobuf.Protocol#serdes")

    @staticmethod
    def raw() -> "ProtobufProtocol":
        raise NotPorted("io.deephaven.kafka.protobuf.Protocol#raw")


def protobuf_spec(
    schema: Optional[str] = None,
    schema_version: Optional[int] = None,
    schema_message_name: Optional[str] = None,
    message_class: Optional[str] = None,
    include: Optional[list[str]] = None,
    protocol: Optional[ProtobufProtocol] = None,
) -> KeyValueSpec:
    """A spec for Protobuf records."""
    raise NotPorted("io.deephaven.kafka.KafkaTools.Consume#protobufSpec")


def avro_spec(
    schema: str,
    schema_version: str = "latest",
    mapping: Optional[dict[str, str]] = None,
    mapped_only: bool = False,
) -> KeyValueSpec:
    """A spec for Avro records."""
    try:
        raise NotPorted("io.deephaven.kafka.KafkaTools.Consume#avroSpec")
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def json_spec(
    col_defs: Union[TableDefinition, dict, Sequence],
    mapping: Optional[dict] = None,
) -> KeyValueSpec:
    """A spec for records that are JSON: a column per definition in `col_defs`, each
    filled from the field of its name, or from the field that `mapping` pairs with it. A
    field that starts with `/` is a JSON pointer, which may name a nested field or an
    element of an array (`/a/b/0`).
    """
    try:
        try:
            table_def = TableDefinition(col_defs)
        except DHError:
            table_def = None
        if table_def:
            columns = list(table_def.values())
        else:
            warn(
                "json_spec col_defs for Sequence[tuple[str, DType]] is deprecated for removal, "
                "prefer TableDefinitionLike",
                DeprecationWarning,
                stacklevel=2,
            )
            columns = [col_def(*t) for t in col_defs]
        spec = {
            "kind": "json",
            "columns": [(c.name, dtypes._engine_type(c.data_type)) for c in columns],
            "mapping": None if mapping is None else [(str(f), str(c)) for f, c in mapping.items()],
        }
        return KeyValueSpec(spec)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def simple_spec(col_name: str, data_type: Optional[DType] = None) -> KeyValueSpec:
    """A spec for records whose key or value is one value: one column named `col_name`.
    Without `data_type` the properties give the type."""
    try:
        type_name = None if data_type is None else dtypes._engine_type(data_type)
        return KeyValueSpec({"kind": "simple", "column": col_name, "type": type_name})
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def object_processor_spec(provider) -> KeyValueSpec:
    """A spec that hands records to a Java object processor."""
    raise NotPorted("io.deephaven.kafka.KafkaTools.Consume#objectProcessorSpec")
