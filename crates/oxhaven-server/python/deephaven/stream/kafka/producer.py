"""Publishing tables to Kafka topics.

Behavior: `deephaven.stream.kafka.producer` (the names, the signatures and the checks made
in Python); `io.deephaven.kafka.KafkaTools#produceFromTable` for the rest. `docs/KAFKA.md`
lists what is built: `produce` with simple and JSON specs. What is not raises
`NotImplementedError` with the name of the Java method, inside the `DHError` the
reference raises.
"""

import json
from collections.abc import Sequence
from typing import Callable, Optional

from ... import _engine
from . import _properties
from ..._java_time import JavaError, NotPorted
from ...dherror import DHError
from ...table import Table


class KeyValueSpec:
    """How the key or the value of a record is made from a row. Made by `simple_spec` and
    `json_spec`, or `KeyValueSpec.IGNORE`."""

    def __init__(self, j_spec):
        # what `_oxhaven.kafka_produce` reads; the reference holds a Java object here
        self._j_spec = j_spec

    @property
    def j_object(self):
        return self._j_spec


KeyValueSpec.IGNORE = KeyValueSpec({"kind": "ignore"})


def produce(
    table: Table,
    kafka_config: dict,
    topic: Optional[str],
    key_spec: KeyValueSpec,
    value_spec: KeyValueSpec,
    last_by_key_columns: bool = False,
    publish_initial: bool = True,
    partition: Optional[int] = None,
    topic_col: Optional[str] = None,
    partition_col: Optional[str] = None,
    timestamp_col: Optional[str] = None,
) -> Callable[[], None]:
    """Publishes `table` to `topic`: a record per row it has (unless `publish_initial` is
    False), then a record per row each update cycle adds, modifies or removes. A removed
    row is a record of its old key and no value; a blink table's removes are not published.
    With `last_by_key_columns`, `last_by` of the table by the key's columns is published.
    `partition` is the partition of every record; Kafka's partitioner chooses without it.

    Returns a callable that stops publishing. Keep it: publishing also stops when it is
    dropped.
    """
    try:
        if key_spec is KeyValueSpec.IGNORE and value_spec is KeyValueSpec.IGNORE:
            raise ValueError(
                "at least one argument for 'key_spec' or 'value_spec' must be different from KeyValueSpec.IGNORE"
            )
        if not publish_initial and not table.is_refreshing:
            raise ValueError("publish_initial == False and table.is_refreshing == False")
        config = _properties(kafka_config)
        # The reference passes these to Java only when they are true, so 0 is no partition.
        topic = topic if topic else None
        partition = partition if partition else None
        for name, column in (("topicColumn", topic_col), ("partitionColumn", partition_col),
                             ("timestampColumn", timestamp_col)):
            if column:
                raise NotPorted(f"io.deephaven.kafka.KafkaPublishOptions#{name}")
        if topic is None:
            raise JavaError("java.lang.IllegalArgumentException: Must set topic or topicColumn (or both)")
        publication = _engine.native.kafka_produce(
            table.j_table,
            config,
            topic,
            None if partition is None else int(partition),
            json.dumps(key_spec.j_object),
            json.dumps(value_spec.j_object),
            bool(last_by_key_columns and key_spec is not KeyValueSpec.IGNORE),
            bool(publish_initial),
        )

        def cleanup():
            try:
                publication.stop()
            except Exception as ex:
                raise DHError(ex, "failed to stop publishing to Kafka and the clean-up.") from ex

        return cleanup
    except Exception as e:
        raise DHError(e, "failed to start producing Kafka messages.") from e


def avro_spec(
    schema: str,
    schema_version: str = "latest",
    field_to_col_mapping: Optional[dict[str, str]] = None,
    timestamp_field: Optional[str] = None,
    include_only_columns: Optional[Sequence[str]] = None,
    exclude_columns: Optional[Sequence[str]] = None,
    publish_schema: bool = False,
    schema_namespace: Optional[str] = None,
    column_properties: Optional[dict[str, str]] = None,
) -> KeyValueSpec:
    """A spec for Avro records (task 7.5.2)."""
    try:
        raise NotPorted("io.deephaven.kafka.KafkaTools.Produce#avroSpec")
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e


def json_spec(
    include_columns: Optional[Sequence[str]] = None,
    exclude_columns: Optional[Sequence[str]] = None,
    mapping: Optional[dict[str, str]] = None,
    nested_delim: Optional[str] = None,
    output_nulls: bool = False,
    timestamp_field: Optional[str] = None,
) -> KeyValueSpec:
    """A spec that writes a JSON object of columns: those of `include_columns` in that
    order, or all of the table's. `mapping` gives a column a field of another name;
    `nested_delim` splits field names into nested objects (`a__b` with `__` is the field
    `b` of the object `a`); `output_nulls` writes a null cell as `null` instead of leaving
    the field out; `timestamp_field` adds a field with the time the record was made, in
    nanoseconds, as text.
    """
    try:
        if include_columns is not None and exclude_columns is not None:
            raise ValueError("One of include_columns and exclude_columns must be None.")
        if exclude_columns:
            # The reference hands Java a set where `jsonSpec` takes a predicate, so jpy
            # finds no method to call.
            raise JavaError("no matching Java method overloads found")
        if include_columns is not None:
            include_columns = [str(column) for column in include_columns]
        spec = {
            "kind": "json",
            "include": include_columns,
            "mapping": None if mapping is None else [[str(c), str(f)] for c, f in mapping.items()],
            "nested_delim": nested_delim,
            "output_nulls": bool(output_nulls),
            "timestamp_field": timestamp_field,
        }
        return KeyValueSpec(spec)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e


def simple_spec(col_name: str) -> KeyValueSpec:
    """A spec that writes one column with Kafka's serializer for its type."""
    try:
        return KeyValueSpec({"kind": "simple", "column": col_name})
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e
