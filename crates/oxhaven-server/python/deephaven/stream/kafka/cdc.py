"""Change data capture: a Kafka topic of Debezium change events as the database table
they describe.

Behavior: `deephaven.stream.kafka.cdc` (the names, the signatures and the checks made in
Python, and how jpy hands each argument to Java); `io.deephaven.kafka.CdcTools` for the
rest. `docs/KAFKA.md`, "Change data capture", says what is built and what the reference
does with each argument.
"""

from typing import Optional

from ... import _engine, dtypes
from ...dherror import DHError
from ...table import Table
from . import _properties
from .consumer import TableType, _JTableType

# jpy's text when no Java method takes the Python values of a call.
_NO_OVERLOAD = "no matching Java method overloads found"


class CDCSpec:
    """A specification for how to consume a CDC Kafka stream."""

    def __init__(self, j_spec):
        # The topic, and the subject and version of the key's and the value's schemas, as
        # `_oxhaven.kafka_cdc_consume` reads them; the reference holds a Java object here.
        self._j_spec = j_spec

    @property
    def j_object(self):
        return self._j_spec


def _java_strings(*values):
    """The values as Java `String`s: a `str`, or `None` for Java's null. jpy finds no method
    for anything else."""
    for value in values:
        if value is not None and not isinstance(value, str):
            raise RuntimeError(_NO_OVERLOAD)
    return values


def _partitions(partitions):
    """The partitions as a list, or `None` for all of them.

    Behavior: `deephaven.stream.kafka.consumer._j_partitions`, through `dtypes.array`,
    which hands an `int` to `jpy.array` as the length of an array of zeros (probes
    part_int, part_int2, part_true, part_int_neg)
    """
    if partitions is None:
        return None
    if isinstance(partitions, int):
        if partitions < 0:
            cause = ValueError(
                "array: argument 2 (init) must be either an integer array length or any sequence"
            )
            raise DHError(cause, "failed to create a Java int array.") from cause
        return [0] * partitions
    return list(dtypes.array(dtypes.int32, partitions))


def consume(
    kafka_config: dict,
    cdc_spec: CDCSpec,
    partitions: Optional[list[int]] = None,
    stream_table: bool = False,
    cols_to_drop: Optional[list[str]] = None,
) -> Table:
    """Consumes a topic of change events (as Debezium writes them, in Avro, with the key's
    and the value's schemas in the registry of `schema.registry.url`) into the database
    table they describe: the `after` columns of the last event of each key, without the
    keys whose last event is a delete. The topic is read from the beginning of each
    partition in `partitions` (all without it). With `stream_table`, the events
    themselves, as a blink table with their `op` column.

    `cols_to_drop` must be `None`: the reference hands the Python value to a Java
    `Collection`, which jpy refuses for a list, so only a Java collection could be given
    there, and there are none here.
    """
    try:
        partitions = _partitions(partitions)
        config = _properties(kafka_config)
        spec = cdc_spec.j_object
        # jpy gives a Java `boolean` an `int` as whether it is not zero (probes st_int0,
        # st_int1, st_int2, st_intneg) and refuses anything else, and refuses any Python
        # value for the `Collection` (probes drop_*).
        if not isinstance(stream_table, int) or cols_to_drop is not None:
            raise RuntimeError(_NO_OVERLOAD)
        return Table(
            _engine.native.kafka_cdc_consume(config, spec, partitions, stream_table != 0, [])
        )
    except Exception as e:
        raise DHError(e, "failed to consume a CDC stream.") from e


def consume_raw(
    kafka_config: dict,
    cdc_spec: CDCSpec,
    partitions: Optional[list[int]] = None,
    table_type: TableType = TableType.Stream,
) -> Table:
    """Consumes the raw events of a topic of change events into a table: a column for each
    field of the key's and the value's schemas, nested records flattened, from the
    beginning of each partition.

    The reference hands `table_type` to Java as it is, and jpy refuses a `TableType`: only
    its `j_object` (`TableType.append().j_object`) is taken, and `None` fails once the
    schemas are fetched.
    """
    try:
        partitions = _partitions(partitions)
        config = _properties(kafka_config)
        spec = cdc_spec.j_object
        if table_type is not None and not isinstance(table_type, _JTableType):
            raise RuntimeError(_NO_OVERLOAD)
        return Table(
            _engine.native.kafka_cdc_consume_raw(
                config, spec, partitions, None if table_type is None else tuple(table_type)
            )
        )
    except Exception as e:
        raise DHError(e, "failed to consume a raw CDC stream.") from e


def cdc_long_spec(
    topic: str,
    key_schema_name: str,
    key_schema_version: str,
    value_schema_name: str,
    value_schema_version: str,
) -> CDCSpec:
    """A CDCSpec of a topic and the registry subjects and versions (`latest` or a number)
    of its key's and its value's schemas.

    Behavior: `io.deephaven.kafka.CdcTools#cdcLongSpec`
    """
    try:
        return CDCSpec(
            _java_strings(
                topic,
                key_schema_name,
                key_schema_version,
                value_schema_name,
                value_schema_version,
            )
        )
    except Exception as e:
        raise DHError(e, "failed to create a CDC spec in cdc_long_spec.") from e


def cdc_short_spec(server_name: str, db_name: str, table_name: str):
    """A CDCSpec in Debezium's naming: the topic is `server_name.db_name.table_name`, the
    key's and the value's schemas are its subjects with `-key` and `-value`, at their
    latest versions.

    Behavior: `io.deephaven.kafka.CdcTools#cdcShortSpec`, which joins the names as Java
    does, a null as `null`
    """
    try:
        names = ["null" if n is None else n for n in _java_strings(server_name, db_name, table_name)]
        topic = ".".join(names)
        return CDCSpec((topic, f"{topic}-key", "latest", f"{topic}-value", "latest"))
    except Exception as e:
        raise DHError(e, "failed to create a CDC spec in cdc_short_spec.") from e
