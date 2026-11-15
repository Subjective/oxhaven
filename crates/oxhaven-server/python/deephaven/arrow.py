"""Conversion between Deephaven tables and pyarrow tables.

Behavior: `deephaven.arrow` of the reference package. There a table reaches pyarrow as
serialized record batches that pyarrow parses, and the other way round; here both sides
are in one process and exchange Arrow arrays through the Arrow C stream interface
(`console/python/data.rs`), without a copy.

A pyarrow table is read as a table a client uploads is, so what an upload does not read
yet is refused here too, with the name of the Java method: an Arrow type that Java
converts while reading (unsigned and half-precision numbers, timestamps that are not in
nanoseconds, dates, times, durations, decimals, binary), a type the engine has no column
type for, and a column name that Java would change into a legal one.
"""

from typing import Optional, Sequence

import pyarrow as pa

from . import _engine
from ._java_time import NotPorted
from .dherror import DHError
from .table import Table

# The Java class a pyarrow type without parameters is read as. A type with parameters
# (a timestamp with a time zone, a list, a decimal) is left for the server to decide.
_JAVA_CLASSES = [
    (pa.null(), "java.lang.Object"),
    (pa.bool_(), "java.lang.Boolean"),
    (pa.int8(), "byte"),
    (pa.int16(), "short"),
    (pa.int32(), "int"),
    (pa.int64(), "long"),
    (pa.uint8(), "short"),
    (pa.uint16(), "char"),
    (pa.uint32(), "long"),
    (pa.uint64(), "java.math.BigInteger"),
    (pa.float16(), "float"),
    (pa.float32(), "float"),
    (pa.float64(), "double"),
    *((pa.time32(unit), "java.time.LocalTime") for unit in ("s", "ms")),
    *((pa.time64(unit), "java.time.LocalTime") for unit in ("us", "ns")),
    *((pa.timestamp(unit), "java.time.Instant") for unit in ("s", "ms", "us", "ns")),
    (pa.date32(), "java.time.LocalDate"),
    (pa.date64(), "java.time.LocalDate"),
    *((pa.duration(unit), "java.time.Duration") for unit in ("s", "ms", "us", "ns")),
    (pa.month_day_nano_interval(), "org.apache.arrow.vector.PeriodDuration"),
    (pa.binary(), "byte[]"),
    (pa.string(), "java.lang.String"),
]

# The pyarrow types with parameters that are read.
_PARAMETERIZED = (
    pa.TimestampType,
    pa.FixedSizeBinaryType,
    pa.Decimal128Type,
    pa.Decimal256Type,
    pa.ListType,
    pa.ListViewType,
    pa.FixedSizeListType,
    pa.MapType,
    pa.DenseUnionType,
    pa.SparseUnionType,
)

SUPPORTED_ARROW_TYPES = [arrow_type for arrow_type, _ in _JAVA_CLASSES]
"""The pyarrow types without parameters that `to_table` accepts."""


def _type_metadata(arrow_type: pa.DataType) -> dict:
    """The field metadata that tells the server which column type a pyarrow type is."""
    for known, java_class in _JAVA_CLASSES:
        if known == arrow_type:
            return {"deephaven:type": java_class}
    if type(arrow_type) in _PARAMETERIZED:
        return {}
    raise DHError(
        message=f"unsupported arrow data type : {arrow_type}, refer to "
        f"deephaven.arrow.SUPPORTED_ARROW_TYPES for the list of supported pyarrow types."
    )


def to_table(pa_table: pa.Table, cols: Optional[Sequence[str]] = None) -> Table:
    """Creates a static table from a pyarrow table, or from the columns `cols` of it.

    The metadata of the pyarrow fields is not read: each column has the type that goes
    with its pyarrow type.
    """
    if cols:
        pa_table = pa_table.select(cols)
    schema = pa.schema(
        [pa.field(field.name, field.type, metadata=_type_metadata(field.type)) for field in pa_table.schema]
    )
    try:
        batches = [
            pa.RecordBatch.from_arrays(batch.columns, schema=schema) for batch in pa_table.to_batches()
        ]
        described = pa.Table.from_batches(batches, schema=schema)
        return Table(_engine.native.table_from_arrow(described))
    except Exception as e:
        raise DHError(e, message="failed to create a Deephaven table from a pyarrow table.") from e


def to_arrow(table: Table, cols: Optional[Sequence[str]] = None) -> pa.Table:
    """Creates a pyarrow table from a consistent snapshot of a table, or of the columns
    `cols` of it, which may be formulas as `Table.view` takes them.

    The pyarrow table has one chunk per column; the reference's has one per record batch
    of the snapshot.
    """
    try:
        if cols:
            table = table.view(formulas=cols)
        return pa.table(_engine.native.arrow_snapshot(table.j_table))
    except Exception as e:
        raise DHError(e, message="failed to create a pyarrow table from a Deephaven table.") from e


def read_feather(path: str) -> Table:
    """Not available yet: the engine does not read Feather files
    (`io.deephaven.extensions.arrow.ArrowWrapperTools#readFeather`)."""
    try:
        raise NotPorted(
            "io.deephaven.extensions.arrow.ArrowWrapperTools#readFeather",
            "a table of an Arrow IPC file",
        )
    except Exception as e:
        raise DHError(e, message=f"failed to read a feather file {path}") from e
