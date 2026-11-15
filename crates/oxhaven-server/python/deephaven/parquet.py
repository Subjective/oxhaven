"""Reading Parquet files as tables.

Behavior: `deephaven.parquet` (the names, signatures and error texts; `docs/STORAGE.md`
lists what a read supports). Writing is not here yet: `write`, `write_partitioned`,
`batch_write`, `delete` and `RowGroupInfo` are missing.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import _oxhaven

from .dherror import DHError
from .table import Table


class UnsignedLongTarget(Enum):
    """The type a Parquet UINT_64 column is read as."""

    BIG_INTEGER = "BIG_INTEGER"
    """A Java BigInteger. The default."""

    LONG = "LONG"
    """A long; a value above 2**63 - 1 is an error when it is read."""

    SIGNED_LONG = "SIGNED_LONG"
    """A long with the same bits, so that a value above 2**63 - 1 reads as negative."""


@dataclass
class ColumnInstruction:
    """Instructions for reading or writing one Parquet column."""

    column_name: Optional[str] = None
    """The name of the column in the table. Required when writing."""
    parquet_column_name: Optional[str] = None
    """The name of the column in the Parquet file. Required when reading."""
    codec_name: Optional[str] = None
    """The class name of an object codec for the column's values."""
    codec_args: Optional[str] = None
    """An argument string for the codec."""
    use_dictionary: bool = False
    """Whether a writer uses dictionary encoding for the column."""
    unsigned_long_target: Optional[UnsignedLongTarget] = None
    """For a read of a UINT_64 column: the type it is read as."""


class ParquetFileLayout(Enum):
    """The layout of the Parquet files of a table."""

    SINGLE_FILE = 1
    """A single Parquet file."""

    FLAT_PARTITIONED = 2
    """A single directory of Parquet files."""

    KV_PARTITIONED = 3
    """A directory tree of Parquet files, with directories named "key=value"."""

    METADATA_PARTITIONED = 4
    """A directory with a _metadata file, or that file itself."""


def _not_ported(java: str, what: str) -> NotImplementedError:
    return NotImplementedError(f"not ported yet: {java}: {what}")


def read(
    path: str,
    col_instructions: Optional[list[ColumnInstruction]] = None,
    is_legacy_parquet: bool = False,
    is_refreshing: bool = False,
    file_layout: Optional[ParquetFileLayout] = None,
    table_definition=None,
    special_instructions=None,
) -> Table:
    """Reads a table from a Parquet file or from a directory of Parquet files.

    Without `file_layout` the layout is decided by the path. `table_definition`,
    `special_instructions`, `is_refreshing` on a directory and directories named
    "key=value" are not supported yet.
    """
    try:
        columns = []
        for ci in col_instructions or []:
            if not ci.parquet_column_name:
                raise ValueError("must specify the parquet column name for read.")
            target = ci.unsigned_long_target
            if target is not None and not isinstance(target, UnsignedLongTarget):
                raise TypeError(f"not an UnsignedLongTarget: {target!r}")
            columns.append((
                ci.parquet_column_name,
                ci.column_name,
                ci.codec_name if ci.column_name else None,
                target.name if target is not None and ci.column_name else None,
            ))
        if file_layout is not None and not isinstance(file_layout, ParquetFileLayout):
            raise DHError(f"Invalid parquet file_layout '{file_layout}'")
        if table_definition is not None:
            raise _not_ported(
                "io.deephaven.parquet.table.ParquetInstructions.Builder#setTableDefinition",
                "a table definition for a read",
            )
        if special_instructions is not None:
            raise _not_ported(
                "io.deephaven.parquet.table.ParquetInstructions.Builder#setSpecialInstructions",
                "special instructions for a read",
            )
        return Table(_oxhaven.read_parquet(
            path,
            file_layout.name if file_layout is not None else None,
            bool(is_refreshing),
            bool(is_legacy_parquet),
            columns,
        ))
    except Exception as e:
        raise DHError(e, "failed to read parquet data.") from e
