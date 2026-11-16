"""Reading Parquet files as tables and writing tables as Parquet files.

Behavior: `deephaven.parquet` (the names, signatures and error texts; `docs/STORAGE.md`
lists what a read and a write support).
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from . import _engine, dtypes
from .column import ColumnType
from .dherror import DHError
from .table import PartitionedTable, Table, TableDefinition


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

    Without `file_layout` the layout is decided by the path. Directories named "key=value"
    become partitioning columns, and with `is_refreshing` the table gains the rows of the
    files that appear in its directory. `table_definition` and `special_instructions` are
    not supported yet.
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
        return Table(_engine.native.read_parquet(
            path,
            file_layout.name if file_layout is not None else None,
            bool(is_refreshing),
            bool(is_legacy_parquet),
            columns,
        ))
    except Exception as e:
        raise DHError(e, "failed to read parquet data.") from e


class RowGroupInfo:
    """How the rows of a table are split into the row groups of a Parquet file. Make one
    with `single_group`, `max_groups`, `max_rows` or `by_groups`.

    Behavior: `deephaven.parquet.RowGroupInfo`, which wraps
    `io.deephaven.parquet.table.metadata.RowGroupInfo`; this one holds the name of the
    method that made it and its arguments.
    """

    def __init__(self, j_row_group_info):
        self.j_row_group_info = j_row_group_info

    @property
    def j_object(self):
        """The reference's Java object; here the method name, the number and the columns."""
        return self.j_row_group_info

    @classmethod
    def single_group(cls) -> "RowGroupInfo":
        """All rows in one row group. The default."""
        return cls(("single_group", None, []))

    @classmethod
    def max_groups(cls, num_row_groups: int) -> "RowGroupInfo":
        """`num_row_groups` row groups of the same size; where the rows do not divide
        evenly, the last ones have one row fewer."""
        if num_row_groups < 1:
            raise RuntimeError(
                "java.lang.IllegalArgumentException: Cannot define less than 1 RowGroup"
            )
        return cls(("max_groups", int(num_row_groups), []))

    @classmethod
    def max_rows(cls, max_rows: int) -> "RowGroupInfo":
        """Row groups of at most `max_rows` rows each."""
        if max_rows <= 0:
            raise RuntimeError("java.lang.IllegalArgumentException: MaxRows must be positive")
        return cls(("max_rows", int(max_rows), []))

    @classmethod
    def by_groups(cls, groups: list[str], max_rows: Optional[int] = None) -> "RowGroupInfo":
        """One row group for each distinct value of the `groups` columns, split further
        as `max_rows` does when it is given. The rows of a value must be next to each
        other in the table, or the write fails."""
        if max_rows and max_rows <= 0:
            raise RuntimeError("java.lang.IllegalArgumentException: MaxRows must be positive")
        groups = list(groups)
        if not groups:
            raise RuntimeError(
                "java.lang.IllegalArgumentException: Must contain at least one group"
            )
        return cls(("by_groups", int(max_rows) if max_rows else None, groups))


def _write_arguments(
    table_definition,
    col_instructions,
    compression_codec_name,
    max_dictionary_keys,
    max_dictionary_size,
    target_page_size,
    generate_metadata_files,
    index_columns,
    row_group_info,
    special_instructions,
    base_name=None,
) -> tuple:
    """The arguments of a write after the tables and where they go, as the native
    functions take them.

    Behavior: `deephaven.parquet._build_parquet_instructions` with `for_read=False`
    """
    columns = []
    for ci in col_instructions or []:
        if not ci.column_name:
            raise ValueError("must specify the table column name for write.")
        columns.append((
            ci.column_name,
            ci.parquet_column_name,
            ci.codec_name or None,
            bool(ci.use_dictionary),
        ))
    definition, definition_of = None, None
    if table_definition is not None:
        if not isinstance(table_definition, TableDefinition):
            table_definition = TableDefinition(table_definition)
        if table_definition._handle is not None:
            # a table's own definition, which may have types `dtypes` has no name for
            definition_of = table_definition._handle
        else:
            definition = [
                (
                    column.name,
                    dtypes._engine_type(column.data_type),
                    column.column_type is ColumnType.PARTITIONING,
                )
                for column in table_definition.values()
            ]
    if row_group_info is not None:
        row_group_info = row_group_info.j_object
    if special_instructions is not None:
        raise _not_ported(
            "io.deephaven.parquet.table.ParquetInstructions.Builder#setSpecialInstructions",
            "special instructions for a write",
        )
    return (
        definition,
        definition_of,
        columns,
        compression_codec_name or None,
        max_dictionary_keys,
        max_dictionary_size,
        target_page_size,
        bool(generate_metadata_files),
        [list(names) for names in index_columns or []],
        row_group_info,
        base_name or None,
    )


def _handle(table):
    if not isinstance(table, Table):
        # what the reference raises when it asks the argument for its Java table
        raise AttributeError(f"'{type(table).__name__}' object has no attribute 'j_table'")
    return table.j_table


def delete(path: str) -> None:
    """Deletes a Parquet table on disk: the file at `path`, or the directory and
    everything in it. A path that names nothing is not an error."""
    try:
        _engine.native.delete_parquet(path)
    except Exception as e:
        raise DHError(e, f"failed to delete a parquet table: {path} on disk.") from e


def write(
    table: Table,
    path: str,
    table_definition=None,
    col_instructions: Optional[list[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    target_page_size: Optional[int] = None,
    generate_metadata_files: Optional[bool] = None,
    index_columns: Optional[Sequence[Sequence[str]]] = None,
    row_group_info: Optional[RowGroupInfo] = None,
    special_instructions=None,
) -> None:
    """Writes a table to a Parquet file.

    `path` must end in ".parquet"; directories that do not exist are created, and removed
    again if the write fails. `table_definition` gives the columns of the file where they
    are not the table's: a column the table does not have is written as nulls.
    `compression_codec_name` defaults to "SNAPPY". With `generate_metadata_files` the
    directory of the file also gets a `_metadata` and a `_common_metadata` file.
    `index_columns`, `special_instructions`, a codec in `col_instructions` and "LZO" are
    not supported yet.
    """
    try:
        arguments = _write_arguments(
            table_definition,
            col_instructions,
            compression_codec_name,
            max_dictionary_keys,
            max_dictionary_size,
            target_page_size,
            generate_metadata_files,
            index_columns,
            row_group_info,
            special_instructions,
        )
        _engine.native.write_parquet([_handle(table)], [path], arguments)
    except Exception as e:
        raise DHError(e, "failed to write to parquet data.") from e


def write_partitioned(
    table,
    destination_dir: str,
    table_definition=None,
    col_instructions: Optional[list[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    target_page_size: Optional[int] = None,
    base_name: Optional[str] = None,
    generate_metadata_files: Optional[bool] = None,
    index_columns: Optional[Sequence[Sequence[str]]] = None,
    row_group_info: Optional[RowGroupInfo] = None,
    special_instructions=None,
) -> None:
    """Writes a table as a directory tree of Parquet files, one directory level named
    "key=value" for each partitioning column, and the rows of each key in a file there,
    without the key columns.

    `table` is a table whose definition, or `table_definition`, has partitioning columns,
    or a partitioned table, whose key columns they then are. `base_name` names the files:
    "{uuid}" in it is a random UUID, which is the default; "{i}" counts the files of one
    key; "{partitions}" is the directory names joined by "_". A file that is there under
    a name this write uses is replaced; every other file under `destination_dir` stays.
    With `generate_metadata_files`, `destination_dir` also gets a `_metadata` and a
    `_common_metadata` file. The other arguments are those of `write`.
    """
    try:
        arguments = _write_arguments(
            table_definition,
            col_instructions,
            compression_codec_name,
            max_dictionary_keys,
            max_dictionary_size,
            target_page_size,
            generate_metadata_files,
            index_columns,
            row_group_info,
            special_instructions,
            base_name,
        )
        if isinstance(table, PartitionedTable):
            source = (None, table.j_object)
        elif isinstance(table, Table):
            source = (table.j_object, None)
        else:
            # what the reference raises when it asks the argument for its Java object
            raise AttributeError(f"'{type(table).__name__}' object has no attribute 'j_object'")
        _engine.native.write_parquet_partitioned(*source, destination_dir, arguments)
    except Exception as e:
        raise DHError(e, "failed to write to parquet data.") from e


def batch_write(
    tables: list[Table],
    paths: list[str],
    table_definition=None,
    col_instructions: Optional[list[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    target_page_size: Optional[int] = None,
    generate_metadata_files: Optional[bool] = None,
    index_columns: Optional[Sequence[Sequence[str]]] = None,
    row_group_info: Optional[RowGroupInfo] = None,
    special_instructions=None,
):
    """Writes tables to Parquet files, one path for each table: all of them or none.

    Without `table_definition` the tables must have the same columns. The other
    arguments are those of `write`.
    """
    try:
        arguments = _write_arguments(
            table_definition,
            col_instructions,
            compression_codec_name,
            max_dictionary_keys,
            max_dictionary_size,
            target_page_size,
            generate_metadata_files,
            index_columns,
            row_group_info,
            special_instructions,
        )
        handles = [_handle(table) for table in tables]
        _engine.native.write_parquet(handles, list(paths), arguments)
    except Exception as e:
        raise DHError(e, "write multiple tables to parquet data failed.") from e
