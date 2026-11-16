"""Listening to the changes of refreshing tables.

Behavior: `deephaven.table_listener` of the reference package. A listener is called in the
update cycle in which its table changed, on the cycle's thread, after the table and the
tables of `dependencies` have changed, and before the command whose write caused the cycle
goes on. `console/python/listener.rs` has the engine's side and the rule for locks.

An update gives its rows as numpy arrays, converted as `deephaven.numpy` converts a
column. In an environment without numpy, which the reference package cannot be installed
in, the values are lists of Python values, as the row iterators of a table give them.
"""

import traceback
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from functools import wraps
from inspect import signature
from typing import Callable, Optional, Union

import _oxhaven

from . import _engine, liveness_scope
from .dherror import DHError
from .table import Table


def _DEFAULT_ON_ERROR_CALLBACK(e: Exception):
    return print(f"An error occurred during table update processing: {e}")


def _error_callback_wrapper(callback: Callable[[Exception], None]) -> Callable[[str], None]:
    """`callback` as the engine calls it, with the text of a failure.

    Behavior: `deephaven.table_factory._error_callback_wrapper`
    """

    @wraps(callback)
    def wrapper(text) -> None:
        callback(RuntimeError(text))

    return wrapper


def _column_names(table: Table, cols) -> list:
    """The names of the columns of `table` that `cols` names, in the table's order; every
    column when `cols` names none.

    Behavior: `deephaven._table_reader._col_defs`
    """
    names = table.column_names
    if not cols:
        return names
    cols = _engine.to_sequence(cols)
    chosen = [name for name in names if name in cols]
    if len(chosen) != len(cols):
        raise ValueError(f"Invalid column names: {set(cols) - set(chosen)}")
    return chosen


def _converter():
    """Whether numpy is there, and the function that makes the values of a column what an
    update hands out."""
    try:
        from .numpy import _column_to_numpy_array
    except ImportError:
        return False, lambda col_def, values: values
    return True, _column_to_numpy_array


class TableUpdate:
    """The change of a table in one update cycle: the rows that were added, removed and
    modified. It can be read while the listener it was given to runs; afterwards it holds
    nothing, and each kind of rows is an empty dict.

    Behavior: `deephaven.table_listener.TableUpdate`
    """

    def __init__(self, table: Table, j_table_update):
        self.table = table
        self.j_table_update = j_table_update

    @property
    def j_object(self):
        return self.j_table_update

    def _rows(self, rows: str, cols) -> dict:
        names = _column_names(self.table, cols)
        numpy, convert = _converter()
        values = self.j_table_update.read(rows, names, numpy)
        if values is None:
            return {}
        definition = self.table.definition
        return {name: convert(definition[name], column) for name, column in zip(names, values)}

    def _chunks(self, rows: str, chunk_size: int, cols) -> Generator[dict, None, None]:
        if chunk_size < 0:
            raise ValueError("chunk_size must not be negative.")
        names = _column_names(self.table, cols)
        numpy, convert = _converter()
        definition = self.table.definition

        def chunks():
            first = 0
            while chunk_size > 0:
                values = self.j_table_update.read(rows, names, numpy, first, chunk_size)
                if values is None:
                    return
                chunk = {name: convert(definition[name], column) for name, column in zip(names, values)}
                read = min((len(column) for column in chunk.values()), default=0)
                if read == 0:
                    return
                yield chunk
                first += read

        return chunks()

    def added(self, cols: Optional[Union[str, list]] = None) -> dict:
        """The added rows: a dict from column name to the values of the column."""
        return self._rows("added", cols)

    def added_chunks(self, chunk_size: int, cols: Optional[Union[str, list]] = None) -> Generator[dict, None, None]:
        """A generator of the added rows, `chunk_size` rows at a time."""
        return self._chunks("added", chunk_size, cols)

    def removed(self, cols: Optional[Union[str, list]] = None) -> dict:
        """The removed rows, as they were before the cycle."""
        return self._rows("removed", cols)

    def removed_chunks(self, chunk_size: int, cols: Optional[Union[str, list]] = None) -> Generator[dict, None, None]:
        """A generator of the removed rows, `chunk_size` rows at a time."""
        return self._chunks("removed", chunk_size, cols)

    def modified(self, cols: Optional[Union[str, list]] = None) -> dict:
        """The modified rows, as they are now."""
        return self._rows("modified", cols)

    def modified_chunks(self, chunk_size: int, cols: Optional[Union[str, list]] = None) -> Generator[dict, None, None]:
        """A generator of the modified rows as they are now, `chunk_size` rows at a time."""
        return self._chunks("modified", chunk_size, cols)

    def modified_prev(self, cols: Optional[Union[str, list]] = None) -> dict:
        """The modified rows, as they were before the cycle."""
        return self._rows("modified_prev", cols)

    def modified_prev_chunks(
        self, chunk_size: int, cols: Optional[Union[str, list]] = None
    ) -> Generator[dict, None, None]:
        """A generator of the modified rows as they were, `chunk_size` rows at a time."""
        return self._chunks("modified_prev", chunk_size, cols)

    @property
    def shifted(self):
        raise NotImplementedError("shifts are not supported yet.")

    @property
    def modified_columns(self) -> list:
        """The names of the columns that changed in the modified rows."""
        columns = self.j_table_update.modified_columns()
        if columns is None:
            # the reference asks a Java update that was released for its column set
            raise AttributeError("'NoneType' object has no attribute 'dirtyColumnNames'")
        return columns


class TableListener(ABC):
    """A listener of one table, as an object."""

    @abstractmethod
    def on_update(self, update: TableUpdate, is_replay: bool) -> None:
        """Called with the change of the table, and whether it is the replay of the rows
        the table had when the listener started."""
        ...

    def on_error(self, e: Exception) -> None:
        """Called when `on_update` raised, or the table failed."""
        print(f"An error occurred during table update processing: {self}, {e}")


class MergedListener(ABC):
    """A listener of several tables, as an object."""

    @abstractmethod
    def on_update(self, updates: dict, is_replay: bool) -> None:
        """Called once per cycle in which a table changed, with the change of each table,
        or `None` for a table that did not change."""
        ...

    def on_error(self, e: Exception) -> None:
        """Called when `on_update` raised, or a table failed."""
        print(f"An error occurred during table update processing: {self}, {e}")


def _in_cycle(call: Callable) -> Callable:
    """What the engine calls for a listener: `call` with the updates. In an update cycle
    it returns the text of what `call` raised, which ends the listener; in a replay the
    exception goes to whoever started the listener."""

    def deliver(updates: list, is_replay: bool):
        if is_replay:
            call(updates, True)
            return None
        try:
            call(updates, False)
        except BaseException as error:  # the cycle's thread must outlive any listener
            trace = error.__traceback__.tb_next if error.__traceback__ else None
            text = "".join(traceback.format_exception(type(error), error, trace))
            return f"Error in Python interpreter:\n{text}"
        return None

    return deliver


def _error_callback(listener, base: type, on_error) -> Callable:
    if isinstance(listener, base):
        if on_error:
            raise DHError(
                message=f"Invalid on_error argument for listeners of {base.__name__} type "
                "which already have an on_error method."
            )
        return _error_callback_wrapper(listener.on_error)
    if callable(listener):
        return _error_callback_wrapper(on_error if on_error else _DEFAULT_ON_ERROR_CALLBACK)
    raise DHError(message=f"listener is neither callable nor {base.__name__} object")


def _dependency_handles(dependencies) -> list:
    """The handles of the tables a listener waits for."""
    handles = []
    for dependency in _engine.to_sequence(dependencies):
        if not isinstance(dependency, Table):
            # the reference takes any Java object that an update can wait for; here that
            # is a table
            raise TypeError(
                f"cannot convert a Python '{type(dependency).__name__}' to a Java "
                "'io.deephaven.engine.updategraph.NotificationQueue$Dependency'"
            )
        handles.append(dependency.j_table)
    return handles


class TableListenerHandle:
    """A listener of one table that can be started and stopped.

    Behavior: `deephaven.table_listener.TableListenerHandle`
    """

    def __init__(
        self,
        t: Table,
        listener: Union[Callable[[TableUpdate, bool], None], TableListener],
        description: Optional[str] = None,
        dependencies: Optional[Union[Table, Sequence[Table]]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        if not t.is_refreshing:
            raise DHError(message="table must be a refreshing table.")
        self.t = t
        self.description = description
        self.dependencies = _engine.to_sequence(dependencies)

        on_error_callback = _error_callback(listener, TableListener, on_error)
        if isinstance(listener, TableListener):
            if len(signature(listener.on_update).parameters) != 2:
                raise ValueError("The on_update method must have 2 (update, is_replay) parameters.")
            function = listener.on_update
        else:
            if len(signature(listener).parameters) != 2:
                raise ValueError("listener function must have 2 (update, is_replay) parameters.")
            function = listener

        @wraps(function)
        def call(updates: list, is_replay: bool) -> None:
            function(TableUpdate(t, updates[0]), is_replay)

        self.listener_wrapped = call
        try:
            self.listener_adapter = _oxhaven.Listener(
                [t.j_table], _dependency_handles(dependencies), _in_cycle(call), on_error_callback
            )
        except Exception as e:
            raise DHError(e, "failed to create a table listener.") from e
        self.started = False
        liveness_scope._created(self)

    @property
    def j_object(self):
        return self.listener_adapter

    def start(self, do_replay: bool = False) -> None:
        """Starts listening; with `do_replay`, the listener is first called with every row
        of the table as added."""
        if self.started:
            raise RuntimeError("Attempting to start an already started listener..")
        try:
            self.listener_adapter.start(do_replay)
        except Exception as e:
            raise DHError(e, "failed to listen to the table changes.") from e
        self.started = True

    def stop(self) -> None:
        """Stops listening."""
        if not self.started:
            return
        self.listener_adapter.stop()
        self.started = False

    def _liveness_release(self) -> bool:
        # a listener made in a liveness scope ends with the scope
        self.listener_adapter.release()
        return True


def listen(
    t: Table,
    listener: Union[Callable[[TableUpdate, bool], None], TableListener],
    description: Optional[str] = None,
    do_replay: bool = False,
    dependencies: Optional[Union[Table, Sequence[Table]]] = None,
    on_error: Optional[Callable[[Exception], None]] = None,
) -> TableListenerHandle:
    """Listens to the changes of the refreshing table `t`: `listener` is a function of
    `(update, is_replay)` or a `TableListener`. The listener lives as long as the handle
    this returns.

    `dependencies` are tables that the listener reads: it is called after they have
    changed in the cycle. `on_error` is called with the exception when the function raised
    or the table failed, after which the listener is not called again; without it the
    error is printed.
    """
    table_listener_handle = TableListenerHandle(
        t=t, listener=listener, description=description, dependencies=dependencies, on_error=on_error
    )
    table_listener_handle.start(do_replay=do_replay)
    return table_listener_handle


class MergedListenerHandle:
    """A listener of several tables that can be started and stopped.

    Behavior: `deephaven.table_listener.MergedListenerHandle`
    """

    def __init__(
        self,
        tables: Sequence[Table],
        listener: Union[Callable[[dict, bool], None], MergedListener],
        description: Optional[str] = None,
        dependencies: Optional[Union[Table, Sequence[Table]]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        if len(tables) < 2:
            raise DHError(message="A merged listener must listen to at least 2 refreshing tables.")
        for table in tables:
            if not table.is_refreshing:
                raise DHError(message="table must be a refreshing table.")
        self.tables = tables
        self.dependencies = dependencies

        on_error_callback = _error_callback(listener, MergedListener, on_error)
        self.listener = listener.on_update if isinstance(listener, MergedListener) else listener
        if len(signature(self.listener).parameters) != 2:
            raise ValueError("merged listener function must have 2 parameters (updates, is_replay).")

        def call(updates: list, is_replay: bool) -> None:
            self.listener(
                {
                    table: None if update is None else TableUpdate(table, update)
                    for table, update in zip(self.tables, updates)
                },
                is_replay,
            )

        try:
            self.merged_listener_adapter = _oxhaven.Listener(
                [table.j_table for table in tables],
                _dependency_handles(dependencies),
                _in_cycle(call),
                on_error_callback,
            )
        except Exception as e:
            raise DHError(e, "failed to create a merged listener adapter.") from e
        self.started = False
        liveness_scope._created(self)

    @property
    def j_object(self):
        return self.merged_listener_adapter

    def start(self, do_replay: bool = False) -> None:
        """Starts listening; with `do_replay`, the listener is first called with every row
        of every table as added."""
        if self.started:
            raise RuntimeError("Attempting to start an already started merged listener..")
        try:
            self.merged_listener_adapter.start(do_replay)
        except Exception as e:
            raise DHError(e, "failed to listen to the table changes.") from e
        self.started = True

    def stop(self) -> None:
        """Stops listening."""
        if not self.started:
            return
        self.merged_listener_adapter.stop()
        self.started = False

    def _liveness_release(self) -> bool:
        self.merged_listener_adapter.release()
        return True


def merged_listen(
    tables: Sequence[Table],
    listener: Union[Callable[[dict, bool], None], MergedListener],
    do_replay: bool = False,
    description: Optional[str] = None,
    dependencies: Optional[Union[Table, Sequence[Table]]] = None,
    on_error: Optional[Callable[[Exception], None]] = None,
) -> MergedListenerHandle:
    """Listens to the changes of several refreshing tables: `listener`, a function of
    `(updates, is_replay)` or a `MergedListener`, is called once per cycle in which at
    least one of them changed, with a dict from each table to its `TableUpdate`, or to
    `None` when it did not change.
    """
    merged_listener_handle = MergedListenerHandle(
        tables=tables, listener=listener, description=description, dependencies=dependencies, on_error=on_error
    )
    merged_listener_handle.start(do_replay=do_replay)
    return merged_listener_handle
