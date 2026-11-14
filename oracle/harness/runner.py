"""Runs a scenario against one server and returns every result table as Arrow.

No Java counterpart: this is the harness. It speaks to the server only through pydeephaven
(gRPC and Arrow Flight), so the same code runs against the Java reference and against Oxhaven.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.flight as paflight
from pydeephaven import NaturalJoinType, Session, SortDirection, agg, updateby
from pydeephaven._arrow import map_arrow_type
from pydeephaven.table import InputTable, Table

from . import scenario as sc
from .connect import open_session
from .results import ResultSet


class RunError(RuntimeError):
    """A scenario could not be run to completion on a server."""


def _import(session: Session, data: pa.Table) -> Table:
    """Uploads an Arrow table.

    pydeephaven's `import_table` never returns for a table with zero rows (it writes no record
    batch, so the DoPut stream is never completed). For that case this sends the same DoPut with
    the same schema annotations and closes the stream without a record batch.

    It must not send a zero-row record batch instead: the Java server then builds the table's
    row set as `RowSetFactory.fromRange(0, -1)` (io.deephaven.server.arrow.ArrowFlightUtil,
    DoPutObserver), and a keyed aggregation of that table fails with
    "asserted chunkSize > 0, instead chunkSize == 0".
    """
    if data.num_rows > 0:
        return session.import_table(data)
    ticket = session.next_export_ticket_number()
    schema = pa.schema(
        [pa.field(f.name, f.type, metadata=map_arrow_type(f.type)) for f in data.schema]
    )
    writer, reader = session.flight_service._flight_client.do_put(  # noqa: SLF001
        paflight.FlightDescriptor.for_path("export", str(ticket)),
        schema,
        paflight.FlightCallOptions(headers=session.grpc_metadata),
    )
    writer.done_writing()
    reader.read()
    return Table(session, ticket=session.make_export_ticket(ticket), size=0, schema=schema)


_AGGS = {
    "sum": agg.sum_,
    "min": agg.min_,
    "max": agg.max_,
    "first": agg.first,
    "last": agg.last,
    "avg": agg.avg,
}
_UPDATE_BY = {"cum_sum": updateby.cum_sum}
_NATURAL_JOIN_TYPES = {
    "error_on_duplicate": NaturalJoinType.ERROR_ON_DUPLICATE,
    "first_match": NaturalJoinType.FIRST_MATCH,
    "last_match": NaturalJoinType.LAST_MATCH,
    "exactly_one_match": NaturalJoinType.EXACTLY_ONE_MATCH,
}


def apply_operation(session: Session, tables: dict[str, Table], op: dict[str, Any]) -> Table:
    """Translates one neutral operation into the server call it stands for."""
    kind = op["op"]
    if kind == "merge":
        return session.merge_tables([tables[name] for name in op["sources"]])
    if kind in ("natural_join", "exact_join", "join", "aj"):
        left, right = tables[op["left"]], tables[op["right"]]
        joins = op.get("joins")
        if kind == "natural_join":
            join_type = _NATURAL_JOIN_TYPES[op.get("type", "error_on_duplicate")]
            return left.natural_join(right, on=op["on"], joins=joins, type=join_type)
        if kind == "exact_join":
            return left.exact_join(right, on=op["on"], joins=joins)
        if kind == "join":
            return left.join(right, on=op["on"], joins=joins)
        return left.aj(right, on=op["on"], joins=joins)

    source = tables[op["source"]]
    if kind == "where":
        return source.where(op["filters"])
    if kind == "sort":
        directions = {
            "ascending": SortDirection.ASCENDING,
            "descending": SortDirection.DESCENDING,
        }
        return source.sort(
            order_by=[by["column"] for by in op["by"]],
            order=[directions[by["direction"]] for by in op["by"]],
        )
    if kind == "head":
        return source.head(op["rows"])
    if kind == "tail":
        return source.tail(op["rows"])
    if kind == "view":
        return source.view(op["formulas"])
    if kind == "select":
        return source.select(op["formulas"])
    if kind == "update":
        return source.update(op["formulas"])
    if kind == "drop_columns":
        return source.drop_columns(op["columns"])
    if kind == "agg_by":
        aggs = [
            agg.count_(a["column"]) if a["agg"] == "count" else _AGGS[a["agg"]](a["columns"])
            for a in op["aggs"]
        ]
        return source.agg_by(aggs, op["by"])
    if kind == "update_by":
        return source.update_by([_UPDATE_BY[o["op"]](o["columns"]) for o in op["ops"]], op["by"])
    raise RunError(f"operation {kind!r} has no translation")


class _Barrier:
    """Waits until the server has finished the update cycle that applied a change.

    The Java server answers AddTableToInputTable / DeleteTableFromInputTable from
    io.deephaven.engine.table.impl.util.BaseArrayBackedInputTable#run, that is, as soon as the
    input table itself has refreshed, while tables derived from it may still be waiting for
    their notifications in the same cycle. A snapshot taken at that moment can still show the
    previous state of a derived table.

    So after each step the runner makes two more changes, one after the other, to a private
    input table. The second is enqueued after the first was applied, so it is applied in a later
    cycle than the first, and the first is applied no earlier than the cycle of the step. When
    the second returns, the step's cycle has completed and every derived table is up to date.
    This uses only the client API and holds for any server that answers an input table change
    after applying it.
    """

    _SCHEMA = pa.schema([("BarrierKey", pa.int32()), ("BarrierTick", pa.int64())])

    def __init__(self, session: Session):
        self._session = session
        self._table = session.input_table(schema=self._SCHEMA, key_cols=["BarrierKey"])
        self._tick = 0

    def wait(self) -> None:
        for _ in range(2):
            self._tick += 1
            row = pa.Table.from_arrays(
                [pa.array([0], pa.int32()), pa.array([self._tick], pa.int64())],
                schema=self._SCHEMA,
            )
            self._table.add(self._session.import_table(row))


def _snapshot(scenario: sc.Scenario, tables: dict[str, Table], step: int, out: ResultSet) -> None:
    for result in scenario.results:
        out.put(step, result.table, tables[result.table].to_arrow())


def run_scenario(scenario: sc.Scenario, url: str) -> ResultSet:
    """Runs `scenario` on the server at `url`.

    Step 0 is the state after the inputs are loaded and the operations applied. Step k (from 1)
    is the state after the k-th entry of `steps`.
    """
    out = ResultSet(scenario.name)
    session = open_session(url)
    try:
        tables: dict[str, Table] = {}
        inputs: dict[str, InputTable] = {}
        for item in scenario.inputs:
            if not item.ticking:
                tables[item.name] = _import(session, sc.arrow_table(item.columns, item.rows))
                continue
            input_table = session.input_table(
                schema=sc.arrow_schema(item.columns), key_cols=list(item.key_columns or ())
            )
            inputs[item.name] = input_table
            tables[item.name] = input_table
        barrier = _Barrier(session) if inputs else None

        # Initial rows of an input table go in through the same path as later additions.
        for item in scenario.inputs:
            if item.ticking and item.rows:
                inputs[item.name].add(_import(session, sc.arrow_table(item.columns, item.rows)))

        for op in scenario.operations:
            tables[op["name"]] = apply_operation(session, tables, op)

        if barrier:
            barrier.wait()
        _snapshot(scenario, tables, 0, out)

        for number, actions in enumerate(scenario.steps, start=1):
            for action in actions:
                item = scenario.input(action.table)
                if action.action == "add":
                    data = sc.arrow_table(item.columns, action.rows)
                    inputs[action.table].add(_import(session, data))
                else:
                    data = sc.arrow_table(sc.key_columns_of(item), action.rows)
                    inputs[action.table].delete(_import(session, data))
            assert barrier is not None
            barrier.wait()
            _snapshot(scenario, tables, number, out)
    except Exception as error:
        raise RunError(f"scenario {scenario.name} failed on {url}: {_describe(error)}") from error
    finally:
        session.close()
    return out


def _describe(error: BaseException) -> str:
    parts = []
    while error is not None:
        text = str(error).strip()
        if text and text not in parts:
            parts.append(text)
        error = error.__cause__
    return " <- ".join(parts)
