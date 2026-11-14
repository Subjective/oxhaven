"""Runs a scenario against one server and returns every result table as Arrow.

No Java counterpart: this is the harness. It speaks to the server only through pydeephaven
(gRPC and Arrow Flight), so the same code runs against the Java reference and against Oxhaven.
"""

from __future__ import annotations

from typing import Any, Callable

import grpc
import pyarrow as pa
import pyarrow.flight as paflight
from deephaven_core.proto import table_pb2
from pydeephaven import NaturalJoinType, Session, agg, updateby
from pydeephaven._arrow import map_arrow_type
from pydeephaven.table import InputTable, MultiJoinInput, Table, multi_join
from pydeephaven.ticket import ExportTicket

from . import scenario as sc
from .connect import open_session
from .results import Failure, ResultSet


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
    "sum": agg.sum_, "abs_sum": agg.abs_sum, "avg": agg.avg, "min": agg.min_, "max": agg.max_,
    "first": agg.first, "last": agg.last, "std": agg.std, "var": agg.var, "group": agg.group,
    "count_distinct": agg.count_distinct, "distinct": agg.distinct, "unique": agg.unique,
    "median": agg.median, "pct": agg.pct, "sorted_first": agg.sorted_first,
    "sorted_last": agg.sorted_last, "weighted_avg": agg.weighted_avg,
    "weighted_sum": agg.weighted_sum, "formula": agg.formula,
}
_NATURAL_JOIN_TYPES = {
    "error_on_duplicate": NaturalJoinType.ERROR_ON_DUPLICATE,
    "first_match": NaturalJoinType.FIRST_MATCH,
    "last_match": NaturalJoinType.LAST_MATCH,
    "exactly_one_match": NaturalJoinType.EXACTLY_ONE_MATCH,
}
_COMPARE = {
    "<": table_pb2.CompareCondition.LESS_THAN,
    "<=": table_pb2.CompareCondition.LESS_THAN_OR_EQUAL,
    ">": table_pb2.CompareCondition.GREATER_THAN,
    ">=": table_pb2.CompareCondition.GREATER_THAN_OR_EQUAL,
    "==": table_pb2.CompareCondition.EQUALS,
    "!=": table_pb2.CompareCondition.NOT_EQUALS,
}


def _aggregation(spec: dict[str, Any]) -> agg.Aggregation:
    kind, args = spec["agg"], spec.get("args", {})
    if kind == "count":
        return agg.count_(spec["column"])
    if kind == "count_where":
        return agg.count_where(spec["column"], args["filters"])
    if kind == "formula":
        return agg.formula(cols=spec["columns"], **args)
    if kind in ("sorted_first", "sorted_last", "weighted_avg", "weighted_sum"):
        first = args["order_by"] if kind.startswith("sorted") else args["wcol"]
        return _AGGS[kind](first, spec["columns"])
    return _AGGS[kind](cols=spec["columns"], **args)


def _update_by(spec: dict[str, Any]) -> updateby.UpdateByOperation:
    function = getattr(updateby, spec["op"])
    args = dict(spec.get("args", {}))
    if "delta_control" in args:
        args["delta_control"] = updateby.DeltaControl[args["delta_control"]]
    if "count_where" in spec["op"]:
        return function(col=spec["columns"][0], **args)
    return function(cols=spec["columns"], **args)


def _raw(session: Session, rpc: str, request: Callable[[Any], Any]) -> Table:
    """One table service call that pydeephaven has no method for. `request` builds the
    request message from the result ticket."""
    ticket = session.make_export_ticket()
    stub = session.table_service._grpc_table_stub  # noqa: SLF001
    response = session.wrap_rpc(getattr(stub, rpc), request(ticket.pb_ticket))
    if not response.success:
        raise RunError(f"{rpc} answered without success: {response.error_info}")
    return Table(session, ticket=ExportTicket(response.result_id.ticket.ticket),
                 schema_header=response.schema_header, size=response.size,
                 is_static=response.is_static)


def _reference(table: Table) -> Any:
    return table_pb2.TableReference(ticket=table.pb_ticket)


def _value(value: dict[str, Any]) -> Any:
    kind, body = next(iter(value.items()))
    if kind == "column":
        return table_pb2.Value(reference=table_pb2.Reference(column_name=body))
    if kind == "long":
        return table_pb2.Value(literal=table_pb2.Literal(long_value=body))
    if kind == "nanos":
        return table_pb2.Value(literal=table_pb2.Literal(nano_time_value=body))
    if isinstance(body, bool):
        return table_pb2.Value(literal=table_pb2.Literal(bool_value=body))
    if isinstance(body, str) and body not in sc.FLOAT_WORDS:
        return table_pb2.Value(literal=table_pb2.Literal(string_value=body))
    return table_pb2.Value(literal=table_pb2.Literal(
        double_value=sc.FLOAT_WORDS.get(body, body)))


def _condition(condition: dict[str, Any]) -> Any:
    """The `Condition` message of table.proto for one structured filter condition."""
    kind, body = next(iter(condition.items()))

    def case(item: dict[str, Any]) -> Any:
        return table_pb2.IGNORE_CASE if item.get("ignore_case") else table_pb2.MATCH_CASE

    def match(item: dict[str, Any]) -> Any:
        return table_pb2.INVERTED if item.get("inverted") else table_pb2.REGULAR

    if kind in ("and", "or"):
        message = table_pb2.AndCondition if kind == "and" else table_pb2.OrCondition
        return table_pb2.Condition(**{kind: message(filters=[_condition(c) for c in body])})
    if kind == "not":
        return table_pb2.Condition(**{"not": table_pb2.NotCondition(filter=_condition(body))})
    if kind == "compare":
        return table_pb2.Condition(compare=table_pb2.CompareCondition(
            operation=_COMPARE[body["op"]], case_sensitivity=case(body),
            lhs=_value(body["lhs"]), rhs=_value(body["rhs"])))
    if kind == "in":
        nan = {None: table_pb2.NAN_COMPARISON_TYPE_NOT_SPECIFIED, True: table_pb2.NAN_EQUALS_NAN,
               False: table_pb2.NAN_NOT_EQUALS_NAN}[body.get("nan_equals")]
        return table_pb2.Condition(**{"in": table_pb2.InCondition(
            target=_value(body["target"]), candidates=[_value(v) for v in body["candidates"]],
            case_sensitivity=case(body), match_type=match(body), nan_comparison=nan)})
    if kind == "is_null":
        return table_pb2.Condition(is_null=table_pb2.IsNullCondition(
            reference=table_pb2.Reference(column_name=body)))
    if kind == "is_nan":
        return table_pb2.Condition(is_nan=table_pb2.IsNaNCondition(
            reference=table_pb2.Reference(column_name=body)))
    if kind == "matches":
        return table_pb2.Condition(matches=table_pb2.MatchesCondition(
            reference=table_pb2.Reference(column_name=body["column"]), regex=body["text"],
            case_sensitivity=case(body), match_type=match(body)))
    if kind == "contains":
        return table_pb2.Condition(contains=table_pb2.ContainsCondition(
            reference=table_pb2.Reference(column_name=body["column"]),
            search_string=body["text"], case_sensitivity=case(body), match_type=match(body)))
    if kind == "invoke":
        target = {"target": _value(body["target"])} if "target" in body else {}
        return table_pb2.Condition(invoke=table_pb2.InvokeCondition(
            method=body["method"], arguments=[_value(v) for v in body["arguments"]], **target))
    return table_pb2.Condition(search=table_pb2.SearchCondition(
        search_string=body["text"],
        optional_references=[table_pb2.Reference(column_name=c)
                             for c in body.get("columns", ())]))


def _sort(session: Session, source: Table, op: dict[str, Any]) -> Table:
    directions = {
        "ascending": table_pb2.SortDescriptor.ASCENDING,
        "descending": table_pb2.SortDescriptor.DESCENDING,
    }
    if op["op"] == "reverse":
        sorts = [table_pb2.SortDescriptor(direction=table_pb2.SortDescriptor.REVERSE)]
    else:
        sorts = [
            table_pb2.SortDescriptor(column_name=by["column"],
                                     direction=directions[by["direction"]],
                                     is_absolute=by.get("absolute", False))
            for by in op["by"]
        ]
    return _raw(session, "Sort", lambda result: table_pb2.SortTableRequest(
        result_id=result, source_id=_reference(source), sorts=sorts))


def apply_operation(session: Session, tables: dict[str, Table], op: dict[str, Any]) -> Table:
    """Translates one neutral operation into the server call it stands for."""
    kind = op["op"]
    if kind == "merge":
        return session.merge_tables([tables[name] for name in op["sources"]])
    if kind == "empty_table":
        return session.empty_table(op["size"])
    if kind == "time_table":
        return session.time_table(op["period"], op["start_time"], op.get("blink", False))
    if kind == "multi_join":
        return multi_join([
            MultiJoinInput(tables[item["table"]], item["on"], item.get("joins"))
            for item in op["inputs"]
        ]).table
    if kind in ("natural_join", "exact_join", "join", "aj", "raj", "left_join", "where_in",
                "range_join"):
        left, right = tables[op["left"]], tables[op["right"]]
        joins = op.get("joins")
        if kind == "natural_join":
            join_type = _NATURAL_JOIN_TYPES[op.get("type", "error_on_duplicate")]
            return left.natural_join(right, on=op["on"], joins=joins, type=join_type)
        if kind == "exact_join":
            return left.exact_join(right, on=op["on"], joins=joins)
        if kind == "join":
            return left.join(right, on=op["on"], joins=joins)
        if kind == "aj":
            return left.aj(right, on=op["on"], joins=joins)
        if kind == "raj":
            return left.raj(right, on=op["on"], joins=joins)
        if kind == "left_join":
            return _raw(session, "LeftJoinTables", lambda result: table_pb2.LeftJoinTablesRequest(
                result_id=result, left_id=_reference(left), right_id=_reference(right),
                columns_to_match=op["on"], columns_to_add=joins or []))
        if kind == "where_in":
            if op.get("inverted", False):
                return left.where_not_in(right, op["on"])
            return left.where_in(right, op["on"])
        # range_join: the last entry of `on` is the range match, the others are exact matches.
        return _raw(session, "RangeJoinTables", lambda result: table_pb2.RangeJoinTablesRequest(
            result_id=result, left_id=_reference(left), right_id=_reference(right),
            exact_match_columns=op["on"][:-1], range_match=op["on"][-1],
            aggregations=[_aggregation(a).make_grpc_message() for a in op["aggs"]]))

    source = tables[op["source"]]
    if kind == "where":
        return source.where(op["filters"])
    if kind == "filter":
        return _raw(session, "Filter", lambda result: table_pb2.FilterTableRequest(
            result_id=result, source_id=_reference(source),
            filters=[_condition(c) for c in op["conditions"]]))
    if kind in ("sort", "reverse"):
        return _sort(session, source, op)
    if kind == "head":
        return source.head(op["rows"])
    if kind == "tail":
        return source.tail(op["rows"])
    if kind == "head_by":
        return source.head_by(op["rows"], op["by"])
    if kind == "tail_by":
        return source.tail_by(op["rows"], op["by"])
    if kind == "slice":
        return source.slice(op["start"], op["stop"])
    if kind == "flatten":
        return _raw(session, "Flatten", lambda result: table_pb2.FlattenRequest(
            result_id=result, source_id=_reference(source)))
    if kind == "view":
        return source.view(op["formulas"])
    if kind == "update_view":
        return source.update_view(op["formulas"])
    if kind == "select":
        return source.select(op["formulas"])
    if kind == "update":
        return source.update(op["formulas"])
    if kind == "lazy_update":
        return source.lazy_update(op["formulas"])
    if kind == "drop_columns":
        return source.drop_columns(op["columns"])
    if kind == "select_distinct":
        return source.select_distinct(op["columns"])
    if kind == "agg_by":
        return source.agg_by([_aggregation(a) for a in op["aggs"]], op["by"])
    if kind == "agg_all_by":
        return source.agg_all_by(_aggregation(op["agg"]), op["by"])
    if kind == "group_by":
        return source.group_by(op["by"])
    if kind == "ungroup":
        return source.ungroup(op.get("columns", []), op.get("null_fill", True))
    if kind == "update_by":
        return source.update_by([_update_by(o) for o in op["ops"]], op["by"])
    if kind == "snapshot":
        return source.snapshot()
    if kind == "snapshot_when":
        return source.snapshot_when(
            tables[op["trigger"]], op.get("stamp_columns", []), op.get("initial", False),
            op.get("incremental", False), op.get("history", False))
    if kind == "meta_table":
        return source.meta_table
    if kind == "column_statistics":
        limit = ({"unique_value_limit": op["unique_value_limit"]}
                 if "unique_value_limit" in op else {})
        return _raw(session, "ComputeColumnStatistics",
                    lambda result: table_pb2.ColumnStatisticsRequest(
                        result_id=result, source_id=_reference(source),
                        column_name=op["column"], **limit))
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


def failure_code(error: BaseException | None) -> str | None:
    """The gRPC status code name behind a client exception, if there is one."""
    while error is not None:
        if isinstance(error, grpc.RpcError):
            return error.code().name
        error = error.__cause__ or error.__context__
    return None


def _do_get_code(session: Session, table: Table) -> str | None:
    """Repeats a DoGet over plain gRPC and returns the status code name it fails with.

    pyarrow.flight turns a failed call into one of a few exception classes and does not expose
    the status code, so the request is sent again without it. The request is the Flight
    `Ticket` message: field 1, length-delimited.
    """
    ticket = table.ticket.bytes
    if len(ticket) > 127:
        raise RunError("ticket too long for the single-byte length this helper writes")
    call = session.grpc_channel.unary_stream(
        "/arrow.flight.protocol.FlightService/DoGet",
        request_serializer=lambda message: message,
        response_deserializer=lambda message: message,
    )
    try:
        for _ in call(b"\x0a" + bytes([len(ticket)]) + ticket, metadata=session.grpc_metadata):
            pass
    except grpc.RpcError as error:
        return error.code().name
    return None


def _snapshot(
    session: Session,
    scenario: sc.Scenario,
    tables: dict[str, Table | Failure],
    step: int,
    out: ResultSet,
) -> None:
    for result in scenario.results:
        table = tables[result.table]
        if isinstance(table, Failure):
            out.put(step, result.table, table)
            continue
        try:
            out.put(step, result.table, table.to_arrow())
        except Exception as error:  # noqa: BLE001 - classified just below
            code = _do_get_code(session, table)
            if code is None:
                raise RunError(
                    f"snapshot of {result.table} failed in the client, not on the server"
                ) from error
            out.put(step, result.table, Failure(code))


def _define(session: Session, tables: dict[str, Table | Failure], op: dict[str, Any]) -> None:
    """Applies one operation; a server refusal becomes the table's `Failure`.

    A table defined from a failed table fails the same way: the request is not sent, as the
    server would refuse it for the missing dependency.
    """
    for name in sc.references(op):
        if isinstance(tables[name], Failure):
            tables[op["name"]] = tables[name]
            return
    try:
        tables[op["name"]] = apply_operation(session, tables, op)
    except Exception as error:  # noqa: BLE001 - classified just below
        code = failure_code(error)
        if code is None:
            raise
        tables[op["name"]] = Failure(code)


def run_scenario(scenario: sc.Scenario, url: str) -> ResultSet:
    """Runs `scenario` on the server at `url`.

    Step 0 is the state after the inputs are loaded and the operations applied. Step k (from 1)
    is the state after the k-th entry of `steps`.
    """
    out = ResultSet(scenario.name)
    try:
        session = open_session(url)
    except Exception as error:  # noqa: BLE001
        raise RunError(f"cannot connect to {url}: {_describe(error)}") from error
    try:
        tables: dict[str, Table | Failure] = {}
        inputs: dict[str, InputTable] = {}
        for item in scenario.inputs:
            if not item.ticking:
                tables[item.name] = _import(session, sc.arrow_table(item.columns, item.rows))
                continue
            input_table = session.input_table(
                schema=sc.arrow_schema(item.columns), key_cols=list(item.key_columns or ()),
                blink_table=item.blink,
            )
            inputs[item.name] = input_table
            tables[item.name] = input_table
        # A time table fills in on the server's own cycles, so it needs the barrier too.
        clocked = any(op["op"] == "time_table" for op in scenario.operations)
        barrier = _Barrier(session) if inputs or clocked else None

        # Initial rows of an input table go in through the same path as later additions.
        for item in scenario.inputs:
            if item.ticking and item.rows:
                inputs[item.name].add(_import(session, sc.arrow_table(item.columns, item.rows)))

        for op in scenario.operations:
            _define(session, tables, op)

        if barrier:
            barrier.wait()
        _snapshot(session, scenario, tables, 0, out)

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
            _snapshot(session, scenario, tables, number, out)
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


class Target:
    """Something a scenario can be run against. The plain one is a server URL."""

    def __init__(self, url: str):
        self.url = url

    def __str__(self) -> str:
        return self.url

    def run(self, scenario: sc.Scenario) -> ResultSet:
        return run_scenario(scenario, self.url)


def open_target(url: str) -> Target:
    """`dh://...` and `dh+psk://...` name a server; `faulty:<fault>@<url>` wraps one so that
    its results come back damaged (see faults.FaultyTarget)."""
    if url.startswith("faulty:"):
        from .faults import FaultyTarget

        fault, _, inner = url[len("faulty:"):].partition("@")
        return FaultyTarget(fault, open_target(inner))
    return Target(url)
