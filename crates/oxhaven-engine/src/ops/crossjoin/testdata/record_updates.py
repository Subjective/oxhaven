"""Records, from the Java reference engine, the updates cross joins emit, with row keys.

No Java counterpart: test fixture tooling.

    oracle/.venv/bin/python -I crates/oxhaven-engine/src/ops/crossjoin/testdata/record_updates.py

The oracle's harness sees a table only through snapshots, so it cannot show the row keys of
a result or the updates it delivers. This script runs the engine of the pinned
`deephaven-server` wheel in this process instead (its gRPC server listens on port 10864 while
the script runs and is not used). It makes a second update graph in unit test mode, so that
it can deliver hand-written updates with chosen row keys, shifts and modified columns, the
way Java's own engine tests drive tables.

Every file `updates/<name>.json` describes the source tables (long columns only; `null` is
a null), the joins of them, and the steps: per table the rows removed, the shifts, the rows
added and the rows modified with the columns given. The script writes
`updates/<name>.java.json` with, for each join, either the refusal (the Java exception's
class) or, before the first step and after each step, the result's row set, its columns in
row order, the update it delivered (added, removed, modified, shifts, modified columns) and
whether it had failed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
NULL_LONG = -(2**63)


def main() -> int:
    from deephaven_server import Server

    server = Server(
        host="127.0.0.1",
        port=10864,
        jvm_args=["-Xmx1g", "-Dauthentication.psk=oxhaven", "-XX:ActiveProcessorCount=2"],
    )
    server.start()

    import jpy

    configuration = jpy.get_type("io.deephaven.configuration.Configuration").getInstance()
    configuration.setProperty("PeriodicUpdateGraph.allowUnitTestMode", "true")
    graph = (
        jpy.get_type("io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph")
        .newBuilder("crossjoin-recorder")
        .existingOrBuild()
    )
    graph.enableUnitTestMode()
    graph.resetForUnitTests(False, False, 0, 0, 0, 0)
    context = jpy.get_type("io.deephaven.engine.context.ExecutionContext").getContext()
    context.withUpdateGraph(graph).open()

    for path in sorted((HERE / "updates").glob("*.json")):
        if path.name.endswith(".java.json"):
            continue
        scenario = json.loads(path.read_text())
        recorded = Recorder(jpy, graph).run(scenario)
        out = path.with_name(path.stem + ".java.json")
        out.write_text(json.dumps(recorded, indent=1) + "\n")
        print(f"recorded {path.name}")
    return 0


class Recorder:
    def __init__(self, jpy, graph):
        self.jpy = jpy
        self.graph = graph
        t = jpy.get_type
        self.RowSetFactory = t("io.deephaven.engine.rowset.RowSetFactory")
        self.QueryTable = t("io.deephaven.engine.table.impl.QueryTable")
        self.LongSource = t("io.deephaven.engine.table.impl.sources.LongSparseArraySource")
        self.Map = t("java.util.LinkedHashMap")
        self.List = t("java.util.ArrayList")
        self.JoinMatch = t("io.deephaven.api.JoinMatch")
        self.JoinAddition = t("io.deephaven.api.JoinAddition")
        self.ShiftBuilder = t("io.deephaven.engine.rowset.RowSetShiftData$Builder")
        self.TableUpdateImpl = t("io.deephaven.engine.table.impl.TableUpdateImpl")
        self.ModifiedColumnSet = t("io.deephaven.engine.table.ModifiedColumnSet")
        self.Runnable = t("io.deephaven.integrations.python.PythonThrowingRunnable")
        self.Listener = t("io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter")

    def rows(self, keys):
        return self.RowSetFactory.fromKeys(keys) if keys else self.RowSetFactory.empty()

    @staticmethod
    def ranges(row_set):
        out = []
        it = row_set.rangeIterator()
        while it.hasNext():
            it.next()
            out.append([it.currentRangeStart(), it.currentRangeEnd()])
        return out

    def make_table(self, spec):
        columns = self.Map()
        sources = {}
        for name in spec["columns"]:
            source = self.LongSource()
            columns.put(name, source)
            sources[name] = source
        for index, key in enumerate(spec["keys"]):
            for name, values in zip(spec["columns"], spec["values"]):
                sources[name].set(key, NULL_LONG if values[index] is None else values[index])
        table = self.QueryTable(self.rows(spec["keys"]).toTracking(), columns)
        if spec.get("refreshing", False):
            table.setRefreshing(True)
            for source in sources.values():
                source.startTrackingPrevValues()
        return table, sources

    def change(self, table, sources, columns, change):
        removed = self.rows(change.get("removed", []))
        builder = self.ShiftBuilder()
        for first, last, delta in change.get("shifted", []):
            builder.shiftRange(first, last, delta)
        shifted = builder.build()
        added = change.get("added", {"keys": [], "values": []})
        modified = change.get("modified", {"keys": [], "columns": [], "values": []})
        row_set = table.getRowSet().writableCast()
        row_set.remove(removed)
        shifted.apply(row_set)
        # A shifted row's values move with it; write them at the new keys.
        moved = []
        for first, last, delta in change.get("shifted", []):
            prev = table.getRowSet().prev()
            it = prev.rangeIterator()
            while it.hasNext():
                it.next()
                for key in range(max(first, it.currentRangeStart()), min(last, it.currentRangeEnd()) + 1):
                    if not removed.containsRange(key, key):
                        moved.append((key, key + delta))
        values = {name: [(new, sources[name].getPrevLong(old)) for old, new in moved] for name in columns}
        for name in columns:
            for new, value in values[name]:
                sources[name].set(new, value)
        for index, key in enumerate(added["keys"]):
            for name, column in zip(columns, added["values"]):
                sources[name].set(key, NULL_LONG if column[index] is None else column[index])
        for index, key in enumerate(modified["keys"]):
            for name, column in zip(modified["columns"], modified["values"]):
                sources[name].set(key, NULL_LONG if column[index] is None else column[index])
        added_rows = self.rows(added["keys"])
        row_set.insert(added_rows)
        update = self.TableUpdateImpl()
        update.added = added_rows
        update.removed = removed
        update.modified = self.rows(modified["keys"])
        update.shifted = shifted
        if modified["keys"]:
            mcs = table.newModifiedColumnSet(modified["columns"])
        else:
            mcs = self.ModifiedColumnSet.EMPTY
        update.modifiedColumnSet = mcs
        table.notifyListeners(update)

    def snapshot(self, table):
        row_set = table.getRowSet()
        columns = {}
        for name in table.getDefinition().getColumnNamesArray():
            source = table.getColumnSource(name)
            out = []
            it = row_set.iterator()
            while it.hasNext():
                value = source.getLong(it.nextLong())
                out.append(None if value == NULL_LONG else value)
            columns[name] = out
        return {"rows": self.ranges(row_set), "columns": columns, "failed": bool(table.isFailed())}

    def run(self, scenario):
        tables = {}
        lock = self.graph.sharedLock()
        lock.lock()
        try:
            for name, spec in scenario["tables"].items():
                tables[name] = (*self.make_table(spec), spec["columns"])
            joins = {}
            for join in scenario["joins"]:
                on = self.List()
                for pair in join.get("on", []):
                    on.add(self.JoinMatch.parse(pair))
                add = self.List()
                for pair in join.get("add", []):
                    add.add(self.JoinAddition.parse(pair))
                left, right = tables[join["left"]][0], tables[join["right"]][0]
                try:
                    joins[join["name"]] = left.join(right, on, add, join.get("reserve", 10))
                except Exception as error:  # noqa: BLE001 - recorded as the refusal
                    joins[join["name"]] = str(error).split(":")[0]
        finally:
            lock.unlock()

        recorded = {"joins": {}}
        updates = {}
        listeners = []
        for name, result in joins.items():
            if isinstance(result, str):
                recorded["joins"][name] = {"refused": result}
                continue
            recorded["joins"][name] = {"steps": [self.snapshot(result)]}
            updates[name] = []
            if result.isRefreshing():
                listeners.append(self.listen(name, result, updates[name]))

        for step in scenario["steps"]:
            for log in updates.values():
                log.append(None)

            def cycle():
                for name, change in step.items():
                    table, sources, columns = tables[name]
                    self.change(table, sources, columns, change)

            self.graph.runWithinUnitTestCycle(self.Runnable(cycle))
            for name, result in joins.items():
                if isinstance(result, str):
                    continue
                snapshot = self.snapshot(result)
                snapshot["update"] = updates[name][-1]
                recorded["joins"][name]["steps"].append(snapshot)
        for listener in listeners:
            listener()
        return recorded

    def listen(self, name, table, log):
        from deephaven.table import Table
        from deephaven.table_listener import listen

        def on_update(update, is_replay):
            j = update.j_table_update
            shifted = j.shifted()
            log[-1] = {
                "added": self.ranges(j.added()),
                "removed": self.ranges(j.removed()),
                "modified": self.ranges(j.modified()),
                "shifted": [
                    [shifted.getBeginRange(i), shifted.getEndRange(i), shifted.getShiftDelta(i)]
                    for i in range(shifted.size())
                ],
                "modified_columns": sorted(j.modifiedColumnSet().dirtyColumnNames()),
            }

        def on_error(error):
            log[-1] = {"error": str(error).split("\n")[0][:200]}

        handle = listen(Table(table), on_update, on_error=on_error)
        return handle.stop


if __name__ == "__main__":
    sys.exit(main())
