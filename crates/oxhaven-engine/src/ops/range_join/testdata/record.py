"""Records the range join scenarios of this directory from the Java reference server.

No Java counterpart: test fixture tooling.

    oracle/bin/start-java.sh 10866
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/range_join/testdata/record.py \
        dh+psk://oxhaven@127.0.0.1:10866
    oracle/bin/stop-java.sh 10866

Every scenario under `scenarios/` is run by the oracle's harness (`oracle/harness`), and its
results are stored under `corpus/<scenario>/` in the format of `oracle/corpus`.

One thing is added to what the harness records. A Java table that failed in an update cycle
still answers a snapshot with the rows it had, so a recording alone does not show the failure.
After every step this script therefore asks the server for `where("true")` of each result
table, which the server refuses for a failed table, and writes the refusals to
`corpus/<scenario>/failed.json` as the step, the table and the status code of each.

One scenario is sent differently from how the harness sends it. The harness sends the range
match of a range join as text (`range_match` of `RangeJoinTablesRequest`), and the text cannot
name two of the nine pairs of rules: the preceding row with a strict end, and the following row
with a strict start. The range joins of `range_join_rule_pairs` are therefore sent with the
separate fields of the request (`left_start_column`, `range_start_rule`, `right_range_column`,
`range_end_rule`, `left_end_column`), read from the match as the scenario writes it.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ORACLE = HERE.parents[5] / "oracle"
sys.path.insert(0, str(ORACLE))

from harness import results as res  # noqa: E402
from harness import runner  # noqa: E402
from harness import scenario as sc  # noqa: E402
from harness.__main__ import REFERENCE  # noqa: E402
from harness.runner import table_pb2  # noqa: E402

PROBE = 'where("true")'
BY_FIELDS = "range_join_rule_pairs"
MATCH = re.compile(r"(<- )?(\w+) (<=?) (\w+) (<=?) (\w+)( ->)?")


def range_join_by_fields(session, tables, op):
    """A range join whose range match goes in the separate fields of the request."""
    preceding, start, start_sign, value, end_sign, end, following = MATCH.fullmatch(
        op["on"][-1]
    ).groups()
    request = table_pb2.RangeJoinTablesRequest
    start_rule = (
        request.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING
        if preceding
        else request.LESS_THAN if start_sign == "<" else request.LESS_THAN_OR_EQUAL
    )
    end_rule = (
        request.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING
        if following
        else request.GREATER_THAN if end_sign == "<" else request.GREATER_THAN_OR_EQUAL
    )
    left, right = tables[op["left"]], tables[op["right"]]
    return runner._raw(
        session,
        "RangeJoinTables",
        lambda result: request(
            result_id=result,
            left_id=runner._reference(left),
            right_id=runner._reference(right),
            exact_match_columns=op["on"][:-1],
            left_start_column=start,
            range_start_rule=start_rule,
            right_range_column=value,
            range_end_rule=end_rule,
            left_end_column=end,
            aggregations=[runner._aggregation(a).make_grpc_message() for a in op["aggs"]],
        ),
    )


def main(url: str) -> int:
    failed: list[dict] = []
    snapshot = runner._snapshot

    def snapshot_and_probe(session, scenario, tables, step, out):
        snapshot(session, scenario, tables, step, out)
        for result in scenario.results:
            table = tables[result.table]
            if isinstance(table, res.Failure):
                continue
            try:
                table.where("true")
            except Exception as error:  # noqa: BLE001 - classified just below
                code = runner.failure_code(error)
                if code is None:
                    raise
                failed.append({"step": step, "table": result.table, "code": code})

    runner._snapshot = snapshot_and_probe
    by_fields = [False]
    apply_operation = runner.apply_operation

    def apply(session, tables, op):
        if by_fields[0] and op["op"] == "range_join":
            return range_join_by_fields(session, tables, op)
        return apply_operation(session, tables, op)

    runner.apply_operation = apply
    target = runner.open_target(url)
    scenarios = sc.load_all(HERE / "scenarios")
    for scenario in scenarios:
        by_fields[0] = scenario.name == BY_FIELDS
        failed.clear()
        recorded = target.run(scenario)
        directory = HERE / "corpus" / scenario.name
        res.save(recorded, directory, scenario.digest(), REFERENCE)
        with open(directory / "failed.json", "w", encoding="utf-8") as handle:
            json.dump({"probe": PROBE, "failed": failed}, handle, indent=2)
            handle.write("\n")
        print(
            f"saved {scenario.name}: {len(recorded.keys())} results, {recorded.failures()} "
            f"refused, {len(failed)} probes of failed tables"
        )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
