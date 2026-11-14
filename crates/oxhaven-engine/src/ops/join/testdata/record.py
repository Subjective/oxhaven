"""Records the join scenarios of this directory from the Java reference server.

No Java counterpart: test fixture tooling.

    oracle/bin/start-java.sh 10810
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/join/testdata/record.py \
        dh+psk://oxhaven@127.0.0.1:10810
    oracle/bin/stop-java.sh 10810

Every scenario under `scenarios/` is run by the oracle's harness (`oracle/harness`), and its
results are stored under `corpus/<scenario>/` in the format of `oracle/corpus`.

One thing is added to what the harness records. A Java table that failed in an update cycle
still answers a snapshot with the rows it had, so a recording alone does not show the failure.
After every step this script therefore asks the server for `where("true")` of each result
table, which the server refuses for a failed table, and writes the refusals to
`corpus/<scenario>/failed.json` as the step, the table and the status code of each.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ORACLE = HERE.parents[5] / "oracle"
sys.path.insert(0, str(ORACLE))

from harness import results as res  # noqa: E402
from harness import runner  # noqa: E402
from harness import scenario as sc  # noqa: E402
from harness.__main__ import REFERENCE  # noqa: E402

PROBE = 'where("true")'


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
    target = runner.open_target(url)
    scenarios = sc.load_all(HERE / "scenarios")
    for scenario in scenarios:
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
