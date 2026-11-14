"""Result sets and their on-disk form (Arrow IPC), used by record and replay.

No Java counterpart: harness storage.

A recording of one scenario is a directory:

    corpus/<scenario>/manifest.json            what was recorded, from which scenario content
    corpus/<scenario>/step<NNN>.<table>.arrow  one Arrow IPC file per result table per step
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

MANIFEST = "manifest.json"
MANIFEST_FORMAT = 1


class RecordingError(RuntimeError):
    """A recording is missing, incomplete, or was made from different scenario content."""


class ResultSet:
    """The result tables of one scenario run, keyed by (step, table), in the order produced."""

    def __init__(self, scenario: str):
        self.scenario = scenario
        self._tables: dict[tuple[int, str], pa.Table] = {}

    def put(self, step: int, table: str, data: pa.Table) -> None:
        self._tables[(step, table)] = data

    def get(self, step: int, table: str) -> pa.Table:
        return self._tables[(step, table)]

    def keys(self) -> list[tuple[int, str]]:
        return list(self._tables)

    def copy(self) -> "ResultSet":
        other = ResultSet(self.scenario)
        other._tables = dict(self._tables)
        return other

    def remove(self, step: int, table: str) -> None:
        del self._tables[(step, table)]


def _file_name(step: int, table: str) -> str:
    return f"step{step:03d}.{table}.arrow"


def save(results: ResultSet, directory: Path, scenario_digest: str, reference: str) -> None:
    """Writes `results` under `directory`, replacing any earlier recording there."""
    directory.mkdir(parents=True, exist_ok=True)
    for stale in directory.glob("*.arrow"):
        stale.unlink()
    entries = []
    for step, table in results.keys():
        data = results.get(step, table)
        name = _file_name(step, table)
        with ipc.new_file(directory / name, data.schema) as writer:
            writer.write_table(data)
        entries.append({"step": step, "table": table, "file": name, "rows": data.num_rows})
    manifest = {
        "format": MANIFEST_FORMAT,
        "scenario": results.scenario,
        "scenario_sha256": scenario_digest,
        "reference": reference,
        "entries": entries,
    }
    with open(directory / MANIFEST, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")


def load(directory: Path, scenario: str, scenario_digest: str) -> ResultSet:
    """Reads the recording of `scenario`; refuses one made from different scenario content."""
    manifest_path = directory / MANIFEST
    if not manifest_path.is_file():
        raise RecordingError(f"no recording for scenario {scenario} at {directory}")
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("format") != MANIFEST_FORMAT or manifest.get("scenario") != scenario:
        raise RecordingError(f"{manifest_path} is not a recording of scenario {scenario}")
    if manifest.get("scenario_sha256") != scenario_digest:
        raise RecordingError(
            f"the recording of scenario {scenario} was made from different scenario content; "
            "record it again against the Java reference"
        )
    results = ResultSet(scenario)
    for entry in manifest["entries"]:
        with ipc.open_file(directory / entry["file"]) as reader:
            results.put(entry["step"], entry["table"], reader.read_all())
    return results
