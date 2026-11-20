"""The remote-table corpus: the remote lines of a scenario, the seed, the wait for a copy,
the recordings, the baseline.

Needs the recordings but no server.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from harness import baseline as bl
from harness import remote, scripts
from harness.__main__ import main
from harness.results import Failure, ResultSet
from harness.scenario import ScenarioError

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = ORACLE / "scenarios" / "remote"
CORPUS = ORACLE / "corpus" / "remote"
SEED = CORPUS / remote.SEED
BASELINE = ORACLE / "remote-baseline.json"

TWO_STEPS = """\
# oracle: results t
# remote: from deephaven import empty_table
# remote: r = empty_table(3)
# remote:     # indentation is kept
# remote-sync: t = r
t = 1
# oracle: step
# oracle: step
# remote: r = r.update("X = i")
# remote-sync: t = r, u = r
"""


# ---------------------------------------------------------------- the remote lines


def test_remote_lines_belong_to_the_step_they_are_written_in() -> None:
    scenario = remote.parse("two", TWO_STEPS)
    assert [step.script for step in scenario.steps] == [
        "from deephaven import empty_table\nr = empty_table(3)\n    # indentation is kept\n",
        "",
        'r = r.update("X = i")\n',
    ]
    assert [step.syncs for step in scenario.steps] == [
        (("t", "r"),), (), (("t", "r"), ("u", "r"))]
    # To the script scenario they are comments: its digest covers them.
    assert len(scenario.local.sections) == 3
    assert scenario.local.digest() != remote.parse("two", TWO_STEPS.replace("i", "ii")).local.digest()


@pytest.mark.parametrize("line", [
    "# remote-sync: t", "# remote-sync: t = ", "# remote-sync: 1t = r", "# remote-sync: t = r, t = s",
    "# remotely: x = 1", "# remote x = 1",
])
def test_remote_lines_refuse_what_they_do_not_understand(line: str) -> None:
    with pytest.raises(ScenarioError):
        remote.parse("bad", f"# oracle: results t\nt = 1\n{line}\n")


# ---------------------------------------------------------------- the seed


def test_the_recordings_were_made_from_the_seed() -> None:
    recorded = json.loads((CORPUS / remote.RECORDED_FROM).read_text(encoding="utf-8"))
    assert recorded == {"seed_sha256": remote.seed_digest(SEED), "reference": "42.9",
                        "remote": "oxhaven"}


def test_a_changed_seed_file_changes_the_digest(tmp_path: Path) -> None:
    for path in SEED.iterdir():
        (tmp_path / path.name).write_bytes(path.read_bytes())
    assert remote.seed_digest(tmp_path) == remote.seed_digest(SEED)
    (tmp_path / remote.SCOPE_SCRIPT).write_text("x = 1\n", encoding="utf-8")
    assert remote.seed_digest(tmp_path) != remote.seed_digest(SEED)


def test_the_gate_refuses_recordings_of_another_seed(tmp_path: Path, capsys) -> None:
    (tmp_path / remote.RECORDED_FROM).write_text(
        json.dumps({"seed_sha256": "0" * 64, "reference": "42.9"}), encoding="utf-8")
    # No server is asked: the refusal comes first.
    assert main(["remote-gate", "--url", "dh://127.0.0.1:1", "--remote", "dh://127.0.0.1:2",
                 "--corpus", str(tmp_path)]) == 2
    assert "not the one the recordings were made from" in capsys.readouterr().err


# ---------------------------------------------------------------- the scenarios


def test_every_remote_scenario_parses_and_has_its_recording() -> None:
    scenarios = remote.load_all(SCENARIOS)
    assert len(scenarios) >= 5
    for scenario in scenarios:
        assert scenario.name.startswith("remote_")
        run = scripts.load(CORPUS / scenario.name, scenario.local)
        assert len(run.outcomes) == len(scenario.steps)
    names = {scenario.name for scenario in scenarios}
    assert {p.name for p in CORPUS.iterdir() if p.is_dir()} - {remote.SEED} == names


def test_every_step_that_changes_the_remote_waits_for_a_copy() -> None:
    # A copy's rows arrive on the client's schedule: a step that changes the remote and
    # does not wait would record whatever had arrived by then.
    for scenario in remote.load_all(SCENARIOS):
        for number, step in enumerate(scenario.steps):
            if number and step.script:
                assert step.syncs, f"{scenario.name} step {number}"


def test_no_scenario_names_the_remote() -> None:
    # The remote's address comes from the environment, on a port the system chose.
    for path in sorted(SCENARIOS.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        assert 'os.environ["ORACLE_REMOTE_TARGET"]' in text, path.name
        assert "localhost" not in text, path.name


def test_the_remote_baseline_lists_every_scenario_and_nothing_else() -> None:
    baseline = bl.load(BASELINE)
    assert set(baseline) == {remote.SECTION}
    names = {scenario.name for scenario in remote.load_all(SCENARIOS)}
    assert set(baseline[remote.SECTION]) == names
    assert bl.dump(baseline) == BASELINE.read_text(encoding="utf-8")


# ---------------------------------------------------------------- the wait for a copy


class _Remote:
    def __init__(self, table: pa.Table):
        self.table = table

    def open_table(self, _name: str):
        table = self.table

        class Opened:
            def to_arrow(self) -> pa.Table:
                return table

        return Opened()


class _Copy:
    is_refreshing = False

    def __init__(self, table: pa.Table):
        self.table = table

    def to_arrow(self) -> pa.Table:
        return self.table


WANT = pa.table({"K": pa.array([1, 2], pa.int32()), "V": [0.5, float("nan")]})


def test_a_copy_is_asked_for_again_until_it_holds_the_remote_rows(monkeypatch) -> None:
    copies = iter([WANT.slice(0, 0), WANT.slice(0, 1),
                   pa.table({"K": pa.array([1, 2], pa.int32()), "V": [0.5, 1.0]}), WANT])
    asked = []

    def opened(_session, name):
        asked.append(name)
        return _Copy(next(copies))

    monkeypatch.setattr(scripts, "_open", opened)
    monkeypatch.setattr(remote.time, "sleep", lambda _seconds: None)
    far = remote.time.monotonic() + 60
    # NaN equals NaN: a copy is the remote's rows, not a float comparison.
    assert remote._sync(None, _Remote(WANT), ("t", "r"), far) is None
    assert asked == ["t"] * 4


def test_a_copy_that_does_not_come_is_reported_at_the_deadline(monkeypatch) -> None:
    monkeypatch.setattr(scripts, "_open", lambda _session, _name: _Copy(WANT.slice(0, 1)))
    past = remote.time.monotonic() - 1
    assert remote._sync(None, _Remote(WANT), ("t", "r"), past) == \
        "t does not hold the rows of the remote's r (1 rows, the remote has 2)"


def test_a_copy_of_other_column_names_is_no_copy(monkeypatch) -> None:
    other = WANT.rename_columns(["K", "W"])
    monkeypatch.setattr(scripts, "_open", lambda _session, _name: _Copy(other))
    past = remote.time.monotonic() - 1
    assert remote._sync(None, _Remote(WANT), ("t", "r"), past) is not None


def test_the_wait_ends_when_the_copy_has_failed(monkeypatch) -> None:
    monkeypatch.setattr(scripts, "_open", lambda _session, _name: Failure("NOT_FOUND"))
    far = remote.time.monotonic() + 60
    assert remote._sync(None, _Remote(WANT), ("t", "r"), far) is None


def test_cells_are_python_values_and_every_nan_is_one() -> None:
    names, cells = remote._cells(WANT)
    assert names == ["K", "V"] and cells == [[1, 2], [0.5, "NaN"]]


# ---------------------------------------------------------------- the two remotes


def test_two_recordings_are_compared_exactly(tmp_path: Path, capsys) -> None:
    scenario = remote.parse("remote_x", "# oracle: results t\nt = 1\n")
    tables = ResultSet("remote_x")
    tables.put(0, "t", pa.table({"V": [1.0]}))
    expected, actual = tmp_path / "a", tmp_path / "b"
    scripts.save(scripts.Run([None], tables), expected / "remote_x", scenario.local, "42.9")
    scripts.save(scripts.Run([None], tables), actual / "remote_x", scenario.local, "42.9")
    assert remote.compare_recordings([scenario], expected, actual) == 0
    moved = ResultSet("remote_x")
    moved.put(0, "t", pa.table({"V": [1.0000000001]}))
    scripts.save(scripts.Run([None], moved), actual / "remote_x", scenario.local, "42.9")
    # No tolerance: the same console read two servers.
    assert remote.compare_recordings([scenario], expected, actual) == 1
    assert remote.compare_recordings([scenario], expected, tmp_path / "none") == 2
    assert "DIFF  remote_x" in capsys.readouterr().out
