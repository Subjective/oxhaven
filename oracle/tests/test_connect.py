"""Where the harness lets the Python client's finalizers run. Needs no server."""

from __future__ import annotations

import gc
import threading
from typing import Iterator

import pytest

from harness import connect, smoke


class FakeSession:
    def __init__(self, **_: object) -> None:
        pass


class InACycle:
    """Stands in for a `pydeephaven.table.Table` that a failed call's traceback keeps in a
    reference cycle: only the collector finalizes it."""

    def __init__(self, finalized_on: list[threading.Thread]) -> None:
        self.me = self
        self.finalized_on = finalized_on

    def __del__(self) -> None:
        self.finalized_on.append(threading.current_thread())


def allocate_on_another_thread() -> None:
    """Allocates far more container objects than the collector's threshold, as a gRPC thread
    does over time. With automatic collection on, that thread runs the collector."""
    def allocate() -> None:
        kept = [[] for _ in range(100_000)]
        del kept

    thread = threading.Thread(target=allocate)
    thread.start()
    thread.join()


@pytest.fixture
def automatic_collection(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Starts the test as a fresh process starts: automatic collection on, nothing to collect."""
    monkeypatch.setattr(connect, "Session", FakeSession)
    was_enabled = gc.isenabled()
    gc.collect()
    gc.enable()
    try:
        yield
    finally:
        gc.collect()
        if was_enabled:
            gc.enable()
        else:
            gc.disable()


def test_a_table_in_a_cycle_is_finalized_only_where_the_harness_collects(
        automatic_collection: None) -> None:
    finalized_on: list[threading.Thread] = []
    connect.open_session("dh://127.0.0.1:1")
    InACycle(finalized_on)
    allocate_on_another_thread()
    assert finalized_on == [], "finalized by the collector on a thread the harness does not own"
    connect.open_session("dh://127.0.0.1:1")
    assert finalized_on == [threading.main_thread()]


def test_garbage_is_collected_before_each_smoke_check(
        automatic_collection: None, monkeypatch: pytest.MonkeyPatch) -> None:
    finalized_on: list[threading.Thread] = []
    seen: list[int] = []

    def check() -> None:
        # what the check before this one left is gone before this one makes a call
        seen.append(len(finalized_on))
        InACycle(finalized_on)
        allocate_on_another_thread()
        seen.append(len(finalized_on))

    monkeypatch.setitem(smoke._AREAS, "fake", lambda _url: [("a", check), ("b", check)])  # noqa: SLF001
    assert list(smoke.run_checks("fake", "dh://127.0.0.1:1")) == [("a", None), ("b", None)]
    assert seen == [0, 0, 1, 1]
    assert finalized_on == [threading.main_thread()]
