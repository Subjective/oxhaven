"""Health check: `python -m harness.health <url>` exits 0 when the server runs a table operation.

No Java counterpart: harness plumbing.
"""

from __future__ import annotations

import sys

import pyarrow as pa

from .connect import open_session


def check(url: str) -> None:
    """Round-trips one table; raises if the server does not answer correctly."""
    session = open_session(url)
    try:
        table = session.import_table(pa.table({"X": pa.array([1, 2, 3], pa.int32())}))
        result = table.where(["X > 1"]).to_arrow()
        if result.column("X").to_pylist() != [2, 3]:
            raise RuntimeError(f"health query returned {result.column('X').to_pylist()}")
    finally:
        session.close()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python -m harness.health <url>", file=sys.stderr)
        return 2
    try:
        check(argv[1])
    except Exception as error:  # noqa: BLE001 - any failure means "not healthy"
        print(f"unhealthy: {error}", file=sys.stderr)
        return 1
    print("healthy")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
