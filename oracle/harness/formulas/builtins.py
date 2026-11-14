"""The names a formula can use without qualification: the default static imports.

Behavior: io.deephaven.engine.table.lang.impl.QueryLibraryImportsDefaults#statics lists the
classes whose public static methods and fields every formula sees. This module reads those
classes out of the jars of the pinned `deephaven-server` wheel with `javap`, so the list is the
one the oracle runs with, and writes it to `corpus/formulas/builtins.json`.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

# QueryLibraryImportsDefaults#statics, in its order.
STATIC_IMPORTS = (
    "io.deephaven.util.QueryConstants",
    "io.deephaven.function.Basic",
    "io.deephaven.function.BinSearch",
    "io.deephaven.function.BinSearchAlgo",
    "io.deephaven.function.Cast",
    "io.deephaven.function.Logic",
    "io.deephaven.function.Numeric",
    "io.deephaven.function.Parse",
    "io.deephaven.function.Random",
    "io.deephaven.function.Sort",
    "io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils",
    "io.deephaven.time.DateTimeUtils",
    "io.deephaven.base.string.cache.CompressedString",
    "io.deephaven.gui.color.Color",
    "io.deephaven.engine.util.ColorUtilImpl",
    "io.deephaven.engine.table.impl.verify.TableAssertions",
    "io.deephaven.time.calendar.Calendars",
    "io.deephaven.time.calendar.StaticCalendarMethods",
)

_METHOD = re.compile(r"^\s+public static (?:final )?(?:<.*?> )?\S.*?\s(\w+)\(.*\)(?: throws .*)?;$")
_FIELD = re.compile(r"^\s+public static final \S.*?\s(\w+);$")


def default_jars() -> Path:
    import deephaven_server

    return Path(deephaven_server.__file__).parent / "jars"


def read(jars: Path) -> dict:
    """Returns {"functions": {name: [classes]}, "constants": {name: class}}."""
    classpath = ":".join(sorted(str(p) for p in jars.glob("*.jar")))
    functions: dict[str, list[str]] = {}
    constants: dict[str, str] = {}
    for name in STATIC_IMPORTS:
        out = subprocess.run(
            ["javap", "-cp", classpath, "-public", name],
            check=True, capture_output=True, text=True,
        ).stdout
        short = name.rsplit(".", 1)[1]
        for line in out.splitlines():
            method = _METHOD.match(line)
            if method:
                classes = functions.setdefault(method.group(1), [])
                if short not in classes:
                    classes.append(short)
                continue
            field = _FIELD.match(line)
            if field:
                constants.setdefault(field.group(1), short)
    return {
        "functions": {k: functions[k] for k in sorted(functions)},
        "constants": {k: constants[k] for k in sorted(constants)},
    }


def write(path: Path, jars: Path | None = None) -> dict:
    data = read(jars or default_jars())
    lines = ["{", '  "source": "QueryLibraryImportsDefaults#statics, read from the 42.9 jars",',
             '  "functions": {']
    items = list(data["functions"].items())
    for index, (name, classes) in enumerate(items):
        comma = "," if index + 1 < len(items) else ""
        lines.append(f"    {json.dumps(name)}: {json.dumps(classes)}{comma}")
    lines.append("  },")
    lines.append('  "constants": {')
    items = list(data["constants"].items())
    for index, (name, owner) in enumerate(items):
        comma = "," if index + 1 < len(items) else ""
        lines.append(f"    {json.dumps(name)}: {json.dumps(owner)}{comma}")
    lines.append("  }")
    lines.append("}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return data
