"""Puts the test files a script scenario reads into the scenario itself.

    python3 crates/oxhaven-parquet/tests/data/embed.py oracle/scenarios/python/parquet_*.py

A script scenario runs on the Java reference and on Oxhaven, which share no file, so a
scenario that reads Parquet files carries them, base64 encoded, and writes them to a
temporary directory first. The scenario names its files, relative to this directory, on
the line

    # begin files: types.parquet flat/part0.parquet

and this script replaces everything from that line to `# end files` with the line itself
and a `FILES` dictionary of those files. Run it after a file here has changed, and record
the scenarios again.
"""
import base64
import os
import sys

D = os.path.dirname(os.path.abspath(__file__))
BEGIN = "# begin files:"
END = "# end files"
WIDTH = 88


def block(names):
    lines = ["FILES = {"]
    for name in names:
        with open(os.path.join(D, name), "rb") as f:
            text = base64.b64encode(f.read()).decode("ascii")
        lines.append(f'    "{name}": (')
        lines.extend(f'        "{text[at:at + WIDTH]}"' for at in range(0, len(text), WIDTH))
        lines.append("    ),")
    lines.append("}")
    return lines


for path in sys.argv[1:]:
    with open(path) as f:
        lines = f.read().split("\n")
    begin = next(i for i, line in enumerate(lines) if line.startswith(BEGIN))
    end = lines.index(END)
    names = lines[begin][len(BEGIN):].split()
    lines[begin + 1:end] = block(names)
    with open(path, "w") as f:
        f.write("\n".join(lines))
