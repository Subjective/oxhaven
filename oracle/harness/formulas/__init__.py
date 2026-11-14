"""The formula corpus: formula and filter strings with their recorded Java outcome.

No Java counterpart: harness code. `oracle/README.md` ("Formula corpus") describes the format and
the commands; they are run as `python -m harness.formulas <command>` from the `oracle/` directory.

    corpus.py    the on-disk format: fixtures, cases, cell encoding
    tags.py      the lexer and the classifier (accepted subset, feature tags)
    engine.py    runs cases on a server through pydeephaven; reads error details from a Java log
    compare.py   compares an outcome with a recorded one, using `harness.rules`
    grid.py      the systematic grid of operators, casts, literals and filters
    extract.py   extracts formulas from the reference checkout (engine tests, Python tests, docs)
    builtins.py  lists the names the Java formula language imports by default (from the jars)
"""
