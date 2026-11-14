# Task ledger

One file per task, named `<id>-<slug>.md`. The orchestrator creates and assigns tasks; the agent
working a task updates only its `Status` and `Notes`.

Each file has these fields:

- **Status:** `open`, `in progress`, `in review`, `merged`, or `blocked`
- **Needs:** task ids that must be merged first
- **Owns:** the paths this task may change
- **Read:** the Java reference files to port from, and any other inputs
- **Scope:** what to build, and what is explicitly not part of it
- **Gate:** the exact command whose success closes the task
- **Notes:** `unported!` calls added, failing seeds, reasons for a block

Task files for M6 and M7 are written when their tracks start. `docs/SOURCE_MAP.md` shows where
every part of the reference lands and which task owns it.

Gate commands that call `oracle/run.py` name the operations to check; the oracle's README is the
authority on the exact command line.
