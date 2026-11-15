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

The M7 task files (`7.*`) mark their areas and each needs a design before it is worked. `docs/SOURCE_MAP.md` shows where
every part of the reference lands and which task owns it.

## Gates that use the oracle

Gate commands call the oracle harness as `oracle/bin/harness`, from the repository root.
`oracle/README.md` describes its commands, its setup, and the operation names it knows. A gate
uses two servers, named by two environment variables:

- `$JAVA_URL`: the Java reference server. Start it with `oracle/bin/start-java.sh 10000` and use
  `export JAVA_URL=dh+psk://oxhaven@127.0.0.1:10000`. Stop it with `oracle/bin/stop-java.sh 10000`.
  Only `fuzz` needs it running; `replay` compares with the results recorded in `oracle/corpus`.
- `$OXHAVEN_URL`: the Oxhaven server under test. Task 2.1 builds the server binary in
  `crates/oxhaven-server`; until it is merged there is nothing to start, and its notes will give
  the command. The URL has the form `dh://127.0.0.1:<port>` for anonymous authentication and
  `dh+psk://<key>@127.0.0.1:<port>` for a pre-shared key, for example
  `export OXHAVEN_URL=dh+psk://oxhaven@127.0.0.1:10100`. Use a port of your own: other agents
  share the machine.

The three kinds of oracle gate:

- `oracle/bin/harness client-smoke <area> --url "$OXHAVEN_URL"` runs what a real client does in
  one area (`session`, `tables`, `flight`, `barrage`, `inputtable`).
- `oracle/bin/harness replay --url "$OXHAVEN_URL" --ops a,b --within a,b,input_table` compares
  with the recorded Java results on the scenarios that use at least one operation of `--ops` and
  nothing outside `--within`, so a task gates on its own operations. Without `--within` it runs
  every scenario that touches the operations, including those that need other operators.
- `oracle/bin/harness fuzz --a "$JAVA_URL" --b "$OXHAVEN_URL" --ops a,b --seeds N` compares the
  two servers on `N` generated scenarios that use only those operations and input tables, and
  shrinks the first difference to a scenario file. Record the failing seed in the task's Notes.

Where a task names an operation or a command the harness does not have yet, the note under its
gate says so. The harness then stops with exit code 2 and names what it lacks; it never skips
silently.
