# Console checks

What a server with a Python console answers, on the Java reference and on Oxhaven.
`docs/CONSOLE.md` has the design and the list of differences.

| Path | Holds |
| --- | --- |
| `probe.py` | One fixed sequence of console calls; prints what the server answers |
| `java-42.9.txt` | What the Java reference answered |
| `oxhaven.txt` | What Oxhaven answers; the gate of the console tasks |
| `check.sh` | Runs the probe against a server and compares with a transcript |
| `expected-console.json` | Two commands for the console route of `oracle/ide/ide-test.js` |

## The probe

The probe covers `GetConsoleTypes`, `StartConsole`, `GetHeapInfo`, `ExecuteCommand` (tables
created, replaced and removed, values that are not tables, output, four kinds of error),
`FetchTable` of scope tickets, `BindTableToVariable`, `CancelCommand`, `CancelAutoComplete`,
`AutoCompleteStream`, and beside them one `SubscribeToLogs` and one `ListFields` stream. It
leaves variables in the query scope, so every run needs a server that was just started.

Oxhaven has a console only when built with the `python` feature:

```bash
cargo build -p oxhaven-server --features python
target/debug/oxhaven-server --host 127.0.0.1 --port 10100 --psk oxhaven &
oracle/console/check.sh 127.0.0.1:10100            # exit 0: the answers are oxhaven.txt's
```

Against Java:

```bash
oracle/bin/start-java.sh 10000
oracle/console/check.sh 127.0.0.1:10000 oracle/console/java-42.9.txt
oracle/bin/stop-java.sh 10000
```

`diff oracle/console/java-42.9.txt oracle/console/oxhaven.txt` is the list of differences
from Java. A task that removes a difference, or adds a case to the probe, records both
transcripts again and commits them with the change:

```bash
oracle/.venv/bin/python oracle/console/probe.py 127.0.0.1:10000 > oracle/console/java-42.9.txt
oracle/.venv/bin/python oracle/console/probe.py 127.0.0.1:10100 > oracle/console/oxhaven.txt
```

`oxhaven.txt` may only move towards `java-42.9.txt`: a change that makes their `diff` longer
needs a reason in the pull request.

## The IDE

The console route of the IDE acceptance test types Python into the IDE's console and reads
the grid the IDE paints. It needs a server that serves the web IDE (`--web-root`, see
`oracle/ide/README.md`):

```bash
node oracle/ide/ide-test.js --url dh+psk://oxhaven@127.0.0.1:10100 --routes console
node oracle/ide/ide-test.js --url dh+psk://oxhaven@127.0.0.1:10100 --routes console \
  --expected oracle/console/expected-console.json
```

The first types the commands of `oracle/ide/expected.json` (`new_table` with three columns,
then a ticking `time_table`); the second types `from deephaven import empty_table` and
`t = empty_table(5).update(["X = i"])` as one command. Both pass on Java and on Oxhaven.
Run the second on a server that was just started: on Oxhaven it failed when run after the
probe on the same server, and passed after a restart. The cause was not looked into.
