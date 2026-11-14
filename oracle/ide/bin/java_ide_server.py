"""Runs the embedded Java Deephaven server with the tables the IDE acceptance test expects.

No Java counterpart: process wrapper used by start-java-ide.sh. It differs from
oracle/bin/java_server.py only in that tables exist on the server before any client connects,
which is what routes (b) and (c) of the IDE test need.

Modes (--mode):
  scope      Python console. `ide_static` and `ide_ticking` are variables of the query scope
             (the globals of this process), as if a startup script had defined them.
  app        Python console, and application mode: oracle/ide/app.d defines the same two tables
             as fields of the application `ide_app`. Nothing is put in the query scope here.
  disabled   As `scope`, with -Ddeephaven.console.disable=true: the console service refuses
             StartConsole and friends, the tables stay.

The embedded server of the wheel binds only the Python script session, so a server with no
console type at all (-Ddeephaven.console.type=none) cannot be started this way: it stops with
"Console type not found: none". ide-test.js --deny and --empty stand in for it.
"""

import argparse
import os
import signal
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
IDE = os.path.dirname(HERE)
ORACLE = os.path.dirname(IDE)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=10500)
    parser.add_argument("--psk", default="oxhaven")
    parser.add_argument("--heap", default="1g")
    parser.add_argument("--mode", choices=["scope", "app", "disabled"], default="scope")
    parser.add_argument("--no-tables", action="store_true",
                        help="define no tables: the test must then fail")
    parser.add_argument("--jvm-arg", action="append", default=[])
    args = parser.parse_args()

    from deephaven_server import Server, start_jvm

    start_jvm.DEFAULT_JVM_PROPERTIES["logback.configurationFile"] = os.path.join(
        ORACLE, "bin", "logback-oracle.xml"
    )

    jvm_args = [
        f"-Xmx{args.heap}",
        f"-Dauthentication.psk={args.psk}",
        "-XX:ActiveProcessorCount=2",
    ]
    if args.mode == "app" and not args.no_tables:
        jvm_args.append(f"-Ddeephaven.application.dir={os.path.join(IDE, 'app.d')}")
    if args.mode == "disabled":
        jvm_args.append("-Ddeephaven.console.disable=true")
    jvm_args.extend(args.jvm_arg)

    server = Server(host="127.0.0.1", port=args.port, jvm_args=jvm_args)
    server.start()

    if not args.no_tables and args.mode in ("scope", "disabled"):
        sys.path.insert(0, os.path.join(IDE, "app.d"))
        import ide_tables

        static, ticking = ide_tables.make()
        import __main__

        __main__.ide_static = static
        __main__.ide_ticking = ticking

    print(f"ORACLE_READY port={server.port} mode={args.mode}", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    # The JVM keeps non-daemon threads alive; leave without waiting for them.
    os._exit(code)
