"""Runs the embedded Java Deephaven server (deephaven-server from PyPI) until killed.

No Java counterpart: process wrapper used by start-java.sh.
"""

import argparse
import os
import signal
import sys
import threading


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=10000)
    parser.add_argument("--psk", default="oxhaven")
    parser.add_argument("--heap", default="2g")
    args = parser.parse_args()

    from deephaven_server import Server, start_jvm

    # Log to stderr as well as to the server's in-memory buffer (see logback-oracle.xml).
    start_jvm.DEFAULT_JVM_PROPERTIES["logback.configurationFile"] = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "logback-oracle.xml"
    )

    server = Server(
        host="127.0.0.1",
        port=args.port,
        jvm_args=[
            f"-Xmx{args.heap}",
            f"-Dauthentication.psk={args.psk}",
            # Keep the oracle light on a shared machine and independent of the core count.
            "-XX:ActiveProcessorCount=2",
        ],
    )
    server.start()
    print(f"ORACLE_READY port={server.port}", flush=True)

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
