"""Runs a Deephaven 42.9 server (the `deephaven-server` wheel) as the remote of the remote-table
corpus, until it is killed: anonymous authentication, as the URI resolvers connect, and the
applications of the seed directory.

    remote_deephaven.py --app-dir oracle/corpus/remote/seed

Prints `REMOTE_READY port=<n>` once it serves, on a port it chose free.

No Java counterpart: process wrapper used by remote-services.sh, only to check that the
recordings made against an Oxhaven remote are those made against a Deephaven one
(oracle/README.md, "The remote-table corpus").
"""

import argparse
import os
import signal
import socket
import sys
import threading


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-dir", required=True)
    parser.add_argument("--heap", default="1g")
    args = parser.parse_args()

    from deephaven_server import Server

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = Server(host="127.0.0.1", port=port, jvm_args=[
        f"-Xmx{args.heap}",
        "-XX:ActiveProcessorCount=2",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        f"-Ddeephaven.application.dir={os.path.abspath(args.app_dir)}",
    ])
    server.start()
    print(f"REMOTE_READY port={server.port}", flush=True)

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
