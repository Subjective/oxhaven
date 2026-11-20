"""Runs a Deephaven 42.9 server (the `deephaven-server` wheel) until it is killed: with a
pre-shared key, or anonymous, and optionally with an application directory.

No Java counterpart: a process wrapper for `check.py`.
"""

import argparse
import os
import signal
import sys
import threading


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--psk", help="the pre-shared key; anonymous without one")
    parser.add_argument("--app-dir")
    args = parser.parse_args()

    from deephaven_server import Server

    jvm_args = ["-Xmx1g", "-XX:ActiveProcessorCount=2"]
    if args.psk:
        jvm_args.append(f"-Dauthentication.psk={args.psk}")
    else:
        jvm_args.append("-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler")
    if args.app_dir:
        jvm_args.append(f"-Ddeephaven.application.dir={args.app_dir}")
    server = Server(host="127.0.0.1", port=args.port, jvm_args=jvm_args)
    server.start()
    print(f"SERVER_READY port={server.port}", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    # the JVM keeps non-daemon threads alive; leave without waiting for them
    os._exit(code)
