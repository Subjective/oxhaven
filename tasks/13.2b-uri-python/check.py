"""Runs one Python script in a Deephaven 42.9 console and in an Oxhaven console, each
resolving tables of a third server (a Deephaven one) with `deephaven.uri` and
`deephaven.barrage`, and compares what the two consoles made: the tables, row for row, and
the outcome of every call that is expected to fail.

Three servers on free ports: R, the remote Deephaven server (anonymous, with two
applications); J, the Deephaven console; O, the Oxhaven console. J and O run the same script
(`SCRIPT`), then the check changes R's ticking table and waits until both consoles' copies
have the change, then closes the Barrage sessions of the script and sees their tables fail.
Last, J's JVM is asked what `new java.net.URI(scheme, userInfo, host, port, path, query,
fragment)` gives for the cases of `crates/oxhaven-uri/tests/components.rs`.

Run it with a Python that has `deephaven-server`, `pydeephaven` and `pyarrow` (the
environment of `oracle/requirements.txt`), from the repository root:

    PYO3_PYTHON=/usr/bin/python3.11 cargo build -p oxhaven-server --features python
    <python> tasks/13.2b-uri-python/check.py --oxhaven <target>/debug/oxhaven-server

It prints one line per comparison and exits 0 if every one matched or is a difference the
task file lists.

No Java counterpart: a manual check of task 13.2b.
"""

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

PSK = "oxhaven"
PSK_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"
HERE = os.path.dirname(os.path.abspath(__file__))

REMOTE_APP = """\
from deephaven import empty_table
from deephaven.appmode import get_app_state

app = get_app_state()
app["app_t"] = empty_table(10).update(["I = i", "S = i % 3 == 0 ? null : `app` + i"])
"""

REMOTE_SETUP = """\
from deephaven import empty_table, input_table, dtypes
static_t = empty_table(50).update([
    "I = i", "S = i % 4 == 0 ? null : `s` + i", "D = i / 4.0",
    "T = '2024-01-01T00:00:00Z' + i * SECOND", "N = i % 7 == 0 ? NULL_INT : i",
])
ticking_t = input_table(col_defs={"K": dtypes.int32, "V": dtypes.string}, key_cols="K")
"""

# Run in both consoles. @PORT@ is R's port, @DIR@ a directory with the files of the check.
SCRIPT = r"""
from deephaven import empty_table, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.uri import resolve
from deephaven.barrage import barrage_session, BarrageSession

base = "dh+plain://127.0.0.1:@PORT@"
r_static = resolve(base + "/scope/static_t")
r_ticking = resolve(base + "/scope/ticking_t")
r_app = resolve(base + "/app/remote_app/field/app_t")
r_host = resolve(base + "/field/by_host")
r_csv = resolve("csv://@DIR@/plain.csv")
r_csv_file = resolve("csv+file://@DIR@/plain.csv")
r_csv_quoted = resolve("csv://@DIR@/a%20b.csv")
r_csv_percent = resolve("csv://@DIR@/p%2541.csv")
r_parquet = resolve("parquet://@DIR@/t.parquet")

bs = barrage_session("127.0.0.1", @PORT@)
b_sub = bs.subscribe(b"s/ticking_t")
b_static = bs.subscribe(b"s/static_t")
b_snap = bs.snapshot(b"s/static_t")
b_app = bs.snapshot(b"a/remote_app/f/app_t")

local_t = empty_table(3).update("X = i")
local_value = 42

names, outcomes, messages, causes = [], [], [], []

def attempt(name, f):
    try:
        value = f()
        if isinstance(value, Table):
            outcome = "Table rows=%d refreshing=%s" % (value.size, value.is_refreshing)
        else:
            outcome = "value " + repr(value)
        message, cause = "", ""
    except Exception as e:
        outcome = type(e).__name__
        message = getattr(e, "_message", "")
        c = e.__cause__
        cause = str(c).strip().splitlines()[0] if c is not None and str(c).strip() else ""
        if c is not None and type(c).__name__ == "DHError":
            cause = "DHError " + getattr(c, "_message", "")
    names.append(name)
    outcomes.append(outcome)
    messages.append(message)
    causes.append(cause)

attempt("local table", lambda: resolve("dh:///scope/local_t"))
attempt("local value", lambda: resolve("dh:///scope/local_value"))
attempt("local missing", lambda: resolve("dh:///scope/missing"))
attempt("local app missing", lambda: resolve("dh:///app/none/field/f"))
attempt("7.23 host_name is local", lambda: resolve("dh://host_name/scope/local_t"))
attempt("7.23 huge port is local", lambda: resolve("dh://h:99999999999/scope/local_value"))
attempt("no resolver dh+plain local", lambda: resolve("dh+plain:///scope/x"))
attempt("no resolver field", lambda: resolve("dh:///field/f"))
attempt("no resolver case", lambda: resolve("DH:///scope/x"))
attempt("no resolver relative", lambda: resolve("scope/x"))
attempt("no resolver empty", lambda: resolve(""))
attempt("no resolver parquet host", lambda: resolve("parquet://host/x.parquet"))
attempt("syntax space", lambda: resolve("dh:///scope/a b"))
attempt("syntax scheme only", lambda: resolve("dh:"))
attempt("bad variable name", lambda: resolve("dh:///scope/a%2Fb"))
attempt("bad field name", lambda: resolve("dh:///app/a/field/b%20c"))
attempt("not a string", lambda: resolve(5))
attempt("proxied", lambda: resolve(base + "?uri=dh%2Bplain%3A%2F%2Fo%2Fscope%2Fx"))
attempt("custom", lambda: resolve(base + "?uri=https%3A%2F%2Fexample.com%2Fx"))
attempt("remote missing", lambda: resolve(base + "/scope/missing"))
attempt("remote app missing", lambda: resolve(base + "/app/none/field/f"))
attempt("remote tls", lambda: resolve("dh://127.0.0.1:@PORT@/scope/static_t"))
attempt("remote refused", lambda: resolve("dh+plain://127.0.0.1:1/scope/x"))
attempt("csv opaque", lambda: resolve("csv:x.csv"))
attempt("csv missing", lambda: resolve("csv://@DIR@/missing.csv"))
attempt("csv https", lambda: resolve("csv+https://example.invalid/x.csv"))
attempt("parquet missing", lambda: resolve("parquet://@DIR@/missing.parquet"))
attempt("session tls", lambda: barrage_session("127.0.0.1", @PORT@, use_tls=True))
attempt("session certs no tls", lambda: barrage_session("127.0.0.1", @PORT@, tls_root_certs=b"pem"))
attempt("session bad host", lambda: barrage_session("my_host", @PORT@))
attempt("session space", lambda: barrage_session("a b"))
attempt("session refused", lambda: barrage_session("127.0.0.1", 1))
attempt("subscribe missing", lambda: bs.subscribe(b"s/missing"))
attempt("snapshot missing", lambda: bs.snapshot(b"s/missing"))
attempt("subscribe not bytes", lambda: bs.subscribe("s/static_t"))

closed = barrage_session("127.0.0.1", @PORT@)
closed.close()
attempt("close twice", lambda: closed.close())
attempt("snapshot after close", lambda: closed.snapshot(b"s/static_t"))
with barrage_session("127.0.0.1", @PORT@) as scoped:
    b_scoped = scoped.snapshot(b"s/static_t")
attempt("snapshot after with", lambda: scoped.snapshot(b"s/static_t"))

outcomes_t = new_table([
    string_col("Name", names), string_col("Outcome", outcomes),
    string_col("Message", messages), string_col("Cause", causes),
])
"""

TABLES = [
    "r_static", "r_ticking", "r_app", "r_host", "r_csv", "r_csv_file", "r_csv_quoted", "r_csv_percent",
    "r_parquet", "b_sub", "b_static", "b_snap", "b_app", "b_scoped",
]

# Calls whose cause text differs, as tasks/13.2b-uri-python.md lists: the outcome and the
# DHError message must still be the same.
KNOWN = {
    "remote missing": "the remote status, written by the Barrage client, not Java's exception chain",
    "remote app missing": "the same",
    "subscribe missing": "the same",
    "snapshot missing": "the same",
    "remote tls": "TLS refused by name (owner question 7.24); Java tries TLS",
    "session tls": "the same",
    "remote refused": "the connection error of tonic, not of grpc-java",
    "session refused": "the same",
    "snapshot after close": "the closed session's own error, not grpc-java's shut channel",
    "snapshot after with": "the same",
    "csv missing": "the reader's I/O error; Java's is 'Caught exception' with the cause below it",
    "csv https": "reading a URL is not ported (oxhaven-csv); Java fetches it",
    "parquet missing": "the reader's I/O error, not Java's 'makeHandle encountered exception'",
}

TICK = """\
from deephaven import new_table
from deephaven.column import int_col, string_col
ticking_t.add(new_table([int_col("K", %r), string_col("V", %r)]))
"""

# The cases of crates/oxhaven-uri/tests/components.rs, asked of J's JVM.
COMPONENTS = r"""
import jpy
_URI = jpy.get_type("java.net.URI")
cases = [
    ("file", None, None, -1, "/tmp/x.csv", None, None),
    ("file", None, None, -1, "/tmp/a b.csv", None, None),
    ("file", None, None, -1, "/tmp/a%41.csv", None, None),
    ("file", None, None, -1, "/tmp/100%.csv", None, None),
    ("file", None, None, -1, "/a@b:c;d=e/f", None, None),
    ("file", None, None, -1, "/é/ü", None, None),
    ("file", None, None, -1, "/a?b#c[d]", None, None),
    ("file", None, None, -1, "/ x", None, None),
    ("http", "me you", "host", 8080, "/p", "a=b c&[d]", "f g"),
    ("http", None, "::1", -1, "", None, None),
    ("http", None, "[::1]", 1, "/", None, None),
    ("file", None, None, -1, None, None, None),
    ("file", None, None, -1, "x.csv", None, None),
]
built = []
for case in cases:
    try:
        built.append(str(_URI(*case).toString()))
    except Exception as e:
        text = str(e).strip().splitlines()[0]
        built.append("error: " + text.replace("java.net.URISyntaxException: ", "", 1))
from deephaven import new_table
from deephaven.column import string_col
components_t = new_table([string_col("Built", built)])
"""

COMPONENTS_EXPECTED = [
    "file:/tmp/x.csv",
    "file:/tmp/a%20b.csv",
    "file:/tmp/a%2541.csv",
    "file:/tmp/100%25.csv",
    "file:/a@b:c;d=e/f",
    "file:/é/ü",
    "file:/a%3Fb%23c%5Bd%5D",
    "file:/%C2%A0x",
    "http://me%20you@host:8080/p?a=b%20c&[d]#f%20g",
    "http://[::1]",
    "http://[::1]:1/",
    "error: Expected scheme-specific part at index 5: file:",
    "error: Relative path in absolute URI: file:x.csv",
]


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_for(process, log_path, ready, what):
    deadline = time.time() + 240
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("%s exited; see %s" % (what, log_path))
        if ready():
            return
        time.sleep(1)
    raise RuntimeError("%s did not start; see %s" % (what, log_path))


def start_deephaven(port, work, psk=None, app_dir=None):
    log_path = os.path.join(work, "deephaven-%d.log" % port)
    command = [sys.executable, os.path.join(HERE, "start_deephaven.py"), "--port", str(port)]
    if psk:
        command += ["--psk", psk]
    if app_dir:
        command += ["--app-dir", app_dir]
    process = subprocess.Popen(command, stdout=open(log_path, "w"), stderr=subprocess.STDOUT)

    def ready():
        with open(log_path) as text:
            return "SERVER_READY" in text.read()

    wait_for(process, log_path, ready, "Deephaven on port %d" % port)
    return process


def start_oxhaven(binary, port, work):
    log_path = os.path.join(work, "oxhaven-%d.log" % port)
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    process = subprocess.Popen(
        [binary, "--host", "127.0.0.1", "--port", str(port), "--auth", "psk", "--psk", PSK],
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        env=env,
    )

    def ready():
        with socket.socket() as probe:
            return probe.connect_ex(("127.0.0.1", port)) == 0

    wait_for(process, log_path, ready, "Oxhaven on port %d" % port)
    return process


def write_files(directory):
    with open(os.path.join(directory, "plain.csv"), "w") as f:
        f.write("A,B,C\n1,x,1.5\n2,,2.5\n3,z,\n")
    # the file `csv://.../a%20b.csv` names after decoding and quoting again
    with open(os.path.join(directory, "a%20b.csv"), "w") as f:
        f.write("Q\nquoted\n")
    # `csv://.../p%2541.csv` decodes to `p%41.csv`, whose `%` is quoted again
    with open(os.path.join(directory, "p%2541.csv"), "w") as f:
        f.write("R\npercent\n")
    table = pa.table({"P": pa.array([1, None, 3], pa.int64()), "Q": ["a", "b", None]})
    pq.write_table(table, os.path.join(directory, "t.parquet"))


def comparable(table):
    """Names, Deephaven types and values of an Arrow table."""
    fields = [(f.name, (f.metadata or {}).get(b"deephaven:type")) for f in table.schema]
    return fields, [column.to_pylist() for column in table.columns]


class Check:
    def __init__(self):
        self.failures = 0
        self.known = 0

    def report(self, name, problem, known=None):
        if problem is not None and known is not None:
            self.known += 1
            print("KNOWN %s: %s%s" % (name, known, problem), flush=True)
        elif problem is None:
            print("PASS  %s" % name, flush=True)
        else:
            self.failures += 1
            print("FAIL  %s: %s" % (name, problem), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--oxhaven", required=True, help="oxhaven-server built with the python feature")
    args = parser.parse_args()

    from pydeephaven import Session

    work = tempfile.mkdtemp(prefix="uri-python-check-")
    files = os.path.join(work, "files")
    os.makedirs(files)
    write_files(files)
    apps = os.path.join(work, "apps")
    os.makedirs(apps)
    with open(os.path.join(apps, "remote.py"), "w") as f:
        f.write(REMOTE_APP)
    with open(os.path.join(apps, "by_host.py"), "w") as f:
        f.write(REMOTE_APP.replace('app["app_t"]', 'app["by_host"]'))
    with open(os.path.join(apps, "remote.app"), "w") as f:
        f.write("type=script\nscriptType=python\nenabled=true\nid=remote_app\nname=Remote\nfile_0=remote.py\n")
    with open(os.path.join(apps, "by_host.app"), "w") as f:
        f.write("type=script\nscriptType=python\nenabled=true\nid=127.0.0.1\nname=By host\nfile_0=by_host.py\n")

    r_port, j_port, o_port = free_port(), free_port(), free_port()
    print("work directory %s; ports: remote %d, Deephaven console %d, Oxhaven console %d"
          % (work, r_port, j_port, o_port), flush=True)
    processes = []
    check = Check()
    try:
        processes.append(start_deephaven(r_port, work, app_dir=apps))
        processes.append(start_deephaven(j_port, work, psk=PSK))
        processes.append(start_oxhaven(args.oxhaven, o_port, work))
        remote = Session(host="127.0.0.1", port=r_port, auth_type="Anonymous")
        remote.run_script(REMOTE_SETUP)
        remote.run_script(TICK % ([1, 2], ["one", "two"]))
        consoles = {
            "deephaven": Session(host="127.0.0.1", port=j_port, auth_type=PSK_TYPE, auth_token=PSK),
            "oxhaven": Session(host="127.0.0.1", port=o_port, auth_type=PSK_TYPE, auth_token=PSK),
        }
        script = SCRIPT.replace("@PORT@", str(r_port)).replace("@DIR@", files)
        for name, session in consoles.items():
            started = time.time()
            session.run_script(script)
            print("      the script ran in %s in %.1f s" % (name, time.time() - started), flush=True)

        def fetched(name):
            return {console: comparable(session.open_table(name).to_arrow())
                    for console, session in consoles.items()}

        for name in TABLES:
            both = fetched(name)
            same = both["deephaven"] == both["oxhaven"]
            check.report("table %s" % name, None if same else "deephaven %r, oxhaven %r" % (both["deephaven"], both["oxhaven"]))
        source = comparable(remote.open_table("static_t").to_arrow())
        check.report("r_static is R's static_t", None if fetched("r_static")["deephaven"] == source else "differs")

        outcomes = {console: session.open_table("outcomes_t").to_arrow().to_pylist()
                    for console, session in consoles.items()}
        for java, ours in zip(outcomes["deephaven"], outcomes["oxhaven"]):
            same = java == ours
            known = KNOWN.get(java["Name"])
            if known is not None and (java["Outcome"], java["Message"]) != (ours["Outcome"], ours["Message"]):
                known = None
            check.report("outcome %s" % java["Name"], None if same else "\n        deephaven %r\n        oxhaven   %r" % (java, ours), known)
            print("        %s | %s | %s" % (java["Outcome"], java["Message"], java["Cause"]), flush=True)

        # R's ticking table changes; both consoles' copies follow
        remote.run_script(TICK % ([2, 3], ["TWO", "three"]))
        expected = comparable(remote.open_table("ticking_t").to_arrow())
        for name in ["r_ticking", "b_sub"]:
            deadline = time.time() + 15
            while True:
                both = fetched(name)
                if all(value == expected for value in both.values()) or time.time() > deadline:
                    break
                time.sleep(0.2)
            for console, value in both.items():
                check.report("%s follows R in %s" % (name, console), None if value == expected else repr(value))

        # closing the script's session fails the tables it subscribed to
        for console, session in consoles.items():
            session.run_script("bs.close()")
            time.sleep(2.5)
            session.run_script(
                "closed_t = new_table([string_col('F', [str(b_sub.is_failed), str(r_ticking.is_failed)])])"
            )
            flags = session.open_table("closed_t").to_arrow().column("F").to_pylist()
            check.report("after close in %s: b_sub failed, r_ticking not" % console,
                         None if flags == ["True", "False"] else repr(flags))

        consoles["deephaven"].run_script(COMPONENTS)
        built = consoles["deephaven"].open_table("components_t").to_arrow().column("Built").to_pylist()
        for java, ours in zip(built, COMPONENTS_EXPECTED):
            check.report("java.net.URI from components: %s" % java, None if java == ours else "components.rs expects %r" % ours)
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
    print("%d failed, %d known differences" % (check.failures, check.known))
    return 1 if check.failures else 0


if __name__ == "__main__":
    sys.exit(main())
