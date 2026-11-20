"""Checks Oxhaven's Barrage client against a Deephaven 42.9 server.

Starts the reference server of `oracle/bin/java_server.py` on a free port, makes tables on
it, and has Oxhaven's client (`crates/oxhaven-server/examples/barrage_client_check.rs`)
snapshot and subscribe to each. What the client holds is compared, row for row, with what
`pydeephaven` reads of the same table.

Run it with a Python that has `deephaven-server`, `pydeephaven` and `pyarrow` (the
environment of `oracle/requirements.txt`), from the repository root:

    cargo build -p oxhaven-server --example barrage_client_check
    <python> tasks/13.1-barrage-client/check.py --client <target>/debug/examples/barrage_client_check

It prints one line per check and exits 0 if every check passed.

No Java counterpart: a manual check of task 13.1.
"""

import argparse
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

import pyarrow as pa

PSK = "oxhaven"
PSK_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

SETUP = r"""
from deephaven import empty_table, time_table, input_table, new_table, dtypes, agg
from deephaven.column import int_col, string_col, double_col
from deephaven.stream.table_publisher import table_publisher
from deephaven.stream import blink_to_append_only

every_type = empty_table(1000).update([
    "Bo = i % 3 == 0 ? null : (Boolean)(i % 2 == 0)",
    "By = i % 7 == 0 ? NULL_BYTE : (byte)(i % 100 - 50)",
    "Ch = i % 5 == 0 ? NULL_CHAR : (char)('a' + i % 26)",
    "Sh = i % 11 == 0 ? NULL_SHORT : (short)(i * 3 - 1000)",
    "In = i % 13 == 0 ? NULL_INT : i * 7 - 2000",
    "Lo = i % 17 == 0 ? NULL_LONG : (long)i * 1000000007L - 5000000000L",
    "Fl = i % 19 == 0 ? NULL_FLOAT : (float)(i / 3.0 - 100)",
    "Do = i % 23 == 0 ? NULL_DOUBLE : i / 7.0 - 50",
    "St = i % 4 == 0 ? null : `s` + i",
    "Ti = i % 6 == 0 ? null : '2024-01-01T00:00:00Z' + i * 1000000007L",
    "Date = i % 6 == 1 ? null : java.time.LocalDate.ofEpochDay(19000 - 2 * i)",
    "Time = i % 6 == 2 ? null : java.time.LocalTime.ofNanoOfDay(i * 1000000007L)",
    "Dur = i % 6 == 3 ? null : java.time.Duration.ofNanos(i * 1000000007L - 5000)",
    "Per = i % 6 == 4 ? null : java.time.Period.of(i % 3, i % 5 - 1, i % 4)",
    "Zdt = i % 6 == 5 ? null : java.time.ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(1700000000L + i, i), java.time.ZoneId.of(`America/New_York`))",
    "Dec = i % 8 == 0 ? null : java.math.BigDecimal.valueOf(i * 12345L - 500000, i % 5)",
    "Big = i % 8 == 1 ? null : java.math.BigInteger.valueOf(i - 500).pow(7)",
    "Bytes = i % 9 == 0 ? null : new byte[]{(byte)i, (byte)-i}",
    "Ints = i % 9 == 1 ? null : new int[]{i, NULL_INT, i * 2}",
    "Longs = i % 9 == 2 ? null : new long[]{(long)i * 1099511627776L}",
    "Doubles = i % 9 == 3 ? null : new double[]{i / 2.0, NULL_DOUBLE}",
    "Strings = i % 9 == 4 ? null : new String[]{`a` + i, null, ``}",
    "Instants = i % 9 == 5 ? null : new java.time.Instant[]{java.time.Instant.ofEpochSecond(i), null}",
])

vectors = empty_table(300).update([
    "G = i % 20", "I = i % 4 == 0 ? NULL_INT : i", "D = i / 2.0", "S = i % 5 == 0 ? null : `s` + i",
    "L = (long)i", "B = (byte)i", "H = (short)i", "F = (float)i", "C = (char)('a' + i % 26)",
]).group_by("G")

big = empty_table(700000).update(["I = i", "S = `s` + (i % 1000)", "D = i % 97 == 0 ? NULL_DOUBLE : i / 3.0"])

src = input_table(col_defs={"K": dtypes.int32, "S": dtypes.string, "V": dtypes.double}, key_cols="K")
sorted_t = src.sort_descending(["V", "K"])
agged = src.update("G = K % 5").agg_by([agg.sum_("Sum = V"), agg.count_("N"), agg.last("LastS = S")], "G")
filtered = src.where("K % 2 == 0").update("W = V * 2")
grouped = src.update("G = K % 3").group_by("G")
log_t = input_table(col_defs={"K": dtypes.int32, "S": dtypes.string})

blink_t, publisher = table_publisher("blink_t", {"K": dtypes.int32, "S": dtypes.string})
kept_t = blink_to_append_only(blink_t)

clock = time_table("PT0.1S").update(["X = ii", "Y = X % 7"]).drop_columns("Timestamp")

unreadable = empty_table(3).update([
    "Ldt = java.time.LocalDateTime.of(2024, 1, 1, 0, 0).plusSeconds(i)",
])
"""


def step_script(step):
    """What changes the input tables in step `step`."""
    keys = list(range(step * 4, step * 4 + 6))
    changed = [key for key in range(0, step * 4, 3)]
    removed = [key for key in range(1, step * 4, 5)]
    lines = [
        "src.add(new_table([int_col('K', %r), string_col('S', %r), double_col('V', %r)]))"
        % (keys, ["s%d" % key for key in keys], [((key * 37) % 11) + key / 100.0 for key in keys]),
    ]
    if changed:
        lines.append(
            "src.add(new_table([int_col('K', %r), string_col('S', %r), double_col('V', %r)]))"
            % (changed, ["m%d_%d" % (key, step) for key in changed], [float((key * step) % 13) for key in changed])
        )
    if removed:
        lines.append("src.delete(new_table([int_col('K', %r)]))" % (removed,))
    lines.append(
        "log_t.add(new_table([int_col('K', %r), string_col('S', %r)]))" % (keys[:2], ["log%d" % step, None])
    )
    return "\n".join(lines)


def text_of_string(value):
    return "'" + value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def cells(array, dh_type):
    """The texts of the cells of a pyarrow array, as barrage_client_check.rs writes them."""
    kind = array.type
    valid = array.is_valid().to_pylist()

    def of(values, text):
        return [text(value) if ok else "null" for value, ok in zip(values, valid)]

    if pa.types.is_boolean(kind):
        return of(array.to_pylist(), lambda value: "true" if value else "false")
    if pa.types.is_integer(kind):
        return of(array.to_pylist(), str)
    if pa.types.is_float32(kind):
        return of(
            array.to_pylist(),
            lambda value: "nan" if value != value else "f" + struct.pack(">f", value).hex(),
        )
    if pa.types.is_float64(kind):
        return of(
            array.to_pylist(),
            lambda value: "nan" if value != value else "d" + struct.pack(">d", value).hex(),
        )
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return of(array.to_pylist(), text_of_string)
    if pa.types.is_timestamp(kind):
        nanos = array.cast(pa.timestamp("ns", kind.tz)).cast(pa.int64()).to_pylist()
        if dh_type == "java.time.ZonedDateTime":
            # Java's client reads the instant; the zone that arrives is the one the
            # field names
            return of(nanos, lambda value: "{%d|'%s}" % (value, kind.tz))
        return of(nanos, str)
    if pa.types.is_date32(kind):
        return of(array.cast(pa.int32()).to_pylist(), str)
    if pa.types.is_date64(kind):
        return of(array.cast(pa.int64()).to_pylist(), lambda ms: str(int(ms / 86400000)))
    if pa.types.is_time64(kind) or pa.types.is_duration(kind):
        return of(array.cast(pa.int64()).to_pylist(), str)
    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind):
        if dh_type == "byte[]":
            # a byte[] arrives as its bytes, with no validity for the elements: the byte
            # -128 is Deephaven's null byte, as a null element of an int[] is its null int
            return of(
                array.to_pylist(),
                lambda value: "[" + ",".join("null" if b == 128 else str(b - 256 if b > 127 else b) for b in value) + "]",
            )
        return of(array.to_pylist(), lambda value: value.hex())
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        inner = cells(array.flatten(), None)
        offsets = array.offsets.to_pylist()
        out = []
        for row, ok in enumerate(valid):
            if not ok:
                out.append("null")
                continue
            # flatten() drops what lies before the first offset
            first, last = offsets[row] - offsets[0], offsets[row + 1] - offsets[0]
            out.append("[" + ",".join(inner[first:last]) + "]")
        return out
    return ["<%s>" % kind] * len(array)


def rows_of(table, periods=()):
    """The rows of a pyarrow table as the lines `dump` prints. The columns named in
    `periods` hold the total months of a Period."""
    table = table.combine_chunks()
    columns = []
    for field, column in zip(table.schema, table.columns):
        dh_type = (field.metadata or {}).get(b"deephaven:type", b"").decode()
        array = column.chunk(0) if column.num_chunks else pa.array([], field.type)
        texts = cells(array, dh_type)
        if field.name in periods:
            texts = [text if text == "null" else "{0|%s|0}" % text for text in texts]
        columns.append(texts)
    return ["\t".join(row) for row in zip(*columns)] if columns else []


def digest(lines):
    value = 0xCBF29CE484222325
    for line in lines:
        for byte in line.encode() + b"\n":
            value ^= byte
            value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return "%d %016x" % (len(lines), value)


class Client:
    """Oxhaven's client behind its line protocol."""

    def __init__(self, path, port):
        self.process = subprocess.Popen(
            [path, "127.0.0.1", str(port), PSK],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        line = self.process.stdout.readline().strip()
        if line != "ready":
            raise RuntimeError("the client did not start: %r" % line)

    def ask(self, command):
        """The lines the client answers `command` with; raises with its error."""
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        lines = []
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError("the client ended during: " + command)
            line = line.rstrip("\n")
            if line == "end":
                return lines
            if line.startswith("error "):
                raise RuntimeError(line[len("error "):])
            lines.append(line[1:])

    def stop(self):
        self.process.stdin.close()
        self.process.wait(timeout=20)


class Check:
    def __init__(self, session, client):
        self.session = session
        self.client = client
        self.failed = 0
        self.passed = 0

    def report(self, name, problem):
        if problem is None:
            self.passed += 1
            print("ok    %s" % name, flush=True)
        else:
            self.failed += 1
            print("FAIL  %s: %s" % (name, problem), flush=True)

    def java_rows(self, variable):
        """The rows of the server's table as pydeephaven reads them.

        pyarrow cannot hold the month interval a Period arrives as, so a Period column
        is read as the number Java writes for it: its total months, an int.
        """
        table = self.session.open_table(variable)
        periods = [
            field.name
            for field in table.schema
            if (field.metadata or {}).get(b"deephaven:type") == b"java.time.Period"
        ]
        if periods:
            # made by a script: a formula sent over gRPC may not call a method
            formulas = [
                "%s = %s == null ? NULL_INT : (int) %s.toTotalMonths()" % (name, name, name) for name in periods
            ]
            self.session.run_script("check_view = %s.update(%r)" % (variable, formulas))
            table = self.session.open_table("check_view")
        return rows_of(table.to_arrow(), periods)

    def difference(self, expected, actual):
        if expected == actual:
            return None
        if len(expected) != len(actual):
            return "%d rows expected, %d found" % (len(expected), len(actual))
        for index, (a, b) in enumerate(zip(expected, actual)):
            if a != b:
                for column, (x, y) in enumerate(zip(a.split("\t"), b.split("\t"))):
                    if x != y:
                        return "row %d column %d: expected %s, found %s" % (index, column, x, y)
        return "differ"

    def same(self, name, table_id, variable, wait=20.0):
        """Waits until the client's table has the rows pydeephaven reads."""
        deadline = time.time() + wait
        problem = "never compared"
        while True:
            try:
                expected = self.java_rows(variable)
                actual = self.client.ask("dump " + table_id)
                problem = self.difference(expected, actual)
            except RuntimeError as error:
                problem = str(error)
                break
            if problem is None or time.time() > deadline:
                break
            time.sleep(0.1)
        self.report(name, problem)

    def command(self, name, command):
        try:
            self.client.ask(command)
            self.report(name, None)
            return True
        except RuntimeError as error:
            self.report(name, str(error))
            return False


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def start_server(port, log_path):
    log = open(log_path, "w")
    process = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "oracle", "bin", "java_server.py"), "--port", str(port), "--psk", PSK],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 180
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("the Java server exited; see " + log_path)
        with open(log_path) as text:
            if "ORACLE_READY" in text.read():
                return process
        time.sleep(1)
    process.terminate()
    raise RuntimeError("the Java server did not start; see " + log_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True, help="the built barrage_client_check example")
    parser.add_argument("--port", type=int, help="a server that is already running, with the key 'oxhaven'")
    parser.add_argument("--steps", type=int, default=10)
    args = parser.parse_args()

    from pydeephaven import Session

    server = None
    port = args.port
    if port is None:
        port = free_port()
        log_path = os.path.join(tempfile.mkdtemp(prefix="barrage-client-check-"), "java.log")
        print("starting Deephaven on port %d (log: %s)" % (port, log_path), flush=True)
        server = start_server(port, log_path)
    client = None
    try:
        session = Session(host="127.0.0.1", port=port, auth_type=PSK_TYPE, auth_token=PSK)
        session.run_script(SETUP)
        client = Client(args.client, port)
        check = Check(session, client)

        # static tables of every column type, each way the client can ask
        for variable in ["every_type", "vectors"]:
            if check.command("snapshot %s" % variable, "snapshot snap_%s %s" % (variable, variable)):
                check.same("snapshot %s rows" % variable, "snap_" + variable, variable)
            for flags in ["", "nulls", "list", "nulls list"]:
                table_id = "sub_%s_%s" % (variable, flags.replace(" ", "_"))
                label = "subscribe %s [%s]" % (variable, flags)
                if check.command(label, "subscribe %s %s %s" % (table_id, variable, flags)):
                    check.same(label + " rows", table_id, variable)
                    client.ask("drop " + table_id)
        print("\n".join("      " + line for line in client.ask("describe snap_every_type")), flush=True)
        print("\n".join("      " + line for line in client.ask("describe snap_vectors")), flush=True)

        # a table of many snapshot messages
        expected = digest(check.java_rows("big"))
        for command in ["snapshot big_snap big", "subscribe big_sub big nulls", "subscribe big_list big list"]:
            table_id = command.split()[1]
            if check.command(command, command):
                found = client.ask("digest " + table_id)[0]
                check.report("%s rows and hash" % table_id, None if found == expected else "%s, expected %s" % (found, expected))
                client.ask("drop " + table_id)

        # ticking tables: a keyed input table and what the engine derives from it
        ticking = {
            "src": "nulls",
            "sorted_t": "",
            "agged": "nulls",
            "filtered": "list",
            "grouped": "",
            "log_t": "nulls",
        }
        for variable, flags in ticking.items():
            check.command("subscribe %s [%s]" % (variable, flags), "subscribe %s %s %s" % (variable, variable, flags))
        print("\n".join("      " + line for line in client.ask("describe log_t")), flush=True)
        for step in range(args.steps):
            session.run_script(step_script(step))
            for variable in ticking:
                check.same("step %d %s" % (step, variable), variable, variable)
        check.command("snapshot of a ticking table", "snapshot sorted_snap sorted_t")
        check.same("snapshot of a ticking table rows", "sorted_snap", "sorted_t")

        # a blink table: every row it shows after the subscription, once
        if check.command("subscribe blink_t", "subscribe blink blink_t nulls"):
            print("\n".join("      " + line for line in client.ask("describe blink")), flush=True)
            check.command("keep the rows of blink_t", "keep kept blink")
            for batch in range(5):
                keys = list(range(batch * 3, batch * 3 + 3))
                session.run_script(
                    "publisher.add(new_table([int_col('K', %r), string_col('S', %r)]))"
                    % (keys, ["b%d" % key for key in keys])
                )
                time.sleep(0.3 if batch % 2 else 1.2)
                # a cycle of the client between batches, so that rows of two batches are
                # not always shown together
                client.ask("dump blink")
            check.same("rows of blink_t, each once", "kept", "kept_t")
            time.sleep(1.5)
            client.ask("dump blink")
            rows = client.ask("dump blink")
            check.report("blink_t is empty at rest", None if rows == [] else "%d rows" % len(rows))

        # a table that ticks on its own: what the client holds is a prefix of what the
        # server holds a moment later
        if check.command("subscribe clock", "subscribe clock clock"):
            print("\n".join("      " + line for line in client.ask("describe clock")), flush=True)
            sizes = []
            problem = None
            for _ in range(6):
                time.sleep(0.5)
                held = client.ask("dump clock")
                later = check.java_rows("clock")
                sizes.append(len(held))
                if held != later[: len(held)]:
                    problem = "the client's %d rows are not the server's first rows" % len(held)
            if problem is None and not sizes[-1] > sizes[0] > 0:
                problem = "the table did not grow: %r" % sizes
            check.report("clock grows with the server's rows %r" % sizes, problem)

        # a column the Java client cannot read either
        try:
            client.ask("subscribe bad unreadable")
            check.report("a LocalDateTime column is refused", "it was accepted")
        except RuntimeError as error:
            print("      refused with: %s" % error, flush=True)
            check.report("a LocalDateTime column is refused", None if "Ldt" in str(error) else str(error))

        check.command("close", "close")
        print("%d passed, %d failed" % (check.passed, check.failed), flush=True)
        return 1 if check.failed else 0
    finally:
        if client is not None:
            try:
                client.stop()
            except Exception:
                client.process.kill()
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()


if __name__ == "__main__":
    sys.exit(main())
