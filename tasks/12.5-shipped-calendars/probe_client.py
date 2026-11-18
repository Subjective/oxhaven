"""probe_client.py <host> <port> <psk|-> <script> <table> <out file>: runs a probe script
(probe_server.py or probe_figure.py) on a server and writes the lines of its table."""
import pathlib
import sys

from pydeephaven import Session

host, port, psk, script_name, table_name, out = sys.argv[1:7]
script = (pathlib.Path(__file__).parent / script_name).read_text()
if psk == "-":
    session = Session(host=host, port=int(port))
else:
    session = Session(
        host=host,
        port=int(port),
        auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
        auth_token=psk,
    )
session.run_script(script)
lines = session.open_table(table_name).to_arrow().column("L").to_pylist()
pathlib.Path(out).write_text("\n".join(lines) + "\n")
print(len(lines), "lines")
session.close()
