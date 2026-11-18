"""probe_px.py <port> <out>: makes plotly-express figures with calendar= on a server and
writes the first message each sends (its JSON), or the error."""
import json
import pathlib
import sys

from pydeephaven import Session

session = Session(host="127.0.0.1", port=int(sys.argv[1]),
                  auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler", auth_token="oxhaven")
SETUP = """
import deephaven.plot.express as dx
from deephaven import empty_table
_src = empty_table(10).update(["T = '2024-07-01T13:00:00Z' + i * 'PT24H'", "Y = i * 2"])
"""
session.run_script(SETUP)
out = []
for name, call in [
    ("true", "dx.line(_src, x='T', y='Y', calendar=True)"),
    ("utc", "dx.line(_src, x='T', y='Y', calendar='UTC')"),
    ("nyse", "dx.line(_src, x='T', y='Y', calendar='USNYSE_EXAMPLE')"),
    ("bank", "dx.line(_src, x='T', y='Y', calendar='USBANK_EXAMPLE')"),
]:
    try:
        session.run_script(f"px_{name} = {call}")
        client = session.plugin_client(session.exportable_objects[f"px_{name}"])
        payload, _refs = next(client.resp_stream)
        figure = json.loads(payload.decode())
        calendar = figure["figure"]["deephaven"].pop("calendar", None)
        out.append(f"{name}|calendar|{json.dumps(calendar, sort_keys=True)}")
        out.append(f"{name}|rest|{json.dumps(figure, sort_keys=True)}")
        client.close()
    except Exception as error:  # noqa: BLE001
        out.append(f"{name}|error|{str(error)[:300]}")
pathlib.Path(sys.argv[2]).write_text("\n".join(out) + "\n")
session.close()
