"""A TLS front for the remote of the remote-table scenarios: accepts TLS on 127.0.0.1, agrees
to `h2` by ALPN, and relays the plaintext to the remote's port. Neither Oxhaven nor the
Deephaven remote of the corpus is started with TLS, so a client's `dh://` connection is
terminated here.

    python tls_front.py --to <port> --cert <PEM chain> --key <PEM key>

Prints `TLS_FRONT_READY port=<port>` once it listens, then runs until it is stopped.
No Java counterpart: this is the harness.
"""

import argparse
import asyncio
import ssl


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, ssl.SSLError):
        pass
    finally:
        try:
            writer.close()
        except (ConnectionError, ssl.SSLError):
            pass


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", type=int, required=True)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.cert, args.key)
    context.set_alpn_protocols(["h2"])

    async def accepted(near_reader: asyncio.StreamReader,
                       near_writer: asyncio.StreamWriter) -> None:
        try:
            far_reader, far_writer = await asyncio.open_connection("127.0.0.1", args.to)
        except OSError:
            near_writer.close()
            return
        await asyncio.gather(_pipe(near_reader, far_writer), _pipe(far_reader, near_writer))

    # a handshake that fails (an untrusted certificate, a wrong host name) is the client's
    # outcome; the front logs it and keeps serving
    server = await asyncio.start_server(accepted, "127.0.0.1", 0, ssl=context)
    port = server.sockets[0].getsockname()[1]
    print(f"TLS_FRONT_READY port={port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
