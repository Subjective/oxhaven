"""Server URLs and sessions.

No Java counterpart: harness plumbing.

A server is named by one URL so that every command takes the same argument:

    dh://host:port                 anonymous authentication
    dh+psk://<key>@host:port       pre-shared key authentication
"""

from __future__ import annotations

import gc
from dataclasses import dataclass
from urllib.parse import urlparse

from pydeephaven import Session

PSK_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"


@dataclass(frozen=True)
class ServerUrl:
    host: str
    port: int
    psk: str | None

    def __str__(self) -> str:
        if self.psk is None:
            return f"dh://{self.host}:{self.port}"
        return f"dh+psk://{self.psk}@{self.host}:{self.port}"


def parse_url(url: str) -> ServerUrl:
    parsed = urlparse(url)
    if parsed.scheme not in ("dh", "dh+psk"):
        raise ValueError(f"unsupported server URL {url!r}: expected dh:// or dh+psk://")
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"server URL {url!r} needs a host and a port")
    if parsed.scheme == "dh+psk":
        if not parsed.username:
            raise ValueError(f"server URL {url!r} needs a key: dh+psk://<key>@host:port")
        return ServerUrl(parsed.hostname, parsed.port, parsed.username)
    return ServerUrl(parsed.hostname, parsed.port, None)


def collect_garbage() -> None:
    """Turns Python's automatic garbage collection off for the rest of the process and collects
    now, on the calling thread. Call it only where no client call is in progress on that thread.

    The client releases a table's ticket in `pydeephaven.table.Table.__del__`, with a blocking
    `Release` call (and a `GetConfigurationConstants` call before it, from `Session.is_alive`).
    A table that sits in a reference cycle is finalized by the collector, which runs on
    whichever thread allocates the object that crosses its threshold, in the middle of whatever
    that thread is doing. A failed client call makes such cycles: the gRPC future that is the
    exception refers to its traceback, whose frames refer to the future and to the tables the
    call was given. Inside `grpc._channel` the finalizer's call then waits forever, and with it
    every later call on the session's channel:

    - on the channel's spin thread, for a response that only that thread delivers;
    - on a spin thread that is starting, for the channel lock, which the thread that started
      it holds until the new thread has started;
    - on the calling thread while it holds the channel lock, for that same lock.

    The server has answered every call it was sent, so it looks idle. This happens against the
    Java server as well. With the collector run only from here, a table in a cycle is released
    on a thread that is outside gRPC, or not at all once its session is closed.
    """
    gc.disable()
    gc.collect()


def open_session(url: str) -> Session:
    """A session with the server at `url`. Garbage is collected first; see `collect_garbage`."""
    collect_garbage()
    server = parse_url(url)
    if server.psk is None:
        return Session(host=server.host, port=server.port)
    return Session(
        host=server.host,
        port=server.port,
        auth_type=PSK_AUTH_TYPE,
        auth_token=server.psk,
    )
