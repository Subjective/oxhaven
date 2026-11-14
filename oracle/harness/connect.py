"""Server URLs and sessions.

No Java counterpart: harness plumbing.

A server is named by one URL so that every command takes the same argument:

    dh://host:port                 anonymous authentication
    dh+psk://<key>@host:port       pre-shared key authentication
"""

from __future__ import annotations

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


def open_session(url: str) -> Session:
    server = parse_url(url)
    if server.psk is None:
        return Session(host=server.host, port=server.port)
    return Session(
        host=server.host,
        port=server.port,
        auth_type=PSK_AUTH_TYPE,
        auth_token=server.psk,
    )
