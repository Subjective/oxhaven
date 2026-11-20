"""This module defines the BarrageSession wrapper class and provides a factory function to
create an instance of it for accessing resources on remote Deephaven servers.

Behavior: `deephaven.barrage` (the names, signatures, defaults and error texts). The session
is this server's own Barrage client. TLS (`use_tls`, `tls_root_certs`) is not supported yet:
asking for it fails with an error that says so.
"""

from __future__ import annotations

from typing import Optional

from . import _engine
from .dherror import DHError
from .table import Table


def _ticket_bytes(ticket):
    """A ticket as bytes: a string is its UTF-8 bytes.

    Behavior: `io.deephaven.qst.table.TableSpec#ticket(String)`, the overload jpy picks for
    a `str`
    """
    return ticket.encode("utf-8") if isinstance(ticket, str) else ticket


class BarrageSession:
    """A Deephaven Barrage session to a remote server."""

    def __init__(self, j_barrage_session, j_managed_channel=None):
        """Initializes a Barrage session.

        `j_barrage_session` is the session the server holds. The reference's session has a
        gRPC channel of its own, `j_managed_channel`, which `close` shuts down; here the
        channel belongs to the session and goes with it, so `j_managed_channel` is kept only
        as an attribute, and `j_session` is the session itself.
        """
        self.j_barrage_session = j_barrage_session
        self.j_session = j_barrage_session
        self.j_managed_channel = j_managed_channel

    def __enter__(self) -> BarrageSession:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Closes the barrage session. The tables it subscribed to fail on the next update
        cycle."""
        try:
            self.j_barrage_session.close()
        except Exception as e:
            raise DHError(e, "failed to close the barrage session.") from e

    def subscribe(self, ticket: bytes) -> Table:
        """Subscribes to a published remote table with the given ticket.

        Note, if the remote table is closed or its owner session is closed, the ticket
        becomes invalid. If the same ticket is subscribed to multiple times, multiple
        subscriptions will be created.

        Args:
            ticket (bytes): the bytes of the ticket

        Returns:
            a Table

        Raises:
            DHError
        """
        try:
            return Table(self.j_barrage_session.subscribe(_ticket_bytes(ticket)))
        except Exception as e:
            raise DHError(
                e, "failed to subscribe to the remote table with the provided ticket."
            ) from e

    def snapshot(self, ticket: bytes) -> Table:
        """Returns a snapshot of a published remote table with the given ticket.

        Note, if the remote table is closed or its owner session is closed, the ticket
        becomes invalid. If the same ticket is snapshot multiple times, multiple snapshots
        will be created.

        Args:
            ticket (bytes): the bytes of the ticket

        Returns:
            a Table

        Raises:
            DHError
        """
        try:
            return Table(self.j_barrage_session.snapshot(_ticket_bytes(ticket)))
        except Exception as e:
            raise DHError(
                e,
                "failed to take a snapshot of the remote table with the provided ticket.",
            ) from e


def barrage_session(
    host: str,
    port: int = 10000,
    auth_type: str = "Anonymous",
    auth_token: str = "",
    use_tls: bool = False,
    tls_root_certs: Optional[bytes] = None,
    extra_headers: Optional[dict[str, str]] = None,
) -> BarrageSession:
    """Returns a new Deephaven Barrage session to a remote server.

    Args:
        host (str): the host name or IP address of the Deephaven server; an IPv6 address in
            brackets.
        port (int): the port number that the remote Deephaven server is listening on,
            default is 10000.
        auth_type (str): the authentication type string, can be "Anonymous", "Basic", or any
            custom-built authenticator in the server, such as
            "io.deephaven.authentication.psk.PskAuthenticationHandler", default is
            "Anonymous".
        auth_token (str): the authentication token string. When auth_type is "Basic", it
            must be "user:password"; when auth_type is "Anonymous", it will be ignored; when
            auth_type is a custom-built authenticator, it must conform to the specific
            requirement of the authenticator
        use_tls (bool): if True, use a TLS connection, which is not supported yet. Defaults
            to False
        tls_root_certs (Optional[bytes]): PEM encoded root certificates to use for TLS
            connection, or None to use system defaults. If not None implies use a TLS
            connection and the use_tls argument should have been passed as True. Defaults to
            None
        extra_headers (Optional[dict[str, str]]): extra headers to set when configuring the
            gRPC channel. Defaults to None.

    Returns:
        a Deephaven Barrage session

    Raises:
        DHError
    """
    try:
        if tls_root_certs and not use_tls:
            raise DHError(message="tls_root_certs is provided but use_tls is False")

        target_uri = f"{host}:{port}"
        if use_tls:
            target_uri = f"dh://{target_uri}"
        else:
            target_uri = f"dh+plain://{target_uri}"

        if not auth_token:
            auth = auth_type
        else:
            auth = f"{auth_type} {auth_token}"

        headers = list(extra_headers.items()) if extra_headers else []
        j_barrage_session = _engine.native.barrage_session(
            target_uri, auth or None, headers, bytes(tls_root_certs) if tls_root_certs else None
        )
        return BarrageSession(j_barrage_session)
    except Exception as e:
        raise DHError(
            e, "failed to get a barrage session to the target remote Deephaven server."
        ) from e
