# Serving over TLS

The server serves its one port over TLS when it is given a certificate and its private key:
gRPC, Flight and Barrage for native clients, and the web IDE, the embeddable widgets, the JS
API and JS plugins for browsers. Plain HTTP stays the default. The options mean what
Deephaven 42.9's properties mean (`MainHelper#parseSSLConfig`); this server has no property
system, so they are command line options.

| Option | Deephaven property | Holds |
| --- | --- | --- |
| `--tls-cert <file>` | `ssl.identity.type=privatekey`, `ssl.identity.certChainPath` | PEM certificates: the server's first, then those that chain it to a root |
| `--tls-key <file>` | `ssl.identity.privateKeyPath` | the PEM private key of the first certificate (PKCS#8, or the traditional RSA and EC forms), without a password |
| `--tls-trust <file>` | `ssl.trust.type=certs`, `ssl.trust.path` | PEM certificates a client's certificate must chain to |
| `--tls-client-auth <mode>` | `ssl.clientAuthentication` | `NONE` (the default), `WANTED` or `NEEDED` |
| `--port <port>` | `http.port` | the port; the default is 443 with `--tls-cert`, 10000 without (`JettyConfig#port`) |

```bash
cargo build -p oxhaven-server --features tls      # or --features python, which turns tls on
oxhaven-server --tls-cert server-chain.pem --tls-key server-key.pem --port 8443 \
  --web-root /path/to/web-root
```

The server refuses to start, before it does anything else, when one of `--tls-cert` and
`--tls-key` is given without the other (Java: ``Must specify `ssl.identity.certChainPath` and
`ssl.identity.privateKeyPath` ``), when `--tls-trust` or `--tls-client-auth` is given without
them, when a client certificate is asked for without `--tls-trust` (Java: `Trust material
must be present when requesting mutual TLS`), when a file cannot be read or holds no PEM
certificate or key, and when the key is not the first certificate's. A server built without
the `tls` feature refuses `--tls-cert` by name.

## What a connection gets

Observed against Deephaven 42.9 started with `ssl.identity.*` (the Jetty connector of
`JettyBackedGrpcServer#createConnector`) and matched:

- **Protocols and ciphers**: TLS 1.3 and 1.2 only, with Deephaven's `CiphersIntermediate`
  list: the TLS 1.3 suites and, for 1.2, ECDHE with AES-GCM or ChaCha20-Poly1305. No DHE, no
  CBC, nothing older than 1.2.
- **ALPN**: the server's order, `h2` before `http/1.1`. A client that offers `h2` gets
  HTTP/2 whatever its own order; one that offers only `http/1.1`, or uses no ALPN, gets
  HTTP/1.1; one that offers only protocols the server does not speak completes the
  handshake and is then disconnected.
- **The host check** (Jetty's `SecureRequestCustomizer` with `https.sniHostCheck`, on by
  default): every request must name, in its `Host` header or HTTP/2 authority, a host the
  server's certificate names, whatever was sent as SNI; otherwise it is answered `400` with
  Jetty's page "Invalid SNI", before anything else looks at it, a gRPC call included. A
  request without a host (HTTP/1.0) is checked against the address it was sent to. The names
  are the certificate's DNS names and common names, case-insensitive; `*.example.test`
  matches `example.test` and one label more (`a.example.test`, not `a.b.example.test`); an
  address matches the certificate's IP entries when it is written in a form Java reads as
  an address (`127.0.0.01` is `127.0.0.1`; `0127.0.0.1`, `127.1` and `[::ffff:127.0.0.1]` are
  no address and match nothing; `[0:0:0:0:0:ffff:7f00:1]` is `127.0.0.1`).
- **Websockets**: none over TLS, as Java's default `http.websockets` is when TLS is on
  (`JettyConfig#websocketsOrDefault`); the handshake is a request for a file and is answered
  `404`. The JS API on an `https://` page does not use them: it makes its calls as gRPC-web
  over HTTP/2 and its bidirectional ones as `BrowserFlightService/OpenDoExchange` and
  `NextDoExchange` (and `ConsoleService/OpenAutoCompleteStream` and
  `NextAutoCompleteStream`).
- **Client certificates**: with `NEEDED`, a client without a certificate that chains to the
  `--tls-trust` roots is refused in the handshake; with `WANTED`, a client without one is
  served and one with a certificate the roots do not sign is refused; the server names the
  roots' subjects as the authorities it accepts. `--tls-trust` with `NONE` asks for nothing.
  A certificate is only checked: no authentication handler reads it.
- A handshake has 30 seconds, Jetty's idle timeout.

Differences that remain: the TLS alert a refused client certificate gets (OpenSSL sends
`unknown_ca`, Java's JSSE `certificate_unknown`), the signature algorithms offered, the order
of cipher preference, and that a 400 page for an HTTP/1.0 request names version 1.0 in its
status line (Jetty answers in 1.1; this is so over plain HTTP too).

Not ported: the other identity and trust types of `io.deephaven.ssl.config` (key stores,
`TrustJdk` for client certificates, which needs a trust path here), `ssl.protocols` and
`ssl.ciphers` (not read through properties by Java either), an encrypted private key, and
the properties that change the defaults above (`https.sniHostCheck`, `http.websockets`,
`http.http1`).

## Checked with

- `crates/oxhaven-server/src/transport/tls_tests.rs`, with certificates made in memory for
  each run and written to a directory of the run: the Barrage client of this crate (what
  `dh://` and `barrage_session(..., use_tls=True)` use) snapshots a table by name and by
  address; ALPN; the IDE's files over both protocols; gRPC-web over HTTP/2 and no
  websocket; the host check over HTTP/1.1 and HTTP/2; protocol and cipher refusals; client
  certificates in each mode; the certificates and keys the server refuses.
- By hand, with certificates made by `openssl` (task 15.6): `pydeephaven` with
  `use_tls=True, tls_root_certs=...` (and a client certificate against `NEEDED`); the web IDE
  test `node oracle/ide/ide-test.js --url https://localhost:<port> --psk <key>` (panels,
  iframe, jsapi, over gRPC-web on `h2`); a Deephaven 42.9 console with
  `-Doutbound.ssl.trust.type=certs -Doutbound.ssl.trust.path=<root>` reading a static and a
  ticking table of this server with `resolve("dh://localhost:<port>/app/...")` and
  `barrage_session(..., use_tls=True)`; and `openssl s_client` and raw requests for every
  case of "What a connection gets", against Java and this server alike.
