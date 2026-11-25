# The Docker image

An image that runs Oxhaven with the web IDE and a Python console, for trying it out:

```bash
docker build -f docker/Dockerfile -t oxhaven .     # from the repository root
docker run --rm -p 10000:10000 oxhaven
```

The log prints the pre-shared key (`use <key> to connect`); open
<http://localhost:10000/ide/> and type it, or open `http://localhost:10000/ide/?psk=<key>`.

| Path | Holds |
| --- | --- |
| `Dockerfile` | The three stages: the server, the web IDE's files, the image that runs |
| `entrypoint.sh` | Reads `START_OPTS` and starts the server with the image's paths |
| `requirements.txt` | The pinned packages of the console's environment |
| `smoke.sh` | Starts a built image once and checks that it serves, takes the key and stops |

## What is in it

- `oxhaven-server` built in release with the `python` feature (the console, and with it TLS),
  linked against the image's Python 3.13.
- The web IDE, the embeddable widgets and the JS API, extracted at build time from the
  `deephaven-server` 42.9 wheel with `scripts/extract-web-root.sh` (`--build-arg
  DEEPHAVEN_VERSION=<version>` takes another). They are served at `/ide/`, `/iframe/` and
  `/jsapi/`.
- A virtual environment, `/opt/venv`, that the console imports from (`VIRTUAL_ENV`): numpy,
  pandas and pyarrow in the versions of the oracle's recordings, `jedi` and
  `docstring_parser` for completion, and `deephaven-plugin-plotly-express` 0.22.0
  (`import deephaven.plot.express as dx`; docs/FIGURES.md). More packages: build an image
  `FROM oxhaven` with `RUN /opt/venv/bin/pip install <package>`.

The server runs as the user `oxhaven` (uid 1000) in `/data`.

## Options

| Deephaven's image | This image |
| --- | --- |
| `-p 10000:10000` | the same: the server listens on port 10000 of every address |
| `START_OPTS=-Dauthentication.psk=<key>` | the same; or `OXHAVEN_PSK=<key>`, or `--psk <key>` as an argument |
| no key: a random one, printed to the log | the same |
| `/data`, a volume; notebooks and layouts in `/data/storage` | the same |
| other `START_OPTS` | JVM options (`-Xmx4g`, `-XX:...`) are skipped with a note; any other is refused by name |

```bash
docker run --rm -p 10000:10000 -e START_OPTS=-Dauthentication.psk=mykey oxhaven
docker run --rm -p 10000:10000 -v oxhaven-data:/data oxhaven       # keeps the notebooks
```

Arguments after the image's name go to `oxhaven-server` after the image's own
(`--web-root /opt/oxhaven/web --storage-root /data/storage`); `docker run --rm oxhaven
--help` lists them. For example, the scripts of the `.app` files of a directory
(`docs/CONSOLE.md`, "Applications defined by scripts"):

```bash
docker run --rm -p 10000:10000 -v "$PWD/app.d:/app.d:ro" oxhaven --app-dir /app.d
```

`docker stop` sends `SIGINT`, on which the server shuts down (`STOPSIGNAL` in the
Dockerfile).

## HTTPS

Mount the certificate chain and its private key, PEM files the user `oxhaven` can read, and
name them (docs/TLS.md has the options and what a connection gets). With a certificate the
server's default port is 443, so name the port too:

```bash
docker run --rm -p 10000:10000 -v "$PWD/certs:/certs:ro" oxhaven \
  --tls-cert /certs/server-chain.pem --tls-key /certs/server-key.pem --port 10000
```

Then open <https://localhost:10000/ide/>. The certificate must name the host the browser
uses (`localhost` here): the server answers `400` to a request for a host that it does not
name, as Java does (docs/TLS.md, "The host check").

## Building it

The build needs network access to the Python package index (the wheel and the console's
packages), crates.io and `sh.rustup.rs`. The cargo build uses every core;
`--build-arg CARGO_BUILD_JOBS=2` limits it. Behind a proxy that re-signs TLS, base the build
on an image that trusts the proxy's certificate: `--build-arg PYTHON_IMAGE=<image>`, an image
made `FROM python:3.13-slim-bookworm` that adds it.

The workflow `.github/workflows/docker.yml` builds the image and runs `docker/smoke.sh`
against it when a pull request or a push to `main` changes what makes the image (this
directory, `.dockerignore`, `scripts/extract-web-root.sh`, `rust-toolchain.toml`, the
workflow), every Monday, and on demand; nothing is pushed anywhere. It is not part of the
checks of every pull request.

## Licences

The files of this directory are under the repository's licence, Apache-2.0. The image holds
more than this repository: the Debian and Python base image, the Python packages of
`requirements.txt` under their own licences, and the web IDE and JS API, which are build
output of Deephaven's projects under Deephaven's licences. None of it is kept in the
repository.
