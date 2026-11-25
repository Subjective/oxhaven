# Oxhaven with the web IDE and a Python console. Built from the repository root:
#
#   docker build -f docker/Dockerfile -t oxhaven .
#   docker run --rm -p 10000:10000 oxhaven
#
# docker/README.md describes the options. Three stages: the server, built in release with the
# console; the web IDE's files, taken from the deephaven-server wheel; and the image that
# runs, with the same Python as the build and a virtual environment for the console.

# The server embeds the libpython it is linked against (docs/CONSOLE.md), so the build and
# the image that runs share one base: the same Python 3.13, at the same path.
ARG PYTHON_IMAGE=python:3.13-slim-bookworm

FROM ${PYTHON_IMAGE} AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential ca-certificates curl libssl-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
# The toolchain is the one rust-toolchain.toml pins; rustup installs it on the first cargo call.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --no-modify-path --profile minimal --default-toolchain none
WORKDIR /src
COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY crates crates
# Empty for every core; set it to share a machine with other builds.
ARG CARGO_BUILD_JOBS=
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    if [ -n "$CARGO_BUILD_JOBS" ]; then export CARGO_BUILD_JOBS; else unset CARGO_BUILD_JOBS; fi \
 && cargo build --release --locked -p oxhaven-server --features python \
 && cp target/release/oxhaven-server /usr/local/bin/oxhaven-server

# The web IDE, the embeddable widgets and the JS API: build output of Deephaven's projects,
# shipped in the deephaven-server wheel, extracted here and never kept in the repository.
FROM ${PYTHON_IMAGE} AS web
RUN apt-get update \
 && apt-get install -y --no-install-recommends unzip \
 && rm -rf /var/lib/apt/lists/*
ARG DEEPHAVEN_VERSION=42.9
COPY scripts/extract-web-root.sh /src/scripts/extract-web-root.sh
RUN pip download --no-cache-dir --no-deps --dest /wheel "deephaven-server==${DEEPHAVEN_VERSION}" \
 && unzip -q /wheel/*.whl "deephaven_server/jars/deephaven-web-*.jar" -d /wheel \
 && /src/scripts/extract-web-root.sh /web /wheel/deephaven_server/jars/deephaven-web-*.jar \
 && rm -rf /wheel

FROM ${PYTHON_IMAGE}
# The console's packages, in an environment of the image's Python (docs/CONSOLE.md, "The
# Python environment"). deephaven-plugin-plotly-express goes in without its dependencies:
# it asks for the Java reference's deephaven-core and jpy, and the server has its own.
COPY docker/requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
 && /opt/venv/bin/pip install --no-cache-dir --no-deps deephaven-plugin-plotly-express==0.22.0 \
 && rm /tmp/requirements.txt
COPY --from=web /web /opt/oxhaven/web
COPY --from=build /usr/local/bin/oxhaven-server /usr/local/bin/oxhaven-server
COPY docker/entrypoint.sh /usr/local/bin/oxhaven-entrypoint
# Notebooks and layouts are kept in /data/storage, where Deephaven's image keeps them
# (storage.path under its data directory /data).
RUN useradd --uid 1000 --user-group --create-home --shell /usr/sbin/nologin oxhaven \
 && mkdir -p /data/storage \
 && chown -R oxhaven:oxhaven /data
USER oxhaven
WORKDIR /data
VOLUME /data
ENV VIRTUAL_ENV=/opt/venv
EXPOSE 10000
# The server stops on SIGINT (Ctrl-C); as process 1 it would ignore docker stop's SIGTERM.
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/oxhaven-entrypoint"]
# No licence label: the image holds more than this repository (docker/README.md, "Licences").
LABEL org.opencontainers.image.title="Oxhaven" \
      org.opencontainers.image.description="A Rust implementation of the Deephaven Community Core server, with the web IDE and a Python console"
