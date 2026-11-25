#!/usr/bin/env bash
# Starts oxhaven-server in the image: the web IDE's files, the storage under /data and the
# console's environment are given here, and the arguments of `docker run <image> ...` are
# passed on after them (`oxhaven-server --help` lists them).
#
# START_OPTS is read as Deephaven's image reads it, a list of JVM options separated by
# spaces. The one this server understands is -Dauthentication.psk=<key>, the pre-shared key;
# JVM options of the virtual machine itself (-Xmx4g, -XX:...) have no meaning here and are
# skipped with a note. Any other option is refused by name, so that a property that would
# change Deephaven's behaviour is not silently dropped. Without a key, from START_OPTS,
# OXHAVEN_PSK or --psk, the server makes a random one and prints it to the log.
set -euo pipefail

args=(--web-root /opt/oxhaven/web --storage-root /data/storage)

read -r -a opts <<<"${START_OPTS:-}"
for opt in "${opts[@]}"; do
  case "$opt" in
    -Dauthentication.psk=*)
      args+=(--psk "${opt#-Dauthentication.psk=}")
      ;;
    -X*)
      echo "START_OPTS: skipping the JVM option $opt; this server has no JVM" >&2
      ;;
    *)
      echo "START_OPTS: $opt is not understood by this image; it reads only" \
        "-Dauthentication.psk=<key> (docker/README.md). Pass server options as arguments" \
        "of docker run instead: docker run <image> --help lists them." >&2
      exit 2
      ;;
  esac
done

exec /usr/local/bin/oxhaven-server "${args[@]}" "$@"
