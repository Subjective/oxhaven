#!/usr/bin/env bash
# Makes a web root for `oxhaven-server --web-root` from the Deephaven web jar.
#
#   scripts/extract-web-root.sh <directory> [deephaven-web-<version>.jar]
#
# The web IDE, the embeddable widgets and the JS API are build output of the Deephaven
# projects web-client-ui and deephaven-core. They are not part of this repository and must
# not be checked into it: keep <directory> outside the checkout. The jar ships in the
# `deephaven-server` Python wheel as deephaven_server/jars/deephaven-web-<version>.jar.
# Without a jar argument the script looks in the oracle's Python environment (oracle/.venv,
# see oracle/README.md), then in the python3 on the PATH.
#
# The directory receives what the jar has under META-INF/resources: ide/, iframe/, jsapi/.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
fi
dest="$1"
jar="${2:-}"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

find_jar() {
  local python="$1"
  command -v "$python" >/dev/null 2>&1 || return 1
  "$python" - <<'EOF'
import glob, importlib.util, os, sys
spec = importlib.util.find_spec("deephaven_server")
if spec is None or not spec.submodule_search_locations:
    sys.exit(1)
jars = sorted(glob.glob(os.path.join(list(spec.submodule_search_locations)[0], "jars", "deephaven-web-*.jar")))
if not jars:
    sys.exit(1)
print(jars[-1])
EOF
}

if [[ -z "$jar" ]]; then
  jar="$(find_jar "$repo/oracle/.venv/bin/python" || find_jar python3 || true)"
  if [[ -z "$jar" ]]; then
    echo "no deephaven-web jar found: install the deephaven-server wheel or name the jar" >&2
    exit 1
  fi
fi
if [[ ! -f "$jar" ]]; then
  echo "$jar is not a file" >&2
  exit 1
fi
case "$(cd "$(dirname "$dest")" 2>/dev/null && pwd)/" in
  "$repo"/*)
    echo "$dest is inside the repository; the web files must stay out of it" >&2
    exit 1
    ;;
esac

staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT
unzip -q "$jar" 'META-INF/resources/*' -d "$staging"
mkdir -p "$dest"
cp -R "$staging/META-INF/resources/." "$dest/"
echo "$jar"
echo "extracted to $dest: $(find "$dest" -type f | wc -l) files"
