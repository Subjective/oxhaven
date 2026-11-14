#!/usr/bin/env bash
# Fails if the commits since <base> change oracle/replay-baseline.json in any way other than
# making it stricter (differs to clean, fewer kinds, fewer tables). CI runs this for pull
# requests that do not carry the oracle-change label. Needs python3 and nothing else.
#
#   oracle/bin/check-baseline-change.sh <base revision>
set -euo pipefail
base="${1:?usage: check-baseline-change.sh <base revision>}"
oracle="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo="$(dirname "$oracle")"
cd "$repo"
baseline=oracle/replay-baseline.json

fork="$(git merge-base "$base" HEAD)"
if git diff --quiet "$fork" HEAD -- "$baseline"; then
  echo "$baseline did not change"
  exit 0
fi
before="$(mktemp)"
trap 'rm -f "$before"' EXIT
if ! git show "$fork:$baseline" >"$before" 2>/dev/null; then
  echo "$baseline was added: that needs the oracle-change label"
  exit 1
fi
if [[ ! -f "$baseline" ]]; then
  echo "$baseline was removed: that needs the oracle-change label"
  exit 1
fi
PYTHONPATH="$oracle" python3 -m harness.baseline stricter "$before" "$baseline"
