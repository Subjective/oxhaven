#!/usr/bin/env bash
# Runs the console probe against a server that was just started and compares what it
# answers with a recorded transcript. See oracle/console/README.md.
#
#   oracle/console/check.sh <host:port> [transcript]
#
# The transcript defaults to oracle/console/oxhaven.txt; give oracle/console/java-42.9.txt
# for the Java reference. Exit status: 0 when the answers are the transcript's, 1 when they
# differ (the difference is printed), 2 when the probe could not run.
#
# Environment:
#   ORACLE_PSK   pre-shared key (default: oxhaven)
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python="$(dirname "$here")/.venv/bin/python"
address="${1:?usage: check.sh <host:port> [transcript]}"
transcript="${2:-$here/oxhaven.txt}"

[[ -x "$python" ]] || { echo "console check: no Python environment at $python; see oracle/README.md" >&2; exit 2; }
[[ -f "$transcript" ]] || { echo "console check: no transcript at $transcript" >&2; exit 2; }

answers="$(mktemp)"
trap 'rm -f "$answers"' EXIT
if ! "$python" "$here/probe.py" "$address" --psk "${ORACLE_PSK:-oxhaven}" >"$answers" 2>"$answers.err"; then
  echo "console check: the probe failed:" >&2
  tail -n 20 "$answers.err" >&2
  rm -f "$answers.err"
  exit 2
fi
rm -f "$answers.err"

if diff -u "$transcript" "$answers"; then
  echo "console check: the answers are those of $(basename "$transcript")"
else
  echo "console check: the answers differ from $(basename "$transcript")" >&2
  exit 1
fi
