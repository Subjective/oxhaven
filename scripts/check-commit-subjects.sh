#!/usr/bin/env bash
# Fails if any commit subject in the given range breaks the commit rules in PORTING.md.
set -euo pipefail
range="${1:?usage: check-commit-subjects.sh <rev-range>}"
status=0
while IFS= read -r subject; do
  if ! [[ "$subject" =~ ^[a-z0-9][a-z0-9/_.-]*:\ [^\ ].*[^.]$ ]]; then
    echo "not '<scope>: <description>' or ends with a period: $subject"; status=1
  elif [[ "$subject" =~ ^(feat|fix|chore|refactor|style|perf|misc)(\(.*\))?!?:\  ]]; then
    echo "Conventional Commits type or vague scope used as scope: $subject"; status=1
  fi
done < <(git log --no-merges --format=%s "$range")
exit $status
