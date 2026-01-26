#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

if [[ ! -f "${ROOT}/coverage.out" ]]; then
  echo "coverage.out is missing. Run 'make test' first." >&2
  exit 1
fi

current=$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$3); print $3}')
if [[ -z "${current}" ]]; then
  echo "Unable to read coverage from coverage.out." >&2
  exit 1
fi

echo "${current}" > "${ROOT}/coverage.baseline"
printf "Updated coverage.baseline to %s%%\n" "${current}"
