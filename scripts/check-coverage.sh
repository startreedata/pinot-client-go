#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

baseline_file="${ROOT}/coverage.baseline"

if [[ ! -f "${baseline_file}" ]]; then
  echo "coverage.baseline is missing. Run 'make coverage-baseline' to set the baseline." >&2
  exit 1
fi

if [[ ! -f "${ROOT}/coverage.out" ]]; then
  echo "coverage.out is missing. Run 'make test' first." >&2
  exit 1
fi

baseline=$(awk '/^[0-9.]+$/ {print; exit}' "${baseline_file}")
if [[ -z "${baseline}" ]]; then
  echo "coverage.baseline does not contain a numeric percentage." >&2
  exit 1
fi

current=$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$3); print $3}')
if [[ -z "${current}" ]]; then
  echo "Unable to read coverage from coverage.out." >&2
  exit 1
fi

if awk -v cur="${current}" -v base="${baseline}" 'BEGIN {exit (cur+0 < base+0)}'; then
  printf "Coverage OK: %s%% (baseline %s%%)\n" "${current}" "${baseline}"
else
  printf "Coverage decreased: %s%% (baseline %s%%)\n" "${current}" "${baseline}" >&2
  exit 1
fi
