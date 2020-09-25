#!/bin/bash

set -eu

if [ $# -eq 0 ]; then
  echo "$(basename $0) <go/root/dirs>" >&2
  exit 1
fi

GOLINT=${GOLINT:-golangci-lint}
ARGS=""
ROOTS="$@"
FAILED=
GOROOTS=

# only run linter in the root of go modules
for root in ${ROOTS}; do
  NEWROOTS=$(find ${root} -name .git -prune -o -name 'go.mod' -printf '%h\n' | sort -u | xargs)
  if [ -n "${NEWROOTS}" ]; then
    GOROOTS="${NEWROOTS} ${GOROOTS}"
  else
    echo "WARNING: no go modules detected in ${root}"
  fi
done

if [ -z "${GOROOTS}" ]; then
  echo "Exiting; no go modules detected"
  exit 1
fi

for d in ${GOROOTS}; do
  if ! (cd ${d} && ${GOLINT} run); then
    echo "NOT OK: ${d}"
    FAILED=1
  fi
done

if [ -n "${FAILED}" ]; then
  exit 1
fi
