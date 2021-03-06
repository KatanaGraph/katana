#!/bin/bash

GOFMT=${GOFMT:-gofmt}
set -eu

if [ $# -eq 0 ]; then
  echo "$(basename $0) [-fix] <paths>" >&2
  exit 1
fi

FIX=
if [ "$1" == "-fix" ]; then
  FIX=1
  shift 1
fi

ROOTS="$@"
PRUNE_LIST=".git"

FAILED=

emit_prunes() {
  for p in ${PRUNE_LIST}; do echo "-name ${p} -prune -o"; done | xargs
}

FILES=$(find ${ROOTS} $(emit_prunes) -name '*.go' -print | xargs)

if [ -z "$FILES" ]; then
    echo "no files to fix!" >&2
    exit 0
fi

if [ -n "${FIX}" ]; then
  ${GOFMT} -s -w ${FILES}
else
  FAILED=$(${GOFMT} -s -l ${FILES})
fi

if [ -n "${FAILED}" ]; then
  echo "The following files are NOT OK:"
  echo "$FAILED"
  exit 1
fi
