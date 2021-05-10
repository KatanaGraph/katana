#!/bin/bash

# Duplicate config options in pyproject.toml until all repos use pyproject.toml
PYFMT="${PYFMT:-black} --line-length=120"
ISORT="${ISORT:-isort} --profile=black --line-length=120"

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

FAILED=
if [ -n "${FIX}" ]; then
  ${PYFMT} "$@"
  ${ISORT} "$@"
else
  if ! ${PYFMT} --check "$@"; then
    FAILED=1
  fi
  if ! ${ISORT} --check "$@"; then
    FAILED=1
  fi
fi

if [ -n "${FAILED}" ]; then
  exit 1
fi
