#!/bin/bash

PYFMT="${PYFMT:-black}"
ISORT="${ISORT:-isort}"

set -eu

GIT_ROOT=$(readlink -f $(dirname $0)/..)

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
FORMAT_ARGS="--config=${GIT_ROOT}/pyproject.toml"
ISORT_ARGS="--settings-file=${GIT_ROOT}/pyproject.toml"

FAILED=

# Handle arguments individually because Black treats multiple arguments as
# paths within the Python package, which will cause errors when mixing Python
# script directories with Python modules.
for root in "$@"; do
  if [ -n "${FIX}" ]; then
    ${PYFMT} ${FORMAT_ARGS} "${root}"
    ${ISORT} ${ISORT_ARGS} "${root}"
  else
    if ! ${PYFMT} ${FORMAT_ARGS} --check "${root}"; then
      FAILED=1
    fi
    if ! ${ISORT} ${ISORT_ARGS} --diff --check "${root}"; then
      FAILED=1
    fi
  fi
done

if [ -n "${FAILED}" ]; then
  exit 1
fi
