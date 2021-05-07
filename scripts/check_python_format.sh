#!/bin/bash

PYFMT="${PYFMT:-black} --line-length 120"
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

if [ -n "${FIX}" ]; then
  ${PYFMT} "$@"
else
  ${PYFMT} --check "$@"
fi
