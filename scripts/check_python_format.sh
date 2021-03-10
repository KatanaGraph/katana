#!/bin/bash

PYFMT="${PYFMT:-black} --line-length 120"
set -eu

if [ $# -eq 0 ]; then
  echo "$(basename "$0") [-fix] <paths>" >&2
  exit 1
fi

FIX=
if [ "$1" == "-fix" ]; then
  FIX=1
  shift 1
fi

ROOTS=("$@")
PRUNE_LIST="deploy notebook-home .git build*"

emit_prunes() {
  for p in $PRUNE_LIST; do echo "-name $p -prune -o"; done | xargs
}

# shellcheck disable=SC2046
FILES=$(find "${ROOTS[@]}" $(emit_prunes) -name '*.py' -print0 | xargs -0)

if [ -n "${FIX}" ]; then
  # shellcheck disable=SC2046
  ${PYFMT} "$FILES"
else
  # shellcheck disable=SC2046
  ${PYFMT} --check "$FILES"
fi
