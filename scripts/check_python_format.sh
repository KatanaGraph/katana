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
PRUNE_LIST="external deploy notebook-home .git"

emit_prunes() {
  for p in $PRUNE_LIST; do echo "-name $p -prune -o"; done | xargs
}

FILES=$(find ${ROOTS} $(emit_prunes) -name '*.py' -print | xargs)

if [ -n "${FIX}" ]; then
  ${PYFMT} ${FILES}
else
  ${PYFMT} --check ${FILES}
fi
