#!/bin/bash

set -eu

if [ $# -eq 0 ]; then
  echo "$(basename $0) <paths>" >&2
  exit 1
fi

LINT=${PYLINT:-pylint}

exec ${LINT} "$@"
