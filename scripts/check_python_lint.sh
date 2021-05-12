#!/bin/bash

set -eu

GIT_ROOT=$(readlink -f $(dirname $0)/..)

if [ $# -eq 0 ]; then
  echo "$(basename $0) <paths>" >&2
  exit 1
fi

LINT=${PYLINT:-pylint}
ARGS="-j 0 --rcfile=${GIT_ROOT}/pyproject.toml"
ROOTS="$@"
FAILED=
PRUNE_LIST="build .git"

emit_prunes() {
  for p in ${PRUNE_LIST}; do echo "-name ${p} -prune -o"; done | xargs
}

while read -d '' filename; do
  if ! ${LINT} ${ARGS} "${filename}"; then
    echo "${filename} NOT OK"
    FAILED=1
  fi
done < <(find ${ROOTS} $(emit_prunes) -name '*.py' -print0)

if [ -n "${FAILED}" ]; then
  exit 1
fi
