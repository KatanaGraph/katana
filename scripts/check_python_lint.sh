#!/bin/bash

set -eu

GIT_ROOT=$(readlink -f $(dirname $0)/..)

if [ $# -eq 0 ]; then
  echo "$(basename $0) <paths>" >&2
  exit 1
fi

LINT=${PYLINT:-pylint}
ARGS="-j 0 --rcfile=${GIT_ROOT}/.pylintrc --suggestion-mode=n"
ROOTS="$@"
FAILED=
PRUNE_LIST="external deploy docs build notebook-home .git"
TEST_DIRS="${GIT_ROOT}/test/ ${GIT_ROOT}/demosite/katanaclient/test/"
# lint warnings that are useless for tests since tests are run for ci anyway
# eliminate unused-argument since it is triggered by pytest harnesses whose value is not needed.
TEST_NOLINT="import-error no-name-in-module unused-argument"

emit_prunes() {
  for p in ${PRUNE_LIST}; do echo "-name ${p} -prune -o"; done | xargs
}

emit_test_nolint() {
  for msg in ${TEST_NOLINT}; do echo "-d ${msg}"; done | xargs
}

is_test() {
  for d in ${TEST_DIRS}; do
    if [[ "$(readlink -f $1)" =~ ^${d}.* ]]
    then
      return 0
    fi
  done
  return 1
}

TEST_ARGS="${ARGS} $(emit_test_nolint)"
while read -d '' filename; do
  if is_test "${filename}"
  then
    args="${TEST_ARGS}"
  else
    args="${ARGS}"
  fi
  if ! ${LINT} ${args} "${filename}"
  then
    echo "${filename} NOT OK"
    FAILED=1
  fi
done < <(find ${ROOTS} $(emit_prunes) -name '*.py' -print0)

if [ -n "${FAILED}" ]; then
  exit 1
fi
