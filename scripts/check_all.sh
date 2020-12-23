#!/bin/bash

set -eu

if [ $# -eq 0 ]; then
  echo "$(basename $0) [-fix] <paths>" >&2
  exit 1
fi

SCRIPT_DIR=$(dirname $0)

FIX=
if [ "$1" == "-fix" ]; then
  FIX="-fix"
  shift 1
fi

output_on_fail(){
  if ! out=$("${@}" 2>&1); then
    echo -ne "\n"
    echo "${out}"
    return 1
  fi
  return 0
}

echo -ne 'running:  check_format.sh\r'
output_on_fail ${SCRIPT_DIR}/check_format.sh ${FIX} "${@}"
echo -ne 'running:  check_go_format.sh\r'
output_on_fail ${SCRIPT_DIR}/check_go_format.sh ${FIX} "${@}"
echo -ne 'running: check_go_lint.sh\r'
output_on_fail ${SCRIPT_DIR}/check_go_lint.sh "${@}"
echo -ne 'running:  check_python_format.sh\r'
output_on_fail ${SCRIPT_DIR}/check_python_format.sh ${FIX} "${@}"
#echo -ne 'running: check_python_lint.sh\r'
#output_on_fail pipenv run ${SCRIPT_DIR}/check_python_lint.sh "${@}"
echo -ne 'Done!              \n'
