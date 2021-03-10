#!/bin/bash

set -eu

if [ $# -eq 0 ]; then
  echo "$(basename "$0") [-fix] <paths>" >&2
  exit 1
fi

SCRIPT_DIR=$(dirname "$0")

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

display_running() {
    CLEAR_LINE="\e[2K"
    echo -ne "${CLEAR_LINE}\rrunning: ${1}"
}

display_running "check_format.sh"
output_on_fail "${SCRIPT_DIR}"/check_format_cpp.sh ${FIX} "${@}"
display_running "check_go_lint.sh"
output_on_fail "${SCRIPT_DIR}"/check_go_lint.sh "${@}"
display_running "check_python_format.sh"
output_on_fail "${SCRIPT_DIR}"/check_python_format.sh ${FIX} "${@}"
#display_running "check_python_lint.sh"
#output_on_fail pipenv run ${SCRIPT_DIR}/check_python_lint.sh "${@}"
display_running "check_sh_format.sh"
output_on_fail "${SCRIPT_DIR}"/check_sh_format.sh ${FIX} "${@}"
echo -e "${CLEAR_LINE}Done"
