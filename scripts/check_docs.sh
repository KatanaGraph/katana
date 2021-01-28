#!/bin/bash
#
# Doxygen provides a WARN_AS_ERROR option that halts execution after the first
# error. For continuous integration purposes it is more convenient to report
# all the errors then exit so that users can fix multiple errors at once.

set -eu -o pipefail

if [[ ! -f CMakeCache.txt ]]; then
  echo "this script should be run from the root of your build directory" >&2
  exit 1
fi

# match lines like
# a/path/filename.ext:123: warning: ....
make doc 2>&1 | awk 'END { exit status } /^[^:]*:[0-9]+: .*/ {status = 1;} { print; }'
