#!/bin/bash
#
# Run from build directory to create tarball of current set of inputs.

set -eu -o pipefail

if [[ ! -f CMakeCache.txt ]]; then
  echo run from build directory >&2
  exit 1
fi

cat<<EOF | tar czvf inputs.tar.gz --directory=inputs/current --owner=root --group=root --exclude-from=- .
current-*
extracted
.DS_Store
EOF
