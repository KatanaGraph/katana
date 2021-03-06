#!/bin/bash

set -e

SHELLCHECK=${SHELLCHECK:-shellcheck}

if [ $# -eq 0 ]; then
  SCRIPT=$(basename "$0")
  echo " [$SCRIPT -fix] <paths>" >&2
  exit 1
fi

FIX=
if [ "$1" == "-fix" ]; then
  FIX=1
  shift 1
fi

ROOTS="$*"
PRUNE_PATHS=${PRUNE_PATHS:-}
PRUNE_NAMES="build*"

emit_prunes() {
  { for p in $PRUNE_PATHS; do echo "-path $p -prune -o"; done; \
    for p in $PRUNE_NAMES; do echo "-name $p -prune -o"; done; } | xargs
}

while read -r -d '' filename; do
  if [ -n "$FIX" ]; then
    echo "fixing $filename"
    # Ignore the result of this check because we still want to check for
    # unfixable errors unconditionally
    $SHELLCHECK "$filename" --format diff 2>/dev/null| patch "$filename" || true
  fi
  $SHELLCHECK "$filename" -S style
done < <(
    # emit_prunes is a list of arguments so we want it to be unquoted.
    # shellcheck disable=SC2046
    find "$ROOTS" $(emit_prunes) -name '*.sh' -print0
)
