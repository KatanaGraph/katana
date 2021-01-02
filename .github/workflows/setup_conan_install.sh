#! /bin/bash
set -xeuo pipefail
KATANA_REPO_ROOT="$(cd "$(dirname "$0")/../.."; pwd)"

BUILD_DIR="$1"
shift 1

[[ -f ~/.profile ]] && source ~/.profile

$KATANA_REPO_ROOT/scripts/setup_conan.sh

conan remove --locks
# try to install, but if it doesn't work try to rebuild from scratch
# because changes to conan flags can cause it to get stuck
conan install -if "$BUILD_DIR" --build=missing config || \
  (rm -rf ~/.conan && $KATANA_REPO_ROOT/scripts/setup_conan.sh && \
     conan install -if "$BUILD_DIR" --build=missing config)
