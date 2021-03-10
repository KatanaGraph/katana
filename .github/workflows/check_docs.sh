#!/bin/bash
set -xeuo pipefail
KATANA_REPO_ROOT="$(cd "$(dirname "$0")/../.."; pwd)"

BUILD_DIR=$1
shift 1

cmake -S . -B "$BUILD_DIR" \
  -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR"/conan_paths.cmake && \
  cd "$BUILD_DIR" && "$KATANA_REPO_ROOT"/scripts/check_docs.sh
