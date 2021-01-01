#!/bin/bash
set -xeuo pipefail

OS=$1
CXX=$2
CI_BUILD_TYPE=$3
BUILD_DIR=$4
shift 4

case $OS in
  *macOS*) export PATH=$PATH:/usr/local/opt/llvm/bin ;;
  *) ;;
esac

VER="${CXX/*-/}"
if [[ "$VER" == "$CXX" ]]; then
  VER=""
else
  VER="-$VER"
fi

case "$CXX" in
  clang*) CC=clang ;;
  g*)     CC=gcc   ;;
esac

CC="${CC}${VER}"

case $CI_BUILD_TYPE in
  Release)
    BUILD_TYPE=Release
    SANITIZER=""
    SHARED=""
    ;;
  Sanitizer)
    BUILD_TYPE=Release
    SANITIZER="Address;Undefined"
    SHARED=""
    ;;
  Debug)
    BUILD_TYPE=Debug
    SANITIZER=""
    SHARED=""
    ;;
  Shared)
    BUILD_TYPE=Release
    SANITIZER=""
    SHARED="ON"
    ;;
  *)
    echo Unknown build type: $CI_BUILD_TYPE
    exit 1
    ;;
esac

cmake -S . -B $BUILD_DIR \
  -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
  -DCMAKE_TOOLCHAIN_FILE=$BUILD_DIR/conan_paths.cmake \
  -DCMAKE_CXX_COMPILER="$CXX" \
  -DCMAKE_C_COMPILER="$CC" \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DKATANA_USE_SANITIZER="$SANITIZER" \
  -DBUILD_SHARED_LIBS="$SHARED" \
  -DKATANA_FORCE_NON_STATIC="$SHARED" \
  "$@"
