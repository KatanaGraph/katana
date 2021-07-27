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

case "$CXX" in
  *clang++*)   CC=${CXX/clang++/clang} ;;
  *g++*)       CC=${CXX/g++/gcc};;
  *-gnu-c++*)  CC=${CXX/c++/cc};;
esac

case $CI_BUILD_TYPE in
  Release)
    BUILD_TYPE=Release
    SANITIZER=""
    ;;
  Sanitizer)
    BUILD_TYPE=Release
    SANITIZER="Address;Undefined"
    ;;
  Debug)
    BUILD_TYPE=Debug
    SANITIZER=""
    ;;
  *)
    echo Unknown build type: $CI_BUILD_TYPE
    exit 1
    ;;
esac

CMAKE_TOOLCHAIN_ARG=""
if [ -f "$BUILD_DIR/conan_paths.cmake" ]; then
  CMAKE_TOOLCHAIN_ARG="-DCMAKE_TOOLCHAIN_FILE=$BUILD_DIR/conan_paths.cmake"
fi

cmake -S . -B $BUILD_DIR \
  -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
  $CMAKE_TOOLCHAIN_ARG \
  -DCMAKE_CXX_COMPILER="$CXX" \
  -DCMAKE_C_COMPILER="$CC" \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DKATANA_USE_SANITIZER="$SANITIZER" \
  "$@"
