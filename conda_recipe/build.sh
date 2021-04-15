#! /bin/sh
set -ex

CMAKE_CCACHE_OPTION=""
if ccache -V >/dev/null 2>/dev/null; then
  export CCACHE_BASEDIR="$(dirname "$SRC_DIR")"
  CMAKE_CCACHE_OPTION="-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
  echo "Enabling CCache with base: $CCACHE_BASEDIR"
fi

rm -rf build
mkdir build
cd build
if [ -z "${CMAKE_BUILD_PARALLEL_LEVEL}" ]; then
  export CMAKE_BUILD_PARALLEL_LEVEL="$CPU_COUNT"
fi
echo "Building with parallelism: ${CMAKE_BUILD_PARALLEL_LEVEL}"

# Useful debugging addition to the below: -DCMAKE_VERBOSE_MAKEFILE=ON
cmake \
  $CMAKE_CCACHE_OPTION \
  $CMAKE_ARGS \
  -DBUILD_SHARED_LIBS=ON \
  -DBUILD_TESTING=OFF \
  -DKATANA_LANG_BINDINGS=python \
  -DCMAKE_BUILD_TYPE=Release \
  -Wno-dev \
  -S "$SRC_DIR"
make -j${CMAKE_BUILD_PARALLEL_LEVEL}
make install
