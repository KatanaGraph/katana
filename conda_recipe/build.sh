#! /bin/sh
set -ex

#if echo "${HOST}" | grep -q "darwin"; then
#  CMAKE_PLATFORM_FLAG="-DCMAKE_OSX_SYSROOT=${CONDA_BUILD_SYSROOT}"
#else
#  CMAKE_PLATFORM_FLAG="-DCMAKE_TOOLCHAIN_FILE=${RECIPE_DIR}/cross-linux.cmake"
#fi

CMAKE_CCACHE_OPTION=""
if ccache -V >/dev/null 2>/dev/null; then
  export CCACHE_BASEDIR="$(dirname "$SRC_DIR")"
  CMAKE_CCACHE_OPTION="-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
  echo "Enabling CCache with base: $CCACHE_BASEDIR"
fi

rm -rf build
mkdir build
cd build
# Useful debugging addition to the below: -DCMAKE_VERBOSE_MAKEFILE=ON
if [ -z "${CMAKE_BUILD_PARALLEL_LEVEL}" ]; then
  export CMAKE_BUILD_PARALLEL_LEVEL="$CPU_COUNT"
fi
echo "Building with parallelism: ${CMAKE_BUILD_PARALLEL_LEVEL}"

CMAKE_DOCS_OPTION=""
if [ -n "$KATANA_DOCS_OUTPUT" ]; then
    CMAKE_DOCS_OPTION="-DBUILD_DOCS=ON"
fi

cmake \
  $CMAKE_CCACHE_OPTION \
  $CMAKE_ARGS \
  $CMAKE_PLATFORM_FLAG \
  $CMAKE_DOCS_OPTION \
  -DBUILD_SHARED_LIBS=ON \
  -DBUILD_TESTING=OFF \
  -DKATANA_LANG_BINDINGS=python \
  -DCMAKE_BUILD_TYPE=Release \
  -Wno-dev \
  -S "$SRC_DIR"
make -j${CMAKE_BUILD_PARALLEL_LEVEL}
make install

if [ -n "$KATANA_DOCS_OUTPUT" ]; then
  echo "Exporting documentation to $KATANA_DOCS_OUTPUT"
  mkdir -p "$KATANA_DOCS_OUTPUT"
  cp -a katana_python_build/build/sphinx/html "$KATANA_DOCS_OUTPUT"
fi
