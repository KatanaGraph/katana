#! /bin/sh
set -ex

CMAKE_CCACHE_OPTION=""
if ccache -V >/dev/null 2>/dev/null; then
  export CCACHE_COMPILERCHECK="%compiler% --version"
  export CCACHE_NOHASHDIR="true"
  export CCACHE_BASEDIR="$(dirname "$(realpath "$SRC_DIR")")"
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

if [ -z "${KATANA_COMPONENTS}" ]; then
  KATANA_COMPONENTS=""
fi
echo "Building with components: ${KATANA_COMPONENTS}"

if [ -z "${KATANA_GPU_OPTIONS}" ]; then
  KATANA_GPU_OPTIONS=""
  echo "Building without GPU support"
else
  echo "Building with GPU support"
fi

# Useful debugging addition to the below: -DCMAKE_VERBOSE_MAKEFILE=ON
cmake \
  $CMAKE_CCACHE_OPTION \
  $CMAKE_ARGS \
  -DBUILD_TESTING=OFF \
  -DBUILD_DOCS=internal \
  -DKATANA_LANG_BINDINGS=python \
  -DCMAKE_BUILD_TYPE=Release \
  -DKATANA_COMPONENTS="${KATANA_COMPONENTS}" \
  ${KATANA_GPU_OPTIONS} \
  -Wno-dev \
  -S "$SRC_DIR"
cmake --build . --parallel ${CMAKE_BUILD_PARALLEL_LEVEL}
cmake --install .
