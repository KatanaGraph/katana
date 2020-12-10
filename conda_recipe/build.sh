#! /bin/sh
set -e

CMAKE_CCACHE_OPTION=""
if ccache -V > /dev/null 2> /dev/null; then
    export CCACHE_BASEDIR="$(dirname "$SRC_DIR")"
    CMAKE_CCACHE_OPTION="-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    echo "Enabling CCache."
fi
rm -rf build
mkdir build
cd build
# Useful debugging addition to the below: -DCMAKE_VERBOSE_MAKEFILE=ON
cmake \
  -DBUILD_SHARED_LIBS=ON \
  -DBUILD_TESTING=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=$PREFIX \
  $CMAKE_CCACHE_OPTION \
  -S "$SRC_DIR"
make -j2
make install
