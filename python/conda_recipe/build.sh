#! /bin/sh
CMAKE_CCACHE_OPTION=""
if ccache -V > /dev/null 2> /dev/null; then
    export CCACHE_BASEDIR="$(dirname "$SRC_DIR")"
    CMAKE_CCACHE_OPTION="-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    echo "Enabling CCache."
fi
cd $SRC_DIR
rm -rf _skbuild
export GALOIS_CMAKE_ARGS="-DBUILD_LIBGALOIS=OFF -DCMAKE_BUILD_TYPE=Release -DUSE_ARCH=none $CMAKE_CCACHE_OPTION"
$PYTHON setup.py --verbose build_ext --verbose install -- -- -j2
