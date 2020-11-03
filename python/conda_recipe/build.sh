#! /bin/sh
set -e

CMAKE_CCACHE_OPTION=""
if ccache -V > /dev/null 2> /dev/null; then
    export CCACHE_BASEDIR="$(dirname "$SRC_DIR")"
    CMAKE_CCACHE_OPTION="-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    echo "Enabling CCache."
fi

CMAKE_DOCS_OPTION=""
if [ -n "$GALOIS_DOCS_OUTPUT" ]; then
    CMAKE_DOCS_OPTION="-DBUILD_DOCS=ON"
fi

cd $SRC_DIR
rm -rf _skbuild

export GALOIS_CMAKE_ARGS="-DBUILD_LIBGALOIS=OFF -DCMAKE_BUILD_TYPE=Release -DUSE_ARCH=none $CMAKE_CCACHE_OPTION $CMAKE_DOCS_OPTION"
$PYTHON setup.py --verbose build_ext --verbose install -- -- -j2

if [ -n "$GALOIS_DOCS_OUTPUT" ]; then
    echo "Exporting documentation to $GALOIS_DOCS_OUTPUT"
    mkdir -p "$GALOIS_DOCS_OUTPUT"
    cp -a _skbuild/*/cmake-build/docs/html/* "$GALOIS_DOCS_OUTPUT"
fi
