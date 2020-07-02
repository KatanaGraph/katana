#! /bin/sh
cd $SRC_DIR
rm -rf _skbuild
export GALOIS_CMAKE_ARGS="-DBUILD_LIBGALOIS=OFF -DCMAKE_BUILD_TYPE=Release -DUSE_ARCH=none"
$PYTHON setup.py --verbose build_ext --verbose install -- -- -j2
