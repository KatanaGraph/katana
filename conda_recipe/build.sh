#! /bin/sh
# export CCACHE_BASE_DIR=$SRC_DIR
#  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
rm -rf build
mkdir build
cd build
# Useful debugging addition to the below: -DCMAKE_VERBOSE_MAKEFILE=ON
cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$PREFIX -DUSE_ARCH=none -S $SRC_DIR
make -j2
make install
