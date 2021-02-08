#!/bin/sh
set -e

rm -rf build
cmake -B build -S test_app -DBUILD_SHARED_LIBS=ON
cd build
make
./test_app
