#!/bin/sh
rm -rf build
cmake -B build -S test_app
cd build
make
./test_app
