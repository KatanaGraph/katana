#!/bin/bash

set -eu

brew update
brew upgrade

brew install \
  autoconf \
  automake \
  ccache \
  cmake \
  conan \
  libtool \
  llvm \
  openmpi

brew tap cleishm/neo4j
brew install \
  libcypher-parser

# https://github.com/KatanaGraph/homebrew-dependencies
brew tap KatanaGraph/dependencies
brew install \
  KatanaGraph/dependencies/apache-arrow
