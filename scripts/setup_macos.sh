#!/bin/bash

set -eu

# Do not update/upgrade because it causes conflicts with the Github runner
# configuration that are not simple to fix in this script.
#  brew update
#  brew upgrade

brew install \
  autoconf \
  automake \
  ccache \
  conan \
  libtool \
  llvm \
  openmpi

brew tap cleishm/neo4j
brew install \
  libcypher-parser

# https://github.com/KatanaGraph/homebrew-dependencies
#brew tap KatanaGraph/dependencies
brew uninstall apache-arrow || true
