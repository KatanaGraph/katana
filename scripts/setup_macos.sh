#!/bin/bash

set -eu

# Do not update/upgrade because it causes conflicts with the Github runner
# configuration that are not simple to fix in this script.
#  brew update
#  brew upgrade

function brew_install_if_missing {
  if ! brew ls --versions "$1" >/dev/null; then
    HOMEBREW_NO_AUTO_UPDATE=1 brew install "$1"
  fi
}

brew_install_if_missing autoconf
brew_install_if_missing automake
brew_install_if_missing ccache
brew_install_if_missing conan
brew_install_if_missing libtool
brew_install_if_missing llvm
brew_install_if_missing openmpi
brew_install_if_missing shellcheck

brew tap cleishm/neo4j
brew_install_if_missing libcypher-parser

# https://github.com/KatanaGraph/homebrew-dependencies
#brew tap KatanaGraph/dependencies
brew uninstall apache-arrow || true

pip3 install PyGithub packaging
