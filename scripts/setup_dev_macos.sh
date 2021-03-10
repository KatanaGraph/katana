#!/bin/bash
#
# This script sets up a development environment on MacOS.

set -eu

REPO_ROOT=$(cd "$(dirname "$0")"/..; pwd)

bash -x "${REPO_ROOT}/scripts/setup_macos.sh"

"${REPO_ROOT}/scripts/setup_conan.sh"

# If you want a build directory that is a subdir of Katana root
# mkdir build
# cd build
# cmake ../ -DKATANA_AUTO_CONAN=on
# The build does rely on git submodules
# git submoudle update --init
