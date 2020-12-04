#!/bin/bash
#
# This script sets up a development environment on Ubuntu 18.04. Feel free to
# use these as a starting point for your distribution of choice.

set -eu

REPO_ROOT=$(cd "$(dirname $0)"/..; pwd)

EXPECTED_RELEASE="bionic"
RELEASE=$(lsb_release --codename | awk '{print $2}')

if [[ "${RELEASE}" != "${EXPECTED_RELEASE}" ]]
then
  echo "This script was intended for ${EXPECTED_RELEASE} (you have ${RELEASE}) exiting!" >&2
  exit 1
fi

sudo bash -x "${REPO_ROOT}/.github/workflows/setup_ubuntu.sh" --no-setup-toolchain-variants
"${REPO_ROOT}/.github/workflows/setup_conan.sh"

# make clang-{tidy,format}-10 the default
sudo update-alternatives --verbose --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-10 90
sudo update-alternatives --verbose --install /usr/bin/clang-format clang-format /usr/bin/clang-format-10 90

# --If you want a build directory that is a subdir of Katana root and some cmake options
# mkdir build
# cd build
# cmake ../ -DGALOIS_AUTO_CONAN=on -DCMAKE_BUILD_TYPE=Release -DGALOIS_STORAGE_BACKEND="local;s3;azure"
#
# --The build does rely on git submodules
# git submoudle update --init
#
# --Azure cli  https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-apt
# curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
