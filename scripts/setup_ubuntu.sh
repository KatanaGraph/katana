#!/bin/bash

set -xeuo pipefail

RELEASE=$(lsb_release --codename | awk '{print $2}')

SETUP_TOOLCHAIN_VARIANTS=${SETUP_TOOLCHAIN_VARIANTS:-yes}

ORIGINAL_USER=${ORIGINAL_USER:-${SUDO_USER:-}}

for arg in "$@"; do
  case "$arg" in
    --no-setup-toolchain-variants) SETUP_TOOLCHAIN_VARIANTS="" ;;
    --setup-toolchain-variants)    SETUP_TOOLCHAIN_VARIANTS=yes ;;

    *)
      echo "unknown option" >&2
      exit 1
      ;;
  esac
done

run_as_original_user() {
  if [[ -n "${ORIGINAL_USER}" ]]; then
    su - "${ORIGINAL_USER}" bash -c "$*"
  else
    bash -c "$*"
  fi
}

#
# Install bootstrap requirements
#
apt install -yq curl software-properties-common

#
# Add custom repositories
#
curl -fL https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add -
apt-add-repository -y --no-update "deb https://apt.kitware.com/ubuntu/ ${RELEASE} main"

curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt-add-repository -y --no-update "deb http://apt.llvm.org/${RELEASE}/ llvm-toolchain-${RELEASE}-10 main"

add-apt-repository -y --no-update ppa:git-core/ppa

if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt-add-repository -y --no-update ppa:ubuntu-toolchain-r/test
fi

#
# Install dependencies
#

apt update

# Install pip3
apt install -yq --allow-downgrades  python3-pip

run_as_original_user pip3 install --upgrade pip setuptools
run_as_original_user pip3 install conan==1.31

# Developer tools
DEVELOPER_TOOLS="clang-format-10 clang-tidy-10 doxygen graphviz ccache cmake"
# github actions require a more recent git
GIT=git
# Library dependencies
#
# Install llvm via apt instead of as a conan package because existing
# conan packages do yet enable RTTI, which is required for boost
# serialization.
LIBRARIES="libxml2-dev llvm-10-dev"

apt install -yq --allow-downgrades \
  $DEVELOPER_TOOLS \
  $GIT \
  $LIBRARIES

# Toolchain variants
if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt install -yq gcc-9 g++-9 clang-10 clang++-10
fi
