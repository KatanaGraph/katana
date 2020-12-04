#!/bin/bash

set -eu

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
curl -fL --output /tmp/arrow-keyring.deb https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb \
  && apt install -yq /tmp/arrow-keyring.deb \
  && rm /tmp/arrow-keyring.deb

curl -fL https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add -
apt-add-repository -y --no-update 'deb https://apt.kitware.com/ubuntu/ bionic main'

curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt-add-repository -y --no-update 'deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-10 main'

add-apt-repository -y ppa:git-core/ppa

if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt-add-repository -y --no-update ppa:ubuntu-toolchain-r/test
fi

#
# Install dependencies
#

apt update

run_as_original_user pip3 install --upgrade pip setuptools
run_as_original_user pip3 install conan==1.31

# Developer tools
apt install -yq clang-format-10 clang-tidy-10 doxygen graphviz

# github actions require a more recent git
apt upgrade git

# Minimal build environment
apt install -yq --allow-downgrades ccache cmake=3.18.4-0kitware1 cmake-data=3.18.4-0kitware1 python3-pip

# Toolchain variants
if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt install -yq gcc-9 g++-9 clang-10 clang++-10
fi

# Library dependencies
#
# Install llvm via apt instead of as a conan package because existing
# conan packages do yet enable RTTI, which is required for boost
# serialization.
apt install -yq \
  libarrow100 \
  libarrow-dev=1.0.1-1 \
  libcypher-parser-dev \
  libopenmpi-dev \
  libparquet100 \
  libparquet-dev=1.0.1-1 \
  libxml2-dev \
  llvm-10-dev \
  uuid-dev
