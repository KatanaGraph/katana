#!/bin/bash
# TODO(amp):REQUIREMENTS: Versions should be taken from katana_requirements or a lock file.

set -eu

ORIGINAL_USER=${ORIGINAL_USER:-${SUDO_USER:-}}

run_as_original_user() {
  if [[ -n "${ORIGINAL_USER}" ]]; then
    su - "${ORIGINAL_USER}" bash -c "$*"
  else
    bash -c "$*"
  fi
}

#
# Add custom repositories
#
curl -fL --output /etc/apt/preferences.d/cuda-repository-pin-600 https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/cuda-ubuntu1804.pin
apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub
apt-add-repository -y --no-update "deb http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/ /"

#
# Install dependencies
#

apt update

# Minimal build environment
apt install -yq cuda-minimal-build-10-1 cuda-curand-dev-10-1 cuda-nvml-dev-10-1

# libava dependencies
apt install -yq libclang-7-dev clang-7 indent libglib2.0-dev libssl-dev
run_as_original_user "pip3 install 'toposort>=1.5' 'blessings>=1.6' 'astor>=0.7'"
