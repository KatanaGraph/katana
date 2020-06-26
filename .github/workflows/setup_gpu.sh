#!/bin/bash

# Install libgpu dependencies
if [[ ! -d ~/.cache ]]; then
  mkdir ~/.cache
fi

wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/cuda-ubuntu1804.pin
mv cuda-ubuntu1804.pin /etc/apt/preferences.d/cuda-repository-pin-600
apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub
add-apt-repository "deb http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/ /"
apt update
apt install -yq cuda-minimal-build-10-1 cuda-curand-dev-10-1 cuda-nvml-dev-10-1

# Install libava dependencies
apt install -yq libclang-7-dev clang-7 indent libglib2.0-dev libssl-dev
pip3 install toposort>=1.5 blessings>=1.6 astor>=0.7
