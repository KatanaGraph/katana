#!/bin/bash

apt install -yq ccache curl gcc-9 g++-9 libopenmpi-dev

curl -fL --output /tmp/arrow-keyring.deb https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb \
  && apt install -yq /tmp/arrow-keyring.deb \
  && rm /tmp/arrow-keyring.deb
apt update
apt install -yq libarrow-dev libparquet-dev 

apt-add-repository 'deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-10 main'
curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt update
# Install llvm via apt instead of as a conan package because existing
# conan packages do yet enable RTTI, which is required for boost
# serialization.
apt install -yq clang-10 clang++-10 clang-format-10 clang-tidy-10 llvm-10-dev

pip3 install --upgrade pip setuptools
pip3 install conan==1.24

# Install libgpu dependencies
wget http://developer.download.nvidia.com/compute/cuda/10.1/Prod/local_installers/cuda_10.1.243_418.87.00_linux.run
sudo sh cuda_10.1.243_418.87.00_linux.run --silent --toolkit --no-opengl-libs --no-man-page --no-drm

# Install libava dependencies
apt install -yq libclang-7-dev clang-7 indent libglib2.0-dev
pip3 install toposort>=1.5 blessings>=1.6 astor>=0.7
