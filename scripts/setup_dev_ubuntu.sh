#!/bin/bash
#
# This script sets up a development environment on Ubuntu 18.04 and is adapted
# from the CI scripts under .github/workflows. Feel free to adjust these
# instructions for your distribution of choice.

set -eu

EXPECTED_RELEASE="bionic"
RELEASE=$(lsb_release --codename | awk '{print $2}')

if [[ "${RELEASE}" != "${EXPECTED_RELEASE}" ]]
then
  echo "This script was intended for ${EXPECTED_RELEASE} (you have ${RELEASE}) exiting!"
  exit 1
fi

sudo apt install doxygen doxygen-doc graphviz graphviz-doc

sudo apt update
sudo apt install libxml2-dev

sudo apt update
sudo apt install -yq libcypher-parser-dev

# installing up-to-date cmake https://apt.kitware.com/
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null \
  | gpg --dearmor - \
  | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main'
sudo apt-get update
sudo apt upgrade cmake
# alternatively:
#   pip install cmake

# installing arrow and parquet keyring
curl -fL --output /tmp/arrow-keyring.deb \
  https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb \
  && sudo apt install -yq /tmp/arrow-keyring.deb \
  && rm /tmp/arrow-keyring.deb
# installing arrow and parquet
sudo apt update
sudo apt install -yq libarrow-dev libparquet-dev

# installing up-to-date llvm
sudo apt-add-repository 'deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-10 main'
curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
sudo apt update
sudo apt install -yq clang-10 clang++-10 clang-format-10 clang-tidy-10 llvm-10-dev

# make clang-{tidy,format}-10 the default
sudo update-alternatives --verbose --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-10 90
sudo update-alternatives --verbose --install /usr/bin/clang-format clang-format /usr/bin/clang-format-10 90

# install conan, without sudo should install locally $HOME/.local/bin
pip3 install conan
.github/workflows/setup_conan.sh

# If you want a build directory that is a subdir of Katana root
# mkdir build
# cd build
# cmake ../ -DGALOIS_AUTO_CONAN=on -DGALOIS_ENABLE_DIST=on
