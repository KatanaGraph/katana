#!/bin/bash
#
# This script sets up a development environment on selected Ubuntu versions.
# Feel free to use these as a starting point for your distribution of choice.

set -xeuo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.."; pwd)"

EXPECTED_RELEASES=("20.04" "18.04" "16.04")
RELEASE=$(lsb_release --release --short)

SUPPORTED_RELEASE=false
for ver in "${EXPECTED_RELEASES[@]}"; do
  if [ "$RELEASE" = "$ver" ]; then
    SUPPORTED_RELEASE=true
    break;
  fi
done

if ! $SUPPORTED_RELEASE; then
  {
    echo -n "This script was intended for one of [${EXPECTED_RELEASES[*]}]."
    echo " You have ${RELEASE}. exiting!"
  } >&2
  exit 1
fi

sudo bash -x "${REPO_ROOT}/scripts/setup_ubuntu.sh" --no-setup-toolchain-variants
"${REPO_ROOT}/scripts/setup_conan.sh"

# make clang-{tidy,format}-10 the default
sudo update-alternatives --verbose --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-10 90
sudo update-alternatives --verbose --install /usr/bin/clang-format clang-format /usr/bin/clang-format-10 90


cat <<EOF
OK, your system is almost ready to build Katana. Next:

export PATH=\$PATH:\$HOME/.local/bin  # To get conan into your PATH
cd $REPO_ROOT
mkdir build; cd build
conan install -if . $REPO_ROOT/config --build=missing
CC=gcc CXX=g++ cmake -DCMAKE_BUILD_TYPE=Release $REPO_ROOT -DCMAKE_TOOLCHAIN_FILE=conan_paths.cmake
make                 # Feel free to use -j or similar.

(You should be able to copy and paste the whole script above into your terminal.)
EOF

# --Azure cli  https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-apt
# curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
