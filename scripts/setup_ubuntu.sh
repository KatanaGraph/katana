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
  # The second "$@" is expanded in this script and becomes the arguments to a
  # bash script `"$@"` that simply executes its command-line arguments. This
  # protects characters like < from being interpreted as redirections if $@
  # were expanded directly.
  if [[ -n "${ORIGINAL_USER}" ]]; then
    su -s /bin/bash - "${ORIGINAL_USER}" bash -c '"$@"' -- "$@"
  else
    bash -c '"$@"' -- "$@"
  fi
}

#
# Install bootstrap requirements
#
apt install -yq curl software-properties-common

NO_UPDATE=""
if apt-add-repository  --help | grep -q -e "--no-update"; then
  NO_UPDATE="--no-update"
fi

#
# Add custom repositories
#
curl -fL https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add -
apt-add-repository -y $NO_UPDATE "deb https://apt.kitware.com/ubuntu/ ${RELEASE} main"

add-apt-repository -y $NO_UPDATE ppa:git-core/ppa

# Clang isn't present in the xenial repos so we need to add the repo
VERSION=$(lsb_release --release --short | cut -d . -f 1)
if [ "$VERSION" == "16" ]
then
  curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
  apt-add-repository -y $NO_UPDATE "deb http://apt.llvm.org/${RELEASE}/ llvm-toolchain-${RELEASE}-10 main"
fi

if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt-add-repository -y $NO_UPDATE ppa:ubuntu-toolchain-r/test
fi

#
# Install dependencies
#

apt update

# Install pip3
apt install -yq --allow-downgrades  python3-pip

# TODO(amp): Drop 3.6 support, this will require constant updates as other packages drop 3.6.
# Check Python version
if python3 -c 'import sys; sys.exit(not (sys.version_info[0] >= 3 and sys.version_info[1] >= 6))'; then
  PIP_VERSION=""
  SETUPTOOLS_VERSION=""
else
  # If we have Python < 3.6 limit versions. Support for pre-3.6 was dropped in these packages.
  PIP_VERSION="<=20.3.4"
  SETUPTOOLS_VERSION="<=50.3.2"
fi

run_as_original_user pip3 install --upgrade "pip$PIP_VERSION" "setuptools$SETUPTOOLS_VERSION"
run_as_original_user pip3 install conan==1.33

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
  apt install -yq gcc-9 g++-9 clang-10
  # in newer versions of ubuntu clang++-10 is installed as a part of clang-10
  if [ "$VERSION" == "16" ]
  then
    apt install clang++-10
  fi
then
fi
