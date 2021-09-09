#!/bin/bash

set -xeuo pipefail

#
# Install bootstrap requirements
#

apt update --quiet
apt install -yq lsb-release curl software-properties-common

NO_UPDATE=""
if apt-add-repository --help | grep -q -e "--no-update"; then
  NO_UPDATE="--no-update"
fi


RELEASE_CODENAME="$(lsb_release --codename --short)" # "xenial, focal, hirsute"
RELEASE_ID="$(lsb_release --id --short | tr 'A-Z' 'a-z')" # should always be "ubuntu"
VERSION=$(lsb_release --release --short | cut -d . -f 1) # numerical major version, 16, 18, 20, etc

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
# Add custom repositories
#
curl -fL https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add -
if [ "$VERSION" == "21" ]; then
    # no hirsute aka 21.04 apt repo yet, use focal one instead
    apt-add-repository -y $NO_UPDATE "deb https://apt.kitware.com/ubuntu/ focal main"
else
    apt-add-repository -y $NO_UPDATE "deb https://apt.kitware.com/ubuntu/ ${RELEASE_CODENAME} main"
fi

add-apt-repository -y $NO_UPDATE ppa:git-core/ppa

# clang-12 isn't present in the repos for any Ubuntu release up to 20.04
curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt-add-repository -y $NO_UPDATE "deb http://apt.llvm.org/${RELEASE_CODENAME}/ llvm-toolchain-${RELEASE_CODENAME}-12 main"

# Clang isn't present in the xenial repos so we need to add the repo
# Clang-10 isn't present in the hirsute repos any more, so we need to add the upstream repo
if [[ "$VERSION" == "16" ||  "$VERSION" == "21" ]]; then
  apt-add-repository -y $NO_UPDATE "deb http://apt.llvm.org/${RELEASE_CODENAME}/ llvm-toolchain-${RELEASE_CODENAME}-10 main"
fi

if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt-add-repository -y $NO_UPDATE ppa:ubuntu-toolchain-r/test
fi


if [ "$VERSION" == "21" ]; then
    # no hirsute aka 21.04 release of apache arrow yet, use focal one instead
    # we must also get libre2-5 from focal
    curl "https://apache.jfrog.io/artifactory/arrow/$RELEASE_ID/apache-arrow-apt-source-latest-focal.deb" \
         --output /tmp/apache-arrow-apt-source-latest.deb
    curl "http://archive.ubuntu.com/ubuntu/pool/main/r/re2/libre2-5_20200101+dfsg-1build1_amd64.deb" \
         --output /tmp/libre2-5.deb
    apt install -yq /tmp/libre2-5.deb && rm /tmp/libre2-5.deb
else
    curl "https://apache.jfrog.io/artifactory/arrow/$RELEASE_ID/apache-arrow-apt-source-latest-$RELEASE_CODENAME.deb" \
         --output /tmp/apache-arrow-apt-source-latest.deb
fi
apt install -yq /tmp/apache-arrow-apt-source-latest.deb && rm /tmp/apache-arrow-apt-source-latest.deb

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
run_as_original_user pip3 install testresources conan==1.36 PyGithub packaging

# Developer tools
#
# pkg-config is required to build pyarrow correctly (seems to be a bug)
DEVELOPER_TOOLS="clang-format-10 clang-tidy-10 doxygen graphviz ccache cmake shellcheck pkg-config clangd-10 clangd-12"
# github actions require a more recent git
GIT=git
# Library dependencies
#
# Install llvm via apt instead of as a conan package because existing
# conan packages do yet enable RTTI, which is required for boost
# serialization.
LIBRARIES="libxml2-dev
  llvm-10-dev
  llvm-12-dev
  libarrow-dev=4.0.1-1
  libarrow-python-dev=4.0.1-1
  libparquet-dev=4.0.1-1
  libnuma-dev
  python3-numpy"

apt install -yq --allow-downgrades \
  $DEVELOPER_TOOLS \
  $GIT \
  $LIBRARIES

# Toolchain variants
if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt install -yq gcc-9 g++-9 clang-10 clang-12
  # in newer versions of ubuntu clang++-NN is installed as a part of clang-NN
  if [[ "$VERSION" == "16" ]]; then
    apt install clang++-10 clang++-12
  fi
fi

# Install pip libraries that depend on debian packages

# --no-binary is required to cause the pip package to use the debian package's native binaries.
# https://lists.apache.org/thread.html/r4d2e768c330b6545649e066a1d9d1846ca7a3ea1d97e265205211166%40%3Cdev.arrow.apache.org%3E
PYARROW_WITH_PARQUET=1 run_as_original_user pip3 install --no-binary pyarrow 'pyarrow>=4.0,<5.0.0a0'
