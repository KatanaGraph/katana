#!/bin/bash

set -xeuo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.."; pwd)"

# Check the options
INSTALL_CUDA=
if [ "${1-}" == "--with-cuda" ]; then
  INSTALL_CUDA=1
  shift 1
fi

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
VERSION=$(lsb_release --release --short | cut -d . -f 1) # numerical major version, 18, 20, etc

# Supported versions: 18, 20, 21

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
    export SHELL=/bin/bash
    sudo -s -u "${ORIGINAL_USER}" bash -c '"$@"' -- "$@"
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


if [[ "$VERSION" == "18" || "$VERSION" == "20" ]]; then
    # clang-12 isn't present in the repos for any Ubuntu release up to 20.04
    curl -fL https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
    apt-add-repository -y $NO_UPDATE "deb http://apt.llvm.org/${RELEASE_CODENAME}/ llvm-toolchain-${RELEASE_CODENAME}-12 main"
fi

# Clang-10 isn't present in the hirsute repos any more, so we need to add the upstream repo
if [ "$VERSION" == "21" ]; then
  apt-add-repository -y $NO_UPDATE "deb http://apt.llvm.org/${RELEASE_CODENAME}/ llvm-toolchain-${RELEASE_CODENAME}-10 main"
fi

if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt-add-repository -y $NO_UPDATE ppa:ubuntu-toolchain-r/test
fi

if [ "$VERSION" == "21" ]; then
    # TODO(amp):REQUIREMENTS: Versions should be taken from katana_requirements or a lock file.
    # no hirsute aka 21.04 release of apache arrow:4.0.0.1 use focal one instead
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
# Install packages required by the requirements tool
apt install --yes --quiet python3-yaml python3-packaging

if [ "$VERSION" == "18" ]; then
  PKG_SYS_SUFFIX="-18.04"
else
  PKG_SYS_SUFFIX=""
fi
# Use the requirements tool to install apt packages with: apt-get satisfy --allow-downgrades --yes --quiet
"$REPO_ROOT"/scripts/requirements install --arg=--allow-downgrades --arg=--yes --arg=--quiet --label apt --label apt/dev --packaging-system "apt$PKG_SYS_SUFFIX"
# Use the requirements tool to install pip packages with: python3 -m pip --upgrade
run_as_original_user "$REPO_ROOT"/scripts/requirements install --label pip --label pip/dev --packaging-system "pip$PKG_SYS_SUFFIX"

# Toolchain variants
if [[ -n "${SETUP_TOOLCHAIN_VARIANTS}" ]]; then
  apt install -yq gcc-9 g++-9 clang-10 clang-12
fi

# --no-binary is required to cause the pip package to use the debian package's native binaries.
# https://lists.apache.org/thread.html/r4d2e768c330b6545649e066a1d9d1846ca7a3ea1d97e265205211166%40%3Cdev.arrow.apache.org%3E
PYARROW_WITH_PARQUET=1 run_as_original_user "$REPO_ROOT"/scripts/requirements install --arg=--upgrade --arg=--no-binary -apyarrow  --label deb/pip-no-binary --packaging-system pip

# Maybe install CUDA
if [ -n "${INSTALL_CUDA}" ]; then
  # TODO(amp):REQUIREMENTS: Versions should be taken from katana_requirements or a lock file.
  curl https://developer.download.nvidia.com/compute/cuda/11.5.0/local_installers/cuda_11.5.0_495.29.05_linux.run \
    --output /tmp/cuda_11.5.0_495.29.05_linux.run
  sh /tmp/cuda_11.5.0_495.29.05_linux.run --silent --toolkit
fi

if [ "$VERSION" == "18" ]; then
  echo "WARNING: Katana Python bindings will not to work fully due to an incorrect installation of numba. You may be able to get it to install manually."
fi
