#!/bin/bash
#
# This script is intended to run a Docker cross compilation image like
# https://github.com/dockcross/dockcross that has various python installations
# under /opt/python as well as a standard C/C++ compilation toolchain.
set -e -x

SOURCE_DIR="${SOURCE_DIR:-/work}"
OUTPUT_DIR="${OUTPUT_DIR:-/output}"

BASE="$(cd $(dirname $0); pwd)"
PLAT=manylinux2014_x86_64
LEAST_PYBIN=/opt/python/cp35-cp35m/bin

yum install -y -q \
  ccache \
  devtoolset-9-libatomic-devel \
  llvm7.0-devel \
  llvm7.0-static \
  openmpi-devel

ln -s "${LEAST_PYBIN}/conan" /usr/local/bin/conan
"${BASE}/setup_conan.sh"

conan install -if /tmp/conan --build=missing "${SOURCE_DIR}/config"

for pybin in /opt/python/*/bin; do
  if [[ -f "${SOURCE_DIR}/dev-requirements.txt" ]]; then
    "${pybin}/pip" install -r "${SOURCE_DIR}/dev-requirements.txt"
  fi

  # Ideally, we should pass these arguments simply as "pip wheel
  # --build-option=-D..."; however, --build-option disables the use of binary
  # wheels for our own dependencies (cython, cmake) and forces their
  # recompilation from source. Side step this issue by passing build arguments
  # via the environment.
  export GALOIS_CMAKE_ARGS="\
    -DCMAKE_TOOLCHAIN_FILE=/tmp/conan/conan_paths.cmake \
    -DCMAKE_PREFIX_PATH=/usr/lib64/llvm7.0 \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DUSE_ARCH=none \
    "

  "${pybin}/pip" wheel -w /tmp/wheelhouse "${SOURCE_DIR}"

  unset GALOIS_CMAKE_ARGS
done

# Extract any included .so files so we can help auditwheel find them.
# TODO: This is a total hack. The .so files should probably be built separately.
#  This hack actually results in the .so files appearing twice in the resulting whl.
(
    mkdir /tmp/wheelhouse/lib
    cd /tmp/wheelhouse/lib
    for whl in /tmp/wheelhouse/*.whl; do
        # TODO: This will overwrite .so files with the same name in multiple wheels.
        #  I am assuming that this is safe since any data .so files are python version independent.
        unzip -oj "$whl" '*.data/data/lib/*.so'
    done
)

# Provide the build library path to auditwheel so it can properly add libgalois_shmem.so
export LD_LIBRARY_PATH=/tmp/wheelhouse/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

# Add and patch .so files required by the python extensions in the whl
for whl in /tmp/wheelhouse/*.whl; do
  auditwheel repair "$whl" --plat ${PLAT} -w "${OUTPUT_DIR}"
done

"${LEAST_PYBIN}/python" "${SOURCE_DIR}/setup.py" sdist -d "${OUTPUT_DIR}"
