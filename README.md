Overview
========

[![C/C++ CI](https://github.com/KatanaGraph/katana/actions/workflows/cpp.yaml/badge.svg?branch=master)](https://github.com/KatanaGraph/katana/actions/workflows/cpp.yaml?query=branch%3Amaster)
[![Python CI](https://github.com/KatanaGraph/katana/actions/workflows/python.yaml/badge.svg?branch=master)](https://github.com/KatanaGraph/katana/actions/workflows/python.yaml?query=branch%3Amaster)

Katana is a library and set of applications to work with large graphs efficiently.

Highlights include:

- Parallel graph analytics and machine learning algorithms
- Multithreaded execution with load balancing and excellent
  scalability on multi-socket systems
- Interfaces to build your own algorithms
- Scalable concurrent containers such as bag, vector, list, etc.

Katana is released under the BSD-3-Clause license.

Installing
==========

The easiest way to get Katana is to install the Conda packages. See the
[Conda User Guide](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
for details on how to install Conda.

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

Then, install Katana

```bash
conda install -c katanagraph/label/dev -c conda-forge katana-python
```

Currently, only development versions are available through Conda. These builds
reflect the current state of the `master` branch. Releases of stable versions
will be available in the future.

Alternatively, you can build Katana from source.

Building from Source
====================

See the [build instructions](docs/contributing/building.rst) for details.

Dependencies
------------

Katana is tested on 64-bit Linux, specifically Ubuntu LTS 20.04.

At the minimum, Katana depends on the following software:

- A modern C++ compiler compliant with the C++-17 standard (gcc >= 7, Intel >= 19.0.1, clang >= 7.0)
- CMake (>= 3.17)
- Boost library (>= 1.58.0, we recommend building/installing the full library)
- libllvm (>= 7.0 with RTTI support)
- libfmt (>= 4.0)
- libxml2 (>=  2.9.1)
- libarrow (>= 6.0)
- libuuid (>= 2.31.1)
- [nlohmann/json](https://github.com/nlohmann/json) (>= 3.7.3)

Here are the dependencies for the optional features:

- Linux HUGE_PAGES support (please see [www.kernel.org/doc/Documentation/vm/hugetlbpage.txt](https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt)). Performance will most likely degrade without HUGE_PAGES
  enabled. Katana uses 2MB huge page size and relies on the kernel configuration to set aside a large amount of 2MB pages. For example, our performance testing machine (4x14 cores, 192GB RAM) is configured to support up to 65536 2MB pages:
  ```Shell
  cat /proc/meminfo | fgrep Huge
  AnonHugePages:    104448 kB
  HugePages_Total:   65536
  HugePages_Free:    65536
  HugePages_Rsvd:        0
  HugePages_Surp:        0
  Hugepagesize:       2048 kB
  ```

- libnuma support. Performance may degrade without it. Please install
  libnuma-dev on Debian like systems, and numactl-dev on Red Hat like systems.
- Doxygen (>= 1.8.5) for compiling documentation as webpages or latex files
- PAPI (>= 5.2.0.0 ) for profiling sections of code
- Vtune (>= 2017 ) for profiling sections of code
- Eigen (3.3.1 works for us) for some matrix-completion app variants

Documentation
=============

To build documentation for this project, see the documentation
[instructions](docs/contributing/documentation.rst).

Using Katana as a Library
=========================

There are two common ways to use Katana as a library. One way is to copy this
repository into your own CMake project, typically using a git submodule. Then
you can put the following in your `CMakeLists.txt`:

```CMake
add_subdirectory(katana EXCLUDE_FROM_ALL)
add_executable(app ...)
target_link_libraries(app Katana::graph)
```

The other common method is to install Katana outside your project and import it
as a CMake package.

The Katana CMake package is available through the katana-cpp Conda package,
which is a dependency of the katana-python Conda package. You can install both
with:

```Shell
conda install -c katanagraph/label/dev -c conda-forge katana-python
```

Alternatively, you can install Katana from source. The following command will
build and install Katana into `INSTALL_DIR`:

```Shell
cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR $SRC_DIR
make install
```

With Katana installed either from a package or from source, you can put something
like the following in your `CMakeLists.txt`:

```CMake
list(APPEND CMAKE_PREFIX_PATH ${INSTALL_DIR})
find_package(Katana REQUIRED)
add_executable(app ...)
target_link_libraries(app Katana::graph)
```

If you are not using CMake, the corresponding basic commands (although the
specific commands vary by system) are:

```Shell
c++ -std=c++14 app.cpp -I$INSTALL_DIR/include -L$INSTALL_DIR/lib -lkatana_graph
```

Contact Us
==========

For bugs, please raise an
[issue](https://github.com/KatanaGraph/katana/issues) on
GiHub.

If you file an issue, it would help us if you sent (1) the command line and
program inputs and outputs and (2) a core dump, preferably from an executable
built with the debug build.

You can enable core dumps by setting `ulimit -c unlimited` before running your
program. The location where the core dumps will be stored can be determined with
`cat /proc/sys/kernel/core_pattern`.

To create a debug build, assuming you will build Katana in `BUILD_DIR` and the
source is in `SRC_DIR`:

```Shell
cmake -S $SRC_DIR -B $BUILD_DIR -DCMAKE_BUILD_TYPE=Debug
make -C $BUILD_DIR
```

A simple way to capture relevant debugging details is to use the `script`
command, which will record your terminal input and output. For example,

```Shell
script debug-log.txt
ulimit -c unlimited
cat /proc/sys/kernel/core_pattern
make -C $BUILD_DIR <my-app> VERBOSE=1
my-app with-failing-input
exit
```

This will generate a file `debug-log.txt`, which you can supply when opening a
GitHub issue.
