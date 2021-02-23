Overview
========

[![Python CI](https://github.com/KatanaGraph/katana/actions/workflows/python.yaml/badge.svg?branch=master)](https://github.com/KatanaGraph/katana/actions/workflows/python.yaml?query=branch%3Amaster)

The Katana-Python interface allows Python programs to utilize Katana for high-performance parallelism.
The parallel loops execute Python functions compiled with [Numba](https://numba.pydata.org/).
Parallel optimized data structures provided by Katana are also exposed to Python along with atomic operations for optimized reductions.

Currently, Katana-Python only supports 64-bit Linux (Primary testing is on Ubuntu LTS 20.04).
Other OS support will become available eventually.


Building Katana-Python
----------------------

Install [libnuma](https://github.com/numactl/numactl) development files as required in your distribution
(`sudo apt install libnuma-dev` for Ubuntu or Debian, `sudo yum install numactl-devel` for CentOS or RHEL).

The easiest way to setup a Katana-Python build and development environment is Conda.
[Install conda](https://docs.conda.io/en/latest/miniconda.html) if needed.
See the [Conda User Guide](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) for more details.

```Shell
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

You will need to log out and back in again to get conda properly configured.
Then create and activate the development environment:

```Shell
SRC_DIR=<repo/root>
conda config --add channels conda-forge
conda env create --name katana-dev --file $SRC_DIR/conda_recipe/environment.yml
conda activate katana-dev
```

To build Katana-Python, use CMake as for the [C++ build](README.md) and add `-DKATANA_LANG_BINDINGS=python` to the `cmake` command. The steps are repeated below. See [README.md](README.md) for details.
Let's assume that `SRC_DIR` is the directory where the source code for Katana
resides, and you wish to build Katana in some `BUILD_DIR`.

```Shell
SRC_DIR=`pwd` # Or top-level Katana source dir
BUILD_DIR=<path-to-your-build-dir>

mkdir -p $BUILD_DIR
cmake -S $SRC_DIR -B $BUILD_DIR -DCMAKE_BUILD_TYPE=Release -DKATANA_LANG_BINDINGS=python
```

This will build Katana-Python and place the artifacts in `$BUILD_DIR/katana_python_build/build/lib.*`.
If you wish to use these artifacts directly, you can use a script that is also generated, `$BUILD_DIR/python_env.sh`. You can either use this script as a launcher, `$BUILD_DIR/python_env.sh python`, or source it into your shell, `. $BUILD_DIR/python_env.sh`, to enable all Python programs in that shell to access the `katana` package.


Building Katana-Python Conda Packages
-------------------------------------

To build the `katana` and `katana-python` packages in the development environment (created above), run:

```Shell
conda build -c conda-forge $SRC_DIR/conda_recipe/
```
(*WARNING:* This takes a long time, as much as 45 minutes.)

The `conda build` commands will run some simple tests on the packages and will fail if the tests fail.
After each package builds successfully, `conda build` will print the path to the package. You can install it using:

```Shell
conda install <path/to/package>
conda install -c conda-forge -c katanagraph <package-name>
```
(where the `<path/to/package>` is the path printed by `conda build` and `<package-name>` is `katana` or `katana-python`.)

The second `conda install` works around a bug in conda by forcing the installation of dependencies;
conda fails to install dependencies when a package is installed from a local path.
This second command will eventually no longer be needed, but should be harmless.

The CI also builds conda packages for each commit and they can be accessed by downloading the artifact `conda-pkgs-*` and unzip it.
The packages will be in a subdirectory such as `linux-64`.
They will be the only `.tar.bz2` files in the artifact.
Once extracted you can install them the same way locally built packages can be installed (shown above).

You can upload development conda packages (i.e., release candidates or testing packages) to your Anaconda channel using the anaconda client (install `anaconda-client`):

```Shell
anaconda upload --label dev <path to package>
```

To upload a non-development packages remove `--label dev`.
The same commands can be used to upload to the `katanagraph` channel if you have the credentials.
