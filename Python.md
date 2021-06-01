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
conda install numactl-devel-cos6-x86_64 # For x86_64 builds
```

*Do not use Conan*. Conan packages are incompatible with Conda.
If you are building `katana-enterprise`, see `Python.md` in that repository and install additional conda packages.

To build Katana-Python, use CMake as for the [C++ build](README.md) and add `-DKATANA_LANG_BINDINGS=python` to the `cmake` command. The steps are repeated below. See [README.md](README.md) for details.
Let's assume that `SRC_DIR` is the directory where the source code for Katana
resides, and you wish to build Katana in some `BUILD_DIR`.

```Shell
BUILD_DIR=<path-to-your-build-dir>
mkdir -p $BUILD_DIR
cmake -S $SRC_DIR -B $BUILD_DIR -DCMAKE_BUILD_TYPE=Release -DKATANA_LANG_BINDINGS=python
```

This will build Katana-Python and place the artifacts in `$BUILD_DIR/katana_python_build/build/lib.*`.
If you wish to use these artifacts directly, you can use a script that is also generated, `$BUILD_DIR/python_env.sh`. You can either use this script as a launcher, `$BUILD_DIR/python_env.sh python`, or source it into your shell, `. $BUILD_DIR/python_env.sh`, to enable all Python programs in that shell to access the `katana` package.

The Katana Python interface only supports shared library builds.
This is because Katana libraries (e.g., `libgalois`) must be shared between the Python extensions.


Building Documentation
----------------------

To build the C++ (Doxygen) and Python (Sphinx) documentation, add `-DKATANA_DOCS=ON` to the `cmake` command making the full command:

```shell
cmake -S $SRC_DIR -B $BUILD_DIR -DCMAKE_BUILD_TYPE=Release -DKATANA_LANG_BINDINGS=python -DKATANA_DOCS=ON
```

Then make the documentation using:

```shell
cd $BUILD_DIR
make docs
```
(This must build the Python packages to generate their documentation.
You can regenerate the documentation in the same directory by rerunning `make docs` to avoid having to fully rebuild the library.)

The C++ documentation will be in `$BUILD_DIR/docs/cxx`, and the documentation for the open-source Python package will be in `$BUILD_DIR/docs/katana_python`.

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

Conda Performance
-----------------

Conda is slow to install packages.
This makes installing a new development take a few minutes.
More importantly, it makes conda package building very slow (~40 minutes for this repository), because the build process installs at least 7 conda environments.
This can be mitigated by using [Mamba](https://github.com/mamba-org/mamba#the-fast-cross-platform-package-manager).
Mamba is a (mostly) drop-in replacement for the `conda` command that uses a native dependency solver and reduces installation time by 2x in many cases.
However, Mamba is not as stable or well tested as Conda and does not have the same level of support.

To use Mamba, install it in your conda environment with `conda install mamba`.
Then you can use `mamba install` as a drop-in replacement for `conda install`, and similarly for `mamba env create` and `mamba env update`.
To use Mamba during conda package builds, install [Boa](https://github.com/mamba-org/boa#the-fast-conda-and-mamba-package-builder) with `mamba install boa`.
Then you can use `conda mambabuild` (*note:* the top level command is `conda`, *not* `mamba`) as a replacement for `conda build`.
(We are not using Boa proper as the package builder.)

To get a leaner, Mamba using environment in a fresh install, use [Mambaforge](https://github.com/conda-forge/miniforge#mambaforge).
It is an installer, similar to miniconda, which installs an environment with conda-forge packages and mamba pre-installed (boa must still be installed separately).
The Python CI actions use Mambaforge, [mamba](https://github.com/KatanaGraph/katana/blob/master/.github/workflows/python.yaml#L234), and [conda mambabuild](https://github.com/KatanaGraph/katana/blob/master/.github/workflows/python.yaml#L92) to improve CI performance.

Testing
-------

```bash
# Running tests
$BUILD_DIR/python_env.sh pytest python/test

# Run pytest verbosely (-v), do not capture output (-s) and select tests
# matching the filter (-k)
$BUILD_DIR/python_env.sh pytest -v -s -k my_test python/test
```

Some of the tests run a notebook and check to see if the computed output matches the output saved in the notebook.

If you want to automatically update the saved output of a notebook to match the computed output, you can pass the `--nb-force-regen` flag to update the notebook output:

```bash
$BUILD_DIR/python_env.sh pytest -k my_test --nb-force-regen python/test
```

In some cases, the output of the notebook can vary from run to run. To skip checking the output of a particular cell, you can add `nbreg` metadata to a cell in the notebook file:

```json
{
 "cell_type": "code",
 "metadata": {
   "nbreg": {
     "diff_ignore": ["/outputs/0/data/text/plain"]
   }
 },
 "outputs": [
  {
   "data": {
    "text/plain": [
     "<output to ignore>"
    ]
   }
  }
 ]
}
```

See the [pytest-notebook
documentation](https://pytest-notebook.readthedocs.io/en/latest/user_guide/tutorial_config.html)
for more options.
