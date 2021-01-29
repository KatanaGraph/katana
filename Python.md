Overview
========

![Conda Package CI](https://github.com/KatanaGraph/katana/workflows/Conda%20Package%20CI/badge.svg)

The Katana-Python interface allows Python programs to utilize Galois for high-performance parallelism.
The parallel loops execute Python functions compiled with [Numba](https://numba.pydata.org/).
Parallel optimized data structures provided by Galois are also exposed to Python along with atomic operations for optimized reductions.

Currently, Katana-Python only supports 64-bit Linux (Primary testing is on Ubuntu LTS).
Other OS support will become available eventually.

Installing Katana-Python
------------------------

Install [libnuma](https://github.com/numactl/numactl) as required in your distribution
(`sudo apt install libnuma1` for Ubuntu or Debian, `sudo yum install numactl` for CentOS or RHEL).

[Install conda](https://docs.conda.io/en/latest/miniconda.html) if needed. 
See the [Conda User Guide](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) for more details. 

```Shell
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

You will need to log out and back in again to get conda properly configured.
Then create an environment (or use one you already have) and activate it:

```Shell
conda create -n katana-env
conda activate katana-env
```
(Replace `katana-env` with any name you like.)

Then install the `katana-python` package from Katanagraph in your environment.
The `katanagraph` channel contains packages based on the latest Katanagraph release (or snapshots before we do that first release).

```Shell
conda install -c conda-forge -c katanagraph katana-python
```

(You can access a more up to date, but less stable, Galois by adding `-c katanagraph/label/dev`.)


Once the package and its dependencies are installed you can run the Galois utilities (e.g., `graph-properties-convert`).
The installation does not include the Lonestar applications.

You can run the Katana-Python applications using command like this:

```Shell
python -m katana.pagerank_property_graph <path/to/property/graph> \
    -t 16 --maxIterations=10 --tolerance=0.000001
```

For more information about how to invoke each application, invoke the application with `--help`:

```Shell
python -m katana.sssp_property_graph --help
```

The Katana-Python package includes connected components, Jaccard similarity, k-core, Pagerank, and SSSP.
The arguments of these applications are similar to those of the Lonestar applications.
The Python version can also be imported (`from katana.pagerank_property_graph import pagerank_pull_sync_residual`) so that the algorithm can be used from other Python code.


Building Katana-Python
----------------------

Install [libnuma](https://github.com/numactl/numactl) development files as required in your distribution
(`sudo apt install libnuma-dev` for Ubuntu or Debian, `sudo yum install numactl-devel` for CentOS or RHEL).

The easiest way to setup a Katana-Python build and development environment is Conda.
So first, install Conda, as above.
Then create and activate the development environment:

```Shell
SRC_DIR=<repo/root>
conda env create --name katana-dev --file $SRC_DIR/conda_recipe/conda-dev-environment.yaml
conda activate katana-dev
```
(Replace `katana-dev` with any name you like.)

To build Katana-Python, change to the repository root directory and run:

```Shell
python setup.py build
```

This will build Katana-Python and place the artifacts in `$SRC_DIR/_skbuild/linux-x86_64-3.8/cmake-install/`.
If you wish to use these artifacts directly you can set the search paths as follows:

```Shell
export LD_LIBRARY_PATH=$SRC_DIR/_skbuild/linux-x86_64-3.8/cmake-install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export PYTHONPATH=$SRC_DIR/_skbuild/linux-x86_64-3.8/cmake-install/python${PYTHONPATH:+:$PYTHONPATH}
```
It may be useful to put these commands in a script to run easily when restarting your development environment.
The `3.8` will need to be changed to the appropriate Python version number.
If you want to build C++ applications or libraries against the `libgalois` built here, you will need to set a couple more environment variables:

```Shell
export LIBRARY_PATH=$SRC_DIR/_skbuild/linux-x86_64-3.8/cmake-install/lib${LIBRARY_PATH:+:$LIBRARY_PATH}
export CPLUS_INCLUDE_PATH=$SRC_DIR/_skbuild/linux-x86_64-3.8/cmake-install/include${CPLUS_INCLUDE_PATH:+:$CPLUS_INCLUDE_PATH}
```


Building Katana-Python Conda Packages
-------------------------------------

To build the `katana` and `katana-python` packages in the development environment (created above), run:

```Shell
conda build -c katanagraph -c conda-forge $SRC_DIR/conda_recipe/
conda build -c local -c katanagraph -c conda-forge $SRC_DIR/python/conda_recipe/
```
(*WARNING:* This takes a while.)

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
