import sys

assert sys.version_info.major >= 3 and sys.version_info.minor >= 6, "Katana requires Python 3.6 or greater"

import pathlib

sys.path.append(str(pathlib.Path.cwd() / "python"))
import katana_setup


def package_setup():
    # Following PEP-518, use pyproject.toml instead of setup(setup_requires=...) to
    # specify setup dependencies.
    katana_setup.setup(source_dir="python", package_name="katana")


if __name__ == "__main__":
    package_setup()


# This project can generate a pip package, but it's bad and is missing dependencies. If you must generate it, run
# `make katana_python_wheel` in your build directory. To install it:
#
#     Install Katana native library
# conda install -c katanagraph/label/dev katana-cpp
#     Make sure we are the correct Python version
# conda install python==3.8
#     Install Katana Python Conda dependencies (due to problems in the pip pyarrow package we must use the conda pkg)
# conda install pyarrow==2
#     Install Katana Python pip dependencies (using what we can from pip)
# pip install numba
#     Install Katana Python pip package
# pip install <katana_python package .whl>
