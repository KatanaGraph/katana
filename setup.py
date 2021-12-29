import sys
from distutils.extension import Extension

assert sys.version_info.major >= 3 and sys.version_info.minor >= 6, "Katana requires Python 3.6 or greater"

import pathlib

sys.path.append(str(pathlib.Path.cwd() / "python"))
import katana_setup


def package_setup():
    # Following PEP-518, use pyproject.toml instead of setup(setup_requires=...) to
    # specify setup dependencies.
    katana_setup.setup(
        source_dir="python",
        package_name="katana",
        ext_modules=[
            katana_setup.extension(
                ("katana", "local_native"), ["EntityTypeManager.cpp", "LoadAll.cpp", "NUMAArray.cpp", "Reductions.cpp"]
            )
        ],
    )


if __name__ == "__main__":
    package_setup()


# This project can generate a pip package, but it's bad and is missing dependencies. If you must generate it, run
# `make katana_python_wheel` in your build directory. To install it:
#
#     Install Katana native library
# conda install -c katanagraph/label/dev -c conda-forge katana-cpp=={{KATANA_VERSION}}
#     Install Katana Python Conda dependencies (due to problems in the pip pyarrow package)
# conda install -c conda-forge python==3.8 pyarrow==4.0.1
#     Install Katana Python pip dependencies (using what we can from pip)
# pip install numba
#     Install Katana Python pip package
# pip install https://github.com/KatanaGraph/katana-releases/releases/download/{{KATANA_VERSION}}/\
#   katana_python-{{KATANA_VERSION}}-cp38-cp38-linux_x86_64.whl
#
#     Check that it was installed
# python -c "import katana; print(katana.__version__)"
