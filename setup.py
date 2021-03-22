import sys

assert sys.version_info.major >= 3 and sys.version_info.minor >= 6, "Katana requires Python 3.6 or greater"

import pathlib

sys.path.append(str(pathlib.Path.cwd() / "python"))
import katana_setup


def package_setup():
    # Following PEP-518, use pyproject.toml instead of setup(setup_requires=...) to
    # specify setup dependencies.
    katana_setup.setup(source_dir="python", package_name="katana", doc_package_name="Katana Python")


if __name__ == "__main__":
    package_setup()
