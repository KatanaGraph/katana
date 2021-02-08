import sys
import pathlib

sys.path.append(str(pathlib.Path.cwd() / "python"))
import katana_setup


def package_setup():
    # Following PEP-518, use pyproject.toml instead of setup(setup_requires=...) to
    # specify setup dependencies.
    katana_setup.setup(source_dir="python", package_name="katana", doc_package_name="Katana Python")


if __name__ == "__main__":
    package_setup()
