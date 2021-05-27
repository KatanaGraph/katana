import os
import pathlib

NOTEBOOK_DIR = pathlib.Path(__file__).parent.parent / "examples"


def pytest_generate_tests(metafunc):
    notebooks = []
    ids = []
    for root, _, files in os.walk(NOTEBOOK_DIR):
        ext = ".ipynb"
        for filename in files:
            if not filename.endswith(ext):
                continue
            notebooks.append(pathlib.Path(root, filename))
            ids.append(filename[: -len(ext)])

    metafunc.parametrize("notebook", notebooks, ids=ids)


def test_notebooks(nb_regression, notebook):
    nb_regression.check(str(notebook))
