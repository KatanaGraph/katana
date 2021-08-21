import os
import pathlib

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor

NOTEBOOK_DIR = pathlib.Path(__file__).parent.parent / "examples" / "notebooks"

NOTEBOOKS = [
    "jaccard_numba.ipynb",
    "Katana Tutorial.ipynb",
]


def execute_notebook(notebook_filename):
    with open(notebook_filename) as f:
        nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        # the following line will throw CellExecutionError if
        # there is any exception during the execution
        ep.preprocess(nb, {"metadata": {"path": "."}})


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_execute_notebook(notebook):
    execute_notebook(NOTEBOOK_DIR / notebook)
