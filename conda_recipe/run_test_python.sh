#!/bin/sh
set -e

echo "Testing Jupyter..."
jupyter nbconvert --execute --to markdown python/examples/jupyter/jaccard_numba.ipynb

echo "Testing with pytest..."
pytest -v python/test
