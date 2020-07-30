#!/bin/sh
set -e

echo "Testing Jupyter..."
jupyter nbconvert --execute --to markdown test_notebook.ipynb

echo "Testing with pytest..."
pytest -v
