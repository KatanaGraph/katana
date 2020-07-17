#!/bin/sh
echo "Testing Jupyter..."
jupyter nbconvert --execute --to markdown test_notebook.ipynb || exit 1

echo "Testing with pytest..."
pytest -v
