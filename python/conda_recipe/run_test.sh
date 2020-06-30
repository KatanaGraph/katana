#!/bin/sh
jupyter nbconvert --execute --to markdown test_notebook.ipynb || exit 1
