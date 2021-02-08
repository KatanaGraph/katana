# Configuration file for the Sphinx documentation builder.
# Setting that vary are in setup.py

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here.

import pathlib
import sys

# Find the build directory relative to this file (this works because we are doing an in-tree build.
build_dir = pathlib.Path(__file__).parent.parent.parent / "build"
# Find the cython "lib" tree which ahs all the files we need.
lib_dirs = list(build_dir.glob("lib.*"))
if len(lib_dirs) > 1:
    print(f"WARNING: There are multiple lib directories in the python build tree. Picking {lib_dirs[0]}")
elif not lib_dirs:
    raise ValueError(f"Could not find library directory in {build_dir}")
lib_dir = lib_dirs[0]
# Add the lib path to the python search path.
sys.path.insert(0, str(lib_dir))

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.intersphinx",
    #'sphinx.ext.autosummary',
    "sphinx.ext.autodoc",
    # 'sphinx_autodoc_typehints',
    "sphinx.ext.doctest",
    "sphinx.ext.todo",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"
# html_theme_path = []

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
