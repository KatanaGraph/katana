# Configuration file for the Sphinx documentation builder.

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here.

import pathlib
import os
import katana

# Breathe takes a minute or two to parse Doxygen output. If you aren't editing
# C++ documents, set this environment variable for faster edit-render loops.
cxx_disabled = os.environ.get("KATANA_DOCS_DISABLE_CXX", False)

doxygen_path = os.environ["KATANA_DOXYGEN_PATH"]

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "breathe",
    "sphinx.ext.intersphinx",
    #'sphinx.ext.autosummary',
    "sphinx.ext.autodoc",
    # 'sphinx_autodoc_typehints',
    "sphinx.ext.doctest",
    "sphinx.ext.todo",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
]

if cxx_disabled:
    breathe_projects = {}
else:
    breathe_default_project = "katana"
    breathe_projects = {"katana": str(pathlib.Path(doxygen_path))}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = "pydata_sphinx_theme"
html_logo = "_static/logo.png"
html_title = "Katana"
html_theme_options = {"show_prev_next": False}
# html_theme_path = []

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

autodoc_preserve_defaults = True
autodoc_member_order = "groupwise"


# Standard Sphinx values
project = "Katana Graph"
version = katana.__version__
release = katana.__version__
author = "Katana Graph"

# TODO(ddn): Get this from katana.libgalois.version
copyright = "Katana Graph, Inc. 2021"
