=============
Documentation
=============

Building Documentation
======================

To build the documentation, set the ``-DBUILD_DOCS=`` option to either
``internal`` or ``external`` and ``-DKATANA_LANG_BINDINGS=python`` ``cmake``
options and then make the ``docs`` build target.

.. code-block:: bash

   # internal documentation
   cmake -S $SRC_DIR -B $BUILD_DIR -DKATANA_LANG_BINDINGS=python \
      -DBUILD_DOCS=internal

   # external documentation
   cmake -S $SRC_DIR -B $BUILD_DIR -DKATANA_LANG_BINDINGS=python \
      -DBUILD_DOCS=external

   cd $BUILD_DIR
   make docs

Annotating Internal or Draft only Content
=========================================

Files ending in ``-draft.[rst/ipynb]`` or ``-internal.[rst/ipynb]`` will not be
included in external facing documentation.

Whole directories ending in ``-draft/`` will be omitted when building external
documentation.

Restructured Text
=================

Most documentation is written in Restructured Text (``.rst``) format, which is
similar to Markdown (``.md``) in spirit but has a slightly different syntax.

.. list-table:: Summary of differences between Restructured Text and Markdown

   * -
     - Restructured Text
     - Markdown
   * - Inline code
     - .. code-block::

          The ``Foo`` function
     - .. code-block::

          The `Foo` function
   * - Code blocks
     - .. code-block::

         .. code-block:: python

            def foo():
              pass

     - .. code-block::

          ```
          def foo():
            pass
          ```
   * - External links
     - .. code-block::

          Visit `my link <https://invalid/>`_

     - .. code-block::

          Visit [my link](https://invalid/)
   * - Internal links
     - .. code-block::

          .. _label:

          See :ref:`label`
     - (Not applicable)
   * - Sections
     - .. code-block::

          ====
          Part
          ====

          *******
          Chapter
          *******

          Section
          =======

          Subsection
          ----------

          Subsubsection
          ^^^^^^^^^^^^^

          Paragraphs
          """"""""""
     - .. code-block::

          # Level 1

          ## Level 2

          ### Level 3

          #### Level 4

          ##### Level 5

          ###### Level 6
   * - Conditional Include
     - .. code-block::

          .. only:: internal

             Text to only include when internal is set.

       The only condition currently defined is ``internal``. Conditional
       including has limited scope as it cannot include other directives nor
       alter the section structure. The body of the directive is always parsed,
       so references must be valid regardless of the condition.

     - (Not applicable)


- https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html

Jupyter Notebooks
=================

Guides on how to use Katana Graph in Python should be written as Jupyter
Notebooks. They will be parsed similar to Restructured Text (``.rst``) files.

Orphaned Notebooks
------------------

This means that it doesn’t appear in a toctree (see ``index.rst``),
but other pages can still link to it.

Orphaned notebookes require the following to be added to the notebook's JSON
metadata:

.. code-block:: json

   "nbsphinx": {
      "orphan": true
   },

API Documentation
=================

API documentation is a form of reference information, usually embedded in code
files, and is targeted towards people who know the general concepts in question
but need some help with details or specifics, e.g., I have a ``Foo`` object but
what can I do with it?

The Katana codebase spans three languages: C++, Python and Go, but the
principles of good API documentation are common regardless of language.

1. Be precise. Good documentation is correct documentation. If the name of a
   function captures its semantics, there is no need to add more text. If a
   function has a simple name but subtle semantics, it probably deserves a
   better name and extensive documentation.

2. Be humble. API documentation is written by the author of the code, but the
   author's assumptions are usually different than users' assumptions.

While it is possible to use markup in documentation text, e.g., C++ (Doxygen)
supports a form of Markdown and Python (Sphinx) supports Restructured Text, it
is best to keep text simple and communicate using basic text that can be read
easily without being rendered by a separate documentation tool.

C++
---

.. code-block:: cpp

   /// Foo returns the sum of a and b.
   ///
   /// Foo rounds the result away from zero. That is: if the sum is negative,
   /// Foo rounds towards negative infinity, and if sum is positive, Foo rounds
   /// towards positive infinity.
   ///
   /// As a side-effect, Foo updates an internal table of cached sums.
   ///
   /// Foo can be used to simulate arithmetic on older processors like the Bar
   /// M3000, which uses this uncommon rounding mode.
   ///
   /// This function is not safe to call concurrently.
   ///
   /// \param a The first addend
   /// \param b The second addend
   /// \return The sum of a and b
   int32_t Foo(float a, float b) {
      ...
   }

API documentation should begin with ``///`` and should appear only once per
symbol. If a symbol has a separate declaration and definition, put the API
documentation on the declaration.

In some cases, underlying Doxygen C++ parser may issues with parsing valid C++.
You can use the Doxygen macro ``DO_NOT_DOCUMENT`` to skip parsing of that
particular code block.

.. code-block:: cpp

   /// \cond DO_NOT_DOCUMENT
   WeirdCXXSyntax();
   /// \endcode DO_NOT_DOCUMENT

Python
------

.. code-block:: python

   def foo(a: float, b: float) -> int:
      """
      foo returns the sum of a and b.

      Foo rounds the result away from zero. That is: if the sum is negative,
      foo rounds towards negative infinity, and if sum is positive, foo rounds
      towards positive infinity.

      As a side-effect, foo updates an internal table of cached sums.

      Foo can be used to simulate arithmetic on older processors like the Bar
      M3000, which uses this uncommon rounding mode.

      This function is not safe to call concurrently.

      :param a: The first addend
      :param b: The second addend
      :return: The sum of a and b
      """
      ...

Go
--

.. code-block:: go

   // Foo returns the sum of a and b.
   //
   // Foo rounds the result away from zero. That is: if the sum is negative,
   // foo rounds towards negative infinity, and if sum is positive, foo rounds
   // towards positive infinity.
   //
   // As a side-effect, Foo updates an internal table of cached sums.
   //
   // Foo can be used to simulate arithmetic on older processors like the Bar
   // M3000, which uses this uncommon rounding mode.
   //
   // This function is not safe to call concurrently.
   func Foo(a, b float) int32 {
      ...
   }

The `Effective Go Guide <https://golang.org/doc/effective_go#commentary>`_ has
further discussion on best practices for comments.
