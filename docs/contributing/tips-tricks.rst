.. _tips-and-tricks:

=======================
Helpful Tips and Tricks
=======================

Some Helpful Commands
=====================

.. code-block::

   make -C <some subdirectory in the build tree>

This builds only the targets defined in that subdirectory (and their
dependencies). This is useful for compiling a single application or library at
a time.

.. code-block::

   make -j 4

This launches ``4`` jobs to build the system concurrently.

.. code-block::

   make VERBOSE=1

This prints out the commands that are run. This is useful for debugging
installation issues.

.. code-block::

   ctest -L ci

This runs code tests locally on your machine.  Most of the tests require sample
graphs. You can get them with ``make input``. See :ref:`testing` for more
details.

Helpful Tools
=============

`ccmake <https://cmake.org/cmake/help/v3.0/manual/ccmake.1.html>`_: a simple
terminal UI for configuring cmake projects. This is helpful for setting
various build options without having to remember their names exactly.

`ccache <https://ccache.dev/>`_: a wrapper around compilation commands to reuse
object files even if their timestamps change as long as the contents of the
source file and compiler flags are the same. To enable use: ``cmake
-DCMAKE_CXX_COMPILER_LAUNCHER=ccache``

The LLVM project provides many useful development tools for C++:
`clang-format <https://clang.llvm.org/docs/ClangFormat.html>`_,
`clang-tidy <https://clang.llvm.org/extra/clang-tidy/>`_, and
`clangd <https://clangd.llvm.org/>`_.  Most IDEs can be configured to use these
tools automatically.
