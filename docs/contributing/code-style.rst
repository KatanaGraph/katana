==========
Code Style
==========

We automatically test for some code style adherence at merge-time; so the
automatic tools have the final word. Nevertheless, there are some conventions
enforced by maintainers to be aware of:

* ``TODO``/``FIXME``/``XXX`` are common comments used to denote known issues that
  should be fixed soon. In this code base, ``XXX`` is reserved for local changes
  that the author intends to fix immediately, i.e., comments with ``XXX`` should
  never be committed. ``TODO`` and ``FIXME`` should only be used with the name
  of a contributor who is responsible for fixing them (usually you)
  in the style: ``TODO(thunt)``. In general ``TODO`` tasks involve more work to fix
  than ``FIXME`` tasks but the distinction isn't enforced.

* Dates in comments should always take the form ``YYYY-MM-DD``.

In general, we adhere to the `Google C++ Style Guide
<https://google.github.io/styleguide/cppguide.html>`_.

One exception (so far) is that when passing arguments to functions, prefer
pointers to non-const references (which used to be a part of that guide).

In addition to the guidelines in the Google Style Guide, which are quite
general, here is some specific guidance:

* When using the deprecated C++ attribute also provide an upgrade path to users
  to avoid the deprecated functionality. This can be indicated in the attribute
  itself, e.g., ``[[deprecated("use Foo instead")]]``, or in the documentation
  for the deprecated function.

The following are the automated style checkers that we use. The continuous
integration process will check adherence on each Pull Request, but it is faster,
in terms of feedback latency, to run these checks locally first.

* ``scripts/check_docs.sh``: ensures that you didn't break doc generation

* ``scripts/check_ifndef.py [-fix] lib*``: checks that header guards are well
  formed.

* ``scripts/check_cpp_format.sh [-fix] lib*``: applies ``clang-format`` to check style.

* ``scripts/check_go_format.sh [-fix] .``: applies ``gofmt`` to format go code.

* ``scripts/check_go_lint.sh .``: applies ``golangci-lint`` to check check style.

* ``scripts/check_python_format.sh .``: applies ``black`` to format python code.

* ``scripts/check_python_lint.sh .``: applies ``pylint`` to format python code.

* ``pydocstyle --config=./pyproject.toml .``: applies ``pydocstyle`` to check
  for correctly styled docstrings in Python *files*.

We also have a ``clang-tidy`` configuration, which is a useful tool for checking
your code locally. ``clang-tidy`` is not currently enforced by continuous
integration.

None of these checks are exhaustive (on their own or combined)!

New code is expected to follow the above guidelines. But also be aware: there
are older modules that predate these expectations (currently, older modules are
everything that is not ``libtsuba`` and ``libsupport``). In those cases,
it is acceptable to follow the convention in that module; though in general, it
would be preferable to follow the motto of "leave the codebase in better shape
than you found it."
