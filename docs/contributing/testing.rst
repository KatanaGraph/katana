.. _testing:

=======
Testing
=======

Many tests require sample graphs that are too big to keep in this repository.
You can get them with ``make input``.

If you need to update the inputs, they are referenced as
https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-vN.tar.gz
in ``inputs/CMakeLists.txt`` and ``python/katana/exmaple_data.py``.
``vN`` is a monotonically increasing version number.
You can use a command ``inputs/update_inputs.sh`` to create
create a new input collection. After creating the tar file, you will need to
upload the file to the S3 bucket.

Tests are just executables created by the CMake ``add_test`` command.  A test
succeeds if it can be run and it returns a zero exit value; otherwise, the test
execution is considered a failure. A test executable itself may contain many
individual assertions and subtests.

Tests are divided into groups by their label property. A test can be given a
label with the ``set_property``/``set_tests_properties`` command, e.g.,
``set_property(TEST my-test PROPERTY LABEL my-label)``

Labels:

- **quick**: Quick tests have no external dependencies and can be run in parallel
  with other quick tests. Each quick test should run in a second or less. These
  tests are run as part of our continuous integration pipeline.

- **gpu**: Tests that require a GPU.

Running
=======

All tests can be run with ``ctest``.

To run all quick tests,

.. code-block:: bash

   ctest -L quick

To run tests whose names match ``regex``, with verbose output, and with ``4``
tests runnning in parallel at a time,

.. code-block:: bash

   ctest -R regex -V --parallel 4

To run tests with valgrind memcheck,

.. code-block:: bash

   ctest --test-action memcheck

To rerun just the tests that failed during the last test run,

.. code-block:: bash

   ctest --rerun-failed

While ``ctest`` is the main way to run C++ tests and ``ctest`` will run tests for Python code. It is
also possible to run Python tests directly with ``pytest``:

.. code-block:: bash

   # Running tests
   $BUILD_DIR/python_env.sh pytest python/test

   # Running tests in one file
   $BUILD_DIR/python_env.sh pytest python/test/my_test_file.py

   # Running one test
   $BUILD_DIR/python_env.sh pytest python/test/my_test_file.py::my_test

   # Running all tests with a given marker (similar to ctest labels)
   $BUILD_DIR/python_env.sh pytest -m my_marker python/test

   # Run pytest verbosely (-v), do not capture output (-s) and select tests
   # matching the filter (-k)
   $BUILD_DIR/python_env.sh pytest -v -s -k my_test python/test

   # Running tests with a more sophisticated filter
   # -k expressions can include python operators and can match
   # against functions, classes, or even files
   $BUILD_DIR/python_env.sh pytest -k 'my_test and not YourClass' python/test

To run Python tests on katana cluster deployed in GCP:

.. code-block:: bash

   # Run tests on katana cluster deployed in GCP
   $BUILD_DIR/python_env.sh pytest python/test --datasource gcp


Notebooks
---------

Some of the tests run a notebook and check to see if the computed output matches the output saved in the notebook.

If you want to automatically update the saved output of a notebook to match the computed output, you can pass the ``--nb-force-regen`` flag to update the notebook output:

.. code-block:: bash

   $BUILD_DIR/python_env.sh pytest -k my_test --nb-force-regen python/test

In some cases, the output of the notebook can vary from run to run. To skip checking the output of a particular cell, you can add ``nbreg`` metadata to a cell in the notebook file:

.. code-block:: json

   {
    "cell_type": "code",
    "metadata": {
      "nbreg": {
        "diff_ignore": ["/outputs/0/data/text/plain"]
      }
    },
    "outputs": [
     {
      "data": {
       "text/plain": [
        "<output to ignore>"
       ]
      }
     }
    ]
   }

See the `pytest-notebook
documentation <https://pytest-notebook.readthedocs.io/en/latest/user_guide/tutorial_config.html>`_
for more options.

Coverage
=========

Collecting coverage is enabled for Python and C++.

Python
------

Export ``COVERAGE_RCFILE`` before running the build command:

.. code-block:: bash

   export COVERAGE_RCFILE="$SRC_DIR/.coveragerc"

Once the build step is done, you can use the following sequence of
commands to run tests and obtain (html) coverage report:

.. code-block:: bash

   export COVERAGE_PROCESS_START="$COVERAGE_RCFILE"
   $BUILD_DIR/python_env.sh coverage run -m pytest python/test -s
   coverage combine
   coverage html

The output is available in ``$(pwd)/pythoncov``.

C++
---

Include the following options when running the ``cmake`` command
(i.e., when configuring your build):

.. code-block:: bash

   -DKATANA_USE_COVERAGE=ON -DCMAKE_BUILD_TYPE=Debug

After the build is done, binaries will be instrumented to collect
profiling data.

Now you can run any test that you wish.  For example:

.. code-block:: bash

   ctest -L quick

There are several tools you can use to process obtained profiling data
after test run.  One popular tool is ``gcovr``.  You can obtain (html)
report by executing the following command:

.. code-block:: bash

   cd $SRC_DIR
   gcovr -r . --html --html-medium-threshold=50


Debugging
=========

Printing and its more production-oriented cousin, logging, are simple ways to
get started with debugging, especially if you are in an environment where you
can build executables from source. Just remember to prefix your debugging messages
with an easy-to-find string like `XXX` so you can find and remove them later.

For more interactive debugging, you can use `gdb`. A typical `gdb` session looks
like this:

.. code-block::

   gdb --args application arg1 arg2 arg3
   > break SourceFile.cpp:LineNumber
   > run
   > next
   > print
   # edit some code
   > make
   > run

If you are debugging an MPI application, you can use a command like `mpirun -np
4 xterm -e gdb application` to spawn a `gdb` session for each MPI host or use
[tmpi](https://github.com/Azrael3000/tmpi) which will spawn `gdb` sessions in
`tmux` panes instead of `xterm` windows. These commands work best if all the
MPI processes are running on the same machine. If not, you will have to work
out how to open connections to each worker machine. The OpenMPI project gives
some [pointers](https://www.open-mpi.org/faq/?category=debugging), but in
practice, it is usually easier to fallback to print-statement debugging or
trying to reproduce your issue on a single host if possible.

An alternative to running a debugger is to load a core dump. Most machines
disable core dumps by default, but you can enable them with:

.. code-block::

   ulimit -c unlimited
   sudo sysctl -w kernel.core_pattern=/tmp/core-%e.%p.%h.%t

And you can load them in `gdb`:

.. code-block::

   gdb application -c core-file

Dealing with Errors in CI
=========================

If the error is due to a transient external failure, you can re-run jobs in the
GitHub UI.

When debugging a CI failure, it is good to confirm that tests pass locally in
your developer environment first. The CI runs on the merge of your PR and the
branch you want to merge with (usually master), so if you have issues
reproducing locally make sure your PR branch is up to date as well.

You can also run many of the source checks locally as well (usually
``scripts/check_*``), and most of them accept a ``-fix`` option to automatically
correct the errors they check for. Take a look at the GitHub workflow
definitions under ``.github`` directory to see what script and build parameters
are used.

Manually Controlling CI Jobs
============================

You can disable CI jobs selectively on a given PR using "magic words" in the PR
body text. All magic words are case-insensitive. Changing the magic words will
not cause jobs to run. You will need to manually trigger the jobs to runs again
either by triggering a rerun as above or by pushing a new commit.

.. list-table::

   - * Magic Word
     * Jobs Skipped
   - * ``[no test]``
     * build and test jobs
   - * ``[no package]``
     * packaging jobs
   - * ``[no Python]``
     * all Python jobs
   - * ``[no Python test]``
     * Python build and test jobs
   - * ``[no Python package]``
     * Python packaging jobs
   - * ``[no C++]``
     * all C++ jobs
   - * ``[no C++ test]``
     * C++ build and test jobs
   - * ``[no C++ package]``
     * C++ packaging jobs

Github natively supports disabling CI entirely for specific commits as
documented at:
https://docs.github.com/en/actions/guides/about-continuous-integration#skipping-workflow-runs

Caching in CI
=============

GitHub actions allows for build data to be cached between CI runs. For
reference, the caches (``actions/cache``) are scoped to
[branches](https://github.com/actions/cache#cache-scopes). The cache matching
policy is:

1. Exact key match on the current branch
2. Prefix match of a restore key on the current branch. If there are multiple
   matching keys, return the most recent entry.
3. Repeat from 1 for the default branch

Keys should be unique because once a cache entry is created it will
never be updated by ``actions/cache``.

If you need to create a cache that simply stores the latest values, create a
common prefix with a unique suffix (e.g., ``github.sha``) and use the common
prefix as a restore key. The unique key will not match any existing key but
upon lookup there will be multiple matching cache entries sharing the common
prefix, and ``actions/cache`` will return the most recent one.

One common use of ``actions/cache`` is to store a ccache cache. There is no limit
on the number of caches, but once the overall size of a cache exceeds 5 GB
(compressed), GitHub will start evicting old entries. 5 GB isn't particularly
large for a ccache so we currently manually limit the size of each ccache to a
certain number of files (``ccache --max-files``) to more directly control cache
behavior and ensure fairer eviction among GitHub caches. The downside is these
limits need to be periodically reassessed.
