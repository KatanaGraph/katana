# Contributing to Katana

The instructions in README.md give unopinionated instructions on how to install
and use this repository, which is appropriate to general users. This file
contains instructions and details more appropriate to people who need to
develop and improve the Katana system itself.

The quickest way to start hacking on Katana is to look at
`scripts/setup_dev_ubuntu.sh` and use that as the basis for installing a
development environment on your own machine.

With those developer dependencies installed, you can run the following commands
from the project root directory to build the system:

```shell
case $(uname) in
  Darwin*)
    ;;
  *) conan profile update settings.compiler.libcxx=libstdc++11 default
    ;;
esac
conan remote add kmaragon https://api.bintray.com/conan/kmaragon/conan

mkdir build
cd build
conan install ../config --build=missing
cmake -DCMAKE_TOOLCHAIN_FILE=conan_paths.cmake ..
make
```

Note: Katana builds with a variety of compilers as long as they support the
C++17 standard. See [README.md](README.md) for specific tested versions.

## Repository History

This repository began life as a fork the [Galois project](https://github.com/IntelligentSoftwareSystems/Galois).
As part of the commercialization process, this repository was combined with proprietary parts of the Katana system and later disentangled.
Disentangling the open-source and proprietary components required rewriting the git history of this repository.
Because of this, commits from 2020-06-30 until 2020-10-29 (marked with `[REWRITTEN COMMIT: incomplete subtree]`) are unbuildable and inconsistent; they are included to provide context only.
Commits from 2020-10-29 until 2020-01-01 are consistent (marked with `[REWRITTEN COMMIT: self-contained subtree]`), but may have removed changes mentioned in the commit messages.

## Specifying and Resolving C++ Dependencies

The above instructions should work if you have installed the C++ library
dependencies in `scripts/setup_dev_ubuntu.sh` (e.g., llvm-dev, arrow) in their
standard system locations (typically `/usr/lib` or `/usr/local/lib`). If you
need to tell `cmake` about additional library locations, you can use the CMake
option `CMAKE_PREFIX_PATH`, as in:

```shell
cmake -DCMAKE_TOOLCHAIN_FILE=conan_paths.cmake \
  -DCMAKE_PREFIX_PATH=<path/to/cmakefiles/for/library>;<another/path> ..
```

As a sidenote, CMake toolchain file is simply a method for initially defining
`CMAKE_PREFIX_PATH` and other CMake options. You can verify this by looking at
the contents of `conan_paths.cmake`.

A common issue is that you have multiple versions of the same dependency,
located in different directories, and CMake picks the wrong version.

The process by which CMake finds packages is involved, and the [CMake
documentation](https://cmake.org/cmake/help/latest/command/find_package.html#search-procedure)
contains all the gory details. One implication, though, is that CMake adds
directories in your path to its set of search locations.

Thus, if the LLVM C++ compiler (clang++) is in your path, CMake will attempt to
use the LLVM support libraries (e.g., libLLVMSupport.a, libclang.so) associated
with your compiler installation by default, even though your compiler and the
version of the LLVM support libraries you use are not strictly related to each
other.

You can work around this by putting the location of the LLVM support libraries
in `CMAKE_PREFIX_PATH` because that takes precedence over locations in your
path. Alternatively, you can indicate the location of the LLVM libraries
directly with `LLVM_DIR`:

```shell
cmake -DCMAKE_TOOLCHAIN_FILE=conan_paths.cmake \
  -DLLVM_DIR="$(llvm-config-X --cmakedir)" ..
```

# Submitting Changes

The way to contribute code to the Katana project is to make a fork of this
repository and create a pull request with your desired changes. For a general
overview of this process, this
[description](https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow)
from Atlassian.

# Merging Changes

If you are an external contributor (i.e., someone who does not have write
access to this repo), the main reviewer of your pull request is responsible for
finally merging your code after it has been reviewed.

If you are an internal contributor (e.g., a member of Katana Graph), you are
responsible for merging your code after it has been reviewed.

When writing your change and during the review process, it is common to just
append commits to your pull request branch, but prior to merging, these process
commits should be reorganized with the interest of future developers in mind.
This usually means structuring commits to be less about your process and more
about the logical outcomes.

A common pattern is a refactoring commit followed by the actual new feature.
Each commit is usually buildable and testable on its own but altogether they
comprise the work done. You can look at the git history of this repo for
inspiration, but if you have any doubts, squashing all your commits into one is
usually a safe choice.

You can use `git rebase --interactive` to reorganize your commits and  `git
push --force`  to update your branch. Depending on your IDE, there are various
features or plugins to streamline this process (e.g.,
[vim-fugitive](https://github.com/tpope/vim-fugitive),
[magit](https://magit.vc/)).

# Some Helpful Commands

```shell
make -C <some subdirectory in the build tree>
```

This builds only the targets defined in that subdirectory (and their
dependencies). This is useful for compiling a single application or library at
a time.

```shell
make -j 4
```

This launches `4` jobs to build the system concurrently.

```shell
make VERBOSE=1
```

This prints out the commands that are run. This is useful for debugging
installation issues.

```shell
ctest -L quick
```

This runs code tests locally on your machine.  Most of the tests require sample
graphs. You can get them with `make input`.

```shell
ctest -R regex --parallel 4
```

Run tests matching the regular expression and use 4 parallel jobs to execute
tests.

```shell
ctest --test-action memcheck
```

Run tests with valgrind memcheck.

```shell
ctest --rerun-failed
```

Rerun just the tests that failed during the last run.

```shell
bin/graph-properties-convert <infile> <out directory/s3>
```

This converts property graphs into katana form

# Some Helpful tools

[ccmake](https://cmake.org/cmake/help/v3.0/manual/ccmake.1.html): a simple
terminal UI for configuring cmake projects. This is helpful for setting
various build options without having to remember their names exactly.

[ccache](https://ccache.dev/): a wrapper around compilation commands to reuse
object files even if their timestamps change as long as the contents of the
source file and compiler flags are the same. To enable use: `cmake
-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`

The LLVM project provides many useful development tools for C++:
[clang-format](https://clang.llvm.org/docs/ClangFormat.html),
[clang-tidy](https://clang.llvm.org/extra/clang-tidy/), and
[clangd](https://clangd.llvm.org/).  Most IDEs can be configured to use these
tools automatically.

# Adding New E(x)ternal Dependencies

Adding new dependencies should generally be avoided since it makes it more
likely that satisfying local development requirements, conda build requirements,
production library requirements, etc. will become impossible. If you do choose
to require a new 3rd party library for a good reason you should:

  0. Choose a version of the library that is available both in
  [conda-forge](https://anaconda.org/conda-forge/repo) and in
  [ConanCenter](https://conan.io/center/). If it is not available in both places,
  ubuntu package managers like `apt` or `snap` can work but adding it will be
  different (and you should consider picking another library since this puts an
  extra burden on developers).

  1. Add the dependency to the [conan config](config/conanfile.py) in the style
  of the dependendices that are already there.

  2. Add the dependency to the [conda recipe](conda_recipe/meta.yaml) in the
  style of what's there. There are two sections; `host` and `run`.
  Any runtime deps need to be added to both sections. But deps which are totally
  compiled into galois (i.e., they are not exposed in our API and don't require
  a shared library at run time), can be in `host` only.

  3. It is possible that you may have to modify the
  [package config](cmake/GaloisConfig.cmake.in) as well so `cmake` will find
  your dependency during the conda build (again the best advice is to look at how other
  dependencies handle this). This should only be necessary if the new dependency
  is a runtime or user-code dependency. For instance, this should not be
  necessary for header-only libraries that are not used in public headers.

If you do end up choosing a library that is not in conda-forge/ConanCenter
(really?) make sure to update the dependency list in [README.md](README.md), and
make sure the
[script for setting up a dev environment](scripts/setup_dev_ubuntu.sh) is
updated as well. There will likely also be changes to the CI scripts that are
needed.

You should be particularly weary of libraries that are not in conda-forge. If
absolutely necessary, discuss it with the current conda package maintainer
(currently @arthurp). Not handling them correctly there will totally break the
conda packages.

# Debugging

Printing and its more production-oriented cousin, logging, are simple ways to
get started with debugging, especially if you are in an environment where you
can build executables from source. Just remember to prefix your debugging messages
with an easy-to-find string like `XXX` so you can find and remove them later.

For more interactive debugging, you can use `gdb`. A typical `gdb` session looks
like this:
```shell
gdb --args application arg1 arg2 arg3
> break SourceFile.cpp:LineNumber
> run
> next
> print
# edit some code
> make
> run
```

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

```shell
ulimit -c unlimited
sudo sysctl -w kernel.core_pattern=/tmp/core-%e.%p.%h.%t
```

And you can load them in `gdb`:

```shell
gdb application -c core-file
```

# Testing

Many tests require sample graphs that are too big to keep in this repository.
You can get them with `make input`.

If you need to update the inputs, they are referenced as
  https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-vN.tar.gz
in `.github/workflows`, `inputs/CMakeLists.txt` and
`external/katana/python/galois/exmaple_utils.py`.  `vN` is a monotonically
increasing version number. You can use a command `inputs/update_inputs.sh` to
create create a new input collection. After creating the tar file, you will
need to upload the file to the public S3 bucket.

Tests are just executables created by the CMake `add_test` command.  A test
succeeds if it can be run and it returns a zero exit value; otherwise, the test
execution is considered a failure. A test executable itself may contain many
individual assertions and subtests.

Tests are divided into groups by their label property. A test can be given a
label with the `set_property`/`set_tests_properties` command, e.g.,
`set_property(TEST my-test PROPERTY LABEL my-label)`

So far there is only one useful label:

- **quick**: Quick tests have no external dependencies and can be run in parallel
  with other quick tests. Each quick test should run in a second or less. These
  tests are run as part of our continuous integration pipeline.

# Code Style and Conventions

We automatically test for some code style adherence at merge-time; so the
automatic tools have the final word. Nevertheless, there are some conventions
enforced by maintainers to be aware of:

 * `TODO`/`FIXME`/`XXX` are common comments used to denote known issues that
   should be fixed soon. In this code base, `XXX` is reserved for local changes
   that the author intends to fix immediately, i.e., comments with `XXX` should
   never be committed. `TODO` and `FIXME` should only be used with the name
   of a contributor who is responsible for fixing them (usually you)
   in the style: `TODO(thunt)`. In general `TODO` tasks involve more work to fix
   than `FIXME` tasks but the distinction isn't enforced.

 * Dates in comments should always take the form `YYYY-MM-DD`.

 * In general, we try to adhere to the [Google C++ Style
   Guide](https://google.github.io/styleguide/cppguide.html). One exception
   (so far) is that when passing arguments to functions, prefer pointers
   to non-const references (which used to be a part of that guide).

The following are the automated style checkers that we use. The continuous
integration process will check adherence on each Pull Request, but it is faster,
in terms of feedback latency, to run these checks locally first.
 * `scripts/check_docs.sh`: ensures that you didn't break doc generation
 * `scripts/check_ifndef.py [-fix] lib*`: checks that header guards are well
 formed.
 * `scripts/check_format.sh [-fix] lib*`: applies `clang-format` to check style.
 * `scripts/check_go_format.sh [-fix] .`: applies `gofmt` to format go code.
 * `scripts/check_go_lint.sh .`: applies `golangci-lint` to check check style.
 * `scripts/check_python_format.sh .`: applies `black` to format python code.

We also have a `clang-tidy` and `pylint` configuration. Both these are useful
tools for checking your code locally but they are not currently by continuous
integration.

None of these checks are exhaustive (on their own or combined)!

New code is expected to follow the above guidelines. But also be aware: there
are older modules that predate these expectations (currently, older modules are
everything that is not `libtsuba`, `libsupport` and `libquery`). In those cases,
it is acceptable to follow the convention in that module; though in general, it
would be preferable to follow the motto of "leave the codebase in better shape
than you found it."

## galois::Result

On older compilers, auto conversion to `galois::Result` will fail for types
that can't be copied. One symptom is compiler errors on GCC 7 but not on GCC 9.
We've adopted the workaround of returning such objects like so:

```cpp
Result<Thing> MakeMoveOnlyThing() {
  Thing t;
  ....
  return Thing(std::move(t));
}
```

If you are looking to simplify error handling, if a function returns a
`galois::Result<void>`, you can define the result and check it in a single if
statement:

```cpp
if (auto r = ReturnsAResult(); !r) {
  return r.error();
}
```

# Continuous Integration

## Dealing with CI Errors

If the error is due to a transient external failure, you can re-run jobs in the
GitHub UI.

When debugging a CI failure, it is good to confirm that tests pass locally in
your developer environment first. You can also run many of the source checks
locally as well. Take a look at the GitHub workflow definitions under `.github`
directory to see what script and build parameters are used.

## Caching

GitHub actions allows for build data to be cached between CI runs. For
reference, the caches (`actions/cache`) are scoped to
[branches](https://github.com/actions/cache#cache-scopes). The cache matching
policy is:

1. Exact key match on the current branch
2. Prefix match of a restore key on the current branch. If there are multiple
   matching keys, return the most recent entry.
3. Repeat from 1 for the default branch

Keys should be unique because once a cache entry is created it will
never be updated by `actions/cache`.

If you need to create a cache that simply stores the latest values, create a
common prefix with a unique suffix (e.g., `github.sha`) and use the common
prefix as a restore key. The unique key will not match any existing key but
upon lookup there will be multiple matching cache entries sharing the common
prefix, and `actions/cache` will return the most recent one.

One common use of `actions/cache` is to store a ccache cache. There is no limit
on the number of caches, but once the overall size of a cache exceeds 5 GB
(compressed), GitHub will start evicting old entries. 5 GB isn't particularly
large for a ccache so we currently manually limit the size of each ccache to a
certain number of files (`ccache --max-files`) to more directly control cache
behavior and ensure fairer eviction among GitHub caches. The downside is these
limits need to be periodically reassessed.
