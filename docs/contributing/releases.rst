========
Releases
========

Build and Release Versions
==========================

Release versions of Katana are semantic versions with an release candidate tag
(``rc#``) appended for release candidates.

Build/development versions of Katana are of the form:
``[release].dev+N.0.######``.

* a release version
* ``.dev``
* ``+``
* the number of commits on the nearest major branch (e.g., ``master`` or ``release/v*``)
* ``0``
* an abbreviated commit id

The ``0`` is a placeholder for the enterprise commit count in enterprise builds
(enterprise builds also add an enterprise commit hash). The commit hashes are
replaced with ``dirty`` if the tree is dirty. For example,
``0.1.10rc1.dev+40.0.04ac6f``.

The version number of a commit is computed from the content of the repository.
The process is:

* If the commit has a tag in the form ``v{version number}`` (e.g., ``v0.1.10rc1``)
  then that is the version regardless of other information.
* Otherwise, the content of ``config/version.txt`` specifies the upcoming release
  and the version of the commit is computed based on the commit history and
  branches.

Version Management Tool
=======================

The tool ``scripts/version`` performs a number of tasks related to version
management. Day to day uses are mentioned here and a full list of commands and
options is available from: ``scripts/version --help`` (and ``--help`` on each
subcommand).

**To show the current version**, run ``scripts/version show``. This will print out
the version of the working tree. To see the version of a specific commit run
``scripts/version show {commit}``. This will print out the version of a clean
checkout of that commit.

**To create a release branch**, run ``scripts/version release_branch {next version}``
and merge the PRs it creates as soon as possible (to limit the number of commits
assigned incorrectly ordered version numbers). This will create a new release
branch: ``release/v{current version}``. The first commit on that branch suffixes
the version number stored in ``config/version.txt`` with ``rc1`` (e.g.,
``0.1.11rc1``). The commit on master immediately after the branch changes the
version number to ``{next version}``. The release branch should never be rebased
as it represents a permanent history of the release process. We can stabilize
the code as needed on the release branch before releasing the first release
candidate (e.g., ``0.1.11rc1``).

**To create a release**, run ``scripts/version release {next version}`` and merge
the PRs it creates as soon as possible. This tags the current ``HEAD`` commit
with ``v{current version}`` (e.g., ``v0.1.10rc1``) and creates commits to increase
the version number to ``{next version}``. The release tags should be annotated
tags to store who created the tag and the date. The same commands are used
regardless of the kind of release: final release (semantic version only),
release candidate (``rc``), or post-release (``.post``).

**To tag an existing version as a release** (e.g., to tag a release candidate as
the final release), run ``scripts/version tag {version}``. This tags the current
``HEAD`` commit with ``v{version}`` (e.g., ``v0.1.10``) to mark it as the ``{version}``
release.

**To begin work on a post-release** (a maintenance release for a previous final
release), switch to the release branch (``release/v{version}``) and run
``scripts/version bump {version}.post1`` and merge the PRs it creates. This will
update the version number to ``release/v{version number}`` in preparation for
work on a post release. After development on the first post-release is
complete, create and tag post-releases just like any other release.
Post-release development is entirely on the original release branch.
Post-releases should be avoided if possible. Master can be merged into the
release branch if needed, or commits from master can be cherry picked into the
release. Changes should not be developed on the release branch. The exception
is for maintenance on code that has been removed permanently from master, but
this should be avoided whenever possible.

Example
-------

Let us walk through a development example showing the steps and commands used.
The following tables have alternating rows: the state of the repositories and
the computed version, and the actions taken by the development team. Commit
hashes are replaced with # for brevity.

The steps leading up to branching for the 0.1.11 release:

1. Developer steps
2. Computed version: ``0.1.11.dev+1.1.#.#``; Repository status:  ``version.txt = 0.1.11``
3. 10 commits in katana and 20 commits in katana-enterprise.
4. Computed version: ``0.1.11.dev+11.21.#.#``; Repository status:  ``version.txt = 0.1.11``
5. Run ``scripts/version release_branch 0.1.12`` and merge the created PRs on
   both master and the release branch release/v0.1.11.

The steps on the release branch release/v0.1.11 to release 0.1.11:

1. Computed version: ``0.1.11rc1.dev+1.1.#.#``; Repository status:  ``version.txt = 0.1.11rc1``
2. 1 commit in katana and 1 commit in katana-enterprise.
3. Computed version: ``0.1.11rc1.dev+2.2.#.#``; Repository status:  ``version.txt = 0.1.11rc1``
4. Run ``scripts/version release 0.1.11rc2`` and merge the created PR on the
   release branch release/v0.1.11. Once this commit is tagged v0.1.11rc1 it is
   also version 0.1.11rc1 and can be released. 
5. Once 0.1.11rc1 passes QC, run ``scripts/version tag 0.1.11`` to tag the same
   commit as the final release 0.1.11. Once this commit is tagged v0.1.11 it is
   also version 0.1.11 and can be released. At this point the commit will have
   3 versions: 0.1.11rc1.dev+2.2.#.#, 0.1.11rc1, and 0.1.11. 

The state of master after branching for 0.1.11:

1. Computed version: ``0.1.12.dev+1.1.#.#``; Repository status: ``version.txt = 0.1.12``
2. Continue development

Conda Packages
==============

Conda packages are uploaded to ``katanagraph`` Conda channel by CI on each
merge to master. You can install the latest development release with

.. code-block:: bash

   conda install -c katanagraph/label/dev katana katana-python

Each PR also creates Conda packages. You can find them as an artifact
associated with each build (``conda-pkgs-*``). You can install these packages
by downloading the build artifact, unzipping it, and then pointing ``conda
install`` to the package (``.tar.bz2``) you want to install. This will be in
a subdirectory like ``linux-64``.

.. code-block:: bash

   conda install <path/to/package>

If you instead want to build Conda packages locally, make sure you have a
working Conda build (:ref:`building`), then activate your Conda environment and
run ``conda build``.

.. code-block:: bash

   conda activate katana-dev
   conda build -c conda-forge $SRC_DIR/conda_recipe/

.. warning:

   ``conda build`` may take up to an hour to finish.

The ``conda build`` commands will run some simple tests on the packages and
will fail if the tests fail. After each package builds successfully, ``conda
build`` will print the path to the package.

You can install the Conda packages with

.. code-block:: bash

   conda install <path/to/package>
   conda install -c conda-forge -c katanagraph katana katana-python

where the ``<path/to/package>`` is the path printed by ``conda build``.
``katana`` is the C++ library and applications, ``katana-python`` is the Python
library, which depends on the C++ library.

The second ``conda install`` works around a bug in conda by forcing the installation of dependencies;
Conda fails to install dependencies when a package is installed from a local path.
This second command will eventually no longer be needed, but should be harmless.

You can upload development Conda packages (i.e., release candidates or testing packages) to your Anaconda channel using the anaconda client (install `anaconda-client`):

```Shell
anaconda upload --label dev <path to package>
```

To upload a non-development packages remove `--label dev`.
The same commands can be used to upload to the `katanagraph` channel if you have the credentials.
