=========
Git Style
=========

The main development branch ``master`` should build and pass tests, but may be
unstable at times. Regressions may be introduced post-merge or a PR may be more
clearly expressed with intermediate commits that do not pass all tests. We
prefer a clear history over "atomic" commits. During our release process, we
specifically address stability and regression issues.

Submitting Changes
==================

The way to contribute code to the Katana project is to make a fork of this
repository and create a pull request with your desired changes. For a general
overview of this process, this
`description <https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow>`_
from Atlassian.

Merging Changes
===============

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

You can use ``git rebase --interactive`` to reorganize your commits and  ``git
push --force``  to update your branch. Depending on your IDE, there are various
features or plugins to streamline this process (e.g.,
`vim-fugitive <https://github.com/tpope/vim-fugitive>`_,
`magit <https://magit.vc/>`_). Git also provides ``git commit --fixup`` and
``git rebase --autosquash`` which can streamline this process.

