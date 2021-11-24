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

It is also possible to ask GitHub to squash your commits for you. This is the
"squash and merge" option when merging your PR.

Pull Request Style
==================

Include the Jira issue keys that are being addressed at the end of the pull
request title as a comma separated list in brackets.

Example PR title::

  Reduce memory usage of Foo [KAT-1000, KAT-1001]

If you squash and merge your PR, by default, the PR description and title are
turned into the commit message for your squashed PR, so apply the
recommendations about commit message style below to your PR description instead.

Jira issue keys referenced in the description of the PR do not get picked up by
the Jira-GitHub integration, but do get automatically converted into a link by
GitHub's Jira bot. This is helpful for reviewers to quickly navigate to the
related Jira issue.

Commit Message Style
====================

If you intend to rebase and merge, please still include Jira issue keys as described in
pull request style above.

If you intend to squash and merge, apply the recommendations in this section to your
pull request message instead of your commit message.

There are three main things we accomplish with commit messages:

1. Facilitate review of our changes by reviewers

2. Provide context to future readers about our changes

3. Ground the scope of our changes to ourselves.

If we struggle to write a simple explanation of why we made a change, it is a
sign that the underlying code changes are also not as clear as they could be.

Commit messages are part of the repository in the same way as the files in a
repository are. And as developers may use ``grep`` to orient themselves around
the repository, developers may also use ``git log`` and ``git blame`` to
understand more of the history of a file and its context. So, it pays to be
mindful of the content and structure of commits and their messages.

Messages should be written with your reviewers in mind as well as future
developers (or generally the codebase itself). Changes to the repository are
already included in a commit so use your commit message to focus on things that
would not readily be apparent from just looking at the textual changes.

Here are some examples of useful things to note for your reviewers and future
developers:

- Why was this change made?

- What plausible alternative implementations did you consider but rule out?

- What general thing or abstraction is this change in furtherance of?

- If this change fixes a bug, what was the original bug or its symptoms?

- Are there any notable API changes? Call them out specifically so that other
  developers browsing ``git log`` or resolving a conflict can adjust their
  current work accordingly.

- If you did some additional changes along the way to facilitate the main
  thrust of your work, explain the rational behind any non-obvious but required
  changes.

At the same time, there is no reason to agonize over the content of a commit
message or be overly verbose. Applying an automatic formatter changes a lot of
files but doesn't require a lot of context, while a one-line change might merit
several paragraphs of context if the rationale is subtle or non-obvious.

The subject line (i.e., the first line) of a commit message should be short,
generally less than 80 characters long, and start with a verb in the imperative
mood (present tense, second person). You can think of the message as directing
the codebase itself to do something. Optionally, the subject line may begin
with a component followed by a colon if the change is specific to a commonly
known component or library.

Examples:

- ``Apply clang-format``

- ``tsuba: Support compressed and uncompressed files``

- ``support: Fix release order when handling errors``

Commit messages are traditionally viewed in a terminal. Hard wrap your lines to
some reasonable column width (e.g., 80 or 120 characters) and use a blank line
to separate paragraphs. Avoid markup or excessive decoration on text. A commit
message is plain text.

Refrain from using external links unless you are sure they will be valid
indefinitely. Assume that readers do not have access to external links and
summarize relevant details in the commit message itself. This includes
references to issue and work trackers.

An example of a typical commit message:

.. code-block::

   support: Reduce memory usage of Foo

   A Foo instance maintains references to Bar objects. The number of Bar
   objects can grow when a Foo is used multiple times. This can reach 10GB in
   typical usage.

   In practice, once a Foo is reused only some Bar objects need to be retained.
   So, add a dispose API that allows Foo users to indicate which Bar objects to
   release on reuse. The default behavior is to retain all Bars.

   In most cases, it is easy to infer which Bar objects to dispose of by
   inspecting the code. The few remaining cases where it is not possible to use
   the dispose API are on less frequently executed recovery paths.

   After this change, typical memory usage reduces to a few KB.
