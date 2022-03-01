=============
Documentation
=============

********
Overview
********

People turn to documentation for many different reasons, so it is important to
consider the audience and what their needs are. What a person wants out of
documentation depends on their background, what they want to do and their
experience with the Katana system.

The initial experience for users will be focused around answering questions like
"what does this system do?", "how can I test drive it?", "how can I install
it?", while a developer looking to modify the Katana system itself, will be more
interested in how the code is organized. And once someone gains experience,
their focus will shift from tutorials and getting started guides, which help
explain new concepts, to reference information, which serves as a reminder about
details once a concept is known.

The documentation for the Katana system is organized in major groups, which
answer specific questions:

- :ref:`getting-started`: How do I install the system? What are its basic
  requirements?
- :ref:`user-guides`: After I've installed Katana, how do I do common
  activities?
- :ref:`reference`: If I know the concept but am a little fuzzy on the details,
  can you give me a refresher?
- :ref:`contributing`: I'd like to improve to Katana itself. What should I
  know?

**********************
Building Documentation
**********************

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

*****************************************
Annotating Internal or Draft only Content
*****************************************

Files ending in ``-draft.[rst/ipynb]`` or ``-internal.[rst/ipynb]`` will not be
included in external facing documentation.

Whole directories ending in ``-draft/`` or ``-internal/`` will be omitted when
building external documentation.


*****************
Jupyter Notebooks
*****************

Guides on how to use Katana Graph in Python should be written as Jupyter
Notebooks. They will be parsed similar to Restructured Text (``.rst``) files.

Style
=====

* Keep titles and headings in sentence case (capitalize first letter of first
  word, proper nouns, first letter of subheading after colon, and no
  punctuation).
* Code cells in user guides must be evaluated with results less than 30 lines.
* Do not number headings of step titles in step by step guide. For substep
  sequences, use numbered bullet points under a given step title.
* Write in the second person: "Delete your database."
* Assertions should be kept to hidden code cells when not part of a method.
  Enclose the assertion in an individual code cell and add
  `"nbsphinx": "hidden"` to that cell's metadata.

How-to template
===============

``katana/docs/contributing/how_to_template.ipynb`` is a template for creating a
how-to guide in the form of a Jupyter Notebook. Use it when you wish to take a
reader through a detailed series of steps required to do an individual task or
procedure. Be sure to follow the section structure in the notebook and to use
the above Katana Graph writing style.

Customization
-------------

#. Choose a filename that matches or abbreviates what you wish to title the
   guide. Add the ``-draft`` tag (with hyphen) to the end of the filename. For
   example, ``const_cool_graphs-draft.ipynb`` for a guide titled "Constructing
   Cool Graphs."
#. The guide should be committed to
   ``/docs/user-guides/<most relevant directory>``. Create a new directory in
   ``/docs/user-guides/`` if a relevant directory does not exist.
#. In the above directory, ensure that the filename is added to the list under
   ``.. toctree::`` located in that directory's ``index.rst`` file. The name is
   to be included without the file extension. For example,
   ``const_cool_graphs-draft``. If you created a directory in the previous step,
   you will need to create a new ``index.rst`` and copy the format used in
   another user guide subdirectory.
#. Use a descriptive title according to the style guide.

Fill out the Requirements section
---------------------------------

This section prevents readers from getting halfway through and discovering that
they need to go and read other documentation before they can continue.
Prerequisites can include other articles or information to read, or it can be
technical dependencies. If there is more than one prerequisite, a bulleted list
is good to make the needs clearer. Describe what the audience needs to know, or
needs to have, before they attempt the how-to. By stating the requirements
up-front, you prevent your readers from having a bad experience with your
how-to. You must include links to procedures or information about how to get
what they need. If not possible, give useful pointers.

Explain steps and process
-------------------------

Images
^^^^^^

When you are explaining steps in a process, it can be useful to include images
(such as screenshots) for each key part of the process. This can help readers
orientate themselves as they move through the steps. It can also help someone
who is evaluating the software see how it works without having to install it.
When an image is quicker to interpret than descriptive text, put the screenshot
first, otherwise lead with the text.

Ordered lists
^^^^^^^^^^^^^

In general, ordered lists should be avoided in favor of section titles presented
in order to the reader. When unavoidable, provide a lead-in sentence before the
ordered list.

Code
^^^^

Break up your code where possible into smaller code cells and provide a lead-in
sentence explaining the code snippet. Describe what you are doing and your
expected result. If a large code block cannot be broken up, provide comments in
your code as well. Your code must not be pseudocode and the notebook as a whole
must be fully executable with no errors or warnings.

Orphaned Notebooks
==================

This means that it doesn't appear in a toctree (see ``index.rst``),
but other pages can still link to it.

Orphaned notebookes require the following to be added to the notebook's JSON
metadata:

.. code-block:: javascript

   "nbsphinx": {
      "orphan": true
   }

*****************
API Documentation
*****************

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
===

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
======

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
==

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
