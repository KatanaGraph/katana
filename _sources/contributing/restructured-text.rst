=================
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
