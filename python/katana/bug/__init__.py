"""
:py:mod:`katana.bug` contains tools for capturing debugging information for bug reporting purposes.

The currently available entry point is :py:func:`~katana.bug.capture_environment`.
This function can be invoked from the command-line by running (with the Katana library installed):

.. code-block:: sh

   python -m katana.bug

You can specify the output filename as a command-line argument or use ``--help`` to get more information about the
command.
"""

from .environment import capture_environment

__all__ = ["capture_environment"]
