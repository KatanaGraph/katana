.. warning::

   Not all conda installation scripts are well-behaved. In particular, the
   default behavior of the ``cudatoolkit-dev`` package's post-installation
   script is to write a log to the hard-coded path ``/tmp/cuda-installer.log``
   and not delete it. Once one user on a system has written this file all other
   users will fail to install the package because they won't be able to write
   the log file. The only known workaround is to manually delete the log file.
   It is best practice to delete your own log file immediately after you have
   updated your environment. If you are on a system where someone else has
   written the log file and you can't overwrite it please contact that user to
   delete it. You may also contact a user with root privileges.

.. code-block:: bash

   # If you are sharing a system with other users,
   # please perform the following manual clean-up:
   conda env update --name katana-dev --file $SRC_DIR/conda_recipe/environment.yml
   rm /tmp/cuda-installer.log
