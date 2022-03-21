You will need to log out and back in again to ensure conda is properly
configured. Then, create and activate the development environment:

.. code-block:: bash

   SRC_DIR=<repo/root>
   conda config --add channels conda-forge
   # For library compatibility reasons, prefer taking dependencies from
   # higher priority channels even if newer versions exist in lower priority
   # channels.
   conda config --set channel_priority strict
   # Create the environment
   conda create --name katana-dev
   # Install the dependencies
   $SRC_DIR/scripts/update_conda.sh katana-dev
   conda activate katana-dev
   conda install numactl-devel-cos6-x86_64 # For x86_64 builds
