import subprocess
import sys

import pytest


def run_python_subprocess(program):
    subprocess.check_call([sys.executable, "-c", program])


# Most of the tests below succeed because they only use scalar typed NUMAArrays which can be deallocated after
# SharedMemSys shutdown.


def test_normal_order():
    run_python_subprocess(
        """
import gc
import katana.local.import_data
from katana.example_data import get_input
katana.local.initialize()

g = katana.local.Graph(get_input("propertygraphs/rmat15"))
print(g.num_nodes())

del g
gc.collect()
"""
    )


def test_implicit_order():
    run_python_subprocess(
        """
import katana.local.import_data
from katana.example_data import get_input
katana.local.initialize()

g = katana.local.Graph(get_input("propertygraphs/rmat15"))
print(g.num_nodes())
"""
    )


def test_out_of_order():
    run_python_subprocess(
        """
import gc
import katana.local.import_data
from katana.example_data import get_input
katana.local.initialize()

g = katana.local.Graph(get_input("propertygraphs/rmat15"))
print(g.num_nodes())

katana.set_active_threads(2)
katana.reset_runtime_sys()
print(g.num_nodes())

del g
"""
    )


def test_out_of_order_numaarray():
    run_python_subprocess(
        """
import gc
import katana.local
katana.local.initialize()

a = katana.local.NUMAArray[int]()
a.allocateBlocked(100000)

katana.reset_runtime_sys()

del a
"""
    )


@pytest.mark.xfail("This crashes because of direct access to the thread pool.")
def test_set_threads_after_shutdown():
    run_python_subprocess(
        """
import katana.local
katana.local.initialize()

katana.set_active_threads(2)
katana.reset_runtime_sys()
katana.set_active_threads(2)
"""
    )
