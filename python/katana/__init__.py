"""
Galois Python is a Python library designed to simplify parallel programming. It is especially
focused on computations that are difficult to parallelize efficiently, such as
loops with

* irregular amount of work per iteration
* irregular memory accesses and branching patterns
* dependencies between iterations
* dynamic work creation

A typical Galois user is a Python programmer who understands parallelism in his/her
algorithm and wishes to express it using high-level constructs such as parallel
loops and concurrent data structures, without having to deal with low-level
parallel programming details such as threads, mutexes, barriers, condition
variables, work stealing, etc.

Galois Python utilizes the underlying Galois C++ library for most operations.
Galois Python also leverages numba to compile the "operators" which are run
by Galois C++.
"""

from typing import Type, Dict, Union

# Initialize the galois runtime immediately.
import katana.galois
from katana.version import get_katana_version 

def load_ipython_extension(ipython):
    import cython

    cython.load_ipython_extension(ipython)
    from .ipython import GaloisMagics

    ipython.register_magics(GaloisMagics)


class TsubaError(IOError):
    pass


class GaloisError(RuntimeError):
    pass


class QueryError(RuntimeError):
    pass


error_category_to_exception_class: Dict[str, Type[Exception]] = {
    "TsubaError": TsubaError,
    "GaloisError": GaloisError,
    "QueryError": QueryError,
}

__version__ = get_katana_version()
