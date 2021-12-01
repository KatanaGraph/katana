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

import atexit
import warnings
from typing import Dict, Type

import katana.plugin
from katana.dataframe import DataFrame
from katana.plugin import installed_plugins

try:
    import katana.globals
    from katana.globals import get_active_threads, set_active_threads, set_busy_wait

    __version__ = katana.globals.get_version()

    from katana._loops import OrderedByIntegerMetric, PerSocketChunkFIFO, UserContext, do_all, for_each
    from katana.loop_operators import do_all_operator, for_each_operator, obim_metric
except ImportError as e:
    if "libkatana" in str(e):
        raise ImportError(
            "The native libraries required by katana are missing or incorrectly installed. NOTE: The native libraries "
            "are not included in pip packages and must be installed separately (e.g., with `conda install katana-cpp`)."
        ) from e
    raise


__all__ = [
    "GaloisError",
    "OrderedByIntegerMetric",
    "PerSocketChunkFIFO",
    "QueryError",
    "TsubaError",
    "UserContext",
    "do_all",
    "do_all_operator",
    "for_each",
    "for_each_operator",
    "installed_plugins",
    "obim_metric",
    "get_active_threads",
    "set_active_threads",
    "set_busy_wait",
    "DataFrame",
]


# A global variable to hold the Katana runtime "Sys". The type will vary and has no methods. None means no Katana
# runtime is initializes. Otherwise, the type can be used to determine which runtime is in use.
_runtime_sys = None


def set_runtime_sys(cls):
    """
    Create and register a runtime sys. The runtime sys will be deallocated at python exit. It can be explicitly
    deallocated by calling :py:func:`reset_runtime_sys`.

    Once this is called with a given runtime type, it can be called repeatedly with that type idempotently.
    However, a call with a different runtime type will raise an exception.

    :param cls:  The type to use as the runtime sys.
    """
    # pylint: disable=global-statement
    global _runtime_sys
    if _runtime_sys is None:
        _runtime_sys = cls()
    elif isinstance(_runtime_sys, cls):
        # The initialized runtime is already of the correct type.
        pass
    else:
        raise RuntimeError(f"Katana runtime {type(_runtime_sys).__name__} is already initialized")


@atexit.register
def reset_runtime_sys():
    """
    Clear the runtime reference to trigger deallocation and shutdown. This is called automatically at interpreter exit.

    This function is idempotent.
    """
    # pylint: disable=global-statement
    global _runtime_sys
    _runtime_sys = None


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
