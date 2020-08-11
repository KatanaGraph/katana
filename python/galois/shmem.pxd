# distutils: language=c++
# distutils: extra_compile_args=["-std=c++17"]

from .cpp.libgalois.Galois cimport UserContext, iterate, for_each, SharedMemSys, loopname, disable_conflict_detection, no_pushes, do_all, steal
from .cpp.libgalois.Galois cimport MethodFlag
from .cpp.libgalois.graphs.Graph cimport dummy_true, dummy_false, MorphGraph, LC_CSR_Graph, PropertyFileGraph, GraphTopology
from .cpp.libgalois.graphs.ReadGraph cimport readGraph
from .cpp.libgalois.Worklist cimport ChunkFIFO, OrderedByIntegerMetric, wl, Uint_64u, UpdateRequestIndexer, PerSocketChunkFIFO, ReqPushWrap, UpdateRequest
from .cpp.libgalois.Timer cimport Timer
from .cpp.libstd.atomic cimport atomic
from .cpp.libstd.boost cimport *
from libcpp.vector cimport vector
from libcpp cimport bool
from libc.stdint cimport *

# Initialize the Galois runtime when the Python module is loaded.
cdef class _galois_runtime_wrapper:
    cdef SharedMemSys _galois_runtime

cdef extern from * nogil:
    # hack to bind leading arguments by value to something that can be passed
    # to for_each. The returned lambda needs to be usable after the scope
    # where it is created closes, so captured values are captured by value.
    # The by-value capture in turn requires that graphs be passed as
    # pointers. This function is used without exception specification under
    # the assumption that it will always be used as a subexpression of
    # a whole expression that requires exception handling or that it will
    # be used in a context where C++ exceptions are appropriate.
    # There are more robust ways to do this, but this didn't require
    # users to find and include additional C++ headers specific to
    # this interface.
    # Syntactically, this is using the cname of an "external" function
    # to create a one-line macro that can be used like a function.
    # The expected use is bind_leading(function, args).
    cdef void *bind_leading "[](auto f, auto&&... bound_args){return [=](auto&&... pars){return f(bound_args..., pars...);};}"(...)
    # # Similar thing to invoke a function and return an integer.
    # # Useful for verifying that this approach works.
    # cdef int invoke "[](auto f, auto&&... args){return f(args...);}"(...)

#cdef int myfunc(int a, int b, int c):
#    return a + b + c

cdef extern from "algorithm" namespace "std" nogil:
    # This function from <algorithm> isn't currently
    # provided by Cython's known interfaces for the C++ standard library,
    # so this is needed to get it working here.
    # The variadic signature could probably be removed and this could
    # be made to match the original templates more closely, but since
    # this form matches the syntax we need to use, it is good enough.
    int count_if(...) except +

