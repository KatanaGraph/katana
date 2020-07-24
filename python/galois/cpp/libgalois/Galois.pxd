# distutils: language=c++
# distutils: extra_compile_args=["-std=c++17"]

from libc.stdint cimport *
from ..libstd.boost cimport *

# Declaration from "Galois/Threads.h"

# Hack to make auto return type for galois::iterate work.
# It may be necessary to write a wrapper header around for_each,
# but this should be good enough for the forseeable future either way.
cdef extern from * nogil:
    cppclass CPPAuto "auto":
        pass

cdef extern from "galois/Galois.h" namespace "galois" nogil:
    unsigned int setActiveThreads(unsigned int)
    
    cppclass UserContext[T]:
        void push(...)
        void push_back(...)
        bint isFirstPass()
        void cautiousPoint()
        void breakLoop()
        void abort()

    void for_each(...)
    void do_all(...)

    CPPAuto iterate[T](const T &, const T &)
    CPPAuto iterate[T](T &)

    cppclass SharedMemSys:
        SharedMemSys()

    cppclass loopname:
        loopname(char *name)

    cppclass no_pushes:
        no_pushes()

    cppclass steal:
        steal()

    cppclass disable_conflict_detection:
        disable_conflict_detection()

cdef extern from "galois/MethodFlags.h" namespace "galois" nogil:
    cppclass MethodFlag:
        bint operator==(MethodFlag)

    MethodFlag FLAG_UNPROTECTED "galois::MethodFlag::UNPROTECTED"
    MethodFlag FLAG_WRITE "galois::MethodFlag::WRITE"
    MethodFlag FLAG_READ "galois::MethodFlag::READ"
    MethodFlag FLAG_INTERNAL_MASK "galois::MethodFlag::INTERNAL_MASK"
    MethodFlag PREVIOUS "galois::MethodFlag::PREVIOUS"

cdef extern from "galois/Range.h" namespace "galois" nogil:
    cppclass StandardRange[it]:
        it begin()
        it end()

cdef extern from "galois/NoDerefIterator.h" namespace "galois" nogil:
    cppclass NoDerefIterator[it]:
        bint operator==(NoDerefIterator[it])
        bint operator!=(NoDerefIterator[it])
        NoDerefIterator[it] operator++()
        NoDerefIterator[it] operator--()
        it operator*()
