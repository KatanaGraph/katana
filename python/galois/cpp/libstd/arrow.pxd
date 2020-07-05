from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport *
from libcpp.memory cimport shared_ptr
cdef extern from "<arrow/api.h>" namespace "arrow" nogil:
    cdef cppclass arrowTable "arrow::Table":
        pass
    cdef cppclass Array:
        pass
    cdef cppclass ArrayVector:
        shared_ptr[Array] at(uint64_t)
        uint64_t size()
        pass
    cdef cppclass Schema:
        string ToString()
        pass
    cdef cppclass ChunkedArray:
        string ToString()
        uint64_t length()
        ArrayVector& chunks()
        pass
    cdef cppclass UInt64Array:
        uint64_t length()
        uint64_t Value(int64_t)
        pass
    cdef cppclass UInt32Array:
        uint64_t length()
        uint32_t Value(int64_t)
        pass
