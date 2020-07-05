from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport *
from libcpp.memory cimport shared_ptr
cdef extern from "<boost/outcome/outcome.hpp>" namespace "outcome" nogil:

    cdef cppclass std_result[T]:
        T value()
        pass
