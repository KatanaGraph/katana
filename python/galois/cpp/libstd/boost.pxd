from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport *
from libcpp.memory cimport shared_ptr

cdef extern from "<system_error>" namespace "std" nogil:
    cdef cppclass error_code:
        string message()

cdef extern from "<boost/outcome/outcome.hpp>" namespace "BOOST_OUTCOME_V2_NAMESPACE" nogil:
    cdef cppclass std_result[T]:
        T value()
        bint has_value()
        bint has_failure()
        error_code error()
