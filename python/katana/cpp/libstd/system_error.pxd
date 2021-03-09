from libcpp.string cimport string

cdef extern from "<system_error>" namespace "std" nogil:
    cdef cppclass error_category:
        const char* name()

    cdef cppclass error_code:
        int value()
        string message()
        error_category category()
