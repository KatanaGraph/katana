# distutils: language = c++

from libcpp.string cimport string


cdef extern from "<iostream>" namespace "std":
    cdef cppclass ostream:
        pass

cdef extern from "<sstream>" namespace "std":
    cdef cppclass ostringstream(ostream):
        ostringstream() except +
        string str() except +
