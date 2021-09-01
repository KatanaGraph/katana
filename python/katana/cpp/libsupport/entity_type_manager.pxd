from libc.stdint cimport uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "katana/EntityTypeManager.h" namespace "katana" nogil:

    cdef cppclass EntityTypeManager:
        vector[string] GetAtomicTypeNames()
