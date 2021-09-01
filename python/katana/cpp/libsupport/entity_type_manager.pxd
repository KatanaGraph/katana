from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdint cimport uint8_t

cdef extern from "katana/EntityTypeManager.h" namespace "katana" nogil:

    cdef cppclass EntityTypeManager:
        vector[string] GetAtomicTypeNames()
