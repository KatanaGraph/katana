from libc.stdint cimport uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from katana.cpp.libstd.optional cimport optional


cdef extern from "katana/EntityTypeManager.h" namespace "katana" nogil:

    cdef cppclass EntityTypeManager:
        vector[uint8_t] GetAtomicEntityTypeIDs()
        optional[string] GetAtomicTypeName(uint8_t)
