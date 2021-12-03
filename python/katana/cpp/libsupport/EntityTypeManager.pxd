from libc.stdint cimport uint16_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from katana.cpp.libstd.optional cimport optional


cdef extern from "katana/EntityTypeManager.h" namespace "katana" nogil:

    ctypedef uint16_t EntityTypeID

    cdef cppclass EntityTypeManager:
        vector[EntityTypeID] GetAtomicEntityTypeIDs()
        optional[string] GetAtomicTypeName(EntityTypeID)
