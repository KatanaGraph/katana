# cython: cdivision = True

from galois.shmem cimport *
from galois.shmem import *
from libcpp.memory cimport shared_ptr
from cython.operator cimport dereference

#
# Python Property graph 
#
cdef class PropertyGraph:
    cdef:
        shared_ptr[PropertyFileGraph] propertyFileGraph_ptr
        GraphTopology graphTopology

        shared_ptr[PropertyFileGraph] get_ptr(self)
        GraphTopology get_topology(self)
        uint64_t edges(self, uint32_t n)
        ArrayVector get_property_data(self, int32_t)

