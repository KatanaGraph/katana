# cython: cdivision = True

from libc.stdint cimport *
from .cpp.libgalois.graphs.Graph cimport PropertyFileGraph, GraphTopology
from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport Schema

#
# Python Property graph
#
cdef class PropertyGraph:
    cdef:
        shared_ptr[PropertyFileGraph] underlying

    @staticmethod
    cdef uint64_t _property_name_to_id(object prop, Schema schema) except -1

    cdef GraphTopology topology(PropertyGraph)
    cpdef uint64_t num_nodes(PropertyGraph)
    cpdef uint64_t num_edges(PropertyGraph)
    cpdef uint64_t get_edge_dst(PropertyGraph, uint64_t)
