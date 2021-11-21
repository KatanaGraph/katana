from cython import final

from libc.stdint cimport uint64_t
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector
from pyarrow.lib cimport CTable, Schema

from katana.cpp.libgalois.graphs.Graph cimport GraphTopology
from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result

from .entity_type cimport EntityType


cdef _convert_string_list(l)
cdef shared_ptr[_PropertyGraph] handle_result_PropertyGraph(Result[unique_ptr[_PropertyGraph]] res) nogil except *

#
# Python Property Graph
#
cdef class GraphBase:
    cdef _PropertyGraph * underlying_property_graph(self) nogil except NULL

    @staticmethod
    cdef uint64_t _property_name_to_id(object prop, Schema schema) except -1

    @staticmethod
    cdef shared_ptr[CTable] _convert_table(object table, dict kwargs) except *

    @final
    cdef const GraphTopology* topology(PropertyGraphInterface)

    cpdef uint64_t num_nodes(PropertyGraphInterface)

    cpdef uint64_t num_edges(PropertyGraphInterface)

    cpdef uint64_t get_edge_dest(PropertyGraphInterface, uint64_t)


cdef class Graph(GraphBase):
    cdef:
        shared_ptr[_PropertyGraph] _underlying_property_graph

    cdef _PropertyGraph * underlying_property_graph(self) nogil except NULL

    @staticmethod
    cdef Graph make(shared_ptr[_PropertyGraph] u)

cdef class TxnContext:
    cdef CTxnContext _txn_ctx
