from libcpp.memory cimport shared_ptr, unique_ptr

from katana.cpp.libgalois.graphs.Graph cimport GraphTopology
from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result


cdef _PropertyGraph* underlying_property_graph(graph) nogil
cdef CTxnContext* underlying_txn_context(txn_context) nogil

cdef shared_ptr[_PropertyGraph] handle_result_PropertyGraph(Result[unique_ptr[_PropertyGraph]] res) nogil except *
