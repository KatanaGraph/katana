from libc.stdint cimport uintptr_t
from pyarrow.lib cimport to_shared

from katana.cpp.libsupport.result cimport Result, raise_error_code


cdef _PropertyGraph* underlying_property_graph(graph) nogil:
    with gil:
        return <_PropertyGraph*><uintptr_t>graph.__katana_address__

cdef CTxnContext* underlying_txn_context(txn_context) nogil:
    with gil:
        if txn_context:
            return <CTxnContext*><uintptr_t>txn_context.__katana_address__
        return NULL

cdef shared_ptr[_PropertyGraph] handle_result_PropertyGraph(Result[unique_ptr[_PropertyGraph]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return to_shared(res.value())
