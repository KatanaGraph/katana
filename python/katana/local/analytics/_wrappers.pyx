"""
Trivial Algorithms
------------------

"""

from cython.operator cimport dereference as deref
from libc.stdint cimport uint32_t, uint64_t
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.utility cimport move
from pyarrow.lib cimport CUInt64Array

from katana.cpp.libgalois.datastructures cimport NUMAArray
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result, handle_result_void, raise_error_code
from katana.local._graph cimport Graph
from katana.local.datastructures cimport NUMAArray_uint64_t


cdef inline default_value(v, d):
    if v is None:
        return d
    return v

cdef shared_ptr[CUInt64Array] handle_result_shared_cuint64array(Result[shared_ptr[CUInt64Array]] res) \
        nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef unique_ptr[NUMAArray[uint64_t]] handle_result_unique_numaarray_uint64(Result[unique_ptr[NUMAArray[uint64_t]]] res) \
        nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return move(res.value())


# "Algorithms" from Graph

cdef extern from "katana/PropertyGraph.h" namespace "katana" nogil:
    Result[unique_ptr[NUMAArray[uint64_t]]] SortAllEdgesByDest(_PropertyGraph* pg);

    uint64_t FindEdgeSortedByDest(const _PropertyGraph* graph, uint32_t node, uint32_t node_to_find);

    Result[void] SortNodesByDegree(_PropertyGraph* pg);


# TODO(amber/amp): Update the following functions to use Sorted PropertyGraph
# Views, because functions like SortNodesByDegree and FindEdgeSortedByDest etc. are
# deprecated and will be removed in the near future.
def sort_all_edges_by_dest(Graph pg):
    """
    Sort the edges of each node by the node ID of the target. This enables the use of
    :py:func:`find_edge_sorted_by_dest`.

    :return: The permutation vector (mapping from old indices to the new indices) which results due to the sorting.
    :rtype: NUMAArray[np.uint64]
    """
    with nogil:
        res = move(handle_result_unique_numaarray_uint64(SortAllEdgesByDest(pg.underlying_property_graph())))
    return NUMAArray_uint64_t.make_move(move(deref(res.get())))


def find_edge_sorted_by_dest(Graph pg, uint32_t node, uint32_t node_to_find):
    """
    Find an edge based on its incident nodes. The graph must have sorted edges.

    :param pg: The graph
    :param node: The source of the edge
    :param node_to_find: The target of the edge
    :return: The edge ID or None of no such edge exists.

    :see: :func:`sort_all_edges_by_dest`
    """
    with nogil:
        res = FindEdgeSortedByDest(pg.underlying_property_graph(), node, node_to_find)
    if res == pg.edge_ids(node)[-1] + 1:
        return None
    return res


def sort_nodes_by_degree(Graph pg):
    """
    Relabel all nodes in the graph by sorting in the descending order by node degree.
    """
    with nogil:
        handle_result_void(SortNodesByDegree(pg.underlying_property_graph()))
