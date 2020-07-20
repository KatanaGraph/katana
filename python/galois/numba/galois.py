import ctypes

from numba.extending import overload_method

import galois.property_graph


# PropertyGraph

@overload_method(galois.property_graph.PropertyGraph_numba_wrapper.Type, "nodes")
def overload_nodes(self):
    def impl(self):
        return range(self.num_nodes())
    return impl


@overload_method(galois.property_graph.PropertyGraph_numba_wrapper.Type, "edges")
def overload_edges(self, n):
    def impl(self, n):
        if n == 0:
            prev = 0
        else:
            prev = self.edge_index(n-1)
        return range(prev, self.edge_index(n))
    return impl
