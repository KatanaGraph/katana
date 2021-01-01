import ctypes
from typing import Dict

from numba import types
from numba.extending import overload_method, overload

import galois.property_graph
import galois.datastructures

# PropertyGraph


@overload(len)
def overload_PropertyGraph_len(self):
    if isinstance(self, galois.property_graph.PropertyGraph_numba_wrapper.Type):

        def impl(self):
            return self.num_nodes()

        return impl


@overload_method(galois.property_graph.PropertyGraph_numba_wrapper.Type, "nodes")
def overload_PropertyGraph_nodes(self):
    def impl(self):
        return range(self.num_nodes())

    return impl


@overload_method(galois.property_graph.PropertyGraph_numba_wrapper.Type, "edges")
def overload_PropertyGraph_edges(self, n):
    if isinstance(n, types.Integer) and not n.signed:

        def impl(self, n):
            if n == 0:
                prev = 0
            else:
                prev = self.edge_index(n - 1)
            return range(prev, self.edge_index(n))

        return impl
