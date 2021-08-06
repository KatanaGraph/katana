from numba import types
from numba.extending import overload, overload_method

from ._property_graph_numba_native import PropertyGraph_numba_wrapper

# Graph


@overload(len)
def overload_PropertyGraph_len(self):
    if isinstance(self, PropertyGraph_numba_wrapper.Type):

        def impl(self):
            return self.num_nodes()

        return impl
    return None


@overload_method(PropertyGraph_numba_wrapper.Type, "nodes")
def overload_PropertyGraph_nodes(self):
    _ = self

    def impl(self):
        return range(self.num_nodes())

    return impl


@overload_method(PropertyGraph_numba_wrapper.Type, "edges")
def overload_PropertyGraph_edges(self, n):
    if isinstance(n, types.Integer) and not n.signed:
        _ = self

        def impl(self, n):
            if n == 0:
                prev = 0
            else:
                prev = self.edge_index(n - 1)
            return range(prev, self.edge_index(n))

        return impl
    return None
