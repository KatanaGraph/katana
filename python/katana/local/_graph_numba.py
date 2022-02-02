from numba import types
from numba.extending import overload, overload_method

from katana.local_native import Graph

# Graph


@overload(len)
def overload_Graph_len(self):
    if isinstance(self, Graph._numba_type_wrapper.Type):

        def impl(self):
            return self.num_nodes()

        return impl
    return None


@overload_method(Graph._numba_type_wrapper.Type, "nodes")
def overload_Graph_nodes(self):
    _ = self

    def impl(self):
        return range(self.num_nodes())

    return impl


@overload_method(Graph._numba_type_wrapper.Type, "out_edge_ids")
def overload_Graph_out_edge_ids(self, n):
    if isinstance(n, types.Integer) and not n.signed:
        _ = self

        def impl(self, n):
            return range(self._out_edge_begin(n), self._out_edge_end(n))

        return impl
    return None
