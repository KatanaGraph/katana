from numba.extending import overload, overload_method

from katana.local_native import Graph

__all__ = []


@overload_method(Graph._numba_type_wrapper.Type, "node_ids")
def overload_Graph_nodes(self):
    _ = self

    def impl(self):
        return range(self.num_nodes())

    return impl
