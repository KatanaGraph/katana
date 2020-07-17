import ctypes

from numba.extending import overload_method

import galois._loops
import galois.property_graph
from .wrappers import SimpleNumbaPointerWrapper

###### UserContext

UserContextType = SimpleNumbaPointerWrapper(galois._loops.UserContext)

UserContextType.register_method("push", ctypes.CFUNCTYPE(None, ctypes.c_voidp, ctypes.c_uint64))
UserContextType.register_method("push_back", ctypes.CFUNCTYPE(None, ctypes.c_voidp, ctypes.c_uint64))
UserContextType.register_method("isFirstPass", ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_voidp))
UserContextType.register_method("cautiousPoint", ctypes.CFUNCTYPE(None, ctypes.c_voidp))
UserContextType.register_method("breakLoop", ctypes.CFUNCTYPE(None, ctypes.c_voidp))
UserContextType.register_method("abort", ctypes.CFUNCTYPE(None, ctypes.c_voidp))


###### PropertyGraph

PropertyGraphType = SimpleNumbaPointerWrapper(galois.property_graph.PropertyGraph)

PropertyGraphType.register_method("num_nodes", ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp))
PropertyGraphType.register_method("num_edges", ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp))
PropertyGraphType.register_method("edge_index", ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp, ctypes.c_uint64))
PropertyGraphType.register_method("get_edge_dst", ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp, ctypes.c_uint64))

# func_name = "get_node_property"
# typ = ctypes.CFUNCTYPE(ctypes.c_voidp, ctypes.c_voidp, ctypes.c_uint64)
# cython_func_name = None
# addr = get_cython_function_address_with_defaults(
#     cython_func_name, PropertyGraphType.override_module_name, PropertyGraphType.type_name + "_" + func_name)
# func = typ(addr)
# @overload_method(PropertyGraphType.Type, func_name)
# def overload(v, *args):
#     def impl(v, *args):
#         return func(v.ptr, *args)
#     return impl
# # PropertyGraphType.register_method("get_node_property", ctypes.CFUNCTYPE(ctypes.c_voidp, ctypes.c_voidp, ctypes.c_uint64))
# # PropertyGraphType.register_method("get_edge_property", ctypes.CFUNCTYPE(ctypes.c_voidp, ctypes.c_voidp, ctypes.c_uint64))

@overload_method(PropertyGraphType.Type, "nodes")
def overload_nodes(self):
    def impl(self):
        return range(self.num_nodes())
    return impl

@overload_method(PropertyGraphType.Type, "edges")
def overload_edges(self, n):
    def impl(self, n):
        if n == 0:
            prev = 0
        else:
            prev = self.edge_index(n-1)
        return range(prev, self.edge_index(n))
    return impl
