# cython: cdivision = True

from cython.operator cimport dereference as deref

#
# Python Property Graph 
#
cdef class PropertyGraph:
    def __init__(self, filename):
        self.propertyFileGraph_ptr = PropertyFileGraph.Make(bytes(filename, "utf-8")).value()
        self.graphTopology = deref(self.propertyFileGraph_ptr).topology()

    cdef shared_ptr[PropertyFileGraph] get_ptr(self):
        return self.propertyFileGraph_ptr

    cdef GraphTopology get_topology(self):
        return self.graphTopology

    def num_nodes(self):
        return self.graphTopology.num_nodes()

    def num_edges(self):
        return self.graphTopology.num_edges()

    def print_node_schema(self):
        return deref(deref(self.propertyFileGraph_ptr).node_schema()).ToString()

    def print_edge_schema(self):
        return deref(deref(self.propertyFileGraph_ptr).edge_schema()).ToString()

    def num_node_properties(self):
        return deref(self.propertyFileGraph_ptr).NodeProperties().size()

    cdef uint64_t edges(self, uint32_t n):
        if n == 0:
            return deref(self.graphTopology.out_indices).Value(n)
        else:
            return (deref(self.graphTopology.out_indices).Value(n) - deref(self.graphTopology.out_indices).Value(n-1))

    cdef ArrayVector get_property_data(self, int32_t pid):
        cdef shared_ptr[ChunkedArray] chunk_array = deref(self.propertyFileGraph_ptr).NodeProperty(pid)
        return deref(chunk_array).chunks()



