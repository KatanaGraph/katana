# cython: cdivision = True

from galois.shmem cimport *
from galois.graphs cimport LC_CSR_Graph_Directed_double_void
from .cpp.libgalois.Galois cimport FLAG_UNPROTECTED
from libcpp.unordered_set cimport unordered_set

ctypedef LC_CSR_Graph[double, void, dummy_true] Graph_CSR

# Cython bug: using a nested class from a previous typedef doesn't
# work for the time being. Instead, the full template specialization
# must be used to get the member type.
ctypedef LC_CSR_Graph[double, void, dummy_true].GraphNode GNodeCSR

cdef void jaccard_operator(Graph_CSR *g, unordered_set[GNodeCSR] *n1_neighbors, GNodeCSR n2) nogil:
    cdef:
        uint32_t intersection_size = 0
        uint32_t n1_size = n1_neighbors.size()
        uint32_t n2_size = 0
        double similarity
    n2_data = &g.getData(n2)
    edges = g.edges(n2, FLAG_UNPROTECTED)
    for e_iter in edges:
        ne = g.getEdgeDst(e_iter)
        if n1_neighbors.count(ne) > 0:
            intersection_size += 1
        n2_size += 1
    cdef uint32_t union_size = n1_size + n2_size - intersection_size
    if union_size > 0:
        similarity = <double>intersection_size / union_size
    else:
        similarity = 1
    n2_data[0] = similarity


cdef void jaccard_c(Graph_CSR *g, GNodeCSR key_node):
    cdef:
        unordered_set[GNodeCSR] key_neighbors

    with nogil:
        edges = g.edges(key_node)
        for e in edges:
            n = g.getEdgeDst(e)
            key_neighbors.insert(n)

        do_all(iterate(g[0].begin(), g[0].end()),
               bind_leading(&jaccard_operator, g, &key_neighbors),
               steal(),
               loopname("jaccard"))


cdef void printValue(Graph_CSR *g):
    cdef unsigned long numNodes = g[0].size()
    cdef double *data
    print("Number of nodes:", numNodes)
    i = 0
    for n in range(numNodes):
        data = &g[0].getData(n)
        if data[0] > 0:
            print("\t", n, ", ", data[0])
            i += 1
            if i > 100:
                print("\t...")
                break


#
# Main callsite
#
def jaccard(LC_CSR_Graph_Directed_double_void graph, unsigned int key_node):
    jaccard_c(&graph.underlying, <GNodeCSR> key_node)
