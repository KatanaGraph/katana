# cython: cdivision = True

from galois.shmem cimport *
import galois.shmem
from libgalois.Galois cimport FLAG_UNPROTECTED
from cython.operator cimport preincrement, dereference as deref
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


cdef void jaccard(Graph_CSR *g, GNodeCSR key_node):
    cdef:
        Timer T
        unordered_set[GNodeCSR] key_neighbors

    with nogil:
        edges = g.edges(key_node)
        for e in edges:
            n = g.getEdgeDst(e)
            key_neighbors.insert(n)

        T.start()
        do_all(iterate(g[0].begin(), g[0].end()),
               bind_leading(&jaccard_operator, g, &key_neighbors),
               steal(),
               loopname("jaccard"))
        T.stop()
    gPrint(b"Elapsed time:", T.get(), b" milliseconds.\n")


cdef void printValue(Graph_CSR *g):
    cdef unsigned long numNodes = g[0].size()
    cdef double *data
    gPrint(b"Number of nodes : ", numNodes, b"\n")
    i = 0
    for n in range(numNodes):
        data = &g[0].getData(n)
        if data[0] > 0:
            gPrint(b"\t", n, b", ", data[0], b"\n")
            i += 1
            if i > 100:
                gPrint(b"\t...\n")
                break


#
# Main callsite
#
def run_jaccard(int numThreads, string filename, unsigned int key_node):
    cdef int new_numThreads = setActiveThreads(numThreads)
    if new_numThreads != numThreads:
        print("Warning, using fewer threads than requested")

    print("Using {0} thread(s).".format(new_numThreads))
    cdef Graph_CSR graph

    ## Read the CSR format of graph
    ## directly from disk.
    graph.readGraphFromGRFile(filename)
    jaccard(&graph, <GNodeCSR> key_node)
    printValue(&graph)
