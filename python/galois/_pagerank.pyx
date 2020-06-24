# cython: cdivision = True

from galois.shmem cimport *
from galois.shmem import *
from cython.operator cimport preincrement, dereference as deref

ctypedef atomic[uint32_t] atomuint32_t
ctypedef atomic[uint64_t] atomuint64_t
##############################################################################
## Pagerank implementation
###############################################################################
#
# Struct for Pagerank
#
cdef struct NodeTy:
    float rank
    uint32_t nout

ctypedef LC_CSR_Graph[NodeTy, void, dummy_true] Graph

# Cython bug: using a nested class from a previous typedef doesn't
# work for the time being. Instead, the full template specialization
# must be used to get the member type.
ctypedef LC_CSR_Graph[NodeTy, void, dummy_true].GraphNode GNode

#
# Constants for Pagerank
#
cdef float ALPHA = 0.85
cdef float INIT_RESIDUAL = 1 - ALPHA;
cdef float TOLERANCE   = 1.0e-3;
cdef uint32_t MAX_ITER = 1000;

#
# Initialization for Pagerank
#
cdef void InitializePR(Graph *g):
    cdef unsigned long numNodes = g[0].size()
    cdef NodeTy *data
    for n in range(numNodes):
        data = &g[0].getData(n)
        data[0].rank = INIT_RESIDUAL
        data[0].nout = 0

cdef void printValuePR(Graph *g):
    cdef unsigned long numNodes = g[0].size()
    cdef NodeTy *data
    print("Number of nodes:", numNodes)
    for n in range(numNodes):
        data = &g[0].getData(n)
        print(data[0].rank)

#
# Operator for computing outdegree of nodes in the Graph
#
cdef void computeOutDeg_operator(Graph *g, LargeArray[atomuint64_t] *largeArray, GNode n) nogil:
    cdef:
        GNode dst

    edges = g.edges(n)
    for ii in edges:
        dst = g.getEdgeDst(ii)
        largeArray[0][<size_t>dst].fetch_add(1)

#
# Operator for assigning outdegree of nodes in the Graph
#
cdef void assignOutDeg_operator(Graph *g, LargeArray[atomuint64_t] *largeArray, GNode n) nogil:
    cdef NodeTy *src_data

    src_data = &g.getData(n)
    src_data.nout = largeArray[0][<size_t>n].load()

#
# Main callsite for computing outdegree of nodes in the Graph
#
cdef void computeOutDeg(Graph *graph):
    cdef:
        uint64_t numNodes = graph.size()
        LargeArray[atomuint64_t] largeArray

    largeArray.allocateInterleaved(numNodes)
    with nogil:
        do_all(iterate(graph.begin(), graph.end()),
                        bind_leading(&computeOutDeg_operator, graph, &largeArray), steal(),
                        loopname("ComputeDegree"))

        do_all(iterate(graph.begin(), graph.end()),
                        bind_leading(&assignOutDeg_operator, graph, &largeArray))


#
# Operator for PageRank
#
cdef void pagerankPullTopo_operator(Graph *g, GReduceMax[float] *max_delta, GNode n) nogil:
    cdef:
        GNode dst
        NodeTy *dst_data
        NodeTy *src_data
        float sum = 0
        float value = 0
        float diff = 0;
    src_data = &g.getData(n)
    edges = g.edges(n, FLAG_UNPROTECTED)
    for ii in edges:
        dst_data = &g.getData(g.getEdgeDst(ii), FLAG_UNPROTECTED)
        sum += dst_data[0].rank / dst_data[0].nout
    value = sum * ALPHA + (1.0 - ALPHA)
    diff = fabs(value - src_data[0].rank);
    src_data[0].rank = value
    max_delta[0].update(diff)

#
# Pagerank routine: Loop till convergence
#
cdef void pagerankPullTopo(Graph *graph, uint32_t max_iterations):
    cdef:
        GReduceMax[float] max_delta
        float delta = 0
        uint32_t iteration = 0
        Timer T

    T.start()
    while True:
        with nogil:
            do_all(iterate(graph.begin(), graph.end()),
                        bind_leading(&pagerankPullTopo_operator, graph, &max_delta), steal(),
                        loopname("PageRank"))

        delta = max_delta.reduce()
        iteration += 1
        if delta <= TOLERANCE or iteration >= max_iterations:
            break
        max_delta.reset();

    T.stop()
    print("Elapsed time:", T.get(), "milliseconds.")
    if(iteration >= max_iterations):
        print("WARNING: failed to converge in", iteration, "iterations")


#
# Main callsite for Pagerank
#
cdef class pagerank:
    cdef Graph graph

    def __init__(self, uint32_t max_iterations, filename):
        self.graph.readGraphFromGRFile(bytes(filename, "utf-8"))

        InitializePR(&self.graph)
        computeOutDeg(&self.graph)
        pagerankPullTopo(&self.graph, max_iterations)

    cpdef getData(self, int i):
        return self.graph.getData(i).rank

    def __getitem__(self, int i):
        return self.getData(i)

    def __len__(self):
        return self.graph.size()
