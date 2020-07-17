# cython: cdivision= True
from galois.shmem cimport *
from .cpp.libstd.atomic cimport atomic

ctypedef uint32_t Dist
ctypedef atomic[Dist] AtomicDist
ctypedef atomic[uint32_t] atomuint32_t 

ctypedef uint32_t EdgeTy
ctypedef LC_CSR_Graph[AtomicDist, EdgeTy, dummy_true] Graph_CSR

# Cython bug: using a nested class from a previous typedef doesn't
# work for the time being. Instead, the full template specialization
# must be used to get the member type.
ctypedef LC_CSR_Graph[AtomicDist, EdgeTy, dummy_true].GraphNode GNodeCSR

cdef void printValue(Graph_CSR *g):
    cdef unsigned long numNodes = g[0].size()
    cdef AtomicDist *data
    print("Number of nodes:", numNodes)
    for n in range(numNodes):
        data = &g[0].getData(n)
        print("\t", data[0].load())
##############################################################################
## SSSP implementation
###########################################################################
#
# Initialization for SSSP
# Source distance is set to 0; Other nodes distance is set
# to number of nodes 
#
cdef void Initialize(Graph_CSR *g, unsigned long source):
    cdef:
        unsigned long numNodes = g.size()
    for n in range(numNodes):
        data = &g.getData(n)
        if(n == source):
            data[0].store(0)
        else:
            data[0].store(numNodes)
        

ctypedef UpdateRequest[GNodeCSR, Dist] UpdateRequestObj
#
# SSSP Delta step Operator to be executed on each Graph node
#
cdef void ssspOperator(Graph_CSR *g, UpdateRequestObj item, UserContext[UpdateRequestObj] &ctx) nogil:
    cdef:
        unsigned long numNodes = g.size()
        EdgeTy edge_data
        Dist oldDist, newDist
    src_data = &g.getData(item.src, FLAG_UNPROTECTED)
    edges = g.edges(item.src, FLAG_UNPROTECTED)
    if src_data.load() < item.dist:
        return
    for ii in edges:
        dst = g.getEdgeDst(ii)
        dst_data = &g.getData(dst, FLAG_UNPROTECTED)
        edge_data = g.getEdgeData(ii, FLAG_UNPROTECTED)
        newDist = src_data[0].load() + edge_data
        oldDist = atomicMin[Dist](dst_data[0], newDist)
        if newDist < oldDist:
            ctx.push(UpdateRequestObj(dst, newDist))

######
# SSSP Delta step algo using OBIM 
#####
ctypedef ChunkFIFO[Uint_64u] ChunkFIFO_64
ctypedef PerSocketChunkFIFO[Uint_64u] PerSocketChunkFIFO_64
ctypedef OrderedByIntegerMetric[UpdateRequestIndexer, PerSocketChunkFIFO_64] OBIM
cdef void ssspDeltaStep(Graph_CSR *graph, GNodeCSR source, uint32_t shift):
    cdef:
        Timer T
        InsertBag[UpdateRequestObj] initBag
        
    initBag.push(UpdateRequestObj(source, 0))
    T.start()
    with nogil:
        for_each(iterate(initBag),
                    bind_leading(&ssspOperator, graph),
                                wl[OBIM](UpdateRequestIndexer(shift)), 
                                #steal(), 
                                disable_conflict_detection(),
                                loopname("SSSP"))
    T.stop()
    print("Elapsed time:", T.get(), "milliseconds.")



#######################
# Verification routines
#######################
cdef void not_visited_operator(Graph_CSR *graph, atomuint32_t *notVisited, GNodeCSR n):
    cdef: 
        AtomicDist *data
        uint32_t numNodes = graph[0].size()
    data = &graph[0].getData(n)
    if (data[0].load() >= numNodes):
        notVisited[0].fetch_add(1)

cdef void max_dist_operator(Graph_CSR *graph, GReduceMax[uint32_t] *maxDist , GNodeCSR n):
    cdef: 
        AtomicDist *data
        uint32_t numNodes = graph[0].size()
    data = &graph[0].getData(n)
    if(data[0].load() < numNodes):
        maxDist[0].update(data[0].load())

cdef bool verify_sssp(Graph_CSR *graph, GNodeCSR source):
    cdef: 
        atomuint32_t notVisited
        AtomicDist *data
        GReduceMax[uint32_t] maxDist;

    data = &graph[0].getData(source)
    if(data[0].load() is not 0):
        print("ERROR: source has non-zero dist value ==", data[0].load())
    
    notVisited.store(0)
    with nogil:
        do_all(iterate(graph[0]),
                bind_leading(&not_visited_operator, graph, &notVisited), no_pushes(), steal(),
                loopname("not_visited_op"))

    if(notVisited.load() > 0):
        print(notVisited.load(), "unvisited nodes; this is an error if graph is strongly connected")

    with nogil:
        do_all(iterate(graph[0]),
                bind_leading(&max_dist_operator, graph, &maxDist), no_pushes(), steal(),
                loopname("not_visited_op"))

    print("Max distance:", maxDist.reduce())



#
# Main callsite for SSSP
#
cdef class sssp:
    cdef Graph_CSR graph

    def __init__(self, uint32_t shift, unsigned long source, filename):
        self.graph.readGraphFromGRFile(bytes(filename, "utf-8"))

        Initialize(&self.graph, source)
        ssspDeltaStep(&self.graph, <GNodeCSR>source, shift)

    cpdef getData(self, int i):
        if i < self.graph.size():
            return self.graph.getData(i).load()
        else:
            raise IndexError(i)

    def __getitem__(self, int i):
        return self.getData(i)

    def __len__(self):
        return self.graph.size()


