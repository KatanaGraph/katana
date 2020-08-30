# cython: cdivision= True
from galois.shmem cimport *
from galois.cpp.libgalois.Galois cimport FLAG_UNPROTECTED
from galois.cpp.libgalois.atomic cimport atomicMin

from galois.timer import StatTimer
from galois.cpp.libstd.atomic cimport atomic

ctypedef uint32_t ComponentTy
ctypedef atomic[ComponentTy] AtomicComponentTy
ctypedef atomic[uint32_t] atomuint32_t 

#
# Struct for CC
#
cdef struct NodeTy:
    AtomicComponentTy comp_current
    ComponentTy comp_old

ctypedef LC_CSR_Graph[NodeTy, void, dummy_true] Graph

# Cython bug: using a nested class from a previous typedef doesn't
# work for the time being. Instead, the full template specialization
# must be used to get the member type.
ctypedef LC_CSR_Graph[NodeTy, void, dummy_true].GraphNode GNode


#
# Initialization for Components
#
cdef void initializeCompnents(Graph *g):
    cdef:
        unsigned long numNodes = g.size()
    for n in range(numNodes):
        data = &g.getData(n)
        data[0].comp_current.store(n)
        data[0].comp_old = numNodes

##
# LabelProp algorithm operator
##
cdef void labelPropOperator(Graph *g, bool *work_done, GNode n) nogil:
    src_data = &g.getData(n, FLAG_UNPROTECTED)
    if src_data.comp_old > src_data.comp_current.load():
        src_data.comp_old = src_data.comp_current.load()
        work_done[0] = 1
        edges = g.edges(n, FLAG_UNPROTECTED)
        for ii in edges:
            dst_data = &g.getData(g.getEdgeDst(ii), FLAG_UNPROTECTED)
            atomicMin[ComponentTy](dst_data.comp_current, src_data.comp_current.load())
##
# Label Propagation algorithm for 
# finding connected components
##
cdef void labelProp(Graph* graph):
    cdef:
        bool work_done = 1
        Timer T
    rounds = 0
    T.start()
    while(work_done):
        rounds += 1;
        with nogil:
            work_done = 0
            do_all(iterate(graph[0].begin(), graph[0].end()),
                     bind_leading(&labelPropOperator, graph, &work_done), 
                     no_pushes(),
                     steal(),
                     disable_conflict_detection(),
                     loopname("labelPropAlgo"))
    T.stop()
    print("Elapsed time:", T.get(), "milliseconds.")




#
# Main callsite for connected components
#
cdef class connected_components:
    cdef Graph graph

    def __init__(self, filename):
        self.graph.readGraphFromGRFile(bytes(filename, "utf-8"))

        timer = StatTimer("Connected Components Cython")
        timer.start()
        initializeCompnents(&self.graph)
        labelProp(&self.graph)
        timer.stop()

    cpdef getData(self, int i):
        if i < self.graph.size():
            return self.graph.getData(i).comp_current.load()
        else:
            raise IndexError(i)

    def __getitem__(self, int i):
        return self.getData(i)

    def __len__(self):
        return self.graph.size()
