# cython: cdivision = True

from galois.shmem cimport *
from galois.graphs cimport LC_CSR_Graph_Directed_uint32_t_void

ctypedef atomic[uint32_t] atomuint32_t

ctypedef LC_CSR_Graph[uint32_t, void, dummy_true] Graph_CSR

# Cython bug: using a nested class from a previous typedef doesn't
# work for the time being. Instead, the full template specialization
# must be used to get the member type.
ctypedef LC_CSR_Graph[uint32_t, void, dummy_true].GraphNode GNodeCSR

cdef void printValue(Graph_CSR *g):
    cdef unsigned long numNodes = g[0].size()
    cdef uint32_t *data
    print("Number of nodes:", numNodes)
    for n in range(numNodes):
        data = &g[0].getData(n)
        print("\t", data[0])

##############################################################################
## Bfs implementation
###########################################################################
#
# Initialization for BFS
#
cdef void Initialize(Graph_CSR *g, unsigned long source):
    cdef unsigned long numNodes = g[0].size()
    cdef:
        LC_CSR_Graph[uint32_t, void, dummy_true].edge_iterator ii
        LC_CSR_Graph[uint32_t, void, dummy_true].edge_iterator ei
        uint32_t *data
    for n in range(numNodes):
        data = &g.getData(n)
        if(n == source):
            data[0] = 0
        else:
            data[0] = numNodes


#
# BFS Operator to be executed on each Graph node
#
cdef void bfs_operator(Graph_CSR *g, bool *work_done, GNodeCSR n, UserContext[GNodeCSR] &ctx) nogil:
    cdef:
        uint32_t *src_data
        uint32_t *dst_data
    src_data = &g.getData(n)
    edges = g.edges(n)
    for ii in edges:
        dst_data = &g.getData(g.getEdgeDst(ii))
        if src_data[0] > dst_data[0] + 1:
            src_data[0] = dst_data[0] + 1
            work_done[0] = 1

cdef void bfs_pull_topo(Graph_CSR *graph):
    cdef bool work_done = 1
    rounds = 0;
    while(work_done):
        rounds += 1;
        with nogil:
            work_done = 0
            for_each(iterate(graph[0]),
                     bind_leading(&bfs_operator, graph, &work_done), no_pushes())#,
                     #loopname("name1"))


#
# BFS sync operator to be executed on each Graph node
#
cdef void bfs_sync_operator(Graph_CSR *g, InsertBag[GNodeCSR] *next, int nextLevel, GNodeCSR n) nogil:
    cdef:
        uint32_t *src_data
        uint32_t *dst_data
        uint32_t numNodes = g.size()
        GNodeCSR dst
    src_data = &g.getData(n)
    edges = g.edges(n)
    for ii in edges:
        dst = g.getEdgeDst(ii)
        dst_data = &g.getData(dst)
        if dst_data[0] == numNodes:
            dst_data[0] = nextLevel
            next.push(dst)

cdef void bfs_sync(Graph_CSR *graph, GNodeCSR source):
    cdef:
        InsertBag[GNodeCSR] curr, next
        uint32_t nextLevel = 0;

    next.push(source)
    while(not next.empty()):
        curr.swap(next)
        next.clear()
        nextLevel += 1;
        with nogil:
            do_all(iterate(curr),
                     bind_leading(&bfs_sync_operator, graph, &next, nextLevel), no_pushes(), steal(),
                     loopname("bfs_sync"))

cdef void not_visited_operator(Graph_CSR *graph, atomuint32_t *notVisited, GNodeCSR n):
    cdef:
        uint32_t *data
        uint32_t numNodes = graph[0].size()
    data = &graph.getData(n)
    if (data[0] >= numNodes):
        notVisited[0].fetch_add(1)

cdef void max_dist_operator(Graph_CSR *graph, GReduceMax[uint32_t] *maxDist , GNodeCSR n):
    cdef:
        uint32_t *data
        uint32_t numNodes = graph[0].size()
    data = &graph.getData(n)
    if(data[0] < numNodes):
        maxDist.update(data[0])


def verify_bfs(LC_CSR_Graph_Directed_uint32_t_void graph_py, unsigned int source_i):
    cdef:
        atomuint32_t notVisited
        uint32_t *data
        GReduceMax[uint32_t] maxDist
        Graph_CSR *graph = &graph_py.underlying
        GNodeCSR source = <GNodeCSR> source_i

    data = &graph.getData(source)
    if data[0] is not 0:
        print("ERROR: source has non-zero dist value ==", data[0])

    notVisited.store(0)
    with nogil:
        do_all(iterate(graph[0]),
                bind_leading(&not_visited_operator, graph, &notVisited), no_pushes(), steal(),
                loopname("not_visited_op"))

    if notVisited.load() > 0:
        print(notVisited.load(), " unvisited nodes; this is an error if graph is strongly connected")

    with nogil:
        do_all(iterate(graph[0]),
                bind_leading(&max_dist_operator, graph, &maxDist), no_pushes(), steal(),
                loopname("not_visited_op"))

    print("Max distance:", maxDist.reduce())

#
# Main callsite for Bfs
#        
def bfs(LC_CSR_Graph_Directed_uint32_t_void graph, unsigned int source):
    Initialize(&graph.underlying, source)
    bfs_sync(&graph.underlying, <GNodeCSR>source)
    # verify_bfs(&graph.underlying, <GNodeCSR>source)
