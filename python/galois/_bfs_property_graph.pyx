# cython: cdivision = True

from pyarrow.lib cimport *

from galois.shmem cimport *
from cython.operator cimport dereference as deref

from galois.timer import StatTimer

ctypedef atomic[uint32_t] atomuint32_t

cdef extern from * nogil:
    """
std::shared_ptr<arrow::Table> MakeTable(const std::string& name,
                                        const std::vector<uint32_t>& data) {
  arrow::NumericBuilder<arrow::UInt32Type> builder;

  auto append_status = builder.AppendValues(data.begin(), data.end());
  GALOIS_LOG_ASSERT(append_status.ok());

  std::shared_ptr<arrow::Array> array;

  auto finish_status = builder.Finish(&array);
  GALOIS_LOG_ASSERT(finish_status.ok());

  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field(name, arrow::uint32())});

  return arrow::Table::Make(schema, {array});
}
"""
    cdef shared_ptr[CTable] MakeTable(string name, vector[uint32_t] data)


cdef void Initialize(PropertyGraph graph, unsigned long source, vector[uint32_t] &distance):
    cdef uint64_t numNodes = graph.num_nodes()
    for n in range(numNodes):
        if(n == source):
            distance[n] = 0
        else:
            distance[n] = numNodes

cdef void not_visited_operator(uint64_t numNodes, atomuint32_t *notVisited, shared_ptr[CUInt32Array] data, uint32_t n):
    cdef:
        uint32_t val
    val = deref(data).Value(n)
    if val >= numNodes:
        notVisited[0].fetch_add(1)

cdef void max_dist_operator(uint64_t numNodes, GReduceMax[uint32_t] *maxDist, shared_ptr[CUInt32Array] data, uint32_t n):
    cdef:
        uint32_t val
    val = deref(data).Value(n)
    if(val < numNodes):
        maxDist.update(val)

def verify_bfs(PropertyGraph graph, unsigned int source_i, unsigned int property_id):
    cdef:
        atomuint32_t notVisited
        GReduceMax[uint32_t] maxDist
        uint32_t source = <uint32_t> source_i
        shared_ptr[CUInt32Array] chunk
        uint32_t chunk_offset = 0
        uint64_t numNodes = graph.num_nodes()
        uint32_t start, end
    chunk_array = graph.underlying.get().NodeProperty(0)

    notVisited.store(0)
    ### Chunked arrays can have multiple chunks
    for i in range(chunk_array.get().num_chunks()):
        chunk = static_pointer_cast[CUInt32Array, CArray](chunk_array.get().chunk(i))
        start = chunk_offset
        end = chunk_offset + chunk.get().length()
        chunk_offset = chunk_offset + deref(chunk).length()
        with nogil:
            do_all(iterate(start, end),
                    bind_leading(&not_visited_operator, numNodes, &notVisited, chunk), no_pushes(), steal(),
                    loopname("not_visited_op"))

        if notVisited.load() > 0:
            print(notVisited.load(), " unvisited nodes; this is an error if graph is strongly connected")

        with nogil:
            do_all(iterate(start, end),
                    bind_leading(&max_dist_operator, numNodes, &maxDist, chunk), no_pushes(), steal(),
                    loopname("not_visited_op"))

        print("Max distance:", maxDist.reduce())


#
# BFS sync operator to be executed on each Graph node
#
cdef void bfs_sync_operator_pg(shared_ptr[PropertyFileGraph] g, InsertBag[uint32_t] *next, int nextLevel, vector[uint32_t] *distance, uint32_t n) nogil:
    cdef:
        uint64_t numNodes = deref(g).topology().num_nodes()
        uint32_t dst
        uint64_t edges
        uint64_t val

    ###TODO: Better way to access edges
    edges = 0
    if(n > 0):
        edges = deref(deref(g).topology().out_indices).Value(n) - deref(deref(g).topology().out_indices).Value(n - 1)
    for ii in range(edges):
    ###TODO: Better way to access edges
        dst = deref(deref(g).topology().out_dests).Value(ii)
        if distance[0][dst] == numNodes:
            distance[0][dst] = nextLevel
            next.push(dst)

cdef void bfs_sync_pg(PropertyGraph graph, uint32_t source, string propertyName):
    cdef:
        InsertBag[uint32_t] curr, next
        uint32_t nextLevel = 0;
        uint32_t num_nodes = graph.num_nodes()
        ### Vector to calculate distance from source node
        # TODO: Replace with gstl::largeArray
        vector[uint32_t] distance
        shared_ptr[PropertyFileGraph] pfgraph = graph.underlying

    timer = StatTimer("BFS Property Graph Cython")
    timer.start()
    distance.resize(num_nodes)
    Initialize(graph, source, distance)
    next.push(source)
    while(not next.empty()):
        curr.swap(next)
        next.clear()
        nextLevel += 1;
        with nogil:
            ##TODO: Iterators over graph topology
            do_all(iterate(<uint32_t>0, num_nodes),
                     bind_leading(&bfs_sync_operator_pg, pfgraph, &next, nextLevel, &distance), no_pushes(), steal(),
                     loopname("bfs_sync"))
    timer.stop()
    #
    # Append new property
    #
    cdef shared_ptr[CTable] node_table = MakeTable(propertyName, distance)
    graph.underlying.get().AddNodeProperties(node_table)

#
# Main callsite for Bfs
#        
def bfs(PropertyGraph graph, unsigned int source, str propertyName):
    try:
        graph.remove_node_property(propertyName)
    except ValueError:
        pass # Ignore non-existance of the property.
    bfs_sync_pg(graph, source, bytes(propertyName, "utf-8"))
    


