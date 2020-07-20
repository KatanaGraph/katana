import numpy as np
import pyarrow
from numba import jit

from galois.atomic import GAccumulator, GReduceMax
from galois.datastructures import InsertBag
from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads
from galois.timer import StatTimer


@jit(nopython=True)
def initialize(graph: PropertyGraph, source: int, distance: np.ndarray):
    num_nodes = graph.num_nodes()
    for n in range(num_nodes):
        if n == source:
            distance[n] = 0
        else:
            distance[n] = num_nodes


@do_all_operator()
def not_visited_operator(num_nodes: int, not_visited: GAccumulator[int], data, nid):
    val = data[nid]
    if val >= num_nodes:
        not_visited.update(1)


@do_all_operator()
def max_dist_operator(num_nodes: int, max_dist: GReduceMax[int], data, nid):
    val = data[nid]
    if val >= num_nodes:
        max_dist.update(val)


def verify_bfs(graph: PropertyGraph, source_i: int, property_id: int):
    chunk_array = graph.get_node_property(property_id)
    not_visited = GAccumulator[int](0)
    max_dist = GReduceMax[int]()

    do_all(range(len(chunk_array)),
           not_visited_operator(graph.num_nodes(), not_visited, chunk_array),
           loop_name="not_visited_op")

    if not_visited.reduce() > 0:
        print(not_visited.reduce(), " unvisited nodes; this is an error if graph is strongly connected")

    do_all(range(len(chunk_array)),
           max_dist_operator(graph.num_nodes(), max_dist, chunk_array),
           steal=True, loop_name="max_dist_operator")

    print("Max distance:", max_dist.reduce())


@do_all_operator()
def bfs_sync_operator_pg(graph: PropertyGraph, next: InsertBag['uint64_t'], next_level: int, distance: np.ndarray, nid):
    num_nodes = graph.num_nodes()

    for ii in graph.edges(nid):
        dst = graph.get_edge_dst(ii)
        if distance[dst] == num_nodes:
            distance[dst] = next_level
            next.push(dst)


def bfs_sync_pg(graph: PropertyGraph, source, property_name):
    num_nodes = graph.num_nodes()
    next_level = 0

    curr = InsertBag['uint64_t']()
    next = InsertBag['uint64_t']()
    distance = np.empty((num_nodes,), dtype=int)

    timer = StatTimer("BFS Property Graph Numba")
    timer.start()
    initialize(graph, source, distance)
    next.push(source)
    while not next.empty():
        curr.swap(next)
        next.clear()
        next_level += 1
        f = bfs_sync_operator_pg(graph, next, next_level, distance)
        do_all(graph,
               f,
               steal=True, loop_name="bfs_sync_pg")
    timer.stop()

    graph.add_node_property(pyarrow.table({property_name: distance}))

def test_bfs(property_graph):
    print("Using threads:", setActiveThreads(2))

    graph = property_graph
    startNode = 0
    propertyName = "NewProp"

    bfs_sync_pg(graph, startNode, propertyName)

    numNodeProperties = len(graph.node_schema())
    newPropertyId = numNodeProperties - 1
    verify_bfs(graph, startNode, newPropertyId)
