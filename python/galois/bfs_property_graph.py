import numpy as np
import pyarrow
from numba import jit

from galois.atomic import GAccumulator, GReduceMax
from galois.datastructures import InsertBag
from ._bfs_property_graph import bfs as cython_bfs, verify_bfs as cython_verify_bfs
from .loops import do_all, do_all_operator
from .property_graph import PropertyGraph
from .timer import StatTimer


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
    if val < num_nodes:
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
def bfs_sync_operator_pg(graph: PropertyGraph, next: InsertBag[np.uint64], next_level: int, distance: np.ndarray, nid):
    num_nodes = graph.num_nodes()

    for ii in graph.edges(nid):
        dst = graph.get_edge_dst(ii)
        if distance[dst] == num_nodes:
            distance[dst] = next_level
            next.push(dst)


def bfs_sync_pg(graph: PropertyGraph, source, property_name):
    num_nodes = graph.num_nodes()
    next_level = 0

    curr = InsertBag[np.uint64]()
    next = InsertBag[np.uint64]()

    timer = StatTimer("BFS Property Graph Numba: " + property_name)
    timer.start()
    distance = np.empty((num_nodes,), dtype=int)
    initialize(graph, source, distance)
    next.push(source)
    while not next.empty():
        curr.swap(next)
        next.clear()
        next_level += 1
        do_all(curr,
               bfs_sync_operator_pg(graph, next, next_level, distance),
               steal=True, loop_name="bfs_sync_pg")
    timer.stop()

    graph.add_node_property(pyarrow.table({property_name: distance}))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--startNode', type=int, default=0)
    parser.add_argument('--propertyName', type=str, default="NewProperty")
    parser.add_argument('--reportNode', type=int, default=1)
    parser.add_argument('--noverify', action='store_true', default=False)
    parser.add_argument('--cython', action='store_true', default=False)
    parser.add_argument('--threads', '-t', type=int, default=1)
    parser.add_argument('input', type=str)
    args = parser.parse_args()

    from galois.shmem import *
    print("Using threads:", setActiveThreads(args.threads))

    graph = PropertyGraph(args.input)

    if args.cython:
        cython_bfs(graph, args.startNode, args.propertyName)
    else:
        bfs_sync_pg(graph, args.startNode, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.node_schema())
        newPropertyId = numNodeProperties - 1
        if args.cython:
            cython_verify_bfs(graph, args.startNode, newPropertyId)
        else:
            verify_bfs(graph, args.startNode, newPropertyId)
