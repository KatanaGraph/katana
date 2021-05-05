import numpy as np
import pyarrow
from numba import jit

from katana.atomic import GAccumulator, GReduceMax
from katana.datastructures import InsertBag
from ._bfs_property_graph import bfs as cython_bfs, verify_bfs as cython_verify_bfs
from katana.loops import do_all, do_all_operator
from katana.property_graph import PropertyGraph
from katana.timer import StatTimer


# Use the same infinity as C++ bfs
distance_infinity = (2 ** 32) // 4


@jit(nopython=True)
def initialize(graph: PropertyGraph, source: int, distance: np.ndarray):
    num_nodes = graph.num_nodes()
    for n in range(num_nodes):
        if n == source:
            distance[n] = 0
        else:
            distance[n] = distance_infinity


@do_all_operator()
def not_visited_operator(not_visited: GAccumulator[int], data, nid):
    val = data[nid]
    if val >= distance_infinity:
        not_visited.update(1)


@do_all_operator()
def max_dist_operator(max_dist: GReduceMax[int], data, nid):
    val = data[nid]
    if val < distance_infinity:
        max_dist.update(val)


def verify_bfs(graph: PropertyGraph, _source_i: int, property_id: int):
    chunk_array = graph.get_node_property(property_id)
    not_visited = GAccumulator[int](0)
    max_dist = GReduceMax[int]()

    do_all(
        range(len(chunk_array)), not_visited_operator(not_visited, chunk_array), loop_name="not_visited_op",
    )

    if not_visited.reduce() > 0:
        print(
            not_visited.reduce(), " unvisited nodes; this is an error if graph is strongly connected",
        )

    do_all(
        range(len(chunk_array)), max_dist_operator(max_dist, chunk_array), steal=True, loop_name="max_dist_operator",
    )

    print("BFS Max distance:", max_dist.reduce())


@do_all_operator()
def bfs_sync_operator_pg(
    graph: PropertyGraph, next_level: InsertBag[np.uint64], next_level_number: int, distance: np.ndarray, nid,
):
    for ii in graph.edges(nid):
        dst = graph.get_edge_dst(ii)
        if distance[dst] == distance_infinity:
            distance[dst] = next_level_number
            next_level.push(dst)


def bfs_sync_pg(graph: PropertyGraph, source, property_name):
    next_level_number = 0

    curr_level = InsertBag[np.uint64]()
    next_level = InsertBag[np.uint64]()

    timer = StatTimer("BFS Property Graph Numba: " + property_name)
    timer.start()
    distance = np.empty((len(graph),), dtype=np.uint32)
    initialize(graph, source, distance)
    next_level.push(source)
    while not next_level.empty():
        curr_level.swap(next_level)
        next_level.clear()
        next_level_number += 1
        do_all(
            curr_level,
            bfs_sync_operator_pg(graph, next_level, next_level_number, distance),
            steal=True,
            loop_name="bfs_sync_pg",
        )
    timer.stop()

    graph.add_node_property(pyarrow.table({property_name: distance}))


def main():
    import argparse
    from katana.galois import set_active_threads

    parser = argparse.ArgumentParser()
    parser.add_argument("--startNode", type=int, default=0)
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--cython", action="store_true", default=False)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)

    args = parser.parse_args()

    print("Using threads:", set_active_threads(args.threads))

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


if __name__ == "__main__":
    main()
