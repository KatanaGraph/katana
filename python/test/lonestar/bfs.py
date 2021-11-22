import numpy as np
import pyarrow
from numba import jit

from katana import do_all, do_all_operator
from katana.local import Graph
from katana.local.atomic import ReduceMax, ReduceSum
from katana.local.datastructures import InsertBag
from katana.timer import StatTimer

# Use the same infinity as C++ bfs
distance_infinity = (2 ** 32) // 4


@jit(nopython=True)
def initialize(graph: Graph, source: int, distance: np.ndarray):
    num_nodes = graph.num_nodes()
    for n in range(num_nodes):
        if n == source:
            distance[n] = 0
        else:
            distance[n] = distance_infinity


@do_all_operator()
def not_visited_operator(not_visited: ReduceSum[int], data, nid):
    val = data[nid]
    if val >= distance_infinity:
        not_visited.update(1)


@do_all_operator()
def max_dist_operator(max_dist: ReduceMax[int], data, nid):
    val = data[nid]
    if val < distance_infinity:
        max_dist.update(val)


def verify_bfs(graph: Graph, _source_i: int, property_id: int):
    chunk_array = graph.get_node_property(property_id)
    not_visited = ReduceSum[int](0)
    max_dist = ReduceMax[int]()

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
    graph: Graph, next_level: InsertBag[np.uint64], next_level_number: int, distance: np.ndarray, nid,
):
    for ii in graph.edge_ids(nid):
        dst = graph.get_edge_dest(ii)
        if distance[dst] == distance_infinity:
            distance[dst] = next_level_number
            next_level.push(dst)


def bfs_sync_pg(graph: Graph, source, property_name):
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

    import katana.local
    from katana import set_active_threads

    katana.local.initialize()

    parser = argparse.ArgumentParser()
    parser.add_argument("--startNode", type=int, default=0)
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)

    args = parser.parse_args()

    print("Using threads:", set_active_threads(args.threads))

    graph = Graph(args.input)

    bfs_sync_pg(graph, args.startNode, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.loaded_node_schema())
        newPropertyID = numNodeProperties - 1
        verify_bfs(graph, args.startNode, newPropertyID)


if __name__ == "__main__":
    main()
