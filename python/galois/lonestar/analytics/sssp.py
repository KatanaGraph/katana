import numpy as np
import numba.types
import pyarrow

from galois.atomic import atomic_min, GAccumulator, GReduceMax
from galois.datastructures import InsertBag
from galois.loops import (
    for_each_operator,
    for_each,
    UserContext,
    obim_metric,
    OrderedByIntegerMetric,
    do_all_operator,
    do_all,
)
from galois.property_graph import PropertyGraph
from galois.timer import StatTimer
from galois.shmem import setActiveThreads


UpdateRequest = np.dtype([("src", np.uint64), ("dist", np.uint32),])


def create_distance_array(g: PropertyGraph, source):
    inf_distance = numba.types.uint64.maxval
    a = np.empty(len(g), dtype=np.uint64)
    a[:] = inf_distance
    a[source] = 0
    return a


@for_each_operator()
def sssp_operator(g: PropertyGraph, dists: np.ndarray, edge_weights, item, ctx: UserContext):
    if dists[item.src] < item.dist:
        return
    for ii in g.edges(item.src):
        dst = g.get_edge_dst(ii)
        edge_length = edge_weights[ii]
        new_distance = edge_length + dists[item.src]
        old_distance = atomic_min(dists, dst, new_distance)
        if new_distance < old_distance:
            ctx.push((dst, new_distance))


@obim_metric()
def obim_indexer(shift, item):
    return item.dist >> shift


def sssp(graph: PropertyGraph, source, length_property, shift, property_name):
    dists = create_distance_array(graph, source)
    init_bag = InsertBag[UpdateRequest]()
    init_bag.push((source, 0))

    t = StatTimer("Total SSSP")
    t.start()
    for_each(
        init_bag,
        sssp_operator(graph, dists, graph.get_edge_property(length_property)),
        worklist=OrderedByIntegerMetric(obim_indexer(shift)),
        disable_conflict_detection=True,
        loop_name="SSSP",
    )
    t.stop()
    print("Elapsed time: ", t.get(), "milliseconds.")

    graph.add_node_property(pyarrow.table({property_name: dists}))


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


def verify_sssp(graph: PropertyGraph, _source_i: int, property_id: int):
    chunk_array = graph.get_node_property(property_id)
    not_visited = GAccumulator[int](0)
    max_dist = GReduceMax[int]()

    do_all(
        range(len(chunk_array)),
        not_visited_operator(graph.num_nodes(), not_visited, chunk_array),
        loop_name="not_visited_op",
    )

    if not_visited.reduce() > 0:
        print(
            not_visited.reduce(), " unvisited nodes; this is an error if graph is strongly connected",
        )

    do_all(
        range(len(chunk_array)),
        max_dist_operator(graph.num_nodes(), max_dist, chunk_array),
        steal=True,
        loop_name="max_dist_operator",
    )

    print("Max distance:", max_dist.reduce())


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--startNode", type=int, default=0)
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--edgeWeightProperty", type=str, required=True)
    parser.add_argument("--shift", type=int, default=6)
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    print("Using threads:", setActiveThreads(args.threads))

    graph = PropertyGraph(args.input)

    sssp(graph, args.startNode, args.edgeWeightProperty, args.shift, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.node_schema())
        newPropertyId = numNodeProperties - 1
        verify_sssp(graph, args.startNode, newPropertyId)


if __name__ == "__main__":
    main()
