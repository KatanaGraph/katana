import numpy as np
import numba.types
import pyarrow

from galois.atomic import atomic_min, GAccumulator, GReduceMax
from galois.loops import *
from galois.property_graph import PropertyGraph
from galois.timer import StatTimer
from galois.shmem import setActiveThreads


def create_distance_array(g: PropertyGraph, source):
    inf_distance = numba.types.uint64.maxval
    a = np.empty(len(g), dtype=np.uint64)
    a[:] = inf_distance
    a[source] = 0
    return a


@for_each_operator()
def sssp_operator(g: PropertyGraph, dists: np.ndarray, edge_lengths, nid, ctx: UserContext):
    # TODO: We cannot distinguish multiple paths to the same node we we have to do the full amount of work each time.
    #  With some kind of structures as iteration elements (instead of just nid) we could short cut the case where we
    #  have already
    for ii in g.edges(nid):
        dst = g.get_edge_dst(ii)
        edge_length = edge_lengths[ii]
        new_distance = edge_length + dists[nid]
        old_distance = atomic_min(dists, dst, new_distance)
        if new_distance < old_distance:
            ctx.push(dst)


@obim_metric()
def obim_indexer(shift, dists, nid):
    return dists[nid] >> shift


def sssp(graph: PropertyGraph, source, length_property, shift, property_name):
    dists = create_distance_array(graph, source)
    t = StatTimer("Total SSSP")
    t.start()
    for_each(range(source, source+1),
             sssp_operator(graph, dists, graph.get_edge_property(length_property)),
             worklist=OrderedByIntegerMetric(obim_indexer(shift, dists)),
             disable_conflict_detection=True,
             loop_name="SSSP")
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


def verify_sssp(graph: PropertyGraph, source_i: int, property_id: int):
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--startNode', type=int, default=0)
    parser.add_argument('--propertyName', type=str, default="NewProperty")
    parser.add_argument('--edgeWeightProperty', type=str, required=True)
    parser.add_argument('--shift', type=int, default=6)
    parser.add_argument('--reportNode', type=int, default=1)
    parser.add_argument('--noverify', action='store_true', default=False)
    parser.add_argument('--threads', '-t', type=int, default=1)
    parser.add_argument('input', type=str)
    args = parser.parse_args()

    print("Using threads:", setActiveThreads(args.threads))

    graph = PropertyGraph(args.input)

    sssp(graph, args.startNode, args.edgeWeightProperty, args.shift, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.node_schema())
        newPropertyId = numNodeProperties - 1
        verify_sssp(graph, args.startNode, newPropertyId)
