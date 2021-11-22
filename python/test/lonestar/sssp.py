import numpy as np
import pyarrow

from katana import (
    OrderedByIntegerMetric,
    UserContext,
    do_all,
    do_all_operator,
    for_each,
    for_each_operator,
    obim_metric,
    set_active_threads,
)
from katana.local import Graph
from katana.local.atomic import ReduceMax, ReduceSum, atomic_min
from katana.local.datastructures import InsertBag
from katana.timer import StatTimer


def dtype_info(t):
    t = np.dtype(t)
    if t.kind == "f":
        return np.finfo(t)
    return np.iinfo(t)


def create_distance_array(g: Graph, source, length_property):
    a = np.empty(len(g), dtype=dtype_of_pyarrow_array(g.get_edge_property(length_property)))
    # TODO(amp): Remove / 4
    infinity = dtype_info(a.dtype).max / 4
    a[:] = infinity
    a[source] = 0
    return a


def dtype_of_pyarrow_array(a):
    # TODO(amp): This is a hack. But I actually think it will work pretty reliably. Ideally pyarrow would provide a
    #  conversion.
    return str(a.type)


@for_each_operator()
def sssp_operator(g: Graph, dists: np.ndarray, edge_weights, item, ctx: UserContext):
    if dists[item.src] < item.dist:
        return
    for ii in g.edge_ids(item.src):
        dst = g.get_edge_dest(ii)
        edge_length = edge_weights[ii]
        new_distance = edge_length + dists[item.src]
        old_distance = atomic_min(dists, dst, new_distance)
        if new_distance < old_distance:
            ctx.push((dst, new_distance))


@obim_metric()
def obim_indexer(shift, item):
    return item.dist >> shift


def sssp(graph: Graph, source, length_property, shift, property_name):
    dists = create_distance_array(graph, source, length_property)

    # Define the struct type here so it can depend on the type of the weight property
    UpdateRequest = np.dtype([("src", np.uint32), ("dist", dists.dtype)])

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
def not_visited_operator(infinity: int, not_visited: ReduceSum[int], data, nid):
    val = data[nid]
    if val == infinity:
        not_visited.update(1)


@do_all_operator()
def max_dist_operator(infinity: int, max_dist: ReduceMax[int], data, nid):
    val = data[nid]
    if val != infinity:
        max_dist.update(val)


def verify_sssp(graph: Graph, _source_i: int, property_id: int):
    prop_array = graph.get_node_property(property_id)
    not_visited = ReduceSum[int](0)
    max_dist = ReduceMax[int]()
    # TODO(amp): Remove / 4
    infinity = dtype_info(dtype_of_pyarrow_array(prop_array)).max / 4

    do_all(
        range(len(prop_array)), not_visited_operator(infinity, not_visited, prop_array), loop_name="not_visited_op",
    )

    if not_visited.reduce() > 0:
        print(
            not_visited.reduce(), " unvisited nodes; this is an error if graph is strongly connected",
        )

    do_all(
        range(len(prop_array)),
        max_dist_operator(infinity, max_dist, prop_array),
        steal=True,
        loop_name="max_dist_operator",
    )

    print("Max distance:", max_dist.reduce())


def main():
    import argparse

    import katana.local

    katana.local.initialize()

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

    print("Using threads:", set_active_threads(args.threads))

    graph = Graph(args.input)

    sssp(graph, args.startNode, args.edgeWeightProperty, args.shift, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.loaded_node_schema())
        newPropertyID = numNodeProperties - 1
        verify_sssp(graph, args.startNode, newPropertyID)


if __name__ == "__main__":
    main()
