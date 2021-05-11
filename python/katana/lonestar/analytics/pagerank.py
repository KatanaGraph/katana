import numpy as np
import pyarrow

from katana.atomic import (
    GAccumulator,
    GReduceLogicalOr,
    atomic_add,
    GReduceMin,
    GReduceMax,
)
from katana.datastructures import LargeArray, AllocationPolicy
from katana.loops import do_all, do_all_operator
from katana.property_graph import PropertyGraph
from katana.galois import set_active_threads
from katana.timer import StatTimer

# Constants for Pagerank
ALPHA = 0.85
INIT_RESIDUAL = 1 - ALPHA


@do_all_operator()
def initialize_residual_operator(rank, nout, delta, residual, nid):
    rank[nid] = 0
    nout[nid] = 0
    delta[nid] = 0
    residual[nid] = INIT_RESIDUAL


@do_all_operator()
def compute_out_deg_operator(graph: PropertyGraph, nout, nid):
    """Operator for computing outdegree of nodes in the Graph"""
    for ii in graph.edges(nid):
        dst = graph.get_edge_dest(ii)
        atomic_add(nout, dst, 1)


@do_all_operator()
def compute_pagerank_pull_delta_operator(rank, nout, delta, residual, tolerance, changed, nid):
    delta[nid] = 0
    if residual[nid] > tolerance:
        old_residual = residual[nid]
        residual[nid] = 0
        rank[nid] += old_residual
        if nout[nid] > 0:
            delta[nid] = old_residual * ALPHA / nout[nid]
            changed.update(True)


@do_all_operator()
def compute_pagerank_pull_residual_operator(graph: PropertyGraph, delta, residual, nid):
    sum = 0
    for ii in graph.edges(nid):
        dst = graph.get_edge_dest(ii)
        if delta[dst] > 0:
            sum += delta[dst]

    if sum > 0:
        residual[nid] = sum


def pagerank_pull_sync_residual(graph: PropertyGraph, maxIterations, tolerance, property_name):
    num_nodes = graph.num_nodes()

    rank = LargeArray[float](num_nodes, AllocationPolicy.INTERLEAVED)
    nout = LargeArray[np.uint64](num_nodes, AllocationPolicy.INTERLEAVED)
    delta = LargeArray[float](num_nodes, AllocationPolicy.INTERLEAVED)
    residual = LargeArray[float](num_nodes, AllocationPolicy.INTERLEAVED)

    # Initialize
    do_all(
        range(num_nodes),
        initialize_residual_operator(rank.as_numpy(), nout.as_numpy(), delta.as_numpy(), residual.as_numpy(),),
        steal=True,
        loop_name="initialize_pagerank_pull_residual",
    )

    # Compute out-degree for each node
    do_all(
        range(num_nodes), compute_out_deg_operator(graph, nout.as_numpy()), steal=True, loop_name="Compute_out_degree",
    )

    print("Out-degree of 0: ", nout[0])

    changed = GReduceLogicalOr(True)
    iterations = 0
    timer = StatTimer("Pagerank: Property Graph Numba: " + property_name)
    timer.start()
    while iterations < maxIterations and changed.reduce():
        print("Iter: ", iterations, "\n")
        changed.reset()
        iterations += 1
        do_all(
            range(num_nodes),
            compute_pagerank_pull_delta_operator(
                rank.as_numpy(), nout.as_numpy(), delta.as_numpy(), residual.as_numpy(), tolerance, changed,
            ),
            steal=True,
            loop_name="pagerank_delta",
        )

        do_all(
            range(num_nodes),
            compute_pagerank_pull_residual_operator(graph, delta.as_numpy(), residual.as_numpy()),
            steal=True,
            loop_name="pagerank",
        )

    timer.stop()
    # Add the ranks as a new property to the property graph
    graph.add_node_property(pyarrow.table({property_name: rank}))


@do_all_operator()
def sanity_check_operator(
    sum_rank: GAccumulator[int], max_rank: GReduceMax[int], min_rank: GReduceMin[int], data, nid,
):
    val = data[nid]
    sum_rank.update(val)
    max_rank.update(val)
    min_rank.update(val)


def verify_pr(graph: PropertyGraph, property_name: str, topn: int):
    """Check output sanity"""
    chunk_array = graph.get_node_property(property_name)
    sum_rank = GAccumulator[float](0)
    max_rank = GReduceMax[float]()
    min_rank = GReduceMin[float]()

    do_all(
        range(len(chunk_array)),
        sanity_check_operator(sum_rank, max_rank, min_rank, chunk_array),
        steal=True,
        loop_name="sanity_check_operator",
    )

    print("Max rank is ", max_rank.reduce())
    print("Min rank is ", min_rank.reduce())
    print("rank sum is ", sum_rank.reduce())

    # Print top N ranked nodes
    if topn > 0:
        np_array = np.array(chunk_array, dtype=np.float)
        arr = np_array.argsort()[-topn:][::-1]
        for i in arr:
            print(np_array[i], " : ", i, "\n")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--maxIterations", type=int, default=100)
    parser.add_argument("--tolerance", type=float, default=1.0e-3)
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--printTopN", type=int, default=10)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("--reportNode", type=int, default=0)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    print("Using threads:", set_active_threads(args.threads))

    graph = PropertyGraph(args.input)

    pagerank_pull_sync_residual(graph, args.maxIterations, args.tolerance, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        verify_pr(graph, args.propertyName, args.printTopN)


if __name__ == "__main__":
    main()
