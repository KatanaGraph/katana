import numpy as np
import pyarrow

from katana import do_all, do_all_operator, set_active_threads
from katana.local import Graph
from katana.local.atomic import ReduceLogicalOr, ReduceMax, ReduceMin, ReduceSum, atomic_add
from katana.local.datastructures import AllocationPolicy, NUMAArray
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
def compute_out_deg_operator(graph: Graph, nout, nid):
    """Operator for computing outdegree of nodes in the Graph"""
    for ii in graph.edge_ids(nid):
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
def compute_pagerank_pull_residual_operator(graph: Graph, delta, residual, nid):
    total = 0
    for ii in graph.edge_ids(nid):
        dst = graph.get_edge_dest(ii)
        if delta[dst] > 0:
            total += delta[dst]

    if total > 0:
        residual[nid] = total


def pagerank_pull_sync_residual(graph: Graph, maxIterations, tolerance, property_name):
    num_nodes = graph.num_nodes()

    rank = NUMAArray[float](num_nodes, AllocationPolicy.INTERLEAVED)
    nout = NUMAArray[np.uint64](num_nodes, AllocationPolicy.INTERLEAVED)
    delta = NUMAArray[float](num_nodes, AllocationPolicy.INTERLEAVED)
    residual = NUMAArray[float](num_nodes, AllocationPolicy.INTERLEAVED)

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

    changed = ReduceLogicalOr(True)
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
    sum_rank: ReduceSum[int], max_rank: ReduceMax[int], min_rank: ReduceMin[int], data, nid,
):
    val = data[nid]
    sum_rank.update(val)
    max_rank.update(val)
    min_rank.update(val)


def verify_pr(graph: Graph, property_name: str, topn: int):
    """Check output sanity"""
    chunk_array = graph.get_node_property(property_name)
    sum_rank = ReduceSum[float](0)
    max_rank = ReduceMax[float]()
    min_rank = ReduceMin[float]()

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

    import katana.local

    katana.local.initialize()

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

    graph = Graph(args.input)

    pagerank_pull_sync_residual(graph, args.maxIterations, args.tolerance, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        verify_pr(graph, args.propertyName, args.printTopN)


if __name__ == "__main__":
    main()
