import numpy as np
import pyarrow

from katana import do_all, do_all_operator, for_each, for_each_operator, set_active_threads
from katana.local import Graph
from katana.local.atomic import ReduceSum, atomic_sub
from katana.local.datastructures import AllocationPolicy, InsertBag, NUMAArray
from katana.timer import StatTimer


@do_all_operator()
def compute_degree_count_operator(graph: Graph, current_degree, nid):
    """
    Operator to initialize degree fields in graph with current degree. Since symmetric,
    out edge count is equivalent to in-edge count.
    """
    current_degree[nid] = len(graph.edge_ids(nid))


@do_all_operator()
def setup_initial_worklist_operator(
    initial_worklist: InsertBag[np.uint64], current_degree, k_core_num, nid,
):
    """Operator to fill worklist with dead nodes (with degree less than k_core_num) to be processed."""
    if current_degree[nid] < k_core_num:
        initial_worklist.push(nid)


@for_each_operator()
def compute_async_kcore_operator(graph: Graph, current_degree, k_core_num, nid, ctx):
    # Decrement degree of all the neighbors of dead node
    for ii in graph.edge_ids(nid):
        dst = graph.get_edge_dest(ii)
        old_degree = atomic_sub(current_degree, dst, 1)
        # Add new dead nodes to the worklist
        if old_degree == k_core_num:
            ctx.push(dst)


def kcore_async(graph: Graph, k_core_num, property_name):
    num_nodes = graph.num_nodes()
    initial_worklist = InsertBag[np.uint64]()
    current_degree = NUMAArray[np.uint64](num_nodes, AllocationPolicy.INTERLEAVED)

    timer = StatTimer("Kcore: Property Graph Numba: " + property_name)
    timer.start()

    # Initialize
    do_all(
        range(num_nodes), compute_degree_count_operator(graph, current_degree.as_numpy()), steal=True,
    )

    # Setup initial worklist
    do_all(
        range(num_nodes),
        setup_initial_worklist_operator(initial_worklist, current_degree.as_numpy(), k_core_num),
        steal=True,
    )

    # Compute k-core
    for_each(
        initial_worklist,
        compute_async_kcore_operator(graph, current_degree.as_numpy(), k_core_num),
        steal=True,
        disable_conflict_detection=True,
    )

    timer.stop()
    # Add the ranks as a new property to the property graph
    graph.add_node_property(pyarrow.table({property_name: current_degree}))


@do_all_operator()
def sanity_check_operator(alive_nodes: ReduceSum[int], data, k_core_num, nid):
    val = data[nid]
    if val >= k_core_num:
        alive_nodes.update(1)


def verify_kcore(graph: Graph, property_name: str, k_core_num: int):
    """Check output sanity"""
    chunk_array = graph.get_node_property(property_name)
    alive_nodes = ReduceSum[float](0)

    do_all(
        range(len(chunk_array)),
        sanity_check_operator(alive_nodes, chunk_array, k_core_num),
        steal=True,
        loop_name="sanity_check_operator",
    )

    print("Number of nodes in the", k_core_num, "-core is", alive_nodes.reduce())


def main():
    import argparse

    import katana.local

    katana.local.initialize()

    parser = argparse.ArgumentParser()
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("--kcore", "-k", type=int, default=100)
    parser.add_argument("--reportNode", type=int, default=0)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    print("Using threads:", set_active_threads(args.threads))

    graph = Graph(args.input)

    kcore_async(graph, args.kcore, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        verify_kcore(graph, args.propertyName, args.kcore)


if __name__ == "__main__":
    main()
