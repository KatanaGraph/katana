import numpy as np
import pyarrow

from katana import do_all, do_all_operator, set_active_threads
from katana.local import Graph
from katana.local.atomic import ReduceLogicalOr, ReduceSum, atomic_min
from katana.timer import StatTimer


################################################
## Topological pull style connencted components
## NOTE: Requires symmetric graph
################################################
@do_all_operator()
def initialize_cc_pull_operator(comp_current: np.ndarray, nid):
    # Initialize each node in its own component
    comp_current[nid] = nid


@do_all_operator()
def cc_pull_topo_operator(graph: Graph, changed, comp_current: np.ndarray, nid):
    for ii in graph.edge_ids(nid):
        dst = graph.get_edge_dest(ii)
        # Pull the minimum component from your neighbors
        if comp_current[nid] > comp_current[dst]:
            comp_current[nid] = comp_current[dst]
            # Indicates that update happened
            changed.update(True)


def cc_pull_topo(graph: Graph, property_name):
    print("Executing Pull algo\n")
    num_nodes = graph.num_nodes()

    timer = StatTimer("CC: Property Graph Numba: " + property_name)
    timer.start()
    # Stores the component id assignment
    comp_current = np.empty((num_nodes,), dtype=np.uint32)

    # Initialize
    do_all(
        range(num_nodes), initialize_cc_pull_operator(comp_current), steal=True, loop_name="initialize_cc_pull",
    )

    # Execute while component ids are updated
    changed = ReduceLogicalOr()
    changed.update(True)
    while changed.reduce():
        changed.reset()
        do_all(
            range(num_nodes), cc_pull_topo_operator(graph, changed, comp_current), steal=True, loop_name="cc_pull_topo",
        )

    timer.stop()
    # Add the component assignment as a new property to the property graph
    graph.add_node_property(pyarrow.table({property_name: comp_current}))


################################################
## Topological pull style connencted components
## NOTE: Requires symmetric graph
################################################
@do_all_operator()
def initialize_cc_push_operator(graph: Graph, comp_current: np.ndarray, comp_old: np.ndarray, nid):
    # Initialize each node in its own component
    comp_current[nid] = nid
    comp_old[nid] = graph.num_nodes()


@do_all_operator()
def cc_push_topo_operator(graph: Graph, changed, comp_current: np.ndarray, comp_old: np.ndarray, nid):
    if comp_old[nid] > comp_current[nid]:
        comp_old[nid] = comp_current[nid]
        # Indicates that update happened
        changed.update(True)
        for ii in graph.edge_ids(nid):
            dst = graph.get_edge_dest(ii)
            new_comp = comp_current[nid]
            # Push the minimum component to your neighbors
            atomic_min(comp_current, dst, new_comp)


def cc_push_topo(graph: Graph, property_name):
    print("Executing Push algo\n")
    num_nodes = graph.num_nodes()

    timer = StatTimer("CC: Property Graph Numba: " + property_name)
    timer.start()
    # Stores the component id assignment
    comp_current = np.empty((num_nodes,), dtype=np.uint32)
    comp_old = np.empty((num_nodes,), dtype=np.uint32)

    # Initialize
    do_all(
        range(num_nodes),
        initialize_cc_push_operator(graph, comp_current, comp_old),
        steal=True,
        loop_name="initialize_cc_push",
    )

    # Execute while component ids are updated
    changed = ReduceLogicalOr()
    changed.update(True)
    while changed.reduce():
        changed.reset()
        do_all(
            range(num_nodes),
            cc_push_topo_operator(graph, changed, comp_current, comp_old),
            steal=True,
            loop_name="cc_push_topo",
        )

    timer.stop()
    # Add the component assignment as a new property to the property graph
    graph.add_node_property(pyarrow.table({property_name: comp_current}))


################################################
## Verification checks the number of unique
## Components found in the graph
################################################
@do_all_operator()
def verify_cc_operator(num_components: ReduceSum[int], data, nid):
    # Component id == node id
    if data[nid] == nid:
        num_components.update(1)


def verify_cc(graph: Graph, property_id: int):
    chunk_array = graph.get_node_property(property_id)
    num_components = ReduceSum[int](0)

    do_all(
        range(len(chunk_array)), verify_cc_operator(num_components, chunk_array), loop_name="num_components",
    )

    print("Number of components are : ", num_components.reduce())


def main():
    import argparse

    import katana.local

    katana.local.initialize()

    parser = argparse.ArgumentParser()
    parser.add_argument("--algoType", type=str, default="push")
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    print("Using threads:", set_active_threads(args.threads))

    graph = Graph(args.input)

    if args.algoType == "push":
        cc_push_topo(graph, args.propertyName)
    else:
        cc_pull_topo(graph, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.loaded_node_schema())
        newPropertyID = numNodeProperties - 1
        verify_cc(graph, newPropertyID)


if __name__ == "__main__":
    main()
