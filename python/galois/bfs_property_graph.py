from ._bfs_property_graph import bfs, verify_bfs
from .property_graph import PropertyGraph

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--startNode', type=int, default=0)
    parser.add_argument('--propertyID', type=int, default=0)
    parser.add_argument('--propertyName', type=str, default="NewProperty")
    parser.add_argument('--reportNode', type=int, default=1)
    parser.add_argument('--noverify', action='store_true', default=False)
    parser.add_argument('--threads', '-t', type=int, default=1)
    parser.add_argument('input', type=str)
    args = parser.parse_args()

    from galois.shmem import *
    print("Using threads:", setActiveThreads(args.threads))

    graph = PropertyGraph(args.input)

    bfs(graph, args.startNode, args.propertyName)

    print("Node {}: {}".format(args.reportNode, graph.get_node_property(args.propertyName)[args.reportNode]))

    if not args.noverify:
        numNodeProperties = len(graph.node_schema())
        newPropertyId = numNodeProperties - 1
        verify_bfs(graph, args.startNode, newPropertyId)
