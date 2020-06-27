from ._bfs_property_graph import bfs
from .property_graph import PropertyGraph

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--startNode', type=int, default=0)
    parser.add_argument('--propertyID', type=int, default=0)
    parser.add_argument('--propertyName', type=str, default="NewProperty")
    parser.add_argument('--graph', type=propertyGraph, default=0)
    parser.add_argument('--reportNode', type=int, default=1)
    parser.add_argument('--noverify', action='store_true', default=False)
    parser.add_argument('--threads', '-t', type=int, default=1)
    parser.add_argument('input', type=str)
    args = parser.parse_args()

    from galois.shmem import *
    print("Using threads:", setActiveThreads(args.threads))

    bfs(args.graph, args.startNode, args.propertyName)

    ##TODO: Implement getData for property  graphs
    #print("Node {}: {}".format(args.reportNode, g.getData(args.reportNode)))

    if not args.noverify:
        numNodeProperties = graph.num_node_properties()
        newPropertyId = numNodeProperties + 1
        verify_bfs(g, args.startNode, newPropertyId)
