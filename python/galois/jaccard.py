from ._jaccard import jaccard

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--baseNode', type=int, default=0)
    parser.add_argument('--reportNode', type=int, default=1)
    parser.add_argument('--threads', '-t', type=int, default=1)
    parser.add_argument('input', type=str)
    args = parser.parse_args()

    from galois.shmem import *
    print("Using threads:", setActiveThreads(args.threads))

    from .graphs import LC_CSR_Graph_Directed_primitive

    g = LC_CSR_Graph_Directed_primitive["double", "void"]()
    g.readGraphFromGRFile(args.input)

    jaccard(g, args.baseNode)

    print("Node {}: {}".format(args.reportNode, g.getData(args.reportNode)))
