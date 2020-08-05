from galois.timer import StatTimer
from ._bfs import bfs

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--startNode", type=int, default=0)
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--noverify", action="store_true", default=False)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    from galois.shmem import *

    print("Using threads:", setActiveThreads(args.threads))

    from ._bfs import verify_bfs
    from .graphs import LC_CSR_Graph_Directed_primitive

    g = LC_CSR_Graph_Directed_primitive["uint32_t", "void"]()
    g.readGraphFromGRFile(args.input)

    timer = StatTimer("BFS Cython")
    timer.start()
    bfs(g, args.startNode)
    timer.stop()

    print("Node {}: {}".format(args.reportNode, g.getData(args.reportNode)))

    if not args.noverify:
        verify_bfs(g, args.startNode)
