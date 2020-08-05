from ._sssp import sssp

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--startNode", type=int, default=0)
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--delta", type=int, default=13)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    from galois.shmem import *

    print("Using threads:", setActiveThreads(args.threads))

    results = sssp(args.delta, args.startNode, args.input)

    print("Node {}: {}".format(args.reportNode, results.getData(args.reportNode)))
