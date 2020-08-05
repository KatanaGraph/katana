from ._pagerank import pagerank

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--maxIterations", type=int, default=16)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    from galois.shmem import *

    print("Using threads:", setActiveThreads(args.threads))

    results = pagerank(args.maxIterations, args.input)

    print("Node {}: {}".format(args.reportNode, results.getData(args.reportNode)))
