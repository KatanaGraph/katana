from ._connected_components import *

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    from galois.shmem import *

    print("Using threads:", setActiveThreads(args.threads))

    results = connected_components(args.input)

    print("Node {}: {}".format(args.reportNode, results.getData(args.reportNode)))
