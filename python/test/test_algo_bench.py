from scripts.bench_python_cpp_algos import *


def get_args(json_output, input_dir="./", graph="GAP-road", app="bfs", source_nodes="", trails=1, num_sources=4, thread_spin=False, threads=None):

    parser = argparse.ArgumentParser(
        description="Benchmark performance of routines")
    parser.add_argument("--input-dir", default=input_dir,
                        help="Path to the input directory (default: %(default)s)")

    parser.add_argument(
        "--threads",
        type=int,
        default=threads,
        help="Number of threads to use (default: query sinfo). Should match max threads.",
    )
    parser.add_argument("--thread-spin", default=thread_spin,
                        action="store_true", help="Busy wait for work in thread pool.")

    parser.add_argument(
        "--json-output", default=json_output, help="Path at which to save performance data in JSON")

    parser.add_argument(
        "--graph",
        default=graph,
        choices=["GAP-road", "GAP-kron", "GAP-twitter",
                 "GAP-web", "GAP-urand", "rmat15"],
        help="Graph name (default: %(default)s)",
    )
    parser.add_argument(
        "--application",
        default=app,
        choices=["bfs", "sssp", "cc", "bc", "pagerank",
                 "tc", "jaccard", "kcore", "louvain", "all"],
        help="Application to run (default: %(default)s)",
    )
    parser.add_argument("--source-nodes", default=source_nodes,
                        help="Source nodes file(default: %(default)s)")
    parser.add_argument("--trials", type=int, default=1,
                        help="Number of trials (default: %(default)s)")
    parser.add_argument("--num-sources", type=int, default=4,
                        help="Number of sources (default: %(default)s)")

    return parser


def mainTest():
    pass


def GenerateTest():
    pass


if __name__ == "__main__":
    mainTest()
