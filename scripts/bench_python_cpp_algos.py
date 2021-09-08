import argparse
import contextlib
import json
import os
import sys
import time
from datetime import datetime

import numpy as np
import pytz
from pyarrow import Schema

import katana.local
from katana.local import Graph, analytics

# TODO(amp): This script needs to be tested in CI.


@contextlib.contextmanager
def time_block(run_name):
    timer_algo_start = time.perf_counter()
    yield
    timer_algo_end = time.perf_counter()
    print(f"[TIMER] Time to run {run_name} : {round((timer_algo_end - timer_algo_start), 2)} seconds")


def check_schema(graph: Graph, property_name):
    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name


def create_empty_statistics(args):
    now = datetime.now(pytz.timezone("US/Central"))
    data = {
        "graph": args.graph,
        "threads": args.threads,
        "thread-spin": args.thread_spin,
        "source-nodes": args.source_nodes,
        "trials": args.trials,
        "import_time": 0,
        "num_partitions": 0,
        "routines": {},
        "duration": time.time(),
        "datetime": now.strftime("%d/%m/%Y %H:%M:%S"),
        "katana_sha": "",
    }

    return data


def save_statistics_as_json(bench_stats, path="."):
    bench_stats["duration"] = time.time() - bench_stats["duration"]
    with open(f"{path}/experiments.json", "w") as fp:
        json.dump(bench_stats, fp, indent=4)


def bfs(graph: Graph, input_args, source_node_file=""):
    property_name = "NewProp"
    start_node = input_args["source_node"]

    bfs_plan = analytics.BfsPlan.synchronous_direction_opt(15, 18)

    if "road" in input_args["name"]:
        bfs_plan = analytics.BfsPlan.asynchronous()

    if source_node_file:
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        with open(source_node_file, "r") as fi:
            sources = [int(l) for l in fi.readlines()]

        for source in sources:
            with time_block(f"bfs on {source}"):
                analytics.bfs(graph, int(source), property_name, plan=bfs_plan)
            check_schema(graph, property_name)

            analytics.bfs_assert_valid(graph, int(source), property_name)

            stats = analytics.BfsStatistics(graph, property_name)
            print(f"STATS:\n{stats}")
            graph.remove_node_property(property_name)
    else:
        with time_block("bfs"):
            analytics.bfs(graph, start_node, property_name, plan=bfs_plan)

        check_schema(graph, property_name)

        analytics.bfs_assert_valid(graph, start_node, property_name)

        stats = analytics.BfsStatistics(graph, property_name)
        print(f"STATS:\n{stats}")
        graph.remove_node_property(property_name)


def sssp(graph: Graph, input_args, source_node_file=""):
    property_name = "NewProp"
    start_node = input_args["source_node"]
    edge_prop_name = input_args["edge_wt"]

    sssp_plan = analytics.SsspPlan.delta_step(input_args["sssp_delta"])
    if "kron" in input_args["name"] or "urand" in input_args["name"]:
        sssp_plan = analytics.SsspPlan.delta_step_fusion(input_args["sssp_delta"])

    if source_node_file:
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        with open(source_node_file, "r") as fi:
            sources = [int(l) for l in fi.readlines()]

        for source in sources:
            with time_block(f"sssp on {source}"):
                analytics.sssp(graph, source, edge_prop_name, property_name, sssp_plan)

            check_schema(graph, property_name)

            analytics.sssp_assert_valid(graph, source, edge_prop_name, property_name)

            stats = analytics.SsspStatistics(graph, property_name)
            print(f"STATS:\n{stats}")
            graph.remove_node_property(property_name)

    else:
        with time_block("sssp"):
            analytics.sssp(graph, start_node, edge_prop_name, property_name, sssp_plan)

        check_schema(graph, property_name)

        analytics.sssp_assert_valid(graph, start_node, edge_prop_name, property_name)

        stats = analytics.SsspStatistics(graph, property_name)
        print(f"STATS:\n{stats}")
        graph.remove_node_property(property_name)


def jaccard(graph: Graph, input_args):
    property_name = "NewProp"
    compare_node = input_args["source_node"]

    with time_block(f"jaccard on {compare_node}"):
        analytics.jaccard(graph, compare_node, property_name)

    check_schema(graph, property_name)

    similarities: np.ndarray = graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1

    analytics.jaccard_assert_valid(graph, compare_node, property_name)

    stats = analytics.JaccardStatistics(graph, compare_node, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)


def pagerank(graph: Graph, _input_args):
    property_name = "NewProp"

    tolerance = 0.0001
    max_iteration = 1000
    alpha = 0.85

    pagerank_plan = analytics.PagerankPlan.pull_topological(tolerance, max_iteration, alpha)

    with time_block("pagerank"):
        analytics.pagerank(graph, property_name, pagerank_plan)

    check_schema(graph, property_name)

    analytics.pagerank_assert_valid(graph, property_name)

    stats = analytics.PagerankStatistics(graph, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)


def bc(graph: Graph, input_args, source_node_file="", num_sources=4):
    property_name = "NewProp"
    start_node = input_args["source_node"]

    bc_plan = analytics.BetweennessCentralityPlan.level()

    if source_node_file:
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        with open(source_node_file, "r") as fi:
            sources = [int(l) for l in fi.readlines()]

        assert num_sources <= len(sources)
        runs = (len(sources) + num_sources - 1) // num_sources

        for run in range(0, runs):
            start_idx = (num_sources * run) % len(sources)
            rotated_sources = sources[start_idx:] + sources[:start_idx]
            sources = rotated_sources[:num_sources]

            print(f"Using sources: {sources}")
            with time_block("betweenness centrality"):
                analytics.betweenness_centrality(graph, property_name, sources, bc_plan)

            check_schema(graph, property_name)

            stats = analytics.BetweennessCentralityStatistics(graph, property_name)
            print(f"STATS:\n{stats}")
            graph.remove_node_property(property_name)
    else:
        sources = [start_node]
        print(f"Using sources: {sources}")
        with time_block("betweenness centrality"):
            analytics.betweenness_centrality(graph, property_name, sources, bc_plan)

        check_schema(graph, property_name)

        stats = analytics.BetweennessCentralityStatistics(graph, property_name)
        print(f"STATS:\n{stats}")
        graph.remove_node_property(property_name)


def tc(graph: Graph, _input_args):
    analytics.sort_all_edges_by_dest(graph)
    tc_plan = analytics.TriangleCountPlan.ordered_count(edges_sorted=True)

    with time_block("triangle counting"):
        n = analytics.triangle_count(graph, tc_plan)

    print(f"STATS:\nNumber of Triangles: {n}")


def cc(graph: Graph, _input_args):
    property_name = "NewProp"

    with time_block("connected components"):
        analytics.connected_components(graph, property_name)

    check_schema(graph, property_name)

    analytics.connected_components_assert_valid(graph, property_name)

    stats = analytics.ConnectedComponentsStatistics(graph, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)


def kcore(graph: Graph, _input_args):
    property_name = "NewProp"
    k = 10

    with time_block("k-core"):
        analytics.k_core(graph, k, property_name)

    check_schema(graph, property_name)

    analytics.k_core_assert_valid(graph, k, property_name)

    stats = analytics.KCoreStatistics(graph, k, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)


def louvain(graph: Graph, input_args):
    property_name = "NewProp"
    edge_prop_name = input_args["edge_wt"]

    with time_block("louvain"):
        louvain_plan = analytics.LouvainClusteringPlan.do_all(False, 0.0001, 0.0001, 10000, 100)
        analytics.louvain_clustering(graph, edge_prop_name, property_name, louvain_plan)

    check_schema(graph, property_name)

    analytics.louvain_clustering_assert_valid(graph, edge_prop_name, property_name)

    stats = analytics.LouvainClusteringStatistics(graph, edge_prop_name, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)


def run_routine(routine, data, args_trails, argv):

    glb_count = 0
    for _ in range(args_trails):

        start = time.time()
        routine(*argv)
        data["routines"][f"{str(routine.__name__)}_{str(glb_count)}"] = time.time() - start
        glb_count += 1

    return data


def run_all_gap(args):
    katana.local.initialize()
    print("Using threads:", katana.set_active_threads(args.threads))
    if parsed_args.thread_spin:
        katana.set_busy_wait()

    inputs = [
        {
            "name": "GAP-road",
            "symmetric_input": "GAP-road",
            "symmetric_clean_input": "GAP-road",
            "transpose_input": "GAP-road",
            "source_node": 18944626,
            "edge_wt": "value",
            "sssp_delta": 13,
        },
        {
            "name": "rmat15",
            "symmetric_input": "rmat15_symmetric",
            "symmetric_clean_input": "rmat15_cleaned_symmetric",
            "transpose_input": "rmat15_transpose",
            "source_node": 0,
            "edge_wt": "value",
            "sssp_delta": 1,
        },
        {
            "name": "GAP-kron",
            "symmetric_input": "GAP-kron",
            "symmetric_clean_input": "GAP-kron",
            "transpose_input": "GAP-kron",
            "source_node": 71328660,
            "edge_wt": "value",
            "sssp_delta": 1,
        },
        {
            "name": "GAP-twitter",
            "symmetric_input": "GAP-twitter_symmetric",
            "symmetric_clean_input": "GAP-twitter_symmetric_cleaned",
            "transpose_input": "GAP-twitter_transpose",
            "source_node": 19058681,
            "edge_wt": "value",
            "sssp_delta": 1,
        },
        {
            "name": "GAP-web",
            "symmetric_input": "GAP-web_symmetric",
            "symmetric_clean_input": "GAP-web_symmetric_cleaned",
            "transpose_input": "GAP-web_transpose",
            "source_node": 19879527,
            "edge_wt": "value",
            "sssp_delta": 1,
        },
        {
            "name": "GAP-urand",
            "symmetric_input": "GAP-urand",
            "symmetric_clean_input": "GAP-urand",
            "transpose_input": "GAP-urand",
            "source_node": 27691419,
            "edge_wt": "value",
            "sssp_delta": 1,
        },
    ]

    def load_graph(graph_path, edge_properties=None):
        print(f"Running {args.application} on graph: {graph_path}")
        with time_block("read Graph"):
            graph = Graph(graph_path, edge_properties=edge_properties, node_properties=[])
        print(f"#Nodes: {len(graph)}, #Edges: {graph.num_edges()}")
        return graph

    # Load our graph
    input = next(item for item in inputs if item["name"] == args.graph)
    data = create_empty_statistics(args)

    routine_name_args_mappings = {
        "bfs": [bfs, (args.source_nodes)],
        "sssp": (sssp, (args.source_nodes)),
        "jaccard": (jaccard, ()),
        "bc": (bc, (args.source_nodes, 8)),
        "tc": (tc, ()),
        "cc": (cc, ()),
        "kcore": (kcore, ()),
        "louvain": (louvain, ()),
        "pagerank": (pagerank, ()),
    }

    if args.application not in ["all"]:
        if args.application in ["bfs", "sssp", "bc", "jaccard"]:
            graph_path = f"{args.input_dir}/{input['name']}"
            if not os.path.exists(graph_path):
                print(f"Graph doesn't exist: {graph_path}")

            graph = load_graph(graph_path)

        elif args.application in ["tc"]:
            graph_path = f"{args.input_dir}/{input['symmetric_clean_input']}"
            if not os.path.exists(graph_path):
                print(f"Symmetric clean Graph doesn't exist: {graph_path}")

            graph = load_graph(graph_path, [])

        elif args.application in ["cc", "kcore"]:
            graph_path = f"{args.input_dir}/{input['symmetric_input']}"
            if not os.path.exists(graph_path):
                print(f"Symmetric Graph doesn't exist: {graph_path}")

            graph = load_graph(graph_path, [])

        elif args.application in ["louvain"]:
            graph_path = f"{args.input_dir}/{input['symmetric_input']}"
            if not os.path.exists(graph_path):
                print(f"Symmetric Graph doesn't exist: {graph_path}")

            graph = load_graph(graph_path)

        elif args.application in ["pagerank"]:
            # Using transpose file pagerank pull which is expected
            # to perform better than pagerank push algorithm
            graph_path = f"{args.input_dir}/{input['transpose_input']}"
            if not os.path.exists(graph_path):
                print(f"Symmetric Graph doesn't exist: {graph_path}")

            graph = load_graph(graph_path, [])

        routine_args = (graph, input)

        additional_args = routine_name_args_mappings[args.application][1]
        if additional_args:
            routine_args += additional_args

        data = run_routine(routine_name_args_mappings[args.application][0], data, args.trials, routine_args)

    else:
        graph_path = f"{args.input_dir}/{input['transpose_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric Graph doesn't exist: {graph_path}")

        graph_path = f"{args.input_dir}/{input['name']}"
        if not os.path.exists(graph_path):
            print(f"Graph doesn't exist: {graph_path}")

        graph = load_graph(graph_path)

        data = run_routine(bfs, data, args.trials, (graph, input, args.source_nodes))
        data = run_routine(sssp, data, args.trials, (graph, input, args.source_nodes))
        data = run_routine(jaccard, data, args.trials, (graph, input))
        data = run_routine(bc, data, args.trials, (graph, input, args.source_nodes, 4))

        graph_path = f"{args.input_dir}/{input['symmetric_clean_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric clean Graph doesn't exist: {graph_path}")

        graph = load_graph(graph_path, [])

        data = run_routine(tc, data, args.trials, (graph, input))
        data = run_routine(cc, data, args.trials, (graph, input))
        data = run_routine(kcore, data, args.trials, (graph, input))

        graph_path = f"{args.input_dir}/{input['symmetric_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric Graph doesn't exist: {graph_path}")

        graph = load_graph(graph_path)

        data = run_routine(louvain, data, args.trials, (graph, input))

        graph_path = f"{args.input_dir}/{input['transpose_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric Graph doesn't exist: {graph_path}")

        graph = load_graph(graph_path, [])

        data = run_routine(pagerank, data, args.trials, (graph, input))

    if args.save_json:
        save_statistics_as_json(data, args.save_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark performance of routines")

    parser.add_argument("--input-dir", default="./", help="Path to the input directory (default: %(default)s)")

    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of threads to use (default: query sinfo). Should match max threads.",
    )
    parser.add_argument("--thread-spin", default=False, action="store_true", help="Busy wait for work in thread pool.")

    parser.add_argument("--save-json", default=False, help="Saving the benchmarking information as a JSON file.")

    parser.add_argument("--save-dir", default="./", help="Path to the save directory (default: %(default)s)")

    parser.add_argument(
        "--graph",
        default="GAP-road",
        choices=["GAP-road", "GAP-kron", "GAP-twitter", "GAP-web", "GAP-urand", "rmat15"],
        help="Graph name (default: %(default)s)",
    )
    parser.add_argument(
        "--application",
        default="bfs",
        choices=["bfs", "sssp", "cc", "bc", "pagerank", "tc", "jaccard", "kcore", "louvain", "all"],
        help="Application to run (default: %(default)s)",
    )
    parser.add_argument("--source-nodes", default="", help="Source nodes file(default: %(default)s)")
    parser.add_argument("--trials", type=int, default=1, help="Number of trials (default: %(default)s)")

    parsed_args = parser.parse_args()

    if not os.path.isdir(parsed_args.input_dir):
        print(f"input directory : {parsed_args.input_dir} doesn't exist")
        sys.exit(1)

    if not parsed_args.threads:
        parsed_args.threads = int(os.cpu_count())

    print(f"Using input directory: {parsed_args.input_dir} and Threads: {parsed_args.threads}")

    run_all_gap(parsed_args)
