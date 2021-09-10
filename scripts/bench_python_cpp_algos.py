import argparse
import contextlib
import json
import os
import sys
import time
from collections import namedtuple
from datetime import datetime

import numpy as np
import pytz
from pyarrow import Schema

import katana.local
from katana.local import Graph, analytics

# TODO(giorgi): This script needs to be tested in CI.


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
        "num-sources": args.num_sources,
        "routines": {},
        "duration": 0,
        "datetime": now.strftime("%d/%m/%Y %H:%M:%S"),
    }

    return data


def save_statistics_as_json(bench_stats, start_time, file_name, path="."):
    bench_stats["duration"] = time.time() - start_time
    with open(f"{path}/{file_name}.json", "w") as fp:
        try:
            json.dump(bench_stats, fp, indent=4)
        except SystemError:
            print("JSON dump was unsuccessful.")
            return False
    return True


def bfs(graph: Graph, input_args, source_node_file=""):
    property_name = "NewProp"
    start_node = input_args["source_node"]
    time_data = {}

    start_plan = time.time()
    bfs_plan = analytics.BfsPlan.synchronous_direction_opt(15, 18)
    time_data["bfs-plan"] = time.time() - start_plan

    if "road" in input_args["name"]:
        bfs_plan = analytics.BfsPlan.asynchronous()

    if source_node_file:
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        with open(source_node_file, "r") as fi:
            sources = [int(l) for l in fi.readlines()]
        for source in sources:
            with time_block(f"bfs on {source}"):
                start_bfs = time.time()
                analytics.bfs(graph, int(source), property_name, plan=bfs_plan)
                time_data[f"bfs-on-{source}"] = time.time() - start_bfs

            start_schema_check = time.time()
            check_schema(graph, property_name)
            time_data[f"bfs-schema-check-on-{source}"] = time.time() - start_schema_check

            analytics.bfs_assert_valid(graph, int(source), property_name)

            stats = analytics.BfsStatistics(graph, property_name)
            print(f"STATS:\n{stats}")
            graph.remove_node_property(property_name)
    else:

        with time_block("bfs"):
            start_bfs = time.time()
            analytics.bfs(graph, start_node, property_name, plan=bfs_plan)
            time_data[f"bfs"] = time.time() - start_bfs

        start_schema_check = time.time()
        check_schema(graph, property_name)
        time_data["bfs-schema-check"] = time.time() - start_schema_check

        stats = analytics.BfsStatistics(graph, property_name)
        print(f"STATS:\n{stats}")

        start_cleanup = time.time()
        graph.remove_node_property(property_name)
        time_data["bfs-cleanup"] = time.time() - start_cleanup

    return time_data


def sssp(graph: Graph, input_args, source_node_file=""):
    property_name = "NewProp"
    start_node = input_args["source_node"]
    edge_prop_name = input_args["edge_wt"]
    time_data = {}

    start_plan = time.time()
    sssp_plan = analytics.SsspPlan.delta_step(input_args["sssp_delta"])
    if "kron" in input_args["name"] or "urand" in input_args["name"]:
        sssp_plan = analytics.SsspPlan.delta_step_fusion(input_args["sssp_delta"])
    time_data["sssp-plan"] = time.time() - start_plan

    if source_node_file:
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        with open(source_node_file, "r") as fi:
            sources = [int(l) for l in fi.readlines()]

        for source in sources:
            with time_block(f"sssp on {source}"):
                start_sssp = time.time()
                analytics.sssp(graph, source, edge_prop_name, property_name, sssp_plan)
                time_data[f"sssp-on-{source}"] = time.time() - start_sssp

            start_schema_check = time.time()
            check_schema(graph, property_name)
            time_data[f"sssp-schema-check-on-{source}"] = time.time() - start_schema_check

            analytics.sssp_assert_valid(graph, source, edge_prop_name, property_name)

            stats = analytics.SsspStatistics(graph, property_name)
            print(f"STATS:\n{stats}")
            graph.remove_node_property(property_name)

    else:
        with time_block("sssp"):
            start_sssp = time.time()
            analytics.sssp(graph, start_node, edge_prop_name, property_name, sssp_plan)
            time_data["sssp"] = time.time() - start_sssp

        start_schema_check = time.time()
        check_schema(graph, property_name)
        time_data[f"sssp-schema-check"] = time.time() - start_schema_check

        analytics.sssp_assert_valid(graph, start_node, edge_prop_name, property_name)

        stats = analytics.SsspStatistics(graph, property_name)
        print(f"STATS:\n{stats}")
        graph.remove_node_property(property_name)

    return time_data


def jaccard(graph: Graph, input_args):
    property_name = "NewProp"
    compare_node = input_args["source_node"]
    time_data = {}

    with time_block(f"jaccard on {compare_node}"):
        start_jaccard = time.time()
        analytics.jaccard(graph, compare_node, property_name)
        time_data["jaccard"] = time.time() - start_jaccard

    start_schema_check = time.time()
    check_schema(graph, property_name)
    time_data["jaccard-schema-check"] = time.time() - start_schema_check

    similarities: np.ndarray = graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1

    analytics.jaccard_assert_valid(graph, compare_node, property_name)

    stats = analytics.JaccardStatistics(graph, compare_node, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)

    return time_data


def pagerank(graph: Graph, _input_args):
    property_name = "NewProp"

    tolerance = 0.0001
    max_iteration = 1000
    alpha = 0.85
    time_data = {}

    start_plan = time.time()
    pagerank_plan = analytics.PagerankPlan.pull_topological(tolerance, max_iteration, alpha)
    time_data["topological-plan"] = time.time() - start_plan

    with time_block("pagerank"):
        start_pagerank = time.time()
        analytics.pagerank(graph, property_name, pagerank_plan)
        time_data["pagerank"] = time.time() - start_pagerank

    start_schema_check = time.time()
    check_schema(graph, property_name)
    time_data["pagerank-schema-check"] = time.time() - start_schema_check

    analytics.pagerank_assert_valid(graph, property_name)

    stats = analytics.PagerankStatistics(graph, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)

    return time_data


def bc(graph: Graph, input_args, source_node_file="", num_sources=4):
    property_name = "NewProp"
    start_node = input_args["source_node"]
    time_data = {}

    start_plan = time.time()
    bc_plan = analytics.BetweennessCentralityPlan.level()
    time_data["bc-plan"] = time.time() - start_plan

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
                start_bc = time.time()
                analytics.betweenness_centrality(graph, property_name, sources, bc_plan)
                time_data[f"bc-{run}"] = time.time() - start_bc

            start_schema_check = time.time()
            check_schema(graph, property_name)
            time_data["bc-schema-check"] = time.time() - start_schema_check

            stats = analytics.BetweennessCentralityStatistics(graph, property_name)
            print(f"STATS:\n{stats}")
            graph.remove_node_property(property_name)
    else:
        sources = [start_node]
        print(f"Using sources: {sources}")
        with time_block("betweenness centrality"):
            start_bc = time.time()
            analytics.betweenness_centrality(graph, property_name, sources, bc_plan)
            time_data["bc"] = time.time() - start_bc

        start_schema_check = time.time()
        check_schema(graph, property_name)
        time_data["bc-schema-check"] = time.time() - start_schema_check

        stats = analytics.BetweennessCentralityStatistics(graph, property_name)
        print(f"STATS:\n{stats}")
        graph.remove_node_property(property_name)

    return time_data


def tc(graph: Graph, _input_args):
    time_data = {}

    start_sort = time.time()
    analytics.sort_all_edges_by_dest(graph)
    time_data["tc-sort"] = time.time() - start_sort

    start_plan = time.time()
    tc_plan = analytics.TriangleCountPlan.ordered_count(edges_sorted=True)
    time_data["tc-plan"] = time.time() - start_plan

    with time_block("triangle counting"):
        start_tc = time.time()
        n = analytics.triangle_count(graph, tc_plan)
        time_data["tc"] = time.time() - start_tc

    print(f"STATS:\nNumber of Triangles: {n}")
    return time_data


def cc(graph: Graph, _input_args):
    property_name = "NewProp"
    time_data = {}

    with time_block("connected components"):
        start_cc = time.time()
        analytics.connected_components(graph, property_name)
        time_data["cc"] = time.time() - start_cc

    start_schema_check = time.time()
    check_schema(graph, property_name)
    time_data["cc-schema-check"] = time.time() - start_schema_check

    analytics.connected_components_assert_valid(graph, property_name)

    stats = analytics.ConnectedComponentsStatistics(graph, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)
    return time_data


def kcore(graph: Graph, _input_args):
    property_name = "NewProp"
    k = 10
    time_data = {}

    with time_block("k-core"):
        start_kcore = time.time()
        analytics.k_core(graph, k, property_name)
        time_data["k-core"] = time.time() - start_kcore

    start_schema_check = time.time()
    check_schema(graph, property_name)
    time_data["k-core-schema-check"] = time.time() - start_schema_check

    analytics.k_core_assert_valid(graph, k, property_name)

    stats = analytics.KCoreStatistics(graph, k, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)
    return time_data


def louvain(graph: Graph, input_args):
    property_name = "NewProp"
    edge_prop_name = input_args["edge_wt"]
    time_data = {}

    with time_block("louvain"):
        start_plan = time.time()
        louvain_plan = analytics.LouvainClusteringPlan.do_all(False, 0.0001, 0.0001, 10000, 100)
        time_data["louvain-plan"] = time.time() - start_plan

        start_louvain = time.time()
        analytics.louvain_clustering(graph, edge_prop_name, property_name, louvain_plan)
        time_data["louvain"] = time.time() - start_louvain

    start_schema_check = time.time()
    check_schema(graph, property_name)
    time_data["louvain-schema-check"] = time.time() - start_schema_check

    analytics.louvain_clustering_assert_valid(graph, edge_prop_name, property_name)

    stats = analytics.LouvainClusteringStatistics(graph, edge_prop_name, property_name)
    print(f"STATS:\n{stats}")
    graph.remove_node_property(property_name)
    return time_data


def convert_to_milisec(data):
    for v in data:
        data[v] = round(data[v] * 1000)
    return data


def run_routine(routine, data, load_time, args_trails, argv):

    trial_count = 0
    for _ in range(args_trails):
        time_data = routine(*argv)
        data["routines"][f"{str(routine.__name__)}_{str(trial_count)}"] = convert_to_milisec(time_data)
        data["routines"][f"{str(routine.__name__)}_{str(trial_count)}"]["graph-load-time"] = 1000 * load_time

        trial_count += 1
    print("Run Complete!")
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
    PathExt = namedtuple("PathExt", ["warn_prefix", "path_ext"])
    Routine = namedtuple("Routine", ["name", "args", "path", "edge_load"])
    # For a minor optimization group the routines by their edge_load True or False
    routine_name_args_mappings = {
        "tc": Routine(tc, (), PathExt("Symmetric clean", input["symmetric_clean_input"]), True),
        "cc": Routine(cc, (), PathExt("Symmetric", input["symmetric_input"]), True),
        "kcore": Routine(kcore, (), PathExt("Symmetric", input["symmetric_input"]), True),
        "bfs": Routine(bfs, (args.source_nodes), PathExt("", input["name"]), False),
        "sssp": Routine(sssp, (args.source_nodes), PathExt("", input["name"]), False),
        "jaccard": Routine(jaccard, (), PathExt("", input["name"]), False),
        "bc": Routine(bc, (args.source_nodes, args.num_sources), PathExt("", input["name"]), False),
        "louvain": Routine(louvain, (), PathExt("Symmetric", input["symmetric_input"]), False),
        "pagerank": Routine(pagerank, (), PathExt("Symmetric", input["transpose_input"]), False),
    }
    start_time = time.time()
    main_warn = "Graph doesn't exist:"
    if args.application not in ["all"]:

        # Select the routine to run
        routine_to_run = routine_name_args_mappings[args.application]
        graph_path = f"{args.input_dir}/{routine_to_run.path.path_ext}"
        if not os.path.exists(graph_path):
            print(f"{routine_to_run.path.warn_prefix} {main_warn} {graph_path}")

        start_load = time.time()
        if routine_to_run.edge_load:
            graph = load_graph(graph_path, [])
        else:
            graph = load_graph(graph_path)
        load_time = time.time() - start_load

        routine_args = (graph, input)

        additional_args = routine_name_args_mappings[args.application].args
        if additional_args:
            routine_args += additional_args

        data = run_routine(
            routine_name_args_mappings[args.application].name, data, load_time, args.trials, routine_args
        )

    else:
        first_routine = next(iter(routine_name_args_mappings))
        curr_edge_load = not routine_name_args_mappings[first_routine].edge_load
        for k in routine_name_args_mappings:

            routine_to_run = routine_name_args_mappings[k]
            graph_path = f"{args.input_dir}/{routine_to_run.path.path_ext}"
            if not os.path.exists(graph_path):
                print(f"{routine_to_run.path.warn_prefix} {main_warn} {graph_path}")

            if curr_edge_load != routine_to_run.edge_load:
                start_load = time.time()
                if routine_to_run.edge_load:
                    graph = load_graph(graph_path, [])
                else:
                    graph = load_graph(graph_path)
                load_time = time.time() - start_load
                curr_edge_load = routine_to_run.edge_load

            routine_args = (graph, input)

            additional_args = routine_name_args_mappings[k].args
            if additional_args:
                routine_args += additional_args

            data = run_routine(routine_name_args_mappings[k].name, data, load_time, args.trials, routine_args)

    if args.save_json:
        save_success = save_statistics_as_json(data, start_time, args.file_name, args.save_dir)

    if save_success:
        return (True, data)

    return (False, {})


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
        "--file-name", default="benchmark_statistics", help="Name of the saved JSON file (default: %(default)s)"
    )

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
    parser.add_argument("--num-sources", type=int, default=4, help="Number of sources (default: %(default)s)")
    parsed_args = parser.parse_args()

    if not os.path.isdir(parsed_args.input_dir):
        print(f"input directory : {parsed_args.input_dir} doesn't exist")
        sys.exit(1)

    if not parsed_args.threads:
        parsed_args.threads = int(os.cpu_count())

    print(f"Using input directory: {parsed_args.input_dir} and Threads: {parsed_args.threads}")

    run_all_gap(parsed_args)
