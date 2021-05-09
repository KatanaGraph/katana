import numpy as np
from pyarrow import Schema, table

# from pytest import approx, raises
# import pytest
import argparse
import sys
import os
import time


from katana import GaloisError
import katana.galois
from katana.analytics import *
from katana.property_graph import PropertyGraph
from katana.example_utils import get_input
from katana.lonestar.analytics.bfs import verify_bfs
from katana.lonestar.analytics.sssp import verify_sssp


def check_schema(property_graph: PropertyGraph, property_name):
    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name


def run_bfs(property_graph: PropertyGraph, input_args, source_node_file):
    property_name = "NewProp"
    start_node = input_args["source_node"]

    if not source_node_file == "":
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        sources = open(source_node_file, "r").readlines()

        for source in sources:
            timer_algo_start = time.time()
            bfs(property_graph, int(source), property_name)
            timer_algo_end = time.time()
            print(f"[TIMER] Time to run bfs on {source} : {round((timer_algo_end - timer_algo_start), 2)} seconds")
            check_schema(property_graph, property_name)

            bfs_assert_valid(property_graph, property_name)

            stats = BfsStatistics(property_graph, property_name)
            print(f"STATS:\n{stats}")
            property_graph.remove_node_property(property_name)
    else:
        timer_algo_start = time.time()
        bfs(property_graph, start_node, property_name)
        timer_algo_end = time.time()
        print(f"[TIMER] Time to run bfs : {round((timer_algo_end - timer_algo_start), 2)} seconds")

        check_schema(property_graph, property_name)

        bfs_assert_valid(property_graph, property_name)

        stats = BfsStatistics(property_graph, property_name)
        print(f"STATS:\n{stats}")
        property_graph.remove_node_property(property_name)


def run_sssp(property_graph: PropertyGraph, input_args, source_node_file):
    property_name = "NewProp"
    start_node = input_args["source_node"]
    edge_prop_name = input_args["edge_wt"]

    sssp_plan = SsspPlan.delta_step(input_args["sssp_delta"])

    if not source_node_file == "":
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        sources = open(source_node_file, "r").readlines()

        for source in sources:
            timer_algo_start = time.time()
            sssp(property_graph, int(source), edge_prop_name, property_name, sssp_plan)
            timer_algo_end = time.time()
            print(f"[TIMER] Time to run sssp on {source} : {round((timer_algo_end - timer_algo_start), 2)} seconds")

            check_schema(property_graph, property_name)

            sssp_assert_valid(property_graph, int(source), edge_prop_name, property_name)

            stats = SsspStatistics(property_graph, property_name)
            print(f"STATS:\n{stats}")
            property_graph.remove_node_property(property_name)

    else:
        timer_algo_start = time.time()
        sssp(property_graph, start_node, edge_prop_name, property_name, sssp_plan)
        timer_algo_end = time.time()
        print(f"[TIMER] Time to run sssp : {round((timer_algo_end - timer_algo_start), 2)} seconds")

        check_schema(property_graph, property_name)

        sssp_assert_valid(property_graph, start_node, edge_prop_name, property_name)

        stats = SsspStatistics(property_graph, property_name)
        print(f"STATS:\n{stats}")
        property_graph.remove_node_property(property_name)


def run_jaccard(property_graph: PropertyGraph, input_args):
    property_name = "NewProp"
    compare_node = input_args["source_node"]

    timer_algo_start = time.time()
    jaccard(property_graph, compare_node, property_name)
    timer_algo_end = time.time()
    print(f"[TIMER] Time to run jaccard on {compare_node} : {round((timer_algo_end - timer_algo_start), 2)} seconds")

    check_schema(property_graph, property_name)

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1

    jaccard_assert_valid(property_graph, compare_node, property_name)

    stats = JaccardStatistics(property_graph, compare_node, property_name)
    print(f"STATS:\n{stats}")
    property_graph.remove_node_property(property_name)


def run_pagerank(property_graph: PropertyGraph, input_args):
    property_name = "NewProp"

    tolerance = 0.000001
    max_iteration = 1000
    alpha = 0.85

    pagerank_plan = PagerankPlan.pull_residual(tolerance, max_iteration, alpha)

    timer_algo_start = time.time()
    pagerank(property_graph, property_name, pagerank_plan)
    timer_algo_end = time.time()
    print(f"[TIMER] Time to run pagerank : {round((timer_algo_end - timer_algo_start), 2)} seconds")

    check_schema(property_graph, property_name)

    pagerank_assert_valid(property_graph, property_name)

    stats = PagerankStatistics(property_graph, property_name)
    print(f"STATS:\n{stats}")
    property_graph.remove_node_property(property_name)


def run_bc(property_graph: PropertyGraph, input_args, source_node_file):
    property_name = "NewProp"
    start_node = input_args["source_node"]
    edge_prop_name = input_args["edge_wt"]

    bc_plan = BetweennessCentralityPlan.level()

    n = 4
    if not source_node_file == "":
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {graph_path}")
        sources = open(source_node_file, "r").readlines()

        for i in range(0, len(sources), n):
            sources_to_use = [int(i) for i in sources[i : i + n]]
            print(f"Using source: {sources_to_use}")
            timer_algo_start = time.time()
            betweenness_centrality(property_graph, property_name, sources_to_use, bc_plan)
            timer_algo_end = time.time()
            print(
                f"[TIMER] Time to run betweenness centrality : {round((timer_algo_end - timer_algo_start), 2)} seconds"
            )

            check_schema(property_graph, property_name)

            stats = BetweennessCentralityStatistics(property_graph, property_name)
            print(f"STATS:\n{stats}")
            property_graph.remove_node_property(property_name)
    else:
        sources = [start_node]
        timer_algo_start = time.time()
        betweenness_centrality(property_graph, property_name, sources, bc_plan)
        timer_algo_end = time.time()
        print(f"[TIMER] Time to run betweenness centrality : {round((timer_algo_end - timer_algo_start), 2)} seconds")

        check_schema(property_graph, property_name)

        stats = BetweennessCentralityStatistics(property_graph, property_name)
        print(f"STATS:\n{stats}")
        property_graph.remove_node_property(property_name)


def run_tc(property_graph: PropertyGraph, input_args):
    sort_all_edges_by_dest(property_graph)
    tc_plan = TriangleCountPlan.ordered_count(edges_sorted=True)

    timer_algo_start = time.time()
    n = triangle_count(property_graph, tc_plan)
    timer_algo_end = time.time()
    print(f"[TIMER] Time to run triangle counting : {round((timer_algo_end - timer_algo_start), 2)} seconds")

    print(f"STATS:\nNumber of Triangles: {n}")


def run_cc(property_graph: PropertyGraph, input_args):
    property_name = "NewProp"

    timer_algo_start = time.time()
    connected_components(property_graph, property_name)
    timer_algo_end = time.time()
    print(f"[TIMER] Time to run connected components: {round((timer_algo_end - timer_algo_start), 2)} seconds")

    check_schema(property_graph, property_name)

    connected_components_assert_valid(property_graph, property_name)

    stats = ConnectedComponentsStatistics(property_graph, property_name)
    print(f"STATS:\n{stats}")
    property_graph.remove_node_property(property_name)


def run_kcore(property_graph: PropertyGraph, input_args):
    property_name = "NewProp"
    k = 10

    timer_algo_start = time.time()
    k_core(property_graph, k, property_name)
    timer_algo_end = time.time()
    print(f"[TIMER] Time to run k-core: {round((timer_algo_end - timer_algo_start), 2)} seconds")

    check_schema(property_graph, property_name)

    k_core_assert_valid(property_graph, k, property_name)

    stats = KCoreStatistics(property_graph, k, property_name)
    print(f"STATS:\n{stats}")
    property_graph.remove_node_property(property_name)


def run_louvain(property_graph: PropertyGraph, input_args):
    property_name = "NewProp"
    edge_prop_name = input_args["edge_wt"]

    timer_algo_start = time.time()
    louvain_plan = LouvainClusteringPlan.do_all(False, 0.0001, 0.0001, 10000, 100)
    louvain_clustering(property_graph, edge_prop_name, property_name, louvain_plan)
    timer_algo_end = time.time()
    print(f"[TIMER] Time to run louvain: {round((timer_algo_end - timer_algo_start), 2)} seconds")

    check_schema(property_graph, property_name)

    louvain_clustering_assert_valid(property_graph, edge_prop_name, property_name)

    stats = LouvainClusteringStatistics(property_graph, edge_prop_name, property_name)
    print(f"STATS:\n{stats}")
    property_graph.remove_node_property(property_name)


def run_all_gap(args):
    print("Using threads:", katana.galois.set_active_threads(args.threads))
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
    ]

    # Load our graph
    input = next(item for item in inputs if item["name"] == args.graph)
    if args.application in ["bfs", "sssp", "bc", "jaccard"]:
        graph_path = f"{args.input_dir}/{input['name']}"
        if not os.path.exists(graph_path):
            print(f"Graph doesn't exist: {graph_path}")

        print(f"Running {args.application} on graph: {graph_path}")
        timer_graph_construct_start = time.time()
        graph = PropertyGraph(graph_path)
        timer_graph_construct_end = time.time()
        print(
            f"[TIMER] Time to read propertyGraph : {round((timer_graph_construct_end - timer_graph_construct_start), 2)} seconds"
        )
        print(f"#Nodes: {len(graph)}, #Edges: {graph.num_edges()}")

        if args.application == "bfs":
            for t in range(args.trials):
                run_bfs(graph, input, args.source_nodes)

        if args.application == "sssp":
            for t in range(args.trials):
                run_sssp(graph, input, args.source_nodes)

        if args.application == "jaccard":
            for t in range(args.trials):
                run_jaccard(graph, input)

        if args.application == "bc":
            for t in range(args.trials):
                run_bc(graph, input, args.source_nodes)

    elif args.application in ["tc"]:
        graph_path = f"{args.input_dir}/{input['symmetric_clean_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric clean Graph doesn't exist: {graph_path}")

        print(f"Running {args.application} on graph: {graph_path}")
        timer_graph_construct_start = time.time()
        graph = PropertyGraph(graph_path)
        timer_graph_construct_end = time.time()
        print(
            f"[TIMER] Time to read propertyGraph : {round((timer_graph_construct_end - timer_graph_construct_start), 2)} seconds"
        )
        print(f"#Nodes: {len(graph)}, #Edges: {graph.num_edges()}")

        if args.application == "tc":
            for t in range(args.trials):
                run_tc(graph, input)

    elif args.application in ["cc", "kcore", "louvain"]:
        graph_path = f"{args.input_dir}/{input['symmetric_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric Graph doesn't exist: {graph_path}")

        print(f"Running {args.application} on graph: {graph_path}")
        timer_graph_construct_start = time.time()
        graph = PropertyGraph(graph_path)
        timer_graph_construct_end = time.time()
        print(
            f"[TIMER] Time to read propertyGraph : {round((timer_graph_construct_end - timer_graph_construct_start), 2)} seconds"
        )
        print(f"#Nodes: {len(graph)}, #Edges: {graph.num_edges()}")

        if args.application == "cc":
            for t in range(args.trials):
                run_cc(graph, input)

        if args.application == "kcore":
            for t in range(args.trials):
                run_kcore(graph, input)

        if args.application == "louvain":
            for t in range(args.trials):
                run_louvain(graph, input)

    elif args.application in ["pagerank"]:
        ## Using transpose file pagerank pull which is expected
        ## to perform better than pagerank push algorithm
        graph_path = f"{args.input_dir}/{input['transpose_input']}"
        if not os.path.exists(graph_path):
            print(f"Symmetric Graph doesn't exist: {graph_path}")

        print(f"Running {args.application} on graph: {graph_path}")
        timer_graph_construct_start = time.time()
        graph = PropertyGraph(graph_path)
        timer_graph_construct_end = time.time()
        print(
            f"[TIMER] Time to read propertyGraph : {round((timer_graph_construct_end - timer_graph_construct_start), 2)} seconds"
        )
        print(f"#Nodes: {len(graph)}, #Edges: {graph.num_edges()}")

        if args.application == "pagerank":
            run_pagerank(graph, input)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark performance of routines")

    parser.add_argument(
        "--input-dir",
        default="./",
        help="Path to the input directory (default: %(default)s)",
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of threads to use (default: query sinfo). Should match max threads.",
    )
    parser.add_argument(
        "--graph",
        default="GAP-road",
        choices=["GAP-road", "GAP-kron", "GAP-twitter", "GAP-web"],
        help="Graph name (default: %(default)s)",
    )
    parser.add_argument(
        "--application",
        default="bfs",
        choices=["bfs", "sssp", "cc", "bc", "pagerank", "tc", "jaccard", "kcore", "louvain"],
        help="Application to run (default: %(default)s)",
    )
    parser.add_argument(
        "--source-nodes",
        default="",
        help="Source nodes file(default: %(default)s)",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=1,
        help="Number of trials (default: %(default))",
    )

    parsed_args = parser.parse_args()

    if not os.path.isdir(parsed_args.input_dir):
        print(f"input directory : {parsed_args.input_dir} doesn't exist")
        sys.exit(1)

    if not parsed_args.threads:
        parsed_args.threads = int(os.cpu_count())

    print(f"Using input directory: {parsed_args.input_dir} and Threads: {parsed_args.threads}")

    run_all_gap(parsed_args)
