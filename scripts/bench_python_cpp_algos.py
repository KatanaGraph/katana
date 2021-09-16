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
PathExt = namedtuple("PathExt", ["warn_prefix", "path_ext"])
RoutinePaths = namedtuple("RoutinePaths", ["path", "edge_load"])

RoutineFunc = namedtuple(
    "RoutineFunc", ["plan", "routine", "validation", "stats"])
RoutineArgs = namedtuple(
    "RoutineArgs", ["plan", "routine", "validation", "stats"])

Routine = namedtuple("Routine", ["func", "args"])


@contextlib.contextmanager
def time_block(run_name, time_data):
    timer_algo_start = time.perf_counter()
    yield
    timer_algo_end = time.perf_counter()
    print(
        f"[TIMER] Time to run {run_name} : {round((timer_algo_end - timer_algo_start), 2)} seconds")
    time_data[run_name] = round(1000 * (timer_algo_end - timer_algo_start))


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


def save_statistics_as_json(bench_stats, start_time, path):
    bench_stats["duration"] = round(1000 * time.time() - start_time)
    with open(path, "w") as fp:
        try:
            json.dump(bench_stats, fp, indent=4)
        except SystemError:
            print("JSON dump was unsuccessful.")
            return False
    return True


def run_routine(data, load_time, graph, args, input):
    trial_count = 0
    num_sources = None
    if args.application == "bc":
        num_sources = args.num_sources
    for _ in range(args.trials):
        time_data = default_run(args.application, graph,
                                input, num_sources, args.source_nodes)
        data["routines"][f"{args.application}_{trial_count}"] = time_data
        data["routines"][f"{args.application}_{trial_count}"]["graph_load"] = load_time
        trial_count += 1

    print("Run Complete!")
    return data


def single_run(
    graph,
    routine,
    routine_args,
    assert_validation,
    assert_validation_args,
    statistics,
    statistics_args,
    property_name,
    time_data,
    compare_node=None,
    src=0,
):

    with time_block(f"{routine.__name__}_{src}", time_data):
        routine(*routine_args)

    with time_block(check_schema.__name__, time_data):
        check_schema(graph, property_name)

    if compare_node is not None:
        similarities: np.ndarray = graph.get_node_property(
            property_name).to_numpy()
        assert similarities[compare_node] == 1

    if assert_validation is not None:
        assert_validation(*assert_validation_args)
    if statistics:
        full_stats = statistics(*statistics_args)
    print(f"STATS:\n{full_stats}")
    with time_block(f"{graph.remove_node_property.__name__}_{0}", time_data):
        graph.remove_node_property(property_name)


def default_run(name, graph, input_args, num_sources=None, source_node_file=""):

    if name == "tc":
        return tc(graph, "")

    property_name = "NewProp"
    start_node = input_args["source_node"]
    compare_node = input_args["source_node"]
    edge_prop_name = input_args["edge_wt"]

    tolerance = 0.0001
    max_iteration = 1000
    alpha = 0.85
    k = 10

    cc_bc_args = [graph, property_name]
    k_args = [graph, k, property_name]
    jaccard_args = [graph, compare_node, property_name]
    bfs_args = [graph, start_node, property_name]
    sssp_args = [graph, start_node, edge_prop_name, property_name]
    louvain_args = [graph, edge_prop_name, property_name]
    pagerank_args = [graph, property_name]
    time_data = {}
    func_arg_mapping = {
        "cc": Routine(
            RoutineFunc(
                None,
                analytics.connected_components,
                analytics.connected_components_assert_valid,
                analytics.ConnectedComponentsStatistics,
            ),
            RoutineArgs(None, cc_bc_args, cc_bc_args, cc_bc_args),
        ),
        "kcore": Routine(
            RoutineFunc(None, analytics.k_core,
                        analytics.k_core_assert_valid, analytics.KCoreStatistics),
            RoutineArgs(None, k_args, k_args, k_args),
        ),
        "bfs": Routine(
            RoutineFunc(
                analytics.BfsPlan.asynchronous
                if "road" in input_args["name"]
                else analytics.BfsPlan.synchronous_direction_opt,
                analytics.bfs,
                analytics.bfs_assert_valid,
                analytics.BfsStatistics,
            ),
            RoutineArgs([] if "road" in input_args["name"] else [
                        15, 18], bfs_args, bfs_args, (graph, property_name)),
        ),
        "sssp": Routine(
            RoutineFunc(
                analytics.SsspPlan.delta_step
                if "kron" in input_args["name"] or "urand" in input_args["name"]
                else analytics.SsspPlan.delta_step_fusion,
                analytics.sssp,
                analytics.sssp_assert_valid,
                analytics.SsspStatistics,
            ),
            RoutineArgs([input_args["sssp_delta"]], sssp_args,
                        sssp_args, (graph, property_name)),
        ),
        "jaccard": Routine(
            RoutineFunc(None, analytics.jaccard,
                        analytics.jaccard_assert_valid, analytics.JaccardStatistics),
            RoutineArgs(None, jaccard_args, jaccard_args, jaccard_args),
        ),
        "bc": Routine(
            RoutineFunc(
                analytics.BetweennessCentralityPlan.level,
                analytics.betweenness_centrality,
                None,
                analytics.BetweennessCentralityStatistics,
            ),
            RoutineArgs([], cc_bc_args, None, cc_bc_args),
        ),
        "louvain": Routine(
            RoutineFunc(
                analytics.LouvainClusteringPlan.do_all,
                analytics.louvain_clustering,
                analytics.louvain_clustering_assert_valid,
                analytics.LouvainClusteringStatistics,
            ),
            RoutineArgs([False, 0.0001, 0.0001, 10000, 100],
                        louvain_args, louvain_args, louvain_args),
        ),
        "pagerank": Routine(
            RoutineFunc(
                analytics.PagerankPlan.pull_topological,
                analytics.pagerank,
                analytics.pagerank_assert_valid,
                analytics.PagerankStatistics,
            ),
            RoutineArgs((tolerance, max_iteration, alpha),
                        pagerank_args, pagerank_args, pagerank_args),
        ),
    }

    routine = func_arg_mapping[name]
    routine_args = list(routine.args.routine)

    validation_args = None
    if routine.args.validation:
        validation_args = list(routine.args.validation)

    if routine.func.plan:
        with time_block(f"plan_{routine.func.plan.__name__}", time_data):
            plan = routine.func.plan(*routine.args.plan)

        if name == "bc":
            routine_args.append(None)
            routine_args.append(plan)

    if source_node_file:
        if not os.path.exists(source_node_file):
            print(f"Source node file doesn't exist: {source_node_file}")
        with open(source_node_file, "r") as fi:
            sources = [int(l) for l in fi.readlines()]

        if name == "bc":
            assert num_sources <= len(sources)
            runs = (len(sources) + num_sources - 1) // num_sources

            for run in range(0, runs):
                start_idx = (num_sources * run) % len(sources)
                rotated_sources = sources[start_idx:] + sources[:start_idx]
                sources = rotated_sources[:num_sources]
                routine_args[-2] = sources

                single_run(
                    graph,
                    routine.func.routine,
                    routine_args,
                    routine.func.validation,
                    validation_args,
                    routine.func.stats,
                    routine.args.stats,
                    property_name,
                    time_data,
                    src=run,
                )
        else:
            for source in sources:
                routine_args[1] = int(source)
                validation_args[1] = int(source)
                run_args = []
                single_run(
                    graph,
                    routine.func.routine,
                    routine_args,
                    routine.func.validation,
                    validation_args,
                    routine.func.stats,
                    routine.args.stats,
                    property_name,
                    time_data,
                    src=source,
                )

    else:
        if name == "bc":
            routine_args[-2] = [start_node]
        run_args = [
            graph,
            routine.func.routine,
            routine_args,
            routine.func.validation,
            validation_args,
            routine.func.stats,
            routine.args.stats,
            property_name,
            time_data,
        ]

        if name == "jaccard":
            run_args.append(compare_node)

        single_run(*run_args)

    return time_data


def tc(graph: Graph, _input_args):
    time_data = {}

    with time_block(analytics.sort_all_edges_by_dest.__name__, time_data):
        analytics.sort_all_edges_by_dest(graph)

    with time_block(analytics.TriangleCountPlan.__name__, time_data):
        tc_plan = analytics.TriangleCountPlan.ordered_count(edges_sorted=True)

    with time_block(analytics.triangle_count.__name__, time_data):
        n = analytics.triangle_count(graph, tc_plan)

    print(f"STATS:\nNumber of Triangles: {n}")
    return time_data


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
        with time_block("read Graph", {}):
            graph = Graph(
                graph_path, edge_properties=edge_properties, node_properties=[])
        print(f"#Nodes: {len(graph)}, #Edges: {graph.num_edges()}")
        return graph

    # Load our graph
    input = next(item for item in inputs if item["name"] == args.graph)
    data = create_empty_statistics(args)

    # For a minor optimization group the routines by their edge_load True or False
    routine_name_args_mappings = {
        "tc": RoutinePaths(PathExt("Symmetric clean", input["symmetric_clean_input"]), True),
        "cc": RoutinePaths(PathExt("Symmetric", input["symmetric_input"]), True),
        "kcore": RoutinePaths(PathExt("Symmetric", input["symmetric_input"]), True),
        "bfs": RoutinePaths(PathExt("", input["name"]), False),
        "sssp": RoutinePaths(PathExt("", input["name"]), False),
        "jaccard": RoutinePaths(PathExt("", input["name"]), False),
        "bc": RoutinePaths(PathExt("", input["name"]), False),
        "louvain": RoutinePaths(PathExt("Symmetric", input["symmetric_input"]), False),
        "pagerank": RoutinePaths(PathExt("Symmetric", input["transpose_input"]), False),
    }
    start_time = time.time()
    main_warn = "Graph doesn't exist:"
    load_timer = {}
    if args.application not in ["all"]:

        # Select the routine to run
        routine_to_run = routine_name_args_mappings[args.application]
        graph_path = f"{args.input_dir}/{routine_to_run.path.path_ext}"
        if not os.path.exists(graph_path):
            print(f"{routine_to_run.path.warn_prefix} {main_warn} {graph_path}")

        with time_block("graph_load", load_timer):
            if routine_to_run.edge_load:
                graph = load_graph(graph_path, [])
            else:
                graph = load_graph(graph_path)

        data = run_routine(data, load_timer["graph_load"], graph, args, input)

    else:
        first_routine = next(iter(routine_name_args_mappings))
        curr_edge_load = not routine_name_args_mappings[first_routine].edge_load
        for k in routine_name_args_mappings:

            routine_to_run = routine_name_args_mappings[k]
            graph_path = f"{args.input_dir}/{routine_to_run.path.path_ext}"
            if not os.path.exists(graph_path):
                print(f"{routine_to_run.path.warn_prefix} {main_warn} {graph_path}")

            if curr_edge_load != routine_to_run.edge_load:
                with time_block("graph_load", load_timer):
                    if routine_to_run.edge_load:
                        graph = load_graph(graph_path, [])
                    else:
                        graph = load_graph(graph_path)
                curr_edge_load = routine_to_run.edge_load
            args.application = k
            data = run_routine(
                data, load_timer["graph_load"], graph, args, input)

    if args.json_output:
        save_success = save_statistics_as_json(
            data, start_time, args.json_output)

    if save_success:
        return (True, data)

    return (False, {})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark performance of routines")

    parser.add_argument("--input-dir", default="./",
                        help="Path to the input directory (default: %(default)s)")

    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of threads to use (default: query sinfo). Should match max threads.",
    )
    parser.add_argument("--thread-spin", default=False,
                        action="store_true", help="Busy wait for work in thread pool.")

    parser.add_argument(
        "--json-output", help="Path at which to save performance data in JSON")

    parser.add_argument(
        "--graph",
        default="GAP-road",
        choices=["GAP-road", "GAP-kron", "GAP-twitter",
                 "GAP-web", "GAP-urand", "rmat15"],
        help="Graph name (default: %(default)s)",
    )
    parser.add_argument(
        "--application",
        default="bfs",
        choices=["bfs", "sssp", "cc", "bc", "pagerank",
                 "tc", "jaccard", "kcore", "louvain", "all"],
        help="Application to run (default: %(default)s)",
    )
    parser.add_argument("--source-nodes", default="",
                        help="Source nodes file(default: %(default)s)")
    parser.add_argument("--trials", type=int, default=1,
                        help="Number of trials (default: %(default)s)")
    parser.add_argument("--num-sources", type=int, default=4,
                        help="Number of sources (default: %(default)s)")
    parsed_args = parser.parse_args()

    if not os.path.isdir(parsed_args.input_dir):
        print(f"input directory : {parsed_args.input_dir} doesn't exist")
        sys.exit(1)

    if not parsed_args.threads:
        parsed_args.threads = int(os.cpu_count())

    print(
        f"Using input directory: {parsed_args.input_dir} and Threads: {parsed_args.threads}")

    run_all_gap(parsed_args)
