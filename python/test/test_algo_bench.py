import argparse
import contextlib
import json
import os
import sys
import time
from collections import namedtuple
from datetime import datetime

import numpy as np
import pytest
import pytz
from pyarrow import Schema

import katana.benchmarking.bench_python_cpp_algos as katbench


def generate_args(
    json_output,
    input_dir="./",
    graph="GAP-road",
    app="bfs",
    source_nodes="",
    trails=1,
    num_sources=4,
    thread_spin=False,
    threads=1,
):

    parser = argparse.ArgumentParser(description="Benchmark performance of routines")
    parser.add_argument("--input-dir", default=input_dir, help="Path to the input directory (default: %(default)s)")

    parser.add_argument(
        "--threads",
        type=int,
        default=threads,
        help="Number of threads to use (default: query sinfo). Should match max threads.",
    )
    parser.add_argument(
        "--thread-spin", default=thread_spin, action="store_true", help="Busy wait for work in thread pool."
    )

    parser.add_argument("--json-output", default=json_output, help="Path at which to save performance data in JSON")

    parser.add_argument(
        "--graph", default=graph, choices=katbench.GRAPH_CHOICES, help="Graph name (default: %(default)s)",
    )
    parser.add_argument(
        "--application", default=app, choices=katbench.APP_CHOICES, help="Application to run (default: %(default)s)",
    )
    parser.add_argument("--source-nodes", default=source_nodes, help="Source nodes file(default: %(default)s)")
    parser.add_argument("--trials", type=int, default=1, help="Number of trials (default: %(default)s)")
    parser.add_argument("--num-sources", type=int, default=4, help="Number of sources (default: %(default)s)")

    parsed_args = parser.parse_args()

    if not os.path.isdir(parsed_args.input_dir):
        print(f"input directory : {parsed_args.input_dir} doesn't exist")
        sys.exit(1)
    if not parsed_args.threads:
        parsed_args.threads = int(os.cpu_count())
    print(f"Using input directory: {parsed_args.input_dir} and Threads: {parsed_args.threads}")

    return parsed_args


def assert_routine_output(routine_output, max_time=1_000_000):
    for subproccess in routine_output:
        assert (
            0 <= routine_output[subproccess] <= max_time
        ), f"Invalid time for subproccess {subproccess}: took {routine_output[subproccess]} ms"


def assert_types_match(ground_truth, outp):
    for sec in ground_truth:
        assert sec in outp, f"Output missing an element: {sec}"
        truth_type = type(ground_truth[sec])
        out_type = type(outp[sec])
        assert truth_type == out_type, f"Expected types for {sec} do not match - Expected:{truth_type}, Got:{out_type}"

    return True


def test_single_trial_gaps():
    arguments = {
        "json_output": "../../../bench_statistics.json",
        "input_dir": "../../inputs/v24/propertygraphs",
        "graph": "rmat15",
        "app": "all",
        "source_nodes": "",
        "trails": 1,
        "num_sources": np.random.randint(4, 64),
        "thread_spin": False,
        "threads": None,
    }

    options = katbench.initialize_global_vars()

    all_apps = options[0]
    all_graphs = ["rmat15"]

    for graph in all_graphs:
        arguments["graph"] = graph
        for app in all_apps:
            arguments["app"] = app
            run_single_t(arguments)


def run_single_t(arguments):
    args = generate_args(**arguments)
    ground_truth = katbench.create_empty_statistics(args)
    output_tuple = katbench.run_all_gap(args)
    assert output_tuple.write_success, "Writing JSON statistics to disc failed!"
    assert_types_match(ground_truth, output_tuple.write_data)
    for subroutine in output_tuple.write_data["routines"]:
        assert_routine_output(output_tuple.write_data["routines"][subroutine])


if __name__ == "__main__":
    test_single_trial_gaps()
