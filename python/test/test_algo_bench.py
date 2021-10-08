import argparse
import os

import test.benchmarking.bench_python_cpp_algos


def generate_args(
    json_output, input_dir, graph, app, source_nodes, trails, num_sources, thread_spin, threads,
):
    parser = argparse.Namespace()
    parser.input_dir = input_dir
    parser.threads = threads
    parser.thread_spin = thread_spin
    parser.json_output = json_output
    parser.graph = graph
    parser.application = app
    parser.source_nodes = source_nodes
    parser.trials = trails
    parser.num_sources = num_sources

    if not parser.threads:
        parser.threads = int(os.cpu_count())

    return parser


def assert_routine_output(routine_output, max_time=250_000):
    for subproccess in routine_output:
        assert (
            0 <= routine_output[subproccess] <= max_time
        ), f"Invalid completion time for subproccess {subproccess}: took {routine_output[subproccess]} ms"


def assert_types_match(ground_truth, outp):
    for element in ground_truth:
        assert element in outp, f"Output missing an element: {element}"
        truth_type = type(ground_truth[element])
        out_type = type(outp[element])
        assert (
            truth_type == out_type
        ), f"Expected types for {element} do not match - Expected:{truth_type}, Got:{out_type}"

    return True


def get_default_args():
    arguments = {
        "json_output": os.path.join(os.path.dirname(__file__), "../../../../../../bench_statistics.json"),
        "input_dir": os.path.join(os.path.dirname(__file__), "../../../../../inputs/v24/propertygraphs"),
        "graph": "rmat15",
        "app": "all",
        "source_nodes": "",
        "trails": 1,
        "num_sources": 4,
        "thread_spin": False,
        "threads": None,
    }
    return arguments


def test_single_trail_gaps():
    arguments = get_default_args()
    run_on_all_graphs(arguments)


def test_multi_trail_gaps():
    arguments = get_default_args()
    arguments["trails"] = 10
    run_on_all_graphs(arguments)


def test_more_sources_gaps():
    arguments = get_default_args()
    for i in range(6):
        arguments["num_sources"] = pow(2, i)
        run_on_all_graphs(arguments)


def test_thread_gaps():
    arguments = get_default_args()
    for i in range(6):
        arguments["threads"] = pow(2, i)
        run_on_all_graphs(arguments)


def test_thread_spin():
    arguments = get_default_args()
    arguments["thread_spin"] = True
    run_on_all_graphs(arguments)


def run_on_all_graphs(arguments):
    options = test.benchmarking.bench_python_cpp_algos.initialize_global_vars()

    all_apps = options[0]
    all_graphs = ["rmat15"]

    for graph in all_graphs:
        arguments["graph"] = graph
        for app in all_apps:
            arguments["app"] = app
            run_single_t(arguments)


def run_single_t(arguments):
    args = generate_args(**arguments)
    ground_truth = test.benchmarking.bench_python_cpp_algos.create_empty_statistics(
        args)
    output_tuple = test.benchmarking.bench_python_cpp_algos.run_all_gap(
        args)
    assert output_tuple.write_success, "Writing JSON statistics to disc failed!"
    assert_types_match(ground_truth, output_tuple.write_data)
    for subroutine in output_tuple.write_data["routines"]:
        assert_routine_output(output_tuple.write_data["routines"][subroutine])


test_single_trail_gaps()
