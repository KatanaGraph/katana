import argparse
import os
import test.benchmarking.bench_python_cpp_algos

from pytest import approx

from katana.example_data import get_input


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
        "json_output": "",
        "input_dir": get_input("propertygraphs/"),
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
    arguments["trails"] = 3
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


def test_bfs():
    arguments = get_default_args()
    arguments["app"] = "bfs"
    print(arguments)
    stats = run_single_test(arguments)[0]
    assert stats.n_reached_nodes == 29352


def test_sssp():
    arguments = get_default_args()
    arguments["app"] = "sssp"
    stats = run_single_test(arguments)[0]
    assert stats.max_distance == 104.0


def test_jaccard():
    arguments = get_default_args()
    arguments["app"] = "jaccard"
    stats = run_single_test(arguments)[0]
    assert stats.max_similarity == approx(0.060981, abs=0.02)
    assert stats.min_similarity == approx(0.0)
    assert stats.average_similarity == approx(4.1105422339459704e-05)


def test_pagerank():
    arguments = get_default_args()
    arguments["app"] = "pagerank"
    stats = run_single_test(arguments)[0]
    assert stats.min_rank == approx(0.1499999761581421)
    assert stats.max_rank == approx(21345.28515625, abs=100)
    assert stats.average_rank == approx(0.9, abs=0.1)


def test_betweenness_centrality():
    arguments = get_default_args()
    arguments["app"] = "bc"
    stats = run_single_test(arguments)[0]
    print(stats.min_centrality, stats.max_centrality, stats.average_centrality)
    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(361.0888977050781)
    assert stats.average_centrality == approx(0.26196303963661194)


def test_triangle_count():
    arguments = get_default_args()
    arguments["app"] = "tc"
    stats = run_single_test(arguments)[0]
    assert stats == 282617


def test_connected_components():
    arguments = get_default_args()
    arguments["app"] = "cc"
    stats = run_single_test(arguments)[0]
    assert stats.total_components == 3417
    assert stats.total_non_trivial_components == 1
    assert stats.largest_component_size == 29352
    assert stats.largest_component_ratio == approx(0.895752)


def test_k_core():
    arguments = get_default_args()
    arguments["app"] = "kcore"
    stats = run_single_test(arguments)[0]
    assert stats.number_of_nodes_in_kcore == 11958


def run_on_all_graphs(arguments):

    all_apps = test.benchmarking.bench_python_cpp_algos.APPS
    all_graphs = ["rmat15"]

    for graph in all_graphs:
        arguments["graph"] = graph
        for app in all_apps:
            arguments["app"] = app
            run_single_test(arguments)


def run_single_test(arguments):
    args = generate_args(**arguments)
    ground_truth = test.benchmarking.bench_python_cpp_algos.create_empty_statistics(args)
    output_tuple = test.benchmarking.bench_python_cpp_algos.run_all_gap(args)
    assert output_tuple.write_success, "Writing JSON statistics to disk failed!"
    assert_types_match(ground_truth, output_tuple.time_write_data)
    for subroutine in output_tuple.time_write_data["routines"]:
        assert_routine_output(output_tuple.time_write_data["routines"][subroutine])
    return output_tuple.analytics_write_data
