import numpy as np
from pyarrow import Schema, table
from pytest import approx, raises

from katana import GaloisError
from katana.analytics import *
from katana.property_graph import PropertyGraph
from katana.example_utils import get_input
from katana.lonestar.analytics.bfs import verify_bfs
from katana.lonestar.analytics.sssp import verify_sssp

NODES_TO_SAMPLE = 10


def test_assert_valid(property_graph: PropertyGraph):
    with raises(AssertionError):
        bfs_assert_valid(property_graph, "workFrom")
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    v = property_graph.get_node_property(property_name).to_numpy().copy()
    v[0] = 100
    property_graph.add_node_property(table({"Prop2": v}))

    with raises(AssertionError):
        bfs_assert_valid(property_graph, "Prop2")


def test_sort_all_edges_by_dest(property_graph: PropertyGraph):
    original_dests = [[property_graph.get_edge_dst(e) for e in property_graph.edges(n)] for n in range(NODES_TO_SAMPLE)]
    mapping = sort_all_edges_by_dest(property_graph)
    new_dests = [[property_graph.get_edge_dst(e) for e in property_graph.edges(n)] for n in range(NODES_TO_SAMPLE)]
    for n in range(NODES_TO_SAMPLE):
        assert len(original_dests[n]) == len(new_dests[n])
        my_mapping = [mapping[e].as_py() for e in property_graph.edges(n)]
        for i in range(len(my_mapping)):
            assert original_dests[n][i] == new_dests[n][my_mapping[i] - property_graph.edges(n)[0]]
        original_dests[n].sort()

        assert original_dests[n] == new_dests[n]


def test_find_edge_sorted_by_dest(property_graph: PropertyGraph):
    sort_all_edges_by_dest(property_graph)
    assert find_edge_sorted_by_dest(property_graph, 0, 1000) is None
    assert find_edge_sorted_by_dest(property_graph, 0, 1967) == 2


def test_sort_nodes_by_degree(property_graph: PropertyGraph):
    sort_nodes_by_degree(property_graph)
    assert len(property_graph.edges(0)) == 108
    last_node_n_edges = 108
    for n in range(1, NODES_TO_SAMPLE):
        v = len(property_graph.edges(n))
        assert v <= last_node_n_edges
        last_node_n_edges = v


def test_bfs(property_graph: PropertyGraph):
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    bfs_assert_valid(property_graph, property_name)

    stats = BfsStatistics(property_graph, property_name)

    assert stats.source_node == start_node
    assert stats.max_distance == 7

    # Verify with numba implementation of verifier as well
    verify_bfs(property_graph, start_node, new_property_id)


def test_sssp(property_graph: PropertyGraph):
    property_name = "NewProp"
    weight_name = "workFrom"
    start_node = 0

    sssp(property_graph, start_node, weight_name, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    sssp_assert_valid(property_graph, start_node, weight_name, property_name)

    stats = SsspStatistics(property_graph, property_name)

    assert stats.max_distance == 4294967295.0

    # Verify with numba implementation of verifier
    verify_sssp(property_graph, start_node, new_property_id)


def test_jaccard(property_graph: PropertyGraph):
    property_name = "NewProp"
    compare_node = 0

    jaccard(property_graph, compare_node, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    jaccard_assert_valid(property_graph, compare_node, property_name)

    stats = JaccardStatistics(property_graph, compare_node, property_name)

    assert stats.max_similarity == approx(1)
    assert stats.min_similarity == approx(0)
    assert stats.average_similarity == approx(0.000637853)

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.28571428)
    assert similarities[2812] == approx(0.01428571)


def test_jaccard_sorted(property_graph: PropertyGraph):
    sort_all_edges_by_dest(property_graph)

    property_name = "NewProp"
    compare_node = 0

    jaccard(property_graph, compare_node, property_name, JaccardPlan.sorted())

    jaccard_assert_valid(property_graph, compare_node, property_name)

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.28571428)
    assert similarities[2812] == approx(0.01428571)


def test_pagerank(property_graph: PropertyGraph):
    property_name = "NewProp"

    pagerank(property_graph, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    pagerank_assert_valid(property_graph, property_name)

    stats = PagerankStatistics(property_graph, property_name)

    assert stats.min_rank == approx(0.1499999761581421)
    assert stats.max_rank == approx(1328.6629638671875, abs=0.06)
    assert stats.average_rank == approx(0.5205338001251221, abs=0.001)


def test_betweenness_centrality_outer(property_graph: PropertyGraph):
    property_name = "NewProp"

    betweenness_centrality(property_graph, property_name, 16, BetweennessCentralityPlan.outer())

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    stats = BetweennessCentralityStatistics(property_graph, property_name)

    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(8210.38)
    assert stats.average_centrality == approx(1.3645)


def test_betweenness_centrality_level(property_graph: PropertyGraph):
    property_name = "NewProp"

    betweenness_centrality(property_graph, property_name, 16, BetweennessCentralityPlan.level())

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    stats = BetweennessCentralityStatistics(property_graph, property_name)

    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(8210.38)
    assert stats.average_centrality == approx(1.3645)


def test_triangle_count():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    original_first_edge_list = [property_graph.get_edge_dst(e) for e in property_graph.edges(0)]
    n = triangle_count(property_graph)
    assert n == 282617

    n = triangle_count(property_graph, TriangleCountPlan.node_iteration())
    assert n == 282617

    n = triangle_count(property_graph, TriangleCountPlan.edge_iteration())
    assert n == 282617

    assert [property_graph.get_edge_dst(e) for e in property_graph.edges(0)] == original_first_edge_list

    sort_all_edges_by_dest(property_graph)
    n = triangle_count(property_graph, TriangleCountPlan.ordered_count(edges_sorted=True))
    assert n == 282617


def test_triangle_count_presorted():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    sort_nodes_by_degree(property_graph)
    sort_all_edges_by_dest(property_graph)
    n = triangle_count(property_graph, TriangleCountPlan.node_iteration(relabeling=False, edges_sorted=True))
    assert n == 282617


def test_independent_set():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat10_symmetric"))

    independent_set(property_graph, "output")

    stats = IndependentSetStatistics(property_graph, "output")

    independent_set_assert_valid(property_graph, "output")

    independent_set(property_graph, "output2", IndependentSetPlan.pull())

    stats = IndependentSetStatistics(property_graph, "output2")

    independent_set_assert_valid(property_graph, "output2")


def test_connected_components():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat10_symmetric"))

    connected_components(property_graph, "output")

    stats = ConnectedComponentsStatistics(property_graph, "output")

    assert stats.total_components == 69
    assert stats.total_non_trivial_components == 1
    assert stats.largest_component_size == 957
    assert stats.largest_component_ratio == approx(0.93457)

    connected_components_assert_valid(property_graph, "output")


def test_k_core():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat10_symmetric"))

    k_core(property_graph, 10, "output")

    stats = KCoreStatistics(property_graph, 10, "output")

    assert stats.number_of_nodes_in_kcore == 438

    k_core_assert_valid(property_graph, 10, "output")


def test_k_truss():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat10_symmetric"))

    k_truss(property_graph, 10, "output")

    stats = KTrussStatistics(property_graph, 10, "output")

    assert stats.number_of_edges_left == 13338

    k_truss_assert_valid(property_graph, 10, "output")


def test_k_truss_fail():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat10_symmetric"))

    with raises(GaloisError):
        k_truss(property_graph, 2, "output")

    with raises(GaloisError):
        k_truss(property_graph, 1, "output2")
