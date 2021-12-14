from test.lonestar.bfs import verify_bfs
from test.lonestar.sssp import verify_sssp

import numpy as np
from pyarrow import Schema, table
from pytest import approx, raises

from katana import GaloisError, set_busy_wait
from katana.example_data import get_input
from katana.local import Graph
from katana.local.analytics import (
    BetweennessCentralityPlan,
    BetweennessCentralityStatistics,
    BfsStatistics,
    ConnectedComponentsStatistics,
    IndependentSetPlan,
    IndependentSetStatistics,
    JaccardPlan,
    JaccardStatistics,
    KCoreStatistics,
    KTrussStatistics,
    LeidenClusteringStatistics,
    LouvainClusteringStatistics,
    PagerankStatistics,
    SsspStatistics,
    TriangleCountPlan,
    betweenness_centrality,
    bfs,
    bfs_assert_valid,
    connected_components,
    connected_components_assert_valid,
    find_edge_sorted_by_dest,
    independent_set,
    independent_set_assert_valid,
    jaccard,
    jaccard_assert_valid,
    k_core,
    k_core_assert_valid,
    k_truss,
    k_truss_assert_valid,
    leiden_clustering,
    leiden_clustering_assert_valid,
    local_clustering_coefficient,
    louvain_clustering,
    louvain_clustering_assert_valid,
    pagerank,
    pagerank_assert_valid,
    sort_all_edges_by_dest,
    sort_nodes_by_degree,
    sssp,
    sssp_assert_valid,
    subgraph_extraction,
    triangle_count,
)

NODES_TO_SAMPLE = 10


def test_assert_valid(graph: Graph):
    property_name = "NewProp"
    start_node = 0

    with raises(AssertionError):
        bfs_assert_valid(graph, start_node, "workFrom")

    bfs(graph, start_node, property_name)

    v = graph.get_node_property(property_name).to_numpy().copy()
    v[0] = 100
    graph.add_node_property(table({"Prop2": v}))

    with raises(AssertionError):
        bfs_assert_valid(graph, start_node, "Prop2")


def test_sort_all_edges_by_dest(graph: Graph):
    original_dests = [[graph.get_edge_dest(e) for e in graph.edge_ids(n)] for n in range(NODES_TO_SAMPLE)]
    mapping = sort_all_edges_by_dest(graph)
    new_dests = [[graph.get_edge_dest(e) for e in graph.edge_ids(n)] for n in range(NODES_TO_SAMPLE)]
    for n in range(NODES_TO_SAMPLE):
        assert len(original_dests[n]) == len(new_dests[n])
        my_mapping = [mapping[e] for e in graph.edge_ids(n)]
        for i, _ in enumerate(my_mapping):
            assert original_dests[n][i] == new_dests[n][my_mapping[i] - graph.edge_ids(n)[0]]
        original_dests[n].sort()

        assert original_dests[n] == new_dests[n]


def test_find_edge_sorted_by_dest(graph: Graph):
    sort_all_edges_by_dest(graph)
    assert find_edge_sorted_by_dest(graph, 0, 1000) is None
    assert find_edge_sorted_by_dest(graph, 0, 1967) is None


def test_sort_nodes_by_degree(graph: Graph):
    sort_nodes_by_degree(graph)
    assert len(graph.edge_ids(0)) == 103
    last_node_n_edges = 103
    for n in range(1, NODES_TO_SAMPLE):
        v = len(graph.edge_ids(n))
        assert v <= last_node_n_edges
        last_node_n_edges = v


def test_bfs(graph: Graph):
    property_name = "NewProp"
    start_node = 0

    bfs(graph, start_node, property_name)

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert graph.get_node_property(property_name)[start_node].as_py() == 0

    bfs_assert_valid(graph, start_node, property_name)

    stats = BfsStatistics(graph, property_name)

    assert stats.n_reached_nodes == 3

    # Verify with numba implementation of verifier as well
    verify_bfs(graph, start_node, new_property_id)


def test_sssp(graph: Graph):
    property_name = "NewProp"
    weight_name = "workFrom"
    start_node = 0

    sssp(graph, start_node, weight_name, property_name)

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert graph.get_node_property(property_name)[start_node].as_py() == 0

    sssp_assert_valid(graph, start_node, weight_name, property_name)

    stats = SsspStatistics(graph, property_name)

    print(stats)
    assert stats.max_distance == 0.0

    # Verify with numba implementation of verifier
    verify_sssp(graph, start_node, new_property_id)


def test_jaccard(graph: Graph):
    property_name = "NewProp"
    compare_node = 0

    jaccard(graph, compare_node, property_name)

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    jaccard_assert_valid(graph, compare_node, property_name)

    stats = JaccardStatistics(graph, compare_node, property_name)

    assert stats.max_similarity == approx(1)
    assert stats.min_similarity == approx(0)
    assert stats.average_similarity == approx(0.000552534)

    similarities: np.ndarray = graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.0)
    assert similarities[2812] == approx(0.0)


def test_jaccard_sorted(graph: Graph):
    sort_all_edges_by_dest(graph)

    property_name = "NewProp"
    compare_node = 0

    jaccard(graph, compare_node, property_name, JaccardPlan.sorted())

    jaccard_assert_valid(graph, compare_node, property_name)

    similarities: np.ndarray = graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.0)
    assert similarities[2812] == approx(0.0)


def test_pagerank(graph: Graph):
    property_name = "NewProp"

    pagerank(graph, property_name)

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    pagerank_assert_valid(graph, property_name)

    stats = PagerankStatistics(graph, property_name)

    assert stats.min_rank == approx(0.1499999761581421)
    assert stats.max_rank == approx(1347.884765625, abs=0.06)
    assert stats.average_rank == approx(0.5215466022491455, abs=0.001)


def test_betweenness_centrality_outer(graph: Graph):
    property_name = "NewProp"

    betweenness_centrality(graph, property_name, 16, BetweennessCentralityPlan.outer())

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    stats = BetweennessCentralityStatistics(graph, property_name)

    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(7.0)
    assert stats.average_centrality == approx(0.000534295046236366)


def test_betweenness_centrality_level(graph: Graph):
    property_name = "NewProp"

    betweenness_centrality(graph, property_name, 16, BetweennessCentralityPlan.level())

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    stats = BetweennessCentralityStatistics(graph, property_name)

    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(7.0)
    assert stats.average_centrality == approx(0.000534295046236366)


def test_triangle_count():
    graph = Graph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    original_first_edge_list = [graph.get_edge_dest(e) for e in graph.edge_ids(0)]
    n = triangle_count(graph)
    assert n == 282617

    n = triangle_count(graph, TriangleCountPlan.node_iteration())
    assert n == 282617

    n = triangle_count(graph, TriangleCountPlan.edge_iteration())
    assert n == 282617

    assert [graph.get_edge_dest(e) for e in graph.edge_ids(0)] == original_first_edge_list

    sort_all_edges_by_dest(graph)
    n = triangle_count(graph, TriangleCountPlan.ordered_count(edges_sorted=True))
    assert n == 282617


def test_triangle_count_presorted():
    graph = Graph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    sort_nodes_by_degree(graph)
    sort_all_edges_by_dest(graph)
    n = triangle_count(graph, TriangleCountPlan.node_iteration(relabeling=False, edges_sorted=True))
    assert n == 282617


def test_independent_set():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    independent_set(graph, "output")

    IndependentSetStatistics(graph, "output")

    independent_set_assert_valid(graph, "output")

    independent_set(graph, "output2", IndependentSetPlan.pull())

    IndependentSetStatistics(graph, "output2")

    independent_set_assert_valid(graph, "output2")


def test_connected_components():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    connected_components(graph, "output")

    stats = ConnectedComponentsStatistics(graph, "output")

    assert stats.total_components == 69
    assert stats.total_non_trivial_components == 1
    assert stats.largest_component_size == 956
    assert stats.largest_component_ratio == approx(0.933594)

    connected_components_assert_valid(graph, "output")


def test_k_core():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    k_core(graph, 10, "output")

    stats = KCoreStatistics(graph, 10, "output")

    assert stats.number_of_nodes_in_kcore == 438

    k_core_assert_valid(graph, 10, "output")


def test_k_truss():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    k_truss(graph, 10, "output")

    stats = KTrussStatistics(graph, 10, "output")

    assert stats.number_of_edges_left == 13338

    k_truss_assert_valid(graph, 10, "output")


def test_k_truss_fail():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    with raises(GaloisError):
        k_truss(graph, 2, "output")

    with raises(GaloisError):
        k_truss(graph, 1, "output2")


def test_louvain_clustering():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    louvain_clustering(graph, "value", "output")

    louvain_clustering_assert_valid(graph, "value", "output")

    LouvainClusteringStatistics(graph, "value", "output")

    # TODO(amp): Switch to useing deterministic algorithm so we can check results.
    # assert stats.n_clusters == 83
    # assert stats.n_non_trivial_clusters == 13
    # assert stats.largest_cluster_size == 297


def test_leiden_clustering():
    graph = Graph(get_input("propertygraphs/rmat10_symmetric"))

    leiden_clustering(graph, "value", "output")

    leiden_clustering_assert_valid(graph, "value", "output")

    LeidenClusteringStatistics(graph, "value", "output")

    # TODO(amp): Switch to useing deterministic algorithm so we can check results.
    # assert stats.n_clusters == 83
    # assert stats.n_non_trivial_clusters == 13
    # assert stats.largest_cluster_size == 297


def test_local_clustering_coefficient():
    graph = Graph(get_input("propertygraphs/rmat15_cleaned_symmetric"))

    local_clustering_coefficient(graph, "output")
    graph: Graph
    out = graph.get_node_property("output")

    assert out[-1].as_py() == 0
    assert not np.any(np.isnan(out))


def test_subgraph_extraction():
    graph = Graph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    sort_all_edges_by_dest(graph)
    nodes = [1, 3, 11, 120]

    expected_edges = [
        [nodes.index(graph.get_edge_dest(e)) for e in graph.edge_ids(i) if graph.get_edge_dest(e) in nodes]
        for i in nodes
    ]

    pg = subgraph_extraction(graph, nodes)

    assert isinstance(pg, Graph)
    assert len(pg) == len(nodes)
    assert pg.num_edges() == 6

    for i, _ in enumerate(expected_edges):
        assert len(pg.edge_ids(i)) == len(expected_edges[i])
        assert [pg.get_edge_dest(e) for e in pg.edge_ids(i)] == expected_edges[i]


def test_busy_wait(graph: Graph):
    set_busy_wait()
    property_name = "NewProp"
    start_node = 0

    bfs(graph, start_node, property_name)

    node_schema: Schema = graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert graph.get_node_property(property_name)[start_node].as_py() == 0

    bfs_assert_valid(graph, start_node, property_name)

    BfsStatistics(graph, property_name)

    # Verify with numba implementation of verifier as well
    verify_bfs(graph, start_node, new_property_id)
    set_busy_wait(0)
