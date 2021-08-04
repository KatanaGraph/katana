from test.lonestar.bfs import verify_bfs
from test.lonestar.sssp import verify_sssp

import numpy as np
from pyarrow import Schema, table
from pytest import approx, raises

from katana import GaloisError
from katana.example_data import get_input
from katana.galois import set_busy_wait
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
from katana.local.property_graph import PropertyGraph

NODES_TO_SAMPLE = 10


def test_assert_valid(property_graph: PropertyGraph):
    property_name = "NewProp"
    start_node = 0

    with raises(AssertionError):
        bfs_assert_valid(property_graph, start_node, "workFrom")

    bfs(property_graph, start_node, property_name)

    v = property_graph.get_node_property(property_name).to_numpy().copy()
    v[0] = 100
    property_graph.add_node_property(table({"Prop2": v}))

    with raises(AssertionError):
        bfs_assert_valid(property_graph, start_node, "Prop2")


def test_sort_all_edges_by_dest(property_graph: PropertyGraph):
    original_dests = [
        [property_graph.get_edge_dest(e) for e in property_graph.edges(n)] for n in range(NODES_TO_SAMPLE)
    ]
    mapping = sort_all_edges_by_dest(property_graph)
    new_dests = [[property_graph.get_edge_dest(e) for e in property_graph.edges(n)] for n in range(NODES_TO_SAMPLE)]
    for n in range(NODES_TO_SAMPLE):
        assert len(original_dests[n]) == len(new_dests[n])
        my_mapping = [mapping[e] for e in property_graph.edges(n)]
        for i, _ in enumerate(my_mapping):
            assert original_dests[n][i] == new_dests[n][my_mapping[i] - property_graph.edges(n)[0]]
        original_dests[n].sort()

        assert original_dests[n] == new_dests[n]


def test_find_edge_sorted_by_dest(property_graph: PropertyGraph):
    sort_all_edges_by_dest(property_graph)
    assert find_edge_sorted_by_dest(property_graph, 0, 1000) is None
    assert find_edge_sorted_by_dest(property_graph, 0, 1967) is None


def test_sort_nodes_by_degree(property_graph: PropertyGraph):
    sort_nodes_by_degree(property_graph)
    assert len(property_graph.edges(0)) == 103
    last_node_n_edges = 103
    for n in range(1, NODES_TO_SAMPLE):
        v = len(property_graph.edges(n))
        assert v <= last_node_n_edges
        last_node_n_edges = v


def test_bfs(property_graph: PropertyGraph):
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    bfs_assert_valid(property_graph, start_node, property_name)

    stats = BfsStatistics(property_graph, property_name)

    assert stats.n_reached_nodes == 3

    # Verify with numba implementation of verifier as well
    verify_bfs(property_graph, start_node, new_property_id)


def test_sssp(property_graph: PropertyGraph):
    property_name = "NewProp"
    weight_name = "workFrom"
    start_node = 0

    sssp(property_graph, start_node, weight_name, property_name)

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    sssp_assert_valid(property_graph, start_node, weight_name, property_name)

    stats = SsspStatistics(property_graph, property_name)

    print(stats)
    assert stats.max_distance == 0.0

    # Verify with numba implementation of verifier
    verify_sssp(property_graph, start_node, new_property_id)


def test_jaccard(property_graph: PropertyGraph):
    property_name = "NewProp"
    compare_node = 0

    jaccard(property_graph, compare_node, property_name)

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    jaccard_assert_valid(property_graph, compare_node, property_name)

    stats = JaccardStatistics(property_graph, compare_node, property_name)

    assert stats.max_similarity == approx(1)
    assert stats.min_similarity == approx(0)
    assert stats.average_similarity == approx(0.000552534)

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.0)
    assert similarities[2812] == approx(0.0)


def test_jaccard_sorted(property_graph: PropertyGraph):
    sort_all_edges_by_dest(property_graph)

    property_name = "NewProp"
    compare_node = 0

    jaccard(property_graph, compare_node, property_name, JaccardPlan.sorted())

    jaccard_assert_valid(property_graph, compare_node, property_name)

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.0)
    assert similarities[2812] == approx(0.0)


def test_pagerank(property_graph: PropertyGraph):
    property_name = "NewProp"

    pagerank(property_graph, property_name)

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    pagerank_assert_valid(property_graph, property_name)

    stats = PagerankStatistics(property_graph, property_name)

    assert stats.min_rank == approx(0.1499999761581421)
    assert stats.max_rank == approx(1347.884765625, abs=0.06)
    assert stats.average_rank == approx(0.5215466022491455, abs=0.001)


def test_betweenness_centrality_outer(property_graph: PropertyGraph):
    property_name = "NewProp"

    betweenness_centrality(property_graph, property_name, 16, BetweennessCentralityPlan.outer())

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    stats = BetweennessCentralityStatistics(property_graph, property_name)

    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(7.0)
    assert stats.average_centrality == approx(0.000534295046236366)


def test_betweenness_centrality_level(property_graph: PropertyGraph):
    property_name = "NewProp"

    betweenness_centrality(property_graph, property_name, 16, BetweennessCentralityPlan.level())

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    stats = BetweennessCentralityStatistics(property_graph, property_name)

    assert stats.min_centrality == 0
    assert stats.max_centrality == approx(7.0)
    assert stats.average_centrality == approx(0.000534295046236366)


def test_triangle_count():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    original_first_edge_list = [property_graph.get_edge_dest(e) for e in property_graph.edges(0)]
    n = triangle_count(property_graph)
    assert n == 282617

    n = triangle_count(property_graph, TriangleCountPlan.node_iteration())
    assert n == 282617

    n = triangle_count(property_graph, TriangleCountPlan.edge_iteration())
    assert n == 282617

    assert [property_graph.get_edge_dest(e) for e in property_graph.edges(0)] == original_first_edge_list

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

    IndependentSetStatistics(property_graph, "output")

    independent_set_assert_valid(property_graph, "output")

    independent_set(property_graph, "output2", IndependentSetPlan.pull())

    IndependentSetStatistics(property_graph, "output2")

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


def test_louvain_clustering():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat10_symmetric"))

    louvain_clustering(property_graph, "value", "output")

    louvain_clustering_assert_valid(property_graph, "value", "output")

    LouvainClusteringStatistics(property_graph, "value", "output")

    # TODO(amp): This is non-deterministic. Are there bounds on the results we could check?
    # assert stats.n_clusters == 83
    # assert stats.n_non_trivial_clusters == 13
    # assert stats.largest_cluster_size == 297


def test_local_clustering_coefficient():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat15_cleaned_symmetric"))

    local_clustering_coefficient(property_graph, "output")
    property_graph: PropertyGraph
    out = property_graph.get_node_property("output")

    assert out[-1].as_py() == 0
    assert not np.any(np.isnan(out))


def test_subgraph_extraction():
    property_graph = PropertyGraph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    sort_all_edges_by_dest(property_graph)
    nodes = [1, 3, 11, 120]

    expected_edges = [
        [
            nodes.index(property_graph.get_edge_dest(e))
            for e in property_graph.edges(i)
            if property_graph.get_edge_dest(e) in nodes
        ]
        for i in nodes
    ]

    pg = subgraph_extraction(property_graph, nodes)

    assert isinstance(pg, PropertyGraph)
    assert len(pg) == len(nodes)
    assert pg.num_edges() == 6

    for i, _ in enumerate(expected_edges):
        assert len(pg.edges(i)) == len(expected_edges[i])
        assert [pg.get_edge_dest(e) for e in pg.edges(i)] == expected_edges[i]


def test_busy_wait(property_graph: PropertyGraph):
    set_busy_wait()
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    node_schema: Schema = property_graph.loaded_node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    bfs_assert_valid(property_graph, start_node, property_name)

    BfsStatistics(property_graph, property_name)

    # Verify with numba implementation of verifier as well
    verify_bfs(property_graph, start_node, new_property_id)
    set_busy_wait(0)
