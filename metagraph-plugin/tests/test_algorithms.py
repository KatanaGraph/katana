import metagraph as mg
import numpy as np
import pytest


# directed graph
@pytest.fixture(autouse=True)
def kg_from_nx_di_8_12(networkx_weighted_directed_8_12):
    pg_test_case = mg.translate(networkx_weighted_directed_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


# undirected graph
@pytest.fixture(autouse=True)
def kg_from_nx_ud_8_12(networkx_weighted_undirected_8_12):
    pg_test_case = mg.translate(networkx_weighted_undirected_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


def test_bfs(networkx_weighted_directed_8_12, kg_from_nx_di_8_12):
    bfs1_nx = mg.algos.traversal.bfs_iter(networkx_weighted_directed_8_12, 0)
    bfs2_nx = mg.algos.traversal.bfs_iter(networkx_weighted_directed_8_12, 2)
    bfs1_kg = mg.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 0, 2 ** 30 - 1)
    bfs2_kg = mg.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 2, 2 ** 30 - 1)
    assert bfs1_kg.tolist() == bfs1_nx.tolist()
    assert bfs2_kg.tolist() == bfs2_nx.tolist()
    assert bfs1_kg.tolist() == [0, 1, 3, 4, 7]
    assert bfs2_kg.tolist() == [2, 4, 5, 6, 7]


def test_sssp_bellman_ford(networkx_weighted_directed_8_12, kg_from_nx_di_8_12):
    src_node = 0
    sssp_nx = mg.algos.traversal.bellman_ford(networkx_weighted_directed_8_12, src_node)  # source node is 0
    parents_nx = sssp_nx[0]
    distances_nx = sssp_nx[1]
    assert isinstance(parents_nx, dict)
    assert isinstance(distances_nx, dict)
    assert parents_nx == {0: 0, 1: 0, 3: 0, 4: 3, 7: 4}
    assert distances_nx == {0: 0, 1: 4, 3: 2, 4: 3, 7: 7}
    parents_kg, distances_kg = mg.algos.traversal.bellman_ford(kg_from_nx_di_8_12, src_node)
    assert parents_nx == parents_kg
    assert distances_nx == distances_kg


def test_sssp_dijkstra(networkx_weighted_directed_8_12, kg_from_nx_di_8_12):
    src_node = 1
    sssp_nx = mg.algos.traversal.dijkstra(networkx_weighted_directed_8_12, src_node)  # source node is 1
    parents_nx = sssp_nx[0]
    distances_nx = sssp_nx[1]
    assert isinstance(parents_nx, dict)
    assert isinstance(distances_nx, dict)
    assert parents_nx == {1: 1, 3: 1, 4: 3, 7: 4}
    assert distances_nx == {1: 0, 3: 3, 4: 4, 7: 8}
    parents_kg, distances_kg = mg.algos.traversal.dijkstra(kg_from_nx_di_8_12, src_node)
    assert parents_nx == parents_kg
    assert distances_nx == distances_kg


def test_connected_components(networkx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    cc_nx = mg.algos.clustering.connected_components(networkx_weighted_undirected_8_12)
    cc_kg = mg.algos.clustering.connected_components(kg_from_nx_ud_8_12)
    assert isinstance(cc_kg, dict)
    assert isinstance(cc_kg, dict)
    assert cc_kg == cc_nx


def test_pagerank(networkx_weighted_directed_8_12, kg_from_nx_di_8_12):
    pr_nx = mg.algos.centrality.pagerank(networkx_weighted_directed_8_12)
    pr_kg = mg.algos.centrality.pagerank(kg_from_nx_di_8_12)
    assert isinstance(pr_nx, dict)
    assert isinstance(pr_kg, dict)
    assert pr_nx == pr_kg


def test_betweenness_centrality(networkx_weighted_directed_8_12, kg_from_nx_di_8_12):
    bc_nx = mg.algos.centrality.betweenness(networkx_weighted_directed_8_12)
    bc_kg = mg.algos.centrality.betweenness(kg_from_nx_di_8_12)
    assert isinstance(bc_nx, dict)
    assert isinstance(bc_kg, dict)
    assert bc_nx == bc_kg


def test_triangle_counting(networkx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    tc_nx = mg.algos.clustering.triangle_count(networkx_weighted_undirected_8_12)
    tc_kg = mg.algos.clustering.triangle_count(kg_from_nx_ud_8_12)
    assert isinstance(tc_nx, int)
    assert isinstance(tc_kg, int)
    assert tc_nx == tc_kg


def test_louvain_community_detection(networkx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    lc_nx = mg.algos.clustering.louvain_community(networkx_weighted_undirected_8_12)
    lc_kg = mg.algos.clustering.louvain_community(kg_from_nx_ud_8_12)
    assert isinstance(lc_nx[0], dict)
    assert isinstance(lc_kg[0], dict)
    assert isinstance(lc_nx[1], float)
    assert isinstance(lc_kg[1], float)
    assert lc_nx[0] == lc_kg[0]
    assert lc_nx[1] == lc_kg[1]


def test_translation_subgraph_extraction(networkx_weighted_directed_8_12, kg_from_nx_di_8_12):
    se_nx = mg.algos.subgraph.extract_subgraph(networkx_weighted_directed_8_12, {0, 2, 3})
    se_kg = mg.algos.subgraph.extract_subgraph(kg_from_nx_di_8_12, {0, 2, 3})
    assert isinstance(se_nx, mg.wrappers.Graph.NetworkXGraph)
    assert isinstance(se_kg, mg.wrappers.Graph.NetworkXGraph)
    assert list(se_nx.value.edges(data=True)) == list(se_kg.value.edges(data=True))


def test_labal_propagation(networkx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    cd_nx = mg.algos.clustering.label_propagation_community(networkx_weighted_undirected_8_12)
    cd_kg = mg.algos.clustering.label_propagation_community(kg_from_nx_ud_8_12)
    assert isinstance(cd_nx, dict)
    assert isinstance(cd_kg, dict)
    assert cd_nx == cd_kg


def test_jaccard_similarity(networkx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    compare_node = 0
    prop_name = "jaccard_prop_with_" + str(compare_node)
    jcd_nx = mg.algos.traversal.jaccard(networkx_weighted_undirected_8_12, compare_node)
    jcd_kg = mg.algos.traversal.jaccard(kg_from_nx_ud_8_12, compare_node)
    assert isinstance(jcd_nx, np.ndarray)
    assert isinstance(jcd_kg, np.ndarray)
    assert jcd_nx.tolist() == jcd_kg.tolist()
    assert jcd_kg[compare_node] == 1


def test_local_clustering_coefficient(networkx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    prop_name = "output_prop"
    lcc_nx = mg.algos.clustering.local_clustering_coefficient(networkx_weighted_undirected_8_12, prop_name)
    lcc_kg = mg.algos.clustering.local_clustering_coefficient(kg_from_nx_ud_8_12, prop_name)
    assert isinstance(lcc_nx, np.ndarray)
    assert isinstance(lcc_kg, np.ndarray)
    assert lcc_kg.tolist() == lcc_nx.tolist()
    assert lcc_kg[-1] == 0
    assert not np.any(np.isnan(lcc_kg))
