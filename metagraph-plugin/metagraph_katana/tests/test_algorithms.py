import pytest
from pytest import approx
import numpy as np
import metagraph as mg


from katana.lonestar.analytics.bfs import verify_bfs
from katana.analytics import (
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

# directed graph
@pytest.fixture(autouse=True)
def kg_from_nx_di_8_12(nx_weighted_directed_8_12):
    pg_test_case = mg.translate(nx_weighted_directed_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


# undirected graph
@pytest.fixture(autouse=True)
def kg_from_nx_ud_8_12(nx_weighted_undirected_8_12):
    pg_test_case = mg.translate(nx_weighted_undirected_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


# breadth-first search, both the katana-version and the networkx version are implemented
# mg.algos.traversal.bfs_iter(graph: Graph, source_node: NodeID, depth_limit: int = - 1) → Vector
# results of the 2 versions are compared
def test_bfs(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    bfs1_nx = mg.algos.traversal.bfs_iter(nx_weighted_directed_8_12, 0)
    bfs2_nx = mg.algos.traversal.bfs_iter(nx_weighted_directed_8_12, 2)
    bfs1_kg =  mg.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 0, 2**30 - 1)
    bfs2_kg = mg.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 2, 2**30 - 1)
    assert bfs1_kg.tolist() == bfs1_nx.tolist()
    assert bfs2_kg.tolist() == bfs2_nx.tolist()
    assert bfs1_kg.tolist() == [0, 1, 3, 4, 7]
    assert bfs2_kg.tolist() == [2, 4, 5, 6, 7]
    # additional tests in katana, feel free to comment this part
    node_schema = kg_from_nx_di_8_12.value.node_schema()
    assert node_schema[len(node_schema)-2].name == 'bfs_prop_start_from_0'
    assert node_schema[len(node_schema)-1].name == 'bfs_prop_start_from_2'
    bfs_assert_valid(kg_from_nx_di_8_12.value, 0, 'bfs_prop_start_from_0')
    bfs_assert_valid(kg_from_nx_di_8_12.value, 2, 'bfs_prop_start_from_2')
    verify_bfs(kg_from_nx_di_8_12.value, 0, len(node_schema)-2)
    verify_bfs(kg_from_nx_di_8_12.value, 2, len(node_schema)-1)
    stats1 = BfsStatistics(kg_from_nx_di_8_12.value, 'bfs_prop_start_from_0')
    stats2 = BfsStatistics(kg_from_nx_di_8_12.value, 'bfs_prop_start_from_2')
    assert stats1.n_reached_nodes == 5
    assert stats2.n_reached_nodes == 5



# single-source shortest path using bellman ford
# mg.algos.traversal.bellman_ford(graph: Graph(edge_type=’map’, edge_dtype={‘int’, ‘float’}), source_node: NodeID) → Tuple[NodeMap, NodeMap]
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx_sssp (2) nxgraph + nx_sssp
def test_sssp_bellman_ford(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    src_node = 0
    sssp_nx = mg.algos.traversal.bellman_ford(nx_weighted_directed_8_12, src_node) # source node is 0
    parents_nx = sssp_nx[0]
    distances_nx = sssp_nx[1]
    assert isinstance(parents_nx, dict)
    assert isinstance(distances_nx, dict)
    assert parents_nx == {0: 0, 1: 0, 3: 0, 4: 3, 7: 4}
    assert distances_nx == {0: 0, 1: 4, 3: 2, 4: 3, 7: 7}
    parents_kg, distances_kg = mg.algos.traversal.bellman_ford(kg_from_nx_di_8_12, src_node)
    assert parents_nx == parents_kg
    assert distances_nx == distances_kg


# single-source shortest path using dijkstra
# mg.algos.traversal.dijkstra(graph: Graph(edge_type=’map’, edge_dtype={‘int’, ‘float’}, edge_has_negative_weights=False), source_node: NodeID) → Tuple[NodeMap, NodeMap]
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx_sssp (2) nxgraph + nx_sssp
def test_sssp_dijkstra(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    src_node = 1
    sssp_nx = mg.algos.traversal.dijkstra(nx_weighted_directed_8_12, src_node) # source node is 1
    parents_nx = sssp_nx[0]
    distances_nx = sssp_nx[1]
    assert isinstance(parents_nx, dict)
    assert isinstance(distances_nx, dict)
    assert parents_nx == {1: 1, 3: 1, 4: 3, 7: 4}
    assert distances_nx == {1: 0, 3: 3, 4: 4, 7: 8}
    parents_kg, distances_kg = mg.algos.traversal.dijkstra(kg_from_nx_di_8_12, src_node)
    assert parents_nx == parents_kg
    assert distances_nx == distances_kg


# connected components
# mg.algos.clustering.connected_components(graph: Graph(is_directed=False)) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's connected_components (2) nxgraph + nx's connected_components
def test_connected_components(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    cc_nx = mg.algos.clustering.connected_components(nx_weighted_undirected_8_12)
    cc_kg = mg.algos.clustering.connected_components(kg_from_nx_ud_8_12)
    assert isinstance(cc_kg, dict)
    assert isinstance(cc_kg, dict)
    assert cc_kg == cc_nx


# PageRank
# mg.algos.centrality.pagerank(graph: Graph(edge_type=’map’, edge_dtype={‘int’, ‘float’}), damping: float = 0.85, maxiter: int = 50, tolerance: float = 1e-05) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's PageRank (2) nxgraph + nx's PageRank
def test_pagerank(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    pr_nx = mg.algos.centrality.pagerank(nx_weighted_directed_8_12)
    pr_kg = mg.algos.centrality.pagerank(kg_from_nx_di_8_12)
    assert isinstance(pr_nx, dict)
    assert isinstance(pr_kg, dict)
    assert pr_nx == pr_kg
    

# betweenness centrality
# mg.algos.centrality.betweenness(graph: Graph(edge_type=’map’, edge_dtype={‘int’, ‘float’}), nodes: Optional[NodeSet] = None, normalize: bool = False) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's betweenness centrality (2) nxgraph + nx's betweenness centrality
def test_betweenness_centrality(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    bc_nx = mg.algos.centrality.betweenness(nx_weighted_directed_8_12)
    bc_kg = mg.algos.centrality.betweenness(kg_from_nx_di_8_12)
    assert isinstance(bc_nx, dict)
    assert isinstance(bc_kg, dict)
    assert bc_nx == bc_kg

# triangle counting
# mg.algos.clustering.triangle_count(graph: Graph(is_directed=False)) → int
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's triangle counting (2) nxgraph + nx's triangle counting
def test_triangle_counting(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    tc_nx = mg.algos.clustering.triangle_count(nx_weighted_undirected_8_12)
    tc_kg = mg.algos.clustering.triangle_count(kg_from_nx_ud_8_12)
    assert isinstance(tc_nx, int)
    assert isinstance(tc_kg, int)
    assert tc_nx == tc_kg


# Louvain community detection
# mg.algos.clustering.louvain_community(graph: Graph(is_directed=False, edge_type=’map’, edge_dtype={‘int’, ‘float’})) → Tuple[NodeMap, float]
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's Louvain community detection (2) nxgraph + nx's Louvain community detection
def test_louvain_community_detection(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    lc_nx = mg.algos.clustering.louvain_community(nx_weighted_undirected_8_12)
    lc_kg = mg.algos.clustering.louvain_community(kg_from_nx_ud_8_12)
    assert isinstance(lc_nx[0], dict)
    assert isinstance(lc_kg[0], dict)
    assert isinstance(lc_nx[1], float)
    assert isinstance(lc_kg[1], float)
    assert lc_nx[0] == lc_kg[0]
    assert lc_nx[1] == lc_kg[1]


# subgraph extraction
# mg.algos.subgraph.extract_subgraph(graph: Graph, nodes: NodeSet) → Graph
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's subgraph extraction (2) nxgraph + nx's subgraph extraction
def test_subgraph_extraction(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    se_nx = mg.algos.subgraph.extract_subgraph(nx_weighted_directed_8_12, {0,2,3})
    se_kg = mg.algos.subgraph.extract_subgraph(kg_from_nx_di_8_12, {0,2,3})
    assert isinstance(se_nx, mg.wrappers.Graph.NetworkXGraph)
    assert isinstance(se_kg, mg.wrappers.Graph.NetworkXGraph)
    assert list(se_nx.value.edges(data=True)) == list(se_kg.value.edges(data=True))


# community detection using label propagation
# mg.algos.clustering.label_propagation_community(graph: Graph(is_directed=False)) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's label_propagation_community (2) nxgraph + nx's label_propagation_community
def test_labal_propagation(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    cd_nx = mg.algos.clustering.label_propagation_community(nx_weighted_undirected_8_12)
    cd_kg = mg.algos.clustering.label_propagation_community(kg_from_nx_ud_8_12)
    assert isinstance(cd_nx, dict)
    assert isinstance(cd_kg, dict)
    assert cd_nx == cd_kg

# Jaccard similarity
# mg.algos.traversal.jaccard(graph: Graph(is_directed=True, edge_type=’map’, edge_dtype={‘int’, ‘float’}, edge_has_negative_weights=False), compare_node: NodeID) -> Vector
# the results of these 2 approaches are compared:
# (1) katana graph + katana graph's Jaccard similarity (2) nxgraph->katana graph  + katana graph 's Jaccard similarity
def test_jaccard_similarity(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    compare_node = 0
    prop_name = 'jaccard_prop_with_'+str(compare_node)
    jcd_nx = mg.algos.traversal.jaccard(nx_weighted_undirected_8_12, compare_node)
    jcd_kg = mg.algos.traversal.jaccard(kg_from_nx_ud_8_12, compare_node)
    assert isinstance(jcd_nx, np.ndarray)
    assert isinstance(jcd_kg, np.ndarray)
    assert jcd_nx.tolist() == jcd_kg.tolist()
    assert jcd_kg[compare_node] == 1
    # below are the additional tests done by katana, feel free to comment them
    node_schema = kg_from_nx_ud_8_12.value.node_schema()
    node_schema.names[len(node_schema)-1] == prop_name
    assert node_schema[len(node_schema)-1].name == prop_name
    jaccard_assert_valid(kg_from_nx_ud_8_12.value, compare_node, prop_name)
    stats = JaccardStatistics(kg_from_nx_ud_8_12.value, compare_node, prop_name)
    assert stats.max_similarity == approx(0.5)
    assert stats.min_similarity == approx(0)


# local clustering coefficient
# mg.algos.clustering.local_clustering_coefficient(graph: Graph(is_directed=False, edge_type='map', edge_dtype={'int', 'float'}, edge_has_negative_weights=False), prop_name: str='output') -> Vector
# the results of these 2 approaches are compared:
# (1) katana graph + katana graph's local clustering coefficient (2) nxgraph->katana graph  + katana graph 's local clustering coefficient
def test_local_clustering_coefficient(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    prop_name = 'output_prop'
    lcc_nx = mg.algos.clustering.local_clustering_coefficient(nx_weighted_undirected_8_12, prop_name)
    lcc_kg = mg.algos.clustering.local_clustering_coefficient(kg_from_nx_ud_8_12, prop_name)
    assert isinstance(lcc_nx, np.ndarray)
    assert isinstance(lcc_kg, np.ndarray)
    assert lcc_kg.tolist() == lcc_nx.tolist()
    assert lcc_kg[-1] == 0
    assert not np.any(np.isnan(lcc_kg))
