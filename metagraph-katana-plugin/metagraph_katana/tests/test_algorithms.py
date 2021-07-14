import pytest
import sys
import os
import pandas as pd
import numpy as np

# from executing.executing import NodeFinder
import katana.local
from katana.example_utils import get_input
from katana.galois import set_active_threads
from katana.property_graph import PropertyGraph
# from pathlib import Path
from icecream import ic
import numpy as np
import pandas as pd
# import pyarrow as pa
import csv
from scipy.sparse import csr_matrix
# from pyarrow import Schema, table

import metagraph as mg
from metagraph import translator
from metagraph.plugins import has_networkx
from metagraph.plugins.python.types import dtype_casting
from metagraph.plugins.networkx.types import NetworkXGraph

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
    # ic(mg.plan.algos.traversal.bfs_iter(nx_weighted_directed_8_12, 0)) # No translation is needed because we already have a concrete implementation which takes a NetworkXGraph as input.
    # ic(mg.plan.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 0)) # No translation is needed because we already have a concrete implementation which takes a KatanaGraph as input.
    bfs1_nx = mg.algos.traversal.bfs_iter(nx_weighted_directed_8_12, 0)
    # ic(bfs1_nx)
    # ic(type(bfs1_nx))
    bfs2_nx = mg.algos.traversal.bfs_iter(nx_weighted_directed_8_12, 2)
    # ic(bfs2_nx)
    # ic(type(bfs2))
    bfs1_kg =  mg.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 0, 2**30 - 1)
    bfs2_kg = mg.algos.traversal.bfs_iter(kg_from_nx_di_8_12, 2, 2**30 - 1)
    # ic(bfs1_kg)
    # ic(bfs2_kg)
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
    # ic (mg.plan.algos.traversal.bellman_ford(nx_weighted_directed_8_12, src_node)) # no translation
    # ic (mg.plan.algos.traversal.bellman_ford(kg_from_nx_di_8_12, src_node)) # translation required
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
    # ic (parents_nx)
    # ic (distances_nx)
    # ic (mg.plan.algos.traversal.dijkstra(nx_weighted_directed_8_12, src_node)) # no translation
    # ic (mg.plan.algos.traversal.dijkstra(kg_from_nx_di_8_12, src_node)) # translation required
    parents_kg, distances_kg = mg.algos.traversal.dijkstra(kg_from_nx_di_8_12, src_node)
    assert parents_nx == parents_kg
    assert distances_nx == distances_kg


# connected components
# mg.algos.clustering.connected_components(graph: Graph(is_directed=False)) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's connected_components (2) nxgraph + nx's connected_components
def test_cc(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    cc_nx = mg.algos.clustering.connected_components(nx_weighted_undirected_8_12)
    cc_kg = mg.algos.clustering.connected_components(kg_from_nx_ud_8_12)
    assert isinstance(cc_kg, dict)
    assert isinstance(cc_kg, dict)
    assert cc_kg == cc_nx
    # ic (mg.plan.algos.clustering.connected_components(nx_weighted_undirected_8_12))
    # ic (mg.plan.algos.clustering.connected_components(kg_from_nx_ud_8_12))


# PageRank
# mg.algos.centrality.pagerank(graph: Graph(edge_type=’map’, edge_dtype={‘int’, ‘float’}), damping: float = 0.85, maxiter: int = 50, tolerance: float = 1e-05) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's PageRank (2) nxgraph + nx's PageRank
def test_pr(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    pr_nx = mg.algos.centrality.pagerank(nx_weighted_directed_8_12)
    pr_kg = mg.algos.centrality.pagerank(kg_from_nx_di_8_12)
    assert isinstance(pr_nx, dict)
    assert isinstance(pr_kg, dict)
    assert pr_nx == pr_kg
    # ic (mg.plan.algos.centrality.pagerank(nx_weighted_directed_8_12))
    # ic (mg.plan.algos.centrality.pagerank(kg_from_nx_di_8_12))
    

# betweenness centrality
# mg.algos.centrality.betweenness(graph: Graph(edge_type=’map’, edge_dtype={‘int’, ‘float’}), nodes: Optional[NodeSet] = None, normalize: bool = False) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's betweenness centrality (2) nxgraph + nx's betweenness centrality
def test_bc(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    bc_nx = mg.algos.centrality.betweenness(nx_weighted_directed_8_12)
    bc_kg = mg.algos.centrality.betweenness(kg_from_nx_di_8_12)
    assert isinstance(bc_nx, dict)
    assert isinstance(bc_kg, dict)
    assert bc_nx == bc_kg

# triangle counting
# mg.algos.clustering.triangle_count(graph: Graph(is_directed=False)) → int
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's triangle counting (2) nxgraph + nx's triangle counting
def test_tc(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    tc_nx = mg.algos.clustering.triangle_count(nx_weighted_undirected_8_12)
    tc_kg = mg.algos.clustering.triangle_count(kg_from_nx_ud_8_12)
    assert isinstance(tc_nx, int)
    assert isinstance(tc_kg, int)
    assert tc_nx == tc_kg


# Louvain community detection
# mg.algos.clustering.louvain_community(graph: Graph(is_directed=False, edge_type=’map’, edge_dtype={‘int’, ‘float’})) → Tuple[NodeMap, float]
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's Louvain community detection (2) nxgraph + nx's Louvain community detection
def test_lc(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
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
def test_se(nx_weighted_directed_8_12, kg_from_nx_di_8_12):
    se_nx = mg.algos.subgraph.extract_subgraph(nx_weighted_directed_8_12, {0,2,3})
    se_kg = mg.algos.subgraph.extract_subgraph(kg_from_nx_di_8_12, {0,2,3})
    assert isinstance(se_nx, mg.wrappers.Graph.NetworkXGraph)
    assert isinstance(se_kg, mg.wrappers.Graph.NetworkXGraph)
    assert list(se_nx.value.edges(data=True)) == list(se_kg.value.edges(data=True))


# community detection using label propagation
# mg.algos.clustering.label_propagation_community(graph: Graph(is_directed=False)) → NodeMap
# the results of these 2 approaches are compared:
# (1) katanagraph->nxgraph + nx's label_propagation_community (2) nxgraph + nx's label_propagation_community
def test_cd(nx_weighted_undirected_8_12, kg_from_nx_ud_8_12):
    cd_nx = mg.algos.clustering.label_propagation_community(nx_weighted_undirected_8_12)
    cd_kg = mg.algos.clustering.label_propagation_community(kg_from_nx_ud_8_12)
    assert isinstance(cd_nx, dict)
    assert isinstance(cd_kg, dict)
    assert cd_nx == cd_kg

# local clustering coefficient


# Jaccard similarity