from typing import Tuple

import numpy as np
from metagraph import NodeID, abstract_algorithm, concrete_algorithm
from metagraph.plugins.core.types import Graph, Vector
from metagraph.plugins.networkx.types import NetworkXGraph
from metagraph.plugins.numpy.types import NumpyNodeMap, NumpyVectorType

from katana.local.analytics import bfs, jaccard, local_clustering_coefficient

from .types import KatanaGraph


def has_node_prop(kg, node_prop_name):
    nschema = kg.loaded_node_schema()
    for i in range(len(nschema)):
        if nschema[i].name == node_prop_name:
            return True
    return False

# breadth-first search,
@concrete_algorithm("traversal.bfs_iter")
def kg_bfs_iter(graph: KatanaGraph, source_node: NodeID, depth_limit: int) -> NumpyVectorType:
    '''
    .. py:function:: metagraph.algos.traversal.bfs_iter(graph, source_node, depth_limit)

    Use BFS to traverse a graph given a source node and BFS depth limit (implemented by a Katana Graph API)

    :param KatanaGraph graph: The origianl graph to traverse
    :param NodeID source_node: The starting node for BFS
    :param int depth: The BFS depth
    :return: the BFS traversal result in order
    :rtype: NumpyVectorType
    '''
    bfs_prop_name = "bfs_prop_start_from_" + str(source_node)
    depth_limit_internal = 2**30 - 1 if depth_limit == -1 else depth_limit # return all the reachable nodes for the default value of depth_limit (-1)
    start_node = source_node
    if not has_node_prop(graph.value, bfs_prop_name):
        bfs(graph.value, start_node, bfs_prop_name)
    pg_bfs_list = graph.value.get_node_property(bfs_prop_name).to_pandas().values.tolist()
    new_list = [[i, pg_bfs_list[i]] for i in range(len(pg_bfs_list)) if pg_bfs_list[i] < depth_limit_internal]
    sorted_list = []

    for i in range(len(new_list)):
        if (new_list[i][0] == new_list[i][1]):
            sorted_list.append(new_list.pop(i)[0])
            break
    
    i = 0
    while len(new_list) > 0:
        idx_list = []
        for j in range(len(new_list)):
            if sorted_list[i] == new_list[j][1]:
                idx_list.append(j)
        sub_list = []
        for j in idx_list:
            sub_list.append(new_list.pop(j)[0])
        sorted_list += sorted(sub_list)
        i += 1

    bfs_arr = np.array([sorted_list])
    return bfs_arr

# TODO(pengfei):
# single-source shortest path
# connected components
# PageRank
# betweenness centrality
# triangle counting
# Louvain community detection
# subgraph extraction
# community detection using label propagation\


@abstract_algorithm("traversal.jaccard")
def jaccard_similarity(
    graph: Graph(is_directed=False, edge_type="map", edge_dtype={"int", "float"}, edge_has_negative_weights=False),
    compare_node: NodeID,
) -> Vector:
    pass


@concrete_algorithm("traversal.jaccard")
def jaccard_similarity_kg(graph: KatanaGraph, compare_node: NodeID) -> NumpyVectorType:
    jaccard_prop_name = "jaccard_prop_with_" + str(compare_node)
    if not has_node_prop(graph.value, jaccard_prop_name):
        jaccard(graph.value, compare_node, jaccard_prop_name)
    jaccard_similarities = graph.value.get_node_property(jaccard_prop_name).to_numpy()
    return jaccard_similarities


@abstract_algorithm("clustering.local_clustering_coefficient")
def local_clustering(
    graph: Graph(is_directed=False, edge_type="map", edge_dtype={"int", "float"}, edge_has_negative_weights=False),
    prop_name: str = "output",
) -> Vector:
    pass


@concrete_algorithm("clustering.local_clustering_coefficient")
def local_clustering_kg(graph: KatanaGraph, prop_name: str) -> NumpyVectorType:
    if not has_node_prop(graph.value, prop_name):
        local_clustering_coefficient(graph.value, prop_name)
    out = graph.value.get_node_property(prop_name)
    return out.to_pandas().values
