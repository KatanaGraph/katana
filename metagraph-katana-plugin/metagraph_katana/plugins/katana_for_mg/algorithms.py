from .. import has_katana
from metagraph import concrete_algorithm
from metagraph import NodeID
from typing import Tuple
from metagraph.plugins.numpy.types import NumpyNodeMap, NumpyVectorType

if has_katana:
    import numpy as np
    from katana.property_graph import PropertyGraph
    from metagraph.plugins.networkx.types import NetworkXGraph
    from .types import KatanaGraph
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
    # breadth-first search, 
    @concrete_algorithm('traversal.bfs_iter')
    def kg_bfs_iter(graph: KatanaGraph, source_node: NodeID, depth_limit: int) -> NumpyVectorType:
        bfs_prop_name = 'bfs_prop_start_from_'+str(source_node)
        start_node = source_node
        bfs(graph.value, start_node, bfs_prop_name)
        pg_bfs_list = graph.value.get_node_property(bfs_prop_name).to_pandas().values.tolist()
        new_list = [[i, pg_bfs_list[i]] for i in range(len(pg_bfs_list)) if pg_bfs_list[i] < depth_limit]
        sorted_list = sorted(new_list, key=lambda each: (each[1], each[0]))
        bfs_arr = np.array([each[0] for each in sorted_list])
        return bfs_arr
    # single-source shortest path
    # connected components
    # PageRank
    # betweenness centrality
    # triangle counting
    # Louvain community detection
    # subgraph extraction
    # Jaccard similarity
    # community detection using label propagation
    # local clustering coefficient