"""
Analytics algorithms for Katana graphs

Each abstract algorithm has a :ref:`Plan` class, the routine itself, and optionally, a results
checker function and a :ref:`Statistics` class.

.. _Plan:

Plans
-----

.. autoclass:: katana.analytics.Plan

.. _Statistics:

Statistics
----------

.. autoclass:: katana.analytics.Statistics
    :members: __init__

Algorithms
----------

.. automodule:: katana.analytics._betweenness_centrality

.. automodule:: katana.analytics._bfs

.. automodule:: katana.analytics._connected_components

.. automodule:: katana.analytics._independent_set

.. automodule:: katana.analytics._louvain_clustering

.. automodule:: katana.analytics._local_clustering_coefficient

.. automodule:: katana.analytics._subgraph_extraction

.. automodule:: katana.analytics._jaccard

.. automodule:: katana.analytics._k_core

.. automodule:: katana.analytics._k_truss

.. automodule:: katana.analytics._pagerank

.. automodule:: katana.analytics._sssp

.. automodule:: katana.analytics._triangle_count

.. automodule:: katana.analytics._wrappers
    :members:

"""


from katana.analytics._betweenness_centrality import (
    BetweennessCentralityPlan,
    BetweennessCentralityStatistics,
    betweenness_centrality,
)
from katana.analytics._bfs import BfsPlan, BfsStatistics, bfs, bfs_assert_valid
from katana.analytics._connected_components import (
    ConnectedComponentsPlan,
    ConnectedComponentsStatistics,
    connected_components,
    connected_components_assert_valid,
)
from katana.analytics._independent_set import (
    IndependentSetPlan,
    IndependentSetStatistics,
    independent_set,
    independent_set_assert_valid,
)
from katana.analytics._jaccard import JaccardPlan, JaccardStatistics, jaccard, jaccard_assert_valid
from katana.analytics._k_core import KCorePlan, KCoreStatistics, k_core, k_core_assert_valid
from katana.analytics._k_truss import KTrussPlan, KTrussStatistics, k_truss, k_truss_assert_valid
from katana.analytics._local_clustering_coefficient import LocalClusteringCoefficientPlan, local_clustering_coefficient
from katana.analytics._louvain_clustering import (
    LouvainClusteringPlan,
    LouvainClusteringStatistics,
    louvain_clustering,
    louvain_clustering_assert_valid,
)
from katana.analytics._pagerank import PagerankPlan, PagerankStatistics, pagerank, pagerank_assert_valid
from katana.analytics._sssp import SsspPlan, SsspStatistics, sssp, sssp_assert_valid
from katana.analytics._subgraph_extraction import SubGraphExtractionPlan, subgraph_extraction
from katana.analytics._triangle_count import TriangleCountPlan, triangle_count
from katana.analytics._wrappers import find_edge_sorted_by_dest, sort_all_edges_by_dest, sort_nodes_by_degree
from katana.analytics.plan import Architecture, Plan, Statistics
