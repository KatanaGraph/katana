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
    betweenness_centrality,
    BetweennessCentralityPlan,
    BetweennessCentralityStatistics,
)
from katana.analytics._bfs import bfs, bfs_assert_valid, BfsPlan, BfsStatistics
from katana.analytics._connected_components import (
    connected_components,
    connected_components_assert_valid,
    ConnectedComponentsPlan,
    ConnectedComponentsStatistics,
)
from katana.analytics._independent_set import (
    independent_set,
    independent_set_assert_valid,
    IndependentSetPlan,
    IndependentSetStatistics,
)
from katana.analytics._jaccard import jaccard, jaccard_assert_valid, JaccardPlan, JaccardStatistics
from katana.analytics._louvain_clustering import (
    louvain_clustering,
    louvain_clustering_assert_valid,
    LouvainClusteringPlan,
    LouvainClusteringStatistics,
)
from katana.analytics._local_clustering_coefficient import local_clustering_coefficient, LocalClusteringCoefficientPlan
from katana.analytics._subgraph_extraction import subgraph_extraction, SubGraphExtractionPlan
from katana.analytics._k_core import k_core, k_core_assert_valid, KCorePlan, KCoreStatistics
from katana.analytics._k_truss import k_truss, k_truss_assert_valid, KTrussPlan, KTrussStatistics
from katana.analytics._pagerank import pagerank, pagerank_assert_valid, PagerankPlan, PagerankStatistics
from katana.analytics._sssp import sssp, sssp_assert_valid, SsspPlan, SsspStatistics
from katana.analytics._triangle_count import triangle_count, TriangleCountPlan
from katana.analytics._wrappers import find_edge_sorted_by_dest, sort_all_edges_by_dest, sort_nodes_by_degree
from katana.analytics.plan import Architecture, Plan, Statistics
