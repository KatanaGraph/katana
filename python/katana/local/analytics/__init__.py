"""
Analytics algorithms for Katana graphs

Each abstract algorithm has a :ref:`Plan` class, the routine itself, and optionally, a results
checker function and a :ref:`Statistics` class.

.. _Plan:

Plans
-----

.. autoclass:: katana.local.analytics.Plan

.. _Statistics:

Statistics
----------

.. autoclass:: katana.local.analytics.Statistics
    :members: __init__

Algorithms
----------

.. automodule:: katana.local.analytics._betweenness_centrality

.. automodule:: katana.local.analytics._bfs

.. automodule:: katana.local.analytics._cdlp

.. automodule:: katana.local.analytics._connected_components

.. automodule:: katana.local.analytics._independent_set

.. automodule:: katana.local.analytics._louvain_clustering

.. automodule:: katana.local.analytics._local_clustering_coefficient

.. automodule:: katana.local.analytics._subgraph_extraction

.. automodule:: katana.local.analytics._jaccard

.. automodule:: katana.local.analytics._k_core

.. automodule:: katana.local.analytics._k_truss

.. automodule:: katana.local.analytics._pagerank

.. automodule:: katana.local.analytics._sssp

.. automodule:: katana.local.analytics._triangle_count

.. automodule:: katana.local.analytics._wrappers
    :members:

"""


from katana.local.analytics._betweenness_centrality import (
    BetweennessCentralityPlan,
    BetweennessCentralityStatistics,
    betweenness_centrality,
)
from katana.local.analytics._bfs import BfsPlan, BfsStatistics, bfs, bfs_assert_valid
from katana.local.analytics._cdlp import CdlpPlan, CdlpStatistics, cdlp
from katana.local.analytics._connected_components import (
    ConnectedComponentsPlan,
    ConnectedComponentsStatistics,
    connected_components,
    connected_components_assert_valid,
)
from katana.local.analytics._independent_set import (
    IndependentSetPlan,
    IndependentSetStatistics,
    independent_set,
    independent_set_assert_valid,
)
from katana.local.analytics._jaccard import JaccardPlan, JaccardStatistics, jaccard, jaccard_assert_valid
from katana.local.analytics._k_core import KCorePlan, KCoreStatistics, k_core, k_core_assert_valid
from katana.local.analytics._k_truss import KTrussPlan, KTrussStatistics, k_truss, k_truss_assert_valid
from katana.local.analytics._leiden_clustering import (
    LeidenClusteringPlan,
    LeidenClusteringStatistics,
    leiden_clustering,
    leiden_clustering_assert_valid,
)
from katana.local.analytics._local_clustering_coefficient import (
    LocalClusteringCoefficientPlan,
    local_clustering_coefficient,
)
from katana.local.analytics._louvain_clustering import (
    LouvainClusteringPlan,
    LouvainClusteringStatistics,
    louvain_clustering,
    louvain_clustering_assert_valid,
)
from katana.local.analytics._pagerank import PagerankPlan, PagerankStatistics, pagerank, pagerank_assert_valid
from katana.local.analytics._sssp import SsspPlan, SsspStatistics, sssp, sssp_assert_valid
from katana.local.analytics._subgraph_extraction import SubGraphExtractionPlan, subgraph_extraction
from katana.local.analytics._triangle_count import TriangleCountPlan, triangle_count
from katana.local.analytics._wrappers import find_edge_sorted_by_dest, sort_all_edges_by_dest, sort_nodes_by_degree
from katana.local.analytics.plan import Architecture, Plan, Statistics
