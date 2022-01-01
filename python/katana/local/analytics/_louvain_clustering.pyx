"""
Louvain Clustering
------------------

.. autoclass:: katana.local.analytics.LouvainClusteringPlan


.. autoclass:: katana.local.analytics._louvain_clustering._LouvainClusteringPlanAlgorithm


.. autofunction:: katana.local.analytics.louvain_clustering

.. autoclass:: katana.local.analytics.LouvainClusteringStatistics


.. autofunction:: katana.local.analytics.louvain_clustering_assert_valid
"""
from libc.stdint cimport uint32_t, uint64_t
from libcpp cimport bool
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._graph cimport Graph, TxnContext
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/louvain_clustering/louvain_clustering.h" namespace "katana::analytics" nogil:
    cppclass _LouvainClusteringPlan "katana::analytics::LouvainClusteringPlan" (_Plan):
        enum Algorithm:
            kDoAll "katana::analytics::LouvainClusteringPlan::kDoAll"
            kDeterministic "katana::analytics::LouvainClusteringPlan::kDeterministic"

        _LouvainClusteringPlan.Algorithm algorithm() const
        bool enable_vf() const
        double modularity_threshold_per_round() const
        double modularity_threshold_total() const
        uint32_t max_iterations() const
        uint32_t min_graph_size() const

        # LouvainClusteringPlan()

        @staticmethod
        _LouvainClusteringPlan DoAll(
                bool enable_vf,
                double modularity_threshold_per_round,
                double modularity_threshold_total,
                uint32_t max_iterations,
                uint32_t min_graph_size
            )

        @staticmethod
        _LouvainClusteringPlan Deterministic(
            bool enable_vf,
            double modularity_threshold_per_round,
            double modularity_threshold_total,
            uint32_t max_iterations,
            uint32_t min_graph_size)

    bool kDefaultEnableVF "katana::analytics::LouvainClusteringPlan::kDefaultEnableVF"
    double kDefaultModularityThresholdPerRound "katana::analytics::LouvainClusteringPlan::kDefaultModularityThresholdPerRound"
    double kDefaultModularityThresholdTotal "katana::analytics::LouvainClusteringPlan::kDefaultModularityThresholdTotal"
    uint32_t kDefaultMaxIterations "katana::analytics::LouvainClusteringPlan::kDefaultMaxIterations"
    uint32_t kDefaultMinGraphSize "katana::analytics::LouvainClusteringPlan::kDefaultMinGraphSize"

    Result[void] LouvainClustering(_PropertyGraph* pfg, const string& edge_weight_property_name,const string& output_property_name, CTxnContext* txn_ctx, _LouvainClusteringPlan plan)

    Result[void] LouvainClusteringAssertValid(_PropertyGraph* pfg,
            const string& edge_weight_property_name,
            const string& output_property_name
            )

    cppclass _LouvainClusteringStatistics "katana::analytics::LouvainClusteringStatistics":
        uint64_t n_clusters
        uint64_t n_non_trivial_clusters
        uint64_t largest_cluster_size
        double largest_cluster_proportion
        double modularity

        void Print(ostream os)

        @staticmethod
        Result[_LouvainClusteringStatistics] Compute(_PropertyGraph* pfg,
            const string& edge_weight_property_name,
            const string& output_property_name,
            CTxnContext* txn_ctx
            )


class _LouvainClusteringPlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.LouvainClusteringPlan` constructors for algorithm documentation.
    """
    DoAll = _LouvainClusteringPlan.Algorithm.kDoAll
    Deterministic = _LouvainClusteringPlan.Algorithm.kDeterministic


cdef class LouvainClusteringPlan(Plan):
    cdef:
        _LouvainClusteringPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _LouvainClusteringPlanAlgorithm

    @staticmethod
    cdef LouvainClusteringPlan make(_LouvainClusteringPlan u):
        f = <LouvainClusteringPlan>LouvainClusteringPlan.__new__(LouvainClusteringPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> Algorithm:
        return _LouvainClusteringPlanAlgorithm(self.underlying_.algorithm())

    @property
    def enable_vf(self) -> bool:
        return self.underlying_.enable_vf()

    @property
    def modularity_threshold_per_round(self) -> double:
        return self.underlying_.modularity_threshold_per_round()

    @property
    def modularity_threshold_total(self) -> double:
        return self.underlying_.modularity_threshold_total()

    @property
    def max_iterations(self) -> uint32_t:
        return self.underlying_.max_iterations()

    @property
    def min_graph_size(self) -> uint32_t:
        return self.underlying_.min_graph_size()


    @staticmethod
    def do_all(
                bool enable_vf = kDefaultEnableVF,
                double modularity_threshold_per_round = kDefaultModularityThresholdPerRound,
                double modularity_threshold_total = kDefaultModularityThresholdTotal,
                uint32_t max_iterations = kDefaultMaxIterations,
                uint32_t min_graph_size = kDefaultMinGraphSize
            ) -> LouvainClusteringPlan:
        """
        Nondeterministic algorithm.
        """
        return LouvainClusteringPlan.make(_LouvainClusteringPlan.DoAll(
             enable_vf, modularity_threshold_per_round, modularity_threshold_total, max_iterations, min_graph_size))

    @staticmethod
    def deterministic(
            bool enable_vf = kDefaultEnableVF,
            double modularity_threshold_per_round = kDefaultModularityThresholdPerRound,
            double modularity_threshold_total = kDefaultModularityThresholdTotal,
            uint32_t max_iterations = kDefaultMaxIterations,
            uint32_t min_graph_size = kDefaultMinGraphSize
    ) -> LouvainClusteringPlan:
        """
         Deterministic algorithm using delayed updates
        """
        return LouvainClusteringPlan.make(_LouvainClusteringPlan.Deterministic(
            enable_vf, modularity_threshold_per_round, modularity_threshold_total, max_iterations, min_graph_size))

def louvain_clustering(Graph pg, str edge_weight_property_name, str output_property_name, LouvainClusteringPlan plan = LouvainClusteringPlan(), *, TxnContext txn_ctx = None):
    """
    Compute the Louvain Clustering for pg.
    The edge weights are taken from the property named
    edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
    int), and the computed cluster IDs are stored in the property named
    output_property_name (as uint32_t).
    The property named output_property_name is created by this function and may
    not exist before the call.

    :type pg: katana.local.Graph
    :param pg: The graph to analyze.
    :type edge_weight_property_name: str
    :param edge_weight_property_name: may be a 32- or 64-bit sign or unsigned int
    :type output_property_name: str
    :param output_property_name: The output edge property
    :type LouvainClusteringPlan: LouvainClusteringPlan
    :param LouvainClusteringPlan: The Louvain Clustering Plan
    :param txn_ctx: The tranaction context for passing read write sets.

    .. code-block:: python

        import katana.local
        from katana.example_data import get_input
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_input("propertygraphs/ldbc_003"))
        from katana.analytics import louvain_clustering, LouvainClusteringStatistics
        louvain_clustering(graph, "value", "output")
        stats = LouvainClusteringStatistics(graph, "value", "output")
        print(stats)

    """
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        handle_result_void(LouvainClustering(pg.underlying_property_graph(), edge_weight_property_name_str, output_property_name_str, &txn_ctx._txn_ctx, plan.underlying_))


def louvain_clustering_assert_valid(Graph pg, str edge_weight_property_name, str output_property_name ):
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_assert(LouvainClusteringAssertValid(pg.underlying_property_graph(),
                edge_weight_property_name_str,
                output_property_name_str
                ))


cdef _LouvainClusteringStatistics handle_result_LouvainClusteringStatistics(Result[_LouvainClusteringStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class LouvainClusteringStatistics:
    cdef _LouvainClusteringStatistics underlying

    def __init__(self, Graph pg,
            str edge_weight_property_name,
            str output_property_name,
            *, TxnContext txn_ctx = None
            ):
        cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
        cdef string output_property_name_str = bytes(output_property_name, "utf-8")
        txn_ctx = txn_ctx or TxnContext()
        with nogil:
            self.underlying = handle_result_LouvainClusteringStatistics(_LouvainClusteringStatistics.Compute(
                pg.underlying_property_graph(),
                edge_weight_property_name_str,
                output_property_name_str,
                &txn_ctx._txn_ctx
                ))

    @property
    def n_clusters(self) -> uint64_t:
        return self.underlying.n_clusters

    @property
    def n_non_trivial_clusters(self) -> uint64_t:
        return self.underlying.n_non_trivial_clusters

    @property
    def largest_cluster_size(self) -> uint64_t:
        return self.underlying.largest_cluster_size

    @property
    def largest_cluster_proportion(self) -> double:
        return self.underlying.largest_cluster_proportion

    @property
    def modularity(self) -> double:
        return self.underlying.modularity


    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
