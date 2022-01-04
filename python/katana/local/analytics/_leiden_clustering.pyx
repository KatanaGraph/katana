"""
Leiden Clustering
------------------

.. autoclass:: katana.local.analytics.LeidenClusteringPlan


.. autoclass:: katana.local.analytics._leiden_clustering._LeidenClusteringPlanAlgorithm


.. autofunction:: katana.local.analytics.leiden_clustering

.. autoclass:: katana.local.analytics.LeidenClusteringStatistics


.. autofunction:: katana.local.analytics.leiden_clustering_assert_valid
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

# TODO(amp): Module needs documenting.


cdef extern from "katana/analytics/leiden_clustering/leiden_clustering.h" namespace "katana::analytics" nogil:
    cppclass _LeidenClusteringPlan "katana::analytics::LeidenClusteringPlan" (_Plan):
        enum Algorithm:
            kDoAll "katana::analytics::LeidenClusteringPlan::kDoAll"
            kDeterministic "katana::analytics::LeidenClusteringPlan::kDeterministic"

        _LeidenClusteringPlan.Algorithm algorithm() const
        bool enable_vf() const
        double modularity_threshold_per_round() const
        double modularity_threshold_total() const
        uint32_t max_iterations() const
        uint32_t min_graph_size() const
        double resolution() const
        double randomness() const
        # LeidenClusteringPlan()

        @staticmethod
        _LeidenClusteringPlan DoAll(
                bool enable_vf,
                double modularity_threshold_per_round,
                double modularity_threshold_total,
                uint32_t max_iterations,
                uint32_t min_graph_size,
                double resolution,
                double randomness
            )

        @staticmethod
        _LeidenClusteringPlan Deterministic(
                bool enable_vf,
                double modularity_threshold_per_round,
                double modularity_threshold_total,
                uint32_t max_iterations,
                uint32_t min_graph_size,
                double resolution,
                double randomness
            )


    bool kDefaultEnableVF "katana::analytics::LeidenClusteringPlan::kDefaultEnableVF"
    double kDefaultModularityThresholdPerRound "katana::analytics::LeidenClusteringPlan::kDefaultModularityThresholdPerRound"
    double kDefaultModularityThresholdTotal "katana::analytics::LeidenClusteringPlan::kDefaultModularityThresholdTotal"
    double kDefaultResolution "katana::analytics::LeidenClusteringPlan::kDefaultResolution"
    double kDefaultRandomness "katana::analytics::LeidenClusteringPlan::kDefaultModularityThresholdTotal"
    uint32_t kDefaultMaxIterations "katana::analytics::LeidenClusteringPlan::kDefaultMaxIterations"
    uint32_t kDefaultMinGraphSize "katana::analytics::LeidenClusteringPlan::kDefaultMinGraphSize"

    Result[void] LeidenClustering(_PropertyGraph* pfg, const string& edge_weight_property_name,const string& output_property_name, CTxnContext* txn_ctx, _LeidenClusteringPlan plan)

    Result[void] LeidenClusteringAssertValid(_PropertyGraph* pfg,
            const string& edge_weight_property_name,
            const string& output_property_name
            )

    cppclass _LeidenClusteringStatistics "katana::analytics::LeidenClusteringStatistics":
        uint64_t n_clusters
        uint64_t n_non_trivial_clusters
        uint64_t largest_cluster_size
        double largest_cluster_proportion
        double modularity

        void Print(ostream os)

        @staticmethod
        Result[_LeidenClusteringStatistics] Compute(_PropertyGraph* pfg,
            const string& edge_weight_property_name,
            const string& output_property_name,
            CTxnContext* txn_ctx
            )


class _LeidenClusteringPlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.LeidenClusteringPlan` constructors for algorithm documentation.
    """
    DoAll = _LeidenClusteringPlan.Algorithm.kDoAll
    Deterministic = _LeidenClusteringPlan.Algorithm.kDeterministic


cdef class LeidenClusteringPlan(Plan):
    cdef:
        _LeidenClusteringPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _LeidenClusteringPlanAlgorithm

    @staticmethod
    cdef LeidenClusteringPlan make(_LeidenClusteringPlan u):
        f = <LeidenClusteringPlan>LeidenClusteringPlan.__new__(LeidenClusteringPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> Algorithm:
        return _LeidenClusteringPlanAlgorithm(self.underlying_.algorithm())

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
                uint32_t min_graph_size = kDefaultMinGraphSize,
                double resolution = kDefaultResolution,
                double randomness = kDefaultRandomness,
    ) -> LeidenClusteringPlan:
        """
        Nondeterministic algorithm.
        """
        return LeidenClusteringPlan.make(_LeidenClusteringPlan.DoAll(
            enable_vf, modularity_threshold_per_round, modularity_threshold_total, max_iterations,
            min_graph_size, resolution, randomness))

    @staticmethod
    def deterministic(
            bool enable_vf = kDefaultEnableVF,
            double modularity_threshold_per_round = kDefaultModularityThresholdPerRound,
            double modularity_threshold_total = kDefaultModularityThresholdTotal,
            uint32_t max_iterations = kDefaultMaxIterations,
            uint32_t min_graph_size = kDefaultMinGraphSize,
            double resolution = kDefaultResolution,
            double randomness = kDefaultRandomness,
    ) -> LeidenClusteringPlan:
        """
         Deterministic algorithm using delayed updates
        """
        return LeidenClusteringPlan.make(_LeidenClusteringPlan.Deterministic(
            enable_vf, modularity_threshold_per_round, modularity_threshold_total, max_iterations,
            min_graph_size, resolution, randomness))


def leiden_clustering(Graph pg, str edge_weight_property_name, str output_property_name, LeidenClusteringPlan plan = LeidenClusteringPlan(), *, TxnContext txn_ctx = None):
    """
    Compute the Leiden Clustering for pg.
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
    :type LeidenClusteringPlan: LeidenClusteringPlan
    :param LeidenClusteringPlan: The Leiden Clustering Plan
    :param txn_ctx: The tranaction context for passing read write sets.

    .. code-block:: python

        import katana.local
        from katana.example_data import get_input
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_input("propertygraphs/ldbc_003"))
        from katana.analytics import leiden_clustering, LeidenClusteringStatistics
        leiden_clustering(graph, "value", "output")
        stats = LeidenClusteringStatistics(graph, "value", "output")
        print(stats)

    """
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        handle_result_void(LeidenClustering(pg.underlying_property_graph(), edge_weight_property_name_str, output_property_name_str, &txn_ctx._txn_ctx, plan.underlying_))


def leiden_clustering_assert_valid(Graph pg, str edge_weight_property_name, str output_property_name ):
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_assert(LeidenClusteringAssertValid(pg.underlying_property_graph(),
                edge_weight_property_name_str,
                output_property_name_str
                ))


cdef _LeidenClusteringStatistics handle_result_LeidenClusteringStatistics(Result[_LeidenClusteringStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class LeidenClusteringStatistics:
    cdef _LeidenClusteringStatistics underlying

    def __init__(self, Graph pg,
            str edge_weight_property_name,
            str output_property_name,
            *, TxnContext txn_ctx = None
            ):
        cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
        cdef string output_property_name_str = bytes(output_property_name, "utf-8")
        txn_ctx = txn_ctx or TxnContext()
        with nogil:
            self.underlying = handle_result_LeidenClusteringStatistics(_LeidenClusteringStatistics.Compute(
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
