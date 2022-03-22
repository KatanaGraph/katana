"""
K Shortest paths
----------------
.. autoclass:: katana.local.analytics.KssspPlan
.. autoclass:: katana.local.analytics._ksssp._KssspAlgorithm
.. autoclass:: katana.local.analytics._ksssp._KssspReachability
.. autofunction:: katana.local.analytics.ksssp
"""
from enum import Enum

from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint32_t
from libcpp cimport bool
from libcpp.string cimport string

from pyarrow.includes.common cimport *
from pyarrow.lib cimport CTable, Table, pyarrow_wrap_table, pyarrow_unwrap_table

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, raise_error_code

from katana.local import Graph, TxnContext

from katana.local._graph cimport underlying_property_graph, underlying_txn_context
from katana.local.analytics.plan cimport Plan, Statistics, _Plan


cdef extern from "katana/analytics/k_shortest_paths/ksssp.h" namespace "katana::analytics" nogil:
    cppclass _KssspPlan "katana::analytics::KssspPlan" (_Plan):
        enum Algorithm:
            kDeltaTile "katana::analytics::KssspPlan::kDeltaTile"
            kDeltaStep "katana::analytics::KssspPlan::kDeltaStep"
            kDeltaStepBarrier "katana::analytics::KssspPlan::kDeltaStepBarrier"

        enum Reachability:
            asyncLevel "katana::analytics::KssspPlan::asyncLevel"
            syncLevel "katana::analytics::KssspPlan::syncLevel"

        _KssspPlan()
        _KssspPlan(const _PropertyGraph * pg)

        _KssspPlan.Algorithm algorithm() const
        _KssspPlan.Reachability reachability() const
        unsigned delta() const
        ptrdiff_t edge_tile_size() const

        @staticmethod
        _KssspPlan DeltaTile(_KssspPlan.Reachability reachability, unsigned delta, ptrdiff_t edge_tile_size)
        @staticmethod
        _KssspPlan DeltaStep(_KssspPlan.Reachability reachability, unsigned delta)
        @staticmethod
        _KssspPlan DeltaStepBarrier(_KssspPlan.Reachability reachability, unsigned delta)

    _KssspPlan.Reachability kDefaultReach "katana::analytics::KssspPlan::kDefaultReach"
    unsigned kDefaultDelta "katana::analytics::KssspPlan::kDefaultDelta"
    ptrdiff_t kDefaultEdgeTileSize "katana::analytics::KssspPlan::kDefaultEdgeTileSize"

    Result[shared_ptr[CTable]] Ksssp(_PropertyGraph* pg, 
                                            const string& edge_weight_property_name,
                                            size_t start_node, size_t report_node, 
                                            size_t num_paths, const bool& is_symmetric, 
                                            CTxnContext* txn_ctx, _KssspPlan plan)

    cppclass _KssspStatistics "katana::analytics::KssspStatistics":
        void Print(ostream os)

        @staticmethod
        Result[_KssspStatistics] Compute(_PropertyGraph* pg, 
                                         const string& edge_property_name, 
                                         shared_ptr[CTable] table, 
                                         size_t report_node, 
                                         const bool& is_symmetric, 
                                         CTxnContext* txn_ctx)


class _KssspAlgorithm(Enum):
    """
    The concrete algorithms available for Ksssp.
    :see: :py:class`~katana.local.analytics.KssspPlan` constructor for algorithm documentation
    """
    DeltaTile = _KssspPlan.Algorithm.kDeltaTile
    DeltaStep = _KssspPlan.Algorithm.kDeltaStep
    DeltaStepBarrier = _KssspPlan.Algorithm.kDeltaStepBarrier

class _KssspReachability(Enum):
    """
    The algorithms to check reachability for kSSSP
    :see: :py:class: `~katana.local.analytics.AlgoReachability` constructors for algorithm documentation.
    """
    AsyncLevel = _KssspPlan.Reachability.asyncLevel
    SyncLevel = _KssspPlan.Reachability.syncLevel

cdef class KssspPlan(Plan):
    """
    A computational :ref:`Plan` for K-Shortest Paths
    Static method construct KssspPlans using specific algorithms with their required parameters. All parameters are
    optional and have reasonable defaults
    """

    cdef:
        _KssspPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    @staticmethod
    cdef KssspPlan make(_KssspPlan u):
        f = <KssspPlan>KssspPlan.__new__(KssspPlan)
        f.underlying_ = u
        return f

    def __init__(self, graph: Graph = None):
        """
        Construct a plan optimized for `graph` using heuristics, or using default parameter values.
        """
        if graph is None:
            self.underlying_ = _KssspPlan()
        else:
            self.underlying_ = _KssspPlan(underlying_property_graph(graph))

    Algorithm = _KssspAlgorithm
    Reachability = _KssspReachability

    @property
    def algorithm(self) -> _KssspAlgorithm:
        """
        The selected algorithm.
        """
        return _KssspAlgorithm(self.underlying_.algorithm())

    @property
    def reachability(self) -> _KssspReachability:
        """
        The selected reachability method
        """
        return _KssspReachability(self.underlying_.reachability())

    @property
    def delta(self) -> int:
        """
        The exponent of the delta step size (2 based). A delta of 4 will produce a real delta step size of 16
        """
        return self.underlying_.delta()
    @property
    def edge_tile_size(self) -> int:
        """
        The edge tile size.
        """
        return self.underlying_.edge_tile_size()

    def __init__(self):
        """
        Choose an algorithm using heuristics
        """
        super(KssspPlan, self).__init__()

    @staticmethod
    def delta_tile(_KssspPlan.Reachability reachability = kDefaultReach, unsigned delta = kDefaultDelta,
                   ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> KssspPlan:
        """
        Delta stepping tiled
        """
        return KssspPlan.make(_KssspPlan.DeltaTile(reachability, delta, edge_tile_size))

    @staticmethod
    def delta_step(_KssspPlan.Reachability reachability = kDefaultReach, unsigned delta = kDefaultDelta) -> KssspPlan:
        """
        Delta stepping (non-tiled)
        """
        return KssspPlan.make(_KssspPlan.DeltaStep(reachability, delta))

    @staticmethod
    def delta_step_barrier(_KssspPlan.Reachability reachability = kDefaultReach, unsigned delta = kDefaultDelta) -> KssspPlan:
        """
        Delta stepping with barrier
        """
        return KssspPlan.make(_KssspPlan.DeltaStepBarrier(reachability, delta))


cdef Table handle_result_ksssp(Result[shared_ptr[CTable]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
        return pyarrow_wrap_table(res.value())


def ksssp(pg, str edge_weight_property_name, size_t start_node,
          size_t report_node, size_t num_paths, bool is_symmetric=False,
          KssspPlan plan = KssspPlan(), *, txn_ctx = None) -> Table:
    """
    Compute the K-Shortest Path on `pg` using `start_node` as source.

    :type pg: katana.local.Graph
    :param pg: The graph to analyze
    :type edge_weight_property_name: str
    :param edge_weight_property_name: The input property containing edge weights.
    :type start_node: Node ID
    :param start_node: The source node
    :type report_node: Node ID
    :param report_node: The destination node
    :type num_paths: int
    :param num_paths: Number of paths to look for
    :type is_symmetric: bool
    :param is_symmetric: Whether or not the graph is symmetric. Defaults to false.
    :type plan: KssspPlan
    :param plan: The execution plan to use. Defaults to heuristically selecting the plan.
    :param txn_ctx: The transaction context for passing read write sets

    .. code-block:: python

        import katana.local
        from katana.example_data import get_rdg_dataset
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_rdg_dataset("ldbc_003"))
        from katana.local.analytics import ksssp
        weight_name = "workFrom"
        start_node = 0
        report_node = 10
        num_paths = 5
        ksssp(graph, weight_name, start_node, report_node, num_paths)

    """

    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        return handle_result_ksssp(Ksssp(underlying_property_graph(pg), edge_weight_property_name_str,
                    start_node, report_node, num_paths, is_symmetric,
                    underlying_txn_context(txn_ctx), plan.underlying_))


cdef _KssspStatistics handle_result_KssspStatistics(Result[_KssspStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class KssspStatistics(Statistics):
    """
    Compute the :ref:`statistics` of a kSSSP computation on a graph
    """
    cdef _KssspStatistics underlying

    def __init__(self, pg, str edge_property_name, Table table, 
                 size_t report_node, bool is_symmetric=False, txn_ctx = None):
        cdef string edge_weight_property_name_str = bytes(edge_property_name, "utf-8")
        cdef shared_ptr[CTable] table_ptr = pyarrow_unwrap_table(table)
        txn_ctx = txn_ctx or TxnContext()
        with nogil:
            self.underlying = handle_result_KssspStatistics(_KssspStatistics.Compute(
                underlying_property_graph(pg), edge_weight_property_name_str, table_ptr, 
                report_node, is_symmetric, underlying_txn_context(txn_ctx)))

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
