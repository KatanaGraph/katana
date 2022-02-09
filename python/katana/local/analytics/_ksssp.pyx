"""
K Shortest paths
----------------

.. autoclass:: katana.local.analytics.KssspPlan


.. autoclass:: katana.local.analytics.KssspReachability


.. autoclass:: katana.local.analytics._ksssp._KssspAlgorithm


.. autofunction:: katana.local.analytics.ksssp
"""
from enum import Enum

from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint32_t
from libcpp cimport bool
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result

from katana.local import Graph

from katana.local._graph cimport underlying_property_graph
from katana.local.analytics.plan cimport Plan, _Plan

cdef extern from "katana/analytics/sssp/sssp.h" namespace "katana::analytics" nogil:
    cppclass _KssspPlan "katana::analytics:SsspPlan" (_Plan):
        enum Algorithm:
            kDeltaTile "katana::analytics::SsspPlan::kDeltaTile"
            kDeltaStep "katana::analytics::SsspPlan::kDeltaStep"
            kDeltaStepBarrier "katana::analytics::SsspPlan::kDeltaStepBarrier"

        _KssspPlan()
        _KssspPlan(const _PropertyGraph * pg)

        _KssspPlan.Algorithm algorithm() const
        unsigned delta() const
        ptrdiff_t edge_tile_size() const

        @staticmethod
        _KssspPlan DeltaTile(unsigned delta, ptrdiff_t edge_tile_size)
        @staticmethod
        _KssspPlan DeltaStep(unsigned delta)
        @staticmethod
        _KssspPlan DeltaStepBarrier(unsigned delta)
    
    unsigned kDefaultDelta "katana::analytics::SsspPlan::kDefaultDelta"
    ptrdiff_t kDefaultEdgeTileSize "katana::analytics::SsspPlan::kDefaultEdgeTileSize"

cdef extern from "katana/analytics/k_shortest_paths/ksssp.h" namespace "katana::analytics" nogil:
    cppclass _AlgoReachability "katana::analytics::AlgoReachability":
        enum Algorithm:
            asyncLevel "katana::analytics::AlgoReachability::asyncLevel"
            syncLevel "katana::analytics::AlgoReachability::syncLevel"

        _AlgoReachability()

        _AlgoReachability.Algorithm algorithm() const

        @staticmethod
        _AlgoReachability AsyncLevel()
        @staticmethod
        _AlgoReachability SyncLevel()

    Result[void] Ksssp(_PropertyGraph* pg, const string& edge_weight_property_name, 
                       uint32_t start_node, uint32_t report_node, CTxnContext* txn_ctx, 
                       AlgoReachability algo_reachability, uint32_t num_paths, uint32_t step_shift, 
                       const bool& is_symmetric, _KssspPlan plan)

class _KssspAlgorithm(Enum):
    """
    The concrete algorithms available for Ksssp. 

    :see: :py:class`~katana.local.analytics.KssspPlan` constructor for algorithm documentation
    """
    DeltaTile = _KssspPlan.Algorithm.kDeltaTile
    DeltaStep = _KssspPlan.Algorithm.kDeltaStep
    DeltaStepBarrier = _KssspPlan.Algorithm.kDeltaStepBarrier

cdef class KssspPlan(Plan):
    """
    A computational :ref:`Plan` for K-Shortest Paths

    Static method construct KssspPlans using specific algorithms with their required parameters. All parameters are
    opetional and have resonable defaults
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

    @property
    def algorithm(self) -> _KssspAlgorithm:
        """
        The selected algorithm.
        """
        return _KssspAlgorithm(self.underlying_.algorithm())
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
    def delta_tile(unsigned delta = kDefaultDelta, ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> KssspPlan:
        """
        Delta stepping tiled
        """
        return KssspPlan.make(_KssspPlan.DeltaTile(delta, edge_tile_size))

    @staticmethod
    def delta_step(unsigned delta = kDefaultDelta) -> KssspPlan:
        """
        Delta stepping (non-tiled)
        """
        return KssspPlan.make(_KssspPlan.DeltaStep(delta))

    @staticmethod
    def delta_step_barrier(unsigned delta = kDefaultDelta) -> KssspPlan:
        """
        Delta stepping with barrier
        """
        return KssspPlan.make(_KssspPlan.DeltaStepBarrier(delta))


class _KssspAlgorithmReachability(Enum):
    """
    The algorithms to check reachability for kSSSP

    :see: :py:class: `~katana.local.analytics.AlgoReachability` constructors for algorithm documentation.
    """
    AsyncLevel = _AlgoReachability.Algorithm.asyncLevel
    SyncLevel = _AlgoReachability.Algorithm.syncLevel


cdef class AlgoReachability:
    """
    The algorithms available to check reachability between two nodes

    Static method construct AlgoReachability using specific algorithms. 
    """

    cdef:
        _AlgoReachability underlying_
    
    cdef _AlgoReachability* underlying(self) except NULL:
        return &self.underlying_

    @staticmethod
    cdef AlgoReachability make(_AlgoReachability u):
        f = <AlgoReachability>AlgoReachability.__new__(AlgoReachability)
        f.underlying_ = u
        return f

    def __init__(self):
        """
        Construct AlgoReachability using default parameters
        """
        self.underlying_ = _AlgoReachability()
        
    algorithm = _KssspAlgorithmReachability

    @property
    def algorithm(self) -> _KssspAlgorithmReachability:
        """
        The selected algorithm.
        """
        return _KssspAlgorithmReachability(self.underlying_.algorithm())
    
    def __init__(self):
        """
        Choose an algorithm
        """
        super(AlgoReachability, self).__init__()

    @staticmethod
    def async_level() -> AlgoReachability:
        """
        Asynchronous level reachability
        """
        return AlgoReachability.make(_AlgoReachability.AsyncLevel())

    @staticmethod
    def sync_level() -> AlgoReachability:
        """
        Synchronous level reachability
        """
        return AlgoReachability.make(_AlgoReachability.SyncLevel())

    
def ksssp(pg, str edge_weight_property_name, uint32_t start_node, 
          uint32_t report_node, uint32_t num_paths, bool is_symmetric=False, 
          AlgoReachability algo_reachability = AlgoReachability(), 
          KssspPlan plan = KssspPlan(), *, txn_ctx = None):
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
    :param is_symmetric: Whether or not the path is symmetric. Defaults to false. 
    :type algo_reachability: AlgoReachability
    :param algo_reachability: The algorithm to calcualte if path is reachable. Default is syncLevel
    :type plan: KssspPlan
    :param plan: The execution plan to use. Defaults to heuristically selecting the plan. 
    :param txn_ctx: The transaction context for passing read write sets

    .. code-block:: python

        import katana.local
        from katana.example_data import get_rdg_dataset
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_rdg_dataset("ldbc_003"))
        from katana.analytics import ksssp
        weight_name = "workFrom"
        start_node = 0
        report_node = 10
        num_paths = 5
        ksssp(graph, weight_name, start_node, report_node)
    """

    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        handle_result_void(Ksssp(pg.underlying_property_graph(), edge_weight_property_name_str, 
                                 start_node, report_node, txn_ctx, algo_reachability, num_paths, 
                                 is_symmetric, plan))
