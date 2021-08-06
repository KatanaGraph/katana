"""
Single-Source Shortest Path
---------------------------

.. autoclass:: katana.local.analytics.SsspPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. autoclass:: katana.local.analytics._sssp._SsspAlgorithm
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.sssp

.. autoclass:: katana.local.analytics.SsspStatistics
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.sssp_assert_valid
"""
from enum import Enum

from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint64_t
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._graph cimport Graph
from katana.local.analytics.plan cimport Plan, Statistics, _Plan


cdef extern from "katana/analytics/sssp/sssp.h" namespace "katana::analytics" nogil:
    cppclass _SsspPlan "katana::analytics::SsspPlan" (_Plan):
        enum Algorithm:
            kDeltaTile "katana::analytics::SsspPlan::kDeltaTile"
            kDeltaStep "katana::analytics::SsspPlan::kDeltaStep"
            kDeltaStepBarrier "katana::analytics::SsspPlan::kDeltaStepBarrier"
            kDeltaStepFusion "katana::analytics::SsspPlan::kDeltaStepFusion"
            kSerialDeltaTile "katana::analytics::SsspPlan::kSerialDeltaTile"
            kSerialDelta "katana::analytics::SsspPlan::kSerialDelta"
            kDijkstraTile "katana::analytics::SsspPlan::kDijkstraTile"
            kDijkstra "katana::analytics::SsspPlan::kDijkstra"
            kTopological "katana::analytics::SsspPlan::kTopological"
            kTopologicalTile "katana::analytics::SsspPlan::kTopologicalTile"
            kAutomatic "katana::analytics::SsspPlan::kAutomatic"

        _SsspPlan()
        _SsspPlan(const _PropertyGraph * pg)

        _SsspPlan.Algorithm algorithm() const
        unsigned delta() const
        ptrdiff_t edge_tile_size() const

        @staticmethod
        _SsspPlan DeltaTile(unsigned delta, ptrdiff_t edge_tile_size)
        @staticmethod
        _SsspPlan DeltaStep(unsigned delta)
        @staticmethod
        _SsspPlan DeltaStepBarrier(unsigned delta)
        @staticmethod
        _SsspPlan DeltaStepFusion(unsigned delta)
        @staticmethod
        _SsspPlan SerialDeltaTile(unsigned delta, ptrdiff_t edge_tile_size)
        @staticmethod
        _SsspPlan SerialDelta(unsigned delta)
        @staticmethod
        _SsspPlan DijkstraTile(ptrdiff_t edge_tile_size)
        @staticmethod
        _SsspPlan Dijkstra()
        @staticmethod
        _SsspPlan Topological()
        @staticmethod
        _SsspPlan TopologicalTile(ptrdiff_t edge_tile_size)

    unsigned kDefaultDelta "katana::analytics::SsspPlan::kDefaultDelta"
    ptrdiff_t kDefaultEdgeTileSize "katana::analytics::SsspPlan::kDefaultEdgeTileSize"

    Result[void] Sssp(_PropertyGraph* pg, size_t start_node,
        const string& edge_weight_property_name, const string& output_property_name, _SsspPlan plan)

    Result[void] SsspAssertValid(_PropertyGraph* pg, size_t start_node,
                                 const string& edge_weight_property_name, const string& output_property_name);

    cppclass _SsspStatistics  "katana::analytics::SsspStatistics":
        uint64_t n_reached_nodes
        double max_distance
        double average_visited_distance

        void Print(ostream os)

        @staticmethod
        Result[_SsspStatistics] Compute(_PropertyGraph* pg, string output_property_name);


class _SsspAlgorithm(Enum):
    """
    The concrete algorithms available for SSSP.

    :see: :py:class:`~katana.local.analytics.SsspPlan` constructors for algorithm documentation.
    """
    DeltaTile = _SsspPlan.Algorithm.kDeltaTile
    DeltaStep = _SsspPlan.Algorithm.kDeltaStep
    DeltaStepBarrier = _SsspPlan.Algorithm.kDeltaStepBarrier
    DeltaStepFusion = _SsspPlan.Algorithm.kDeltaStepFusion
    SerialDeltaTile = _SsspPlan.Algorithm.kSerialDeltaTile
    SerialDelta = _SsspPlan.Algorithm.kSerialDelta
    DijkstraTile = _SsspPlan.Algorithm.kDijkstraTile
    Dijkstra = _SsspPlan.Algorithm.kDijkstra
    Topological = _SsspPlan.Algorithm.kTopological
    TopologicalTile = _SsspPlan.Algorithm.kTopologicalTile
    Automatic = _SsspPlan.Algorithm.kAutomatic


cdef class SsspPlan(Plan):
    """
    A computational :ref:`Plan` for Single-Source Shortest Path.

    Static method construct SsspPlans using specific algorithms with their required parameters. All parameters are
    optional and have reasonable defaults.
    """

    cdef:
        _SsspPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    @staticmethod
    cdef SsspPlan make(_SsspPlan u):
        f = <SsspPlan>SsspPlan.__new__(SsspPlan)
        f.underlying_ = u
        return f

    def __init__(self, graph: Graph = None):
        """
        Construct a plan optimized for `graph` using heuristics, or using default parameter values.
        """
        if graph is None:
            self.underlying_ = _SsspPlan()
        else:
            if not isinstance(graph, Graph):
                raise TypeError(graph)
            self.underlying_ = _SsspPlan((<Graph>graph).underlying_property_graph())

    Algorithm = _SsspAlgorithm

    @property
    def algorithm(self) -> _SsspAlgorithm:
        """
        The selected algorithm.
        """
        return _SsspAlgorithm(self.underlying_.algorithm())
    @property
    def delta(self) -> int:
        """
        The exponent of the delta step size (2 based). A delta of 4 will produce a real delta step size of 16.
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
        super(SsspPlan, self).__init__()

    @staticmethod
    def delta_tile(unsigned delta = kDefaultDelta, ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        """
        Delta stepping tiled
        """
        return SsspPlan.make(_SsspPlan.DeltaTile(delta, edge_tile_size))

    @staticmethod
    def delta_step(unsigned delta = kDefaultDelta) -> SsspPlan:
        """
        Delta stepping (non-tiled)
        """
        return SsspPlan.make(_SsspPlan.DeltaStep(delta))

    @staticmethod
    def delta_step_barrier(unsigned delta = kDefaultDelta) -> SsspPlan:
        """
        Delta stepping with barrier
        """
        return SsspPlan.make(_SsspPlan.DeltaStepBarrier(delta))

    @staticmethod
    def delta_step_fusion(unsigned delta = kDefaultDelta) -> SsspPlan:
        """
        Delta stepping with barrier and fused buckets
        """
        return SsspPlan.make(_SsspPlan.DeltaStepFusion(delta))

    @staticmethod
    def serial_delta_tile(unsigned delta = kDefaultDelta, ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        """
        Serial delta stepping tiled
        """
        return SsspPlan.make(_SsspPlan.SerialDeltaTile(delta, edge_tile_size))

    @staticmethod
    def serial_delta(unsigned delta = kDefaultDelta) -> SsspPlan:
        """
        Serial delta stepping
        """
        return SsspPlan.make(_SsspPlan.SerialDelta(delta))

    @staticmethod
    def dijkstra_tile(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        """
        Dijkstra's algorithm tiled
        """
        return SsspPlan.make(_SsspPlan.DijkstraTile(edge_tile_size))

    @staticmethod
    def dijkstra() -> SsspPlan:
        """
        Dijkstra's algorithm (non-tiled)
        """
        return SsspPlan.make(_SsspPlan.Dijkstra())

    @staticmethod
    def topological() -> SsspPlan:
        """
        Topological
        """
        return SsspPlan.make(_SsspPlan.Topological())

    @staticmethod
    def topological_tile(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        """
        Topological tiled
        """
        return SsspPlan.make(_SsspPlan.TopologicalTile(edge_tile_size))


def sssp(Graph pg, size_t start_node, str edge_weight_property_name, str output_property_name,
         SsspPlan plan = SsspPlan()):
    """
    Compute the Single-Source Shortest Path on `pg` using `start_node` as the source. The computed path lengths are
    written to the property `output_property_name`.

    :type pg: Graph
    :param pg: The graph to analyze.
    :type start_node: Node ID
    :param start_node: The source node.
    :type edge_weight_property_name: str
    :param edge_weight_property_name: The input property containing edge weights.
    :type output_property_name: str
    :param output_property_name: The output property to write path lengths into. This property must not already exist.
    :type plan: SsspPlan
    :param plan: The execution plan to use. Defaults to heuristically selecting the plan.
    """
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_void(Sssp(pg.underlying_property_graph(), start_node, edge_weight_property_name_str,
                                output_property_name_str, plan.underlying_))

def sssp_assert_valid(Graph pg, size_t start_node, str edge_weight_property_name, str output_property_name):
    """
    Raise an exception if the SSSP results in `pg` with the given parameters appear to be incorrect. This is not an
    exhaustive check, just a sanity check.

    :raises: AssertionError
    """
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_assert(SsspAssertValid(pg.underlying_property_graph(), start_node, edge_weight_property_name_str, output_property_name_str))


cdef _SsspStatistics handle_result_SsspStatistics(Result[_SsspStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class SsspStatistics(Statistics):
    """
    Compute the :ref:`statistics` of an SSSP computation on a graph.
    """
    cdef _SsspStatistics underlying

    def __init__(self, Graph pg, str output_property_name):
        """
        :param pg: The graph on which `sssp` was called.
        :param output_property_name: The output property name passed to `sssp`.
        """
        cdef string output_property_name_str = bytes(output_property_name, "utf-8")
        with nogil:
            self.underlying = handle_result_SsspStatistics(_SsspStatistics.Compute(
                pg.underlying_property_graph(), output_property_name_str))

    @property
    def max_distance(self) -> float:
        """
        The maximum path length.

        :rtype: float
        """
        return self.underlying.max_distance

    @property
    def n_reached_nodes(self) -> int:
        """
        The number of nodes reachable from the source.

        :rtype: int
        """
        return self.underlying.n_reached_nodes

    @property
    def average_visited_distance(self) -> float:
        """
        The average path length.

        :rtype: float
        """
        return self.underlying.average_visited_distance

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
