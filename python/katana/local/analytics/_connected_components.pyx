"""
Connected Components
--------------------

.. autoclass:: katana.local.analytics.ConnectedComponentsPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. [Sutton] M. Sutton, T. Ben-Nun and A. Barak, "Optimizing Parallel Graph
    Connectivity Computation via Subgraph Sampling," 2018 IEEE International
    Parallel and Distributed Processing Symposium (IPDPS), Vancouver, BC, 2018,
    pp. 12-21.

.. autoclass:: katana.local.analytics._connected_components._ConnectedComponentsPlanAlgorithm
    :members:
    :undoc-members:


.. autofunction:: katana.local.analytics.connected_components

.. autoclass:: katana.local.analytics.ConnectedComponentsStatistics
    :members:
    :undoc-members:
"""
from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._property_graph cimport PropertyGraph
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/connected_components/connected_components.h" namespace "katana::analytics" nogil:
    cppclass _ConnectedComponentsPlan "katana::analytics::ConnectedComponentsPlan"(_Plan):
        enum Algorithm:
            kSerial "katana::analytics::ConnectedComponentsPlan::kSerial"
            kLabelProp "katana::analytics::ConnectedComponentsPlan::kLabelProp"
            kSynchronous "katana::analytics::ConnectedComponentsPlan::kSynchronous"
            kAsynchronous "katana::analytics::ConnectedComponentsPlan::kAsynchronous"
            kEdgeAsynchronous "katana::analytics::ConnectedComponentsPlan::kEdgeAsynchronous"
            kEdgeTiledAsynchronous "katana::analytics::ConnectedComponentsPlan::kEdgeTiledAsynchronous"
            kBlockedAsynchronous "katana::analytics::ConnectedComponentsPlan::kBlockedAsynchronous"
            kAfforest "katana::analytics::ConnectedComponentsPlan::kAfforest"
            kEdgeAfforest "katana::analytics::ConnectedComponentsPlan::kEdgeAfforest"
            kEdgeTiledAfforest "katana::analytics::ConnectedComponentsPlan::kEdgeTiledAfforest"

        _ConnectedComponentsPlan.Algorithm algorithm() const
        ptrdiff_t edge_tile_size() const
        uint32_t neighbor_sample_size() const
        uint32_t component_sample_frequency() const

        ConnectedComponentsPlan()

        @staticmethod
        _ConnectedComponentsPlan Serial()

        @staticmethod
        _ConnectedComponentsPlan LabelProp()

        @staticmethod
        _ConnectedComponentsPlan Synchronous()

        @staticmethod
        _ConnectedComponentsPlan Asynchronous()

        @staticmethod
        _ConnectedComponentsPlan EdgeAsynchronous()

        @staticmethod
        _ConnectedComponentsPlan EdgeTiledAsynchronous(ptrdiff_t edge_tile_size)

        @staticmethod
        _ConnectedComponentsPlan BlockedAsynchronous()

        @staticmethod
        _ConnectedComponentsPlan Afforest(uint32_t neighbor_sample_size, uint32_t component_sample_frequency)

        @staticmethod
        _ConnectedComponentsPlan EdgeAfforest(uint32_t neighbor_sample_size, uint32_t component_sample_frequency)

        @staticmethod
        _ConnectedComponentsPlan EdgeTiledAfforest(ptrdiff_t edge_tile_size, uint32_t neighbor_sample_size,
                                                   uint32_t component_sample_frequency)

    ptrdiff_t kDefaultEdgeTileSize "katana::analytics::ConnectedComponentsPlan::kDefaultEdgeTileSize"
    uint32_t kDefaultNeighborSampleSize "katana::analytics::ConnectedComponentsPlan::kDefaultNeighborSampleSize"
    uint32_t kDefaultComponentSampleFrequency "katana::analytics::ConnectedComponentsPlan::kDefaultComponentSampleFrequency"

    Result[void] ConnectedComponents(_PropertyGraph*pg, string output_property_name,
                                     _ConnectedComponentsPlan plan)

    Result[void] ConnectedComponentsAssertValid(_PropertyGraph*pg, string output_property_name)

    cppclass _ConnectedComponentsStatistics "katana::analytics::ConnectedComponentsStatistics":
        uint64_t total_components
        uint64_t total_non_trivial_components
        uint64_t largest_component_size
        double largest_component_ratio

        void Print(ostream os)

        @staticmethod
        Result[_ConnectedComponentsStatistics] Compute(_PropertyGraph*pg, string output_property_name)


class _ConnectedComponentsPlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.ConnectedComponentsPlan` constructors for algorithm documentation.
    """
    Serial = _ConnectedComponentsPlan.Algorithm.kSerial
    LabelProp = _ConnectedComponentsPlan.Algorithm.kLabelProp
    Synchronous = _ConnectedComponentsPlan.Algorithm.kSynchronous
    Asynchronous = _ConnectedComponentsPlan.Algorithm.kAsynchronous
    EdgeAsynchronous = _ConnectedComponentsPlan.Algorithm.kEdgeAsynchronous
    EdgeTiledAsynchronous = _ConnectedComponentsPlan.Algorithm.kEdgeTiledAsynchronous
    BlockedAsynchronous = _ConnectedComponentsPlan.Algorithm.kBlockedAsynchronous
    Afforest = _ConnectedComponentsPlan.Algorithm.kAfforest
    EdgeAfforest = _ConnectedComponentsPlan.Algorithm.kEdgeAfforest
    EdgeTiledAfforest = _ConnectedComponentsPlan.Algorithm.kEdgeTiledAfforest


cdef class ConnectedComponentsPlan(Plan):
    """
    A computational :ref:`Plan` for Connected Components.

    Static method construct ConnectedComponentsPlans using specific algorithms with their required parameters. All
    parameters are optional and have reasonable defaults.
    """
    cdef:
        _ConnectedComponentsPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _ConnectedComponentsPlanAlgorithm

    @staticmethod
    cdef ConnectedComponentsPlan make(_ConnectedComponentsPlan u):
        f = <ConnectedComponentsPlan> ConnectedComponentsPlan.__new__(ConnectedComponentsPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> _ConnectedComponentsPlanAlgorithm:
        return self.underlying_.algorithm()

    @property
    def edge_tile_size(self) -> ptrdiff_t:
        """
        The size of tiles to use for edge-tiling.
        """
        return self.underlying_.edge_tile_size()

    @property
    def neighbor_sample_size(self) -> uint32_t:
        return self.underlying_.neighbor_sample_size()

    @property
    def component_sample_frequency(self) -> uint32_t:
        return self.underlying_.component_sample_frequency()

    @staticmethod
    def serial() -> ConnectedComponentsPlan:
        """
        Serial connected components algorithm. Uses the union-find datastructure.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Serial())
    @staticmethod
    def label_prop() -> ConnectedComponentsPlan:
        """
        Label propagation push-style algorithm. Initially, all nodes are in
        their own component IDs (same as their node IDs). Then, the component
        IDs are set to the minimum component ID in their neighborhood.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.LabelProp())
    @staticmethod
    def synchronous() -> ConnectedComponentsPlan:
        """
        Synchronous connected components algorithm.  Initially all nodes are in
        their own component. Then, we merge endpoints of edges to form the spanning
        tree. Merging is done in two phases to simplify concurrent updates: (1)
        find components and (2) union components.  Since the merge phase does not
        do any finds, we only process a fraction of edges at a time; otherwise,
        the union phase may unnecessarily merge two endpoints in the same
        component.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Synchronous())
    @staticmethod
    def asynchronous() -> ConnectedComponentsPlan:
        """
        Unlike Synchronous algorithm, Asynchronous doesn't restrict path compression
        (UnionFind data structure) and can perform unions and finds concurrently.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Asynchronous())
    @staticmethod
    def edge_asynchronous() -> ConnectedComponentsPlan:
        """
        Similar to Asynchronous, except that work-item is edge instead of node.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeAsynchronous())
    @staticmethod
    def edge_tiled_asynchronous(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> ConnectedComponentsPlan:
        """
        Similar EdgeSynchronous with the work-item as blocks of `edge_tile_size` edges.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeTiledAsynchronous(edge_tile_size))
    @staticmethod
    def blocked_asynchronous() -> ConnectedComponentsPlan:
        """
        Similar Asynchronous with the work-item as block of nodes.
        Improves performance of Asynchronous algorithm by following machine topology.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.BlockedAsynchronous())
    @staticmethod
    def afforest(uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
                 uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) -> ConnectedComponentsPlan:
        """
        Connected-components using Afforest sampling [Sutton]_.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Afforest(
            neighbor_sample_size, component_sample_frequency))
    @staticmethod
    def edge_afforest(uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
                      uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) -> ConnectedComponentsPlan:
        """
        Connected-components using Afforest sampling [Sutton]_ with edge as work-item.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeAfforest(
            neighbor_sample_size, component_sample_frequency))
    @staticmethod
    def edge_tiled_afforest(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize,
                            uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
                            uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) -> ConnectedComponentsPlan:
        """
        Connected-components using Afforest sampling [Sutton]_ with block of edges as work-item.
        """
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeTiledAfforest(
            edge_tile_size, neighbor_sample_size, component_sample_frequency))


def connected_components(PropertyGraph pg, str output_property_name,
                         ConnectedComponentsPlan plan = ConnectedComponentsPlan()) -> int:
    """
    Compute the Connected-components for `pg`. `pg` must be symmetric.

    :type pg: PropertyGraph
    :param pg: The graph to analyze.
    :type output_property_name: str
    :param output_property_name: The output property to write path lengths into. This property must not already exist.
    :type plan: ConnectedComponentsPlan
    :param plan: The execution plan to use. Defaults to heuristically selecting the plan.
    
    .. code-block:: python
    
        import katana.local
        from katana.example_utils import get_input
        from katana.property_graph import PropertyGraph
        katana.local.initialize()

        property_graph = PropertyGraph(get_input("propertygraphs/ldbc_003"))
        from katana.analytics import connected_components, ConnectedComponentsStatistics
        connected_components(property_graph, "output")

        stats = ConnectedComponentsStatistics(property_graph, "output")

        print("Total Components:", stats.total_components)
        print("Total Non-Trivial Components:", stats.total_non_trivial_components)
        print("Largest Component Size:", stats.largest_component_size)
        print("Largest Component Ratio:", stats.largest_component_ratio)

    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        v = handle_result_void(ConnectedComponents(pg.underlying_property_graph(), output_property_name_str, plan.underlying_))
    return v

def connected_components_assert_valid(PropertyGraph pg, str output_property_name):
    """
    Raise an exception if the Connected Components results in `pg` with the given parameters appear to be incorrect.
    This is not an exhaustive check, just a sanity check.

    :raises: AssertionError
    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        handle_result_assert(ConnectedComponentsAssertValid(pg.underlying_property_graph(), output_property_name_str))

cdef _ConnectedComponentsStatistics handle_result_ConnectedComponentsStatistics(
        Result[_ConnectedComponentsStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef class ConnectedComponentsStatistics:
    """
    Compute the :ref:`statistics` of a Connected Components computation on a graph.
    """
    cdef _ConnectedComponentsStatistics underlying

    def __init__(self, PropertyGraph pg, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_ConnectedComponentsStatistics(_ConnectedComponentsStatistics.Compute(
                pg.underlying_property_graph(), output_property_name_str))

    @property
    def total_components(self) -> uint64_t:
        return self.underlying.total_components

    @property
    def total_non_trivial_components(self) -> uint64_t:
        return self.underlying.total_non_trivial_components

    @property
    def largest_component_size(self) -> uint64_t:
        return self.underlying.largest_component_size

    @property
    def largest_component_ratio(self) -> double:
        """
        The faction of the entire graph that is part of the largest component.
        """
        return self.underlying.largest_component_ratio

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
