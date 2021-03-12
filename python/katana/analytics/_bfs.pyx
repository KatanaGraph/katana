"""
Breadth-first Search
--------------------

.. autoclass:: katana.analytics.BfsPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. autoclass:: katana.analytics._bfs._BfsAlgorithm

.. autofunction:: katana.analytics.bfs

.. autoclass:: katana.analytics.BfsStatistics
    :members:
    :undoc-members:

.. autofunction:: katana.analytics.bfs_assert_valid
"""

from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint64_t, uint32_t
from libcpp.string cimport string

from katana.analytics.plan cimport _Plan, Plan
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_void, handle_result_assert, raise_error_code
from katana.property_graph cimport PropertyGraph

from enum import Enum

cdef extern from "katana/Analytics.h" namespace "katana::analytics" nogil:
    cppclass _BfsPlan "katana::analytics::BfsPlan" (_Plan):
        enum Algorithm:
            kAsynchronousTile "katana::analytics::BfsPlan::kAsynchronousTile"
            kAsynchronous "katana::analytics::BfsPlan::kAsynchronous"
            kSynchronousTile "katana::analytics::BfsPlan::kSynchronousTile"
            kSynchronous "katana::analytics::BfsPlan::kSynchronous"

        _BfsPlan.Algorithm algorithm() const
        ptrdiff_t edge_tile_size() const

        @staticmethod
        _BfsPlan AsynchronousTile(ptrdiff_t edge_tile_size)

        @staticmethod
        _BfsPlan Asynchronous()

        @staticmethod
        _BfsPlan SynchronousTile(ptrdiff_t edge_tile_size)

        @staticmethod
        _BfsPlan Synchronous()

    ptrdiff_t kDefaultEdgeTileSize "katana::analytics::BfsPlan::kDefaultEdgeTileSize"

    Result[void] Bfs(_PropertyGraph * pg,
                     size_t start_node,
                     string output_property_name,
                     _BfsPlan algo)

    Result[void] BfsAssertValid(_PropertyGraph* pg,
                                string property_name);

    cppclass _BfsStatistics "katana::analytics::BfsStatistics":
        uint32_t source_node;
        uint32_t max_distance;
        uint64_t total_distance;
        uint32_t n_reached_nodes;

        float average_distance()

        void Print(ostream os)

        @staticmethod
        Result[_BfsStatistics] Compute(_PropertyGraph* pg,
                                       string property_name);

class _BfsAlgorithm(Enum):
    """
    .. py:attribute:: Asynchronous

    .. py:attribute:: AsynchronousTile
        Asynchronous tiled

    .. py:attribute:: Synchronous

        Bulk-synchronous

    .. py:attribute:: SynchronousTile

        Bulk-synchronous tiled

    """
    Asynchronous = _BfsPlan.Algorithm.kAsynchronous
    AsynchronousTile = _BfsPlan.Algorithm.kAsynchronousTile
    Synchronous = _BfsPlan.Algorithm.kSynchronous
    SynchronousTile = _BfsPlan.Algorithm.kSynchronousTile


cdef class BfsPlan(Plan):
    """
    A computational :ref:`Plan` for Breadth-First Search.

    Static method construct BfsPlans using specific algorithms with their required parameters. All parameters are
    optional and have reasonable defaults.
    """
    cdef:
        _BfsPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    @staticmethod
    cdef BfsPlan make(_BfsPlan u):
        f = <BfsPlan>BfsPlan.__new__(BfsPlan)
        f.underlying_ = u
        return f

    Algorithm = _BfsAlgorithm

    @property
    def algorithm(self) -> _BfsAlgorithm:
        return _BfsAlgorithm(self.underlying_.algorithm())

    @property
    def edge_tile_size(self) -> int:
        """
        The size of tiles used for edge tiled algorithms.
        """
        return self.underlying_.edge_tile_size()

    @staticmethod
    def asynchronous_tile(edge_tile_size=kDefaultEdgeTileSize):
        return BfsPlan.make(_BfsPlan.AsynchronousTile(edge_tile_size))

    @staticmethod
    def asynchronous():
        return BfsPlan.make(_BfsPlan.Asynchronous())

    @staticmethod
    def synchronous_tile(edge_tile_size=kDefaultEdgeTileSize):
        return BfsPlan.make(_BfsPlan.SynchronousTile(edge_tile_size))

    @staticmethod
    def synchronous():
        return BfsPlan.make(_BfsPlan.Synchronous())


def bfs(PropertyGraph pg, size_t start_node, str output_property_name, BfsPlan plan = BfsPlan()):
    """
    Compute the Breadth-First Search levels on `pg` using `start_node` as the source. The computed levels are
    written to the property `output_property_name`.

    :type pg: PropertyGraph
    :param pg: The graph to analyze.
    :type start_node: Node ID
    :param start_node: The source node.
    :type output_property_name: str
    :param output_property_name: The output property to write path lengths into. This property must not already exist.
    :type plan: BfsPlan
    :param plan: The execution plan to use.
    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Bfs(pg.underlying.get(), start_node, output_property_name_cstr, plan.underlying_))

def bfs_assert_valid(PropertyGraph pg, str property_name):
    """
    Raise an exception if the BFS results in `pg` appear to be incorrect. This is not an
    exhaustive check, just a sanity check.

    :raises: AssertionError
    """
    output_property_name_bytes = bytes(property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(BfsAssertValid(pg.underlying.get(), output_property_name_cstr))

cdef _BfsStatistics handle_result_BfsStatistics(Result[_BfsStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef class BfsStatistics:
    """
    Compute the :ref:`statistics` of an BFS computation on a graph.
    """
    cdef _BfsStatistics underlying

    def __init__(self, PropertyGraph pg, str property_name):
        output_property_name_bytes = bytes(property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_BfsStatistics(_BfsStatistics.Compute(pg.underlying.get(), output_property_name_cstr))

    @property
    def source_node(self):
        """
        The source node ID. The only node with level 0.
        """
        return self.underlying.source_node

    @property
    def max_distance(self):
        """
        The maximum level.

        :rtype: int
        """
        return self.underlying.max_distance

    @property
    def total_distance(self):
        """
        The sum of all levels.

        :rtype: int
        """
        return self.underlying.total_distance

    @property
    def n_reached_nodes(self):
        """
        The number of nodes reachable from the source.

        :rtype: int
        """
        return self.underlying.n_reached_nodes

    @property
    def average_distance(self):
        """
        The average level.

        :rtype: float
        """
        return self.underlying.average_distance()

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
