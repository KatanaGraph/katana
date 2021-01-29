from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint64_t, uint32_t
from libcpp.string cimport string

from katana.cpp.libstd.boost cimport std_result, handle_result_void, handle_result_assert, raise_error_code
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.property_graph cimport PropertyGraph
from katana.analytics.plan cimport _Plan, Plan

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

    std_result[void] Bfs(_PropertyGraph * pg,
                         size_t start_node,
                         string output_property_name,
                         _BfsPlan algo)

    std_result[void] BfsAssertValid(_PropertyGraph* pg,
                                 string property_name);

    cppclass _BfsStatistics "katana::analytics::BfsStatistics":
        uint32_t source_node;
        uint32_t max_distance;
        uint64_t total_distance;
        uint32_t n_reached_nodes;

        float average_distance()

        void Print(ostream os)

        @staticmethod
        std_result[_BfsStatistics] Compute(_PropertyGraph* pg,
                                           string property_name);

class _BfsAlgorithm(Enum):
    AsynchronousTile = _BfsPlan.Algorithm.kAsynchronousTile
    Asynchronous = _BfsPlan.Algorithm.kAsynchronous
    SynchronousTile = _BfsPlan.Algorithm.kSynchronousTile
    Synchronous = _BfsPlan.Algorithm.kSynchronous


cdef class BfsPlan(Plan):
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
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Bfs(pg.underlying.get(), start_node, output_property_name_cstr, plan.underlying_))

def bfs_assert_valid(PropertyGraph pg, str property_name):
    output_property_name_bytes = bytes(property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(BfsAssertValid(pg.underlying.get(), output_property_name_cstr))

cdef _BfsStatistics handle_result_BfsStatistics(std_result[_BfsStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef class BfsStatistics:
    cdef _BfsStatistics underlying

    def __init__(self, PropertyGraph pg, str property_name):
        output_property_name_bytes = bytes(property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_BfsStatistics(_BfsStatistics.Compute(pg.underlying.get(), output_property_name_cstr))

    @property
    def source_node(self):
        return self.underlying.source_node

    @property
    def max_distance(self):
        return self.underlying.max_distance

    @property
    def total_distance(self):
        return self.underlying.total_distance

    @property
    def n_reached_nodes(self):
        return self.underlying.n_reached_nodes

    @property
    def average_distance(self):
        return self.underlying.average_distance()

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
