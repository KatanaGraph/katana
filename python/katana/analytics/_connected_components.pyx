from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

from katana.analytics.plan cimport Plan, _Plan
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport handle_result_void, handle_result_assert, raise_error_code, Result
from katana.property_graph cimport PropertyGraph

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
    cdef:
        _ConnectedComponentsPlan underlying_

    cdef _Plan*underlying(self) except NULL:
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
        return self.underlying_.edge_tile_size()

    @property
    def neighbor_sample_size(self) -> uint32_t:
        return self.underlying_.neighbor_sample_size()

    @property
    def component_sample_frequency(self) -> uint32_t:
        return self.underlying_.component_sample_frequency()

    @staticmethod
    def serial() -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Serial())
    @staticmethod
    def label_prop() -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.LabelProp())
    @staticmethod
    def synchronous() -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Synchronous())
    @staticmethod
    def asynchronous() -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Asynchronous())
    @staticmethod
    def edge_asynchronous() -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeAsynchronous())
    @staticmethod
    def edge_tiled_asynchronous(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeTiledAsynchronous(edge_tile_size))
    @staticmethod
    def blocked_asynchronous() -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.BlockedAsynchronous())
    @staticmethod
    def afforest(uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
                 uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.Afforest(
            neighbor_sample_size, component_sample_frequency))
    @staticmethod
    def edge_afforest(uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
                      uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeAfforest(
            neighbor_sample_size, component_sample_frequency))
    @staticmethod
    def edge_tiled_afforest(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize,
                            uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
                            uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) -> ConnectedComponentsPlan:
        return ConnectedComponentsPlan.make(_ConnectedComponentsPlan.EdgeTiledAfforest(
            edge_tile_size, neighbor_sample_size, component_sample_frequency))

def connected_components(PropertyGraph pg, str output_property_name,
                         ConnectedComponentsPlan plan = ConnectedComponentsPlan()) -> int:
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        v = handle_result_void(ConnectedComponents(pg.underlying.get(), output_property_name_str, plan.underlying_))
    return v

def connected_components_assert_valid(PropertyGraph pg, str output_property_name):
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        handle_result_assert(ConnectedComponentsAssertValid(pg.underlying.get(), output_property_name_str))

cdef _ConnectedComponentsStatistics handle_result_ConnectedComponentsStatistics(
        Result[_ConnectedComponentsStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef class ConnectedComponentsStatistics:
    cdef _ConnectedComponentsStatistics underlying

    def __init__(self, PropertyGraph pg, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_ConnectedComponentsStatistics(_ConnectedComponentsStatistics.Compute(
                pg.underlying.get(), output_property_name_str))

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
        return self.underlying.largest_component_ratio

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
