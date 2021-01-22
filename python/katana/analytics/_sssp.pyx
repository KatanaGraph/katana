from enum import Enum

from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint32_t
from libcpp.string cimport string

from katana.cpp.libstd.boost cimport std_result, handle_result_void, handle_result_assert, raise_error_code
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libgalois.graphs.Graph cimport PropertyFileGraph
from katana.property_graph cimport PropertyGraph
from katana.analytics.plan cimport _Plan, Plan


cdef extern from "katana/analytics/sssp/sssp.h" namespace "katana::analytics" nogil:
    cppclass _SsspPlan "katana::analytics::SsspPlan" (_Plan):
        enum Algorithm:
            kDeltaTile "katana::analytics::SsspPlan::kDeltaTile"
            kDeltaStep "katana::analytics::SsspPlan::kDeltaStep"
            kDeltaStepBarrier "katana::analytics::SsspPlan::kDeltaStepBarrier"
            kSerialDeltaTile "katana::analytics::SsspPlan::kSerialDeltaTile"
            kSerialDelta "katana::analytics::SsspPlan::kSerialDelta"
            kDijkstraTile "katana::analytics::SsspPlan::kDijkstraTile"
            kDijkstra "katana::analytics::SsspPlan::kDijkstra"
            kTopological "katana::analytics::SsspPlan::kTopological"
            kTopologicalTile "katana::analytics::SsspPlan::kTopologicalTile"
            kAutomatic "katana::analytics::SsspPlan::kAutomatic"

        _SsspPlan()
        _SsspPlan(const PropertyFileGraph * pfg)

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

    std_result[void] Sssp(PropertyFileGraph* pfg, size_t start_node,
        const string& edge_weight_property_name, const string& output_property_name, _SsspPlan plan)

    std_result[void] SsspAssertValid(PropertyFileGraph* pfg, size_t start_node,
                                     const string& edge_weight_property_name, const string& output_property_name);

    cppclass _SsspStatistics  "katana::analytics::SsspStatistics":
        double max_distance
        double total_distance
        uint32_t n_reached_nodes

        double average_distance()

        void Print(ostream os)

        @staticmethod
        std_result[_SsspStatistics] Compute(PropertyFileGraph* pfg, string output_property_name);


class _SsspAlgorithm(Enum):
    DeltaTile = _SsspPlan.Algorithm.kDeltaTile
    DeltaStep = _SsspPlan.Algorithm.kDeltaStep
    DeltaStepBarrier = _SsspPlan.Algorithm.kDeltaStepBarrier
    SerialDeltaTile = _SsspPlan.Algorithm.kSerialDeltaTile
    SerialDelta = _SsspPlan.Algorithm.kSerialDelta
    DijkstraTile = _SsspPlan.Algorithm.kDijkstraTile
    Dijkstra = _SsspPlan.Algorithm.kDijkstra
    Topological = _SsspPlan.Algorithm.kTopological
    TopologicalTile = _SsspPlan.Algorithm.kTopologicalTile
    Automatic = _SsspPlan.Algorithm.kAutomatic


cdef class SsspPlan(Plan):
    cdef:
        _SsspPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    @staticmethod
    cdef SsspPlan make(_SsspPlan u):
        f = <SsspPlan>SsspPlan.__new__(SsspPlan)
        f.underlying_ = u
        return f

    def __init__(self, graph = None):
        if graph is None:
            self.underlying_ = _SsspPlan()
        else:
            if not isinstance(graph, PropertyGraph):
                raise TypeError(graph)
            self.underlying_ = _SsspPlan((<PropertyGraph>graph).underlying.get())

    Algorithm = _SsspAlgorithm

    @property
    def algorithm(self) -> _SsspAlgorithm:
        return _SsspAlgorithm(self.underlying_.algorithm())
    @property
    def delta(self) -> int:
        return self.underlying_.delta()
    @property
    def edge_tile_size(self) -> int:
        return self.underlying_.edge_tile_size()

    @staticmethod
    def delta_tile(unsigned delta = kDefaultDelta, ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.DeltaTile(delta, edge_tile_size))
    @staticmethod
    def delta_step(unsigned delta = kDefaultDelta) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.DeltaStep(delta))
    @staticmethod
    def delta_step_barrier(unsigned delta = kDefaultDelta) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.DeltaStepBarrier(delta))
    @staticmethod
    def serial_delta_tile(unsigned delta = kDefaultDelta, ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.SerialDeltaTile(delta, edge_tile_size))
    @staticmethod
    def serial_delta(unsigned delta = kDefaultDelta) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.SerialDelta(delta))
    @staticmethod
    def dijkstra_tile(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.DijkstraTile(edge_tile_size))
    @staticmethod
    def dijkstra() -> SsspPlan:
        return SsspPlan.make(_SsspPlan.Dijkstra())
    @staticmethod
    def topological() -> SsspPlan:
        return SsspPlan.make(_SsspPlan.Topological())
    @staticmethod
    def topological_tile(ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) -> SsspPlan:
        return SsspPlan.make(_SsspPlan.TopologicalTile(edge_tile_size))


def sssp(PropertyGraph pg, size_t start_node, str edge_weight_property_name, str output_property_name,
         SsspPlan plan = SsspPlan()):
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_void(Sssp(pg.underlying.get(), start_node, edge_weight_property_name_str,
                                output_property_name_str, plan.underlying_))

def sssp_assert_valid(PropertyGraph pg, size_t start_node, str edge_weight_property_name, str output_property_name):
    cdef string edge_weight_property_name_str = bytes(edge_weight_property_name, "utf-8")
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_assert(SsspAssertValid(pg.underlying.get(), start_node, edge_weight_property_name_str, output_property_name_str))


cdef _SsspStatistics handle_result_SsspStatistics(std_result[_SsspStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class SsspStatistics:
    cdef _SsspStatistics underlying

    def __init__(self, PropertyGraph pg, str output_property_name):
        cdef string output_property_name_str = bytes(output_property_name, "utf-8")
        with nogil:
            self.underlying = handle_result_SsspStatistics(_SsspStatistics.Compute(
                pg.underlying.get(), output_property_name_str))

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
