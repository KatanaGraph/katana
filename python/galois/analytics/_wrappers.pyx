from galois.cpp.libstd.boost cimport std_result, handle_result_void, handle_result_bool, raise_error_code
from galois.cpp.libstd.iostream cimport ostream, ostringstream
from libc.stddef cimport ptrdiff_t
from libcpp.string cimport string
from libcpp cimport bool
from galois.cpp.libgalois.graphs.Graph cimport PropertyFileGraph
from galois.property_graph cimport PropertyGraph
from libc.stdint cimport uint64_t, uint32_t

from enum import Enum

cdef extern from "galois/Analytics.h" namespace "galois::analytics" nogil:
    enum Architecture:
        kCPU
        kGPU
        kDistributed

    cppclass Plan:
        Architecture architecture() const

# BFS

cdef extern from "galois/Analytics.h" namespace "galois::analytics" nogil:
    cppclass _BfsPlan "galois::analytics::BfsPlan":
        enum Algorithm:
            kAsyncTile "galois::analytics::BfsPlan::kAsyncTile"
            kAsync "galois::analytics::BfsPlan::kAsync"
            kSyncTile "galois::analytics::BfsPlan::kSyncTile"
            kSync "galois::analytics::BfsPlan::kSync"

        _BfsPlan.Algorithm algorithm() const
        ptrdiff_t edge_tile_size() const

        @staticmethod
        _BfsPlan AsyncTile()
        @staticmethod
        _BfsPlan AsyncTile_1 "AsyncTile"(ptrdiff_t edge_tile_size)

        @staticmethod
        _BfsPlan Async()

        @staticmethod
        _BfsPlan SyncTile()
        @staticmethod
        _BfsPlan SyncTile_1 "SyncTile"(ptrdiff_t edge_tile_size)

        @staticmethod
        _BfsPlan Sync()

        @staticmethod
        _BfsPlan FromAlgorithm(_BfsPlan.Algorithm algo)

    std_result[void] Bfs(PropertyFileGraph * pfg,
                         size_t start_node,
                         string output_property_name,
                         _BfsPlan algo)

    std_result[bool] BfsValidate(PropertyFileGraph* pfg,
                                 string property_name);

    cppclass _BfsStatistics "galois::analytics::BfsStatistics":
        uint32_t source_node;
        uint32_t max_distance;
        uint64_t total_distance;
        uint32_t n_reached_nodes;

        float average_distance()

        void Print(ostream os)

        @staticmethod
        std_result[_BfsStatistics] Compute(PropertyFileGraph* pfg,
                                           string property_name);

class _BfsAlgorithm(Enum):
    AsyncTile = _BfsPlan.Algorithm.kAsyncTile
    Async = _BfsPlan.Algorithm.kAsync
    SyncTile = _BfsPlan.Algorithm.kSyncTile
    Sync = _BfsPlan.Algorithm.kSync


cdef class BfsPlan:
    cdef:
        _BfsPlan underlying

    @staticmethod
    cdef BfsPlan make(_BfsPlan u):
        f = <BfsPlan>BfsPlan.__new__(BfsPlan)
        f.underlying = u
        return f

    Algorithm = _BfsAlgorithm

    @property
    def algorithm(self) -> _BfsAlgorithm:
        return _BfsAlgorithm(self.underlying.algorithm())

    @property
    def edge_tile_size(self) -> int:
        return self.underlying.edge_tile_size()

    @staticmethod
    def async_tile(edge_tile_size=None):

        if edge_tile_size is not None:
            return BfsPlan.make(_BfsPlan.AsyncTile_1(edge_tile_size))
        return BfsPlan.make(_BfsPlan.AsyncTile())

    @staticmethod
    def async_():
        return BfsPlan.make(_BfsPlan.Async())

    @staticmethod
    def sync_tile(edge_tile_size=None):
        if edge_tile_size is not None:
            return BfsPlan.make(_BfsPlan.SyncTile_1(edge_tile_size))
        return BfsPlan.make(_BfsPlan.SyncTile())

    @staticmethod
    def sync():
        return BfsPlan.make(_BfsPlan.Sync())

    @staticmethod
    def from_algorithm(algorithm):
        return BfsPlan.make(_BfsPlan.FromAlgorithm(int(algorithm)))


def bfs(PropertyGraph pg, size_t start_node, str output_property_name, BfsPlan plan = BfsPlan()):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Bfs(pg.underlying.get(), start_node, output_property_name_cstr, plan.underlying))

def bfs_validate(PropertyGraph pg, str property_name):
    output_property_name_bytes = bytes(property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        r = handle_result_bool(BfsValidate(pg.underlying.get(), output_property_name_cstr))
    return r

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


# SSSP

cdef extern from "galois/Analytics.h" namespace "galois::analytics" nogil:
    cppclass _SsspPlan "galois::analytics::SsspPlan":
        enum Algorithm:
            kDeltaTile "galois::analytics::SsspPlan::kDeltaTile"
            kDeltaStep "galois::analytics::SsspPlan::kDeltaStep"
            kDeltaStepBarrier "galois::analytics::SsspPlan::kDeltaStepBarrier"
            kSerialDeltaTile "galois::analytics::SsspPlan::kSerialDeltaTile"
            kSerialDelta "galois::analytics::SsspPlan::kSerialDelta"
            kDijkstraTile "galois::analytics::SsspPlan::kDijkstraTile"
            kDijkstra "galois::analytics::SsspPlan::kDijkstra"
            kTopo "galois::analytics::SsspPlan::kTopo"
            kTopoTile "galois::analytics::SsspPlan::kTopoTile"
            kAutomatic "galois::analytics::SsspPlan::kAutomatic"

        _SsspPlan()
        _SsspPlan(const PropertyFileGraph * pfg)

        _SsspPlan.Algorithm algorithm() const
        unsigned delta() const
        ptrdiff_t edge_tile_size() const

        @staticmethod
        _SsspPlan DeltaTile()
        @staticmethod
        _SsspPlan DeltaTile_2 "DeltaTile"(unsigned delta, ptrdiff_t edge_tile_size)
        @staticmethod
        _SsspPlan DeltaStep()
        @staticmethod
        _SsspPlan DeltaStep_1 "DeltaStep"(unsigned delta)
        @staticmethod
        _SsspPlan DeltaStepBarrier()
        @staticmethod
        _SsspPlan DeltaStepBarrier_1 "DeltaStepBarrier"(unsigned delta)

        @staticmethod
        _SsspPlan SerialDeltaTile()
        @staticmethod
        _SsspPlan SerialDeltaTile_2 "SerialDeltaTile"(unsigned delta, ptrdiff_t edge_tile_size)
        @staticmethod
        _SsspPlan SerialDelta()
        @staticmethod
        _SsspPlan SerialDelta_1 "SerialDelta"(unsigned delta)

        @staticmethod
        _SsspPlan DijkstraTile()
        @staticmethod
        _SsspPlan DijkstraTile_1 "DijkstraTile"(ptrdiff_t edge_tile_size)

        @staticmethod
        _SsspPlan Dijkstra()

        @staticmethod
        _SsspPlan Topo()

        @staticmethod
        _SsspPlan TopoTile()
        @staticmethod
        _SsspPlan TopoTile_1 "TopoTile"(ptrdiff_t edge_tile_size)


    std_result[void] Sssp(PropertyFileGraph* pfg, size_t start_node,
        string edge_weight_property_name, string output_property_name,
        _SsspPlan plan)

    std_result[bool] SsspValidate(
        PropertyFileGraph* pfg, size_t start_node,
        string edge_weight_property_name,
        string output_property_name);

    cppclass _SsspStatistics  "galois::analytics::SsspStatistics":
        double max_distance
        double total_distance
        uint32_t n_reached_nodes

        double average_distance()

        void Print(ostream)

        @staticmethod
        std_result[_SsspStatistics] Compute(
            PropertyFileGraph* pfg, string output_property_name);

class _SsspAlgorithm(Enum):
    DeltaTile = _SsspPlan.Algorithm.kDeltaTile
    DeltaStep = _SsspPlan.Algorithm.kDeltaStep
    DeltaStepBarrier = _SsspPlan.Algorithm.kDeltaStepBarrier
    SerialDeltaTile = _SsspPlan.Algorithm.kSerialDeltaTile
    SerialDelta = _SsspPlan.Algorithm.kSerialDelta
    DijkstraTile = _SsspPlan.Algorithm.kDijkstraTile
    Dijkstra = _SsspPlan.Algorithm.kDijkstra
    Topo = _SsspPlan.Algorithm.kTopo
    TopoTile = _SsspPlan.Algorithm.kTopoTile
    Automatic = _SsspPlan.Algorithm.kAutomatic


cdef default_value(v, d):
    if v is None:
        return d
    return v


cdef class SsspPlan:
    cdef:
        _SsspPlan underlying

    @staticmethod
    cdef SsspPlan make(_SsspPlan u):
        f = <SsspPlan>SsspPlan.__new__(SsspPlan)
        f.underlying = u
        return f


    def __init__(self, graph = None):
        if graph is None:
            self.underlying = _SsspPlan()
        else:
            if not isinstance(graph, PropertyGraph):
                raise TypeError(graph)
            self.underlying = _SsspPlan((<PropertyGraph>graph).underlying.get())

    Algorithm = _SsspAlgorithm

    @property
    def algorithm(self) -> _SsspAlgorithm:
        return _BfsAlgorithm(self.underlying.algorithm())

    @property
    def delta(self) -> int:
        return self.underlying.delta()

    @property
    def edge_tile_size(self) -> int:
        return self.underlying.edge_tile_size()

    @staticmethod
    def delta_tile(delta=None, edge_tile_size=None):
        default = _SsspPlan.DeltaTile()
        return SsspPlan.make(
            _SsspPlan.DeltaTile_2(default_value(delta, default.delta()),
                                  default_value(edge_tile_size, default.edge_tile_size())))

    @staticmethod
    def delta_step(delta=None):
        if delta is None:
            return SsspPlan.make(_SsspPlan.DeltaStep())
        return SsspPlan.make(_SsspPlan.DeltaStep_1(delta))

    @staticmethod
    def delta_step_barrier(delta=None):
        if delta is None:
            return SsspPlan.make(_SsspPlan.DeltaStepBarrier())
        return SsspPlan.make(_SsspPlan.DeltaStepBarrier_1(delta))

    @staticmethod
    def delta_step_barrier(delta=None):
        if delta is None:
            return SsspPlan.make(_SsspPlan.DeltaStepBarrier())
        return SsspPlan.make(_SsspPlan.DeltaStepBarrier_1(delta))

    @staticmethod
    def serial_delta_tile(delta=None, edge_tile_size=None):
        default = _SsspPlan.SerialDeltaTile()
        return SsspPlan.make(
            _SsspPlan.SerialDeltaTile_2(default_value(delta, default.delta()),
                                        default_value(edge_tile_size, default.edge_tile_size())))

    @staticmethod
    def serial_delta_step(delta=None):
        if delta is None:
            return SsspPlan.make(_SsspPlan.SerialDelta())
        return SsspPlan.make(_SsspPlan.SerialDelta_1(delta))

    @staticmethod
    def dijkstra_tile(edge_tile_size=None):
        if edge_tile_size is None:
            return SsspPlan.make(_SsspPlan.DijkstraTile())
        return SsspPlan.make(_SsspPlan.DijkstraTile_1(edge_tile_size))

    @staticmethod
    def dijkstra():
        return SsspPlan.make(_SsspPlan.Dijkstra())

    @staticmethod
    def topo_tile(edge_tile_size=None):
        if edge_tile_size is None:
            return SsspPlan.make(_SsspPlan.TopoTile())
        return SsspPlan.make(_SsspPlan.TopoTile_1(edge_tile_size))

    @staticmethod
    def topo():
        return SsspPlan.make(_SsspPlan.Topo())


def sssp(PropertyGraph pg, size_t start_node, str edge_weight_property_name, str output_property_name,
         SsspPlan plan = SsspPlan()):
    edge_weight_property_name_bytes = bytes(edge_weight_property_name, "utf-8")
    edge_weight_property_name_cstr = <string>edge_weight_property_name_bytes
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Sssp(pg.underlying.get(), start_node, edge_weight_property_name_cstr,
                                output_property_name_cstr, plan.underlying))

def sssp_validate(PropertyGraph pg, size_t start_node, str edge_weight_property_name, str output_property_name):
    edge_weight_property_name_bytes = bytes(edge_weight_property_name, "utf-8")
    edge_weight_property_name_cstr = <string>edge_weight_property_name_bytes
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        r = handle_result_bool(SsspValidate(pg.underlying.get(), start_node, edge_weight_property_name_cstr, output_property_name_cstr))
    return r

cdef _SsspStatistics handle_result_SsspStatistics(std_result[_SsspStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef class SsspStatistics:
    cdef _SsspStatistics underlying

    def __init__(self, PropertyGraph pg, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_SsspStatistics(_SsspStatistics.Compute(
                pg.underlying.get(), output_property_name_cstr))

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
