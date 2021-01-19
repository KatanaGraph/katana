from libcpp.string cimport string
from libc.stdint cimport uint32_t

from katana.cpp.libstd.boost cimport handle_result_void, handle_result_assert, raise_error_code, std_result
from katana.cpp.libstd.iostream cimport ostringstream, ostream
from katana.cpp.libgalois.graphs.Graph cimport PropertyFileGraph
from katana.analytics.plan cimport Plan, _Plan
from katana.property_graph cimport PropertyGraph

from enum import Enum


cdef extern from "katana/analytics/independent_set/independent_set.h" namespace "katana::analytics" nogil:
    cppclass _IndependentSetPlan "katana::analytics::IndependentSetPlan" (_Plan):
        enum Algorithm:
            kSerial "katana::analytics::IndependentSetPlan::kSerial"
            kPull "katana::analytics::IndependentSetPlan::kPull"
            kPriority "katana::analytics::IndependentSetPlan::kPriority"
            kEdgeTiledPriority "katana::analytics::IndependentSetPlan::kEdgeTiledPriority"

        # unsigned int kChunkSize

        _IndependentSetPlan.Algorithm algorithm() const

        IndependentSetPlan()

        @staticmethod
        _IndependentSetPlan FromAlgorithm(_IndependentSetPlan.Algorithm algorithm);

        @staticmethod
        _IndependentSetPlan Serial()
        @staticmethod
        _IndependentSetPlan Pull()
        @staticmethod
        _IndependentSetPlan Priority()
        @staticmethod
        _IndependentSetPlan EdgeTiledPriority()

    std_result[void] IndependentSet(PropertyFileGraph* pfg, string output_property_name, _IndependentSetPlan plan)

    std_result[void] IndependentSetAssertValid(PropertyFileGraph* pfg, string output_property_name)

    cppclass _IndependentSetStatistics "katana::analytics::IndependentSetStatistics":
        uint32_t cardinality

        void Print(ostream os)

        @staticmethod
        std_result[_IndependentSetStatistics] Compute(PropertyFileGraph* pfg, string output_property_name)


class _IndependentSetPlanAlgorithm(Enum):
    Serial = _IndependentSetPlan.Algorithm.kSerial
    Pull = _IndependentSetPlan.Algorithm.kPull
    Priority = _IndependentSetPlan.Algorithm.kPriority
    EdgeTiledPriority = _IndependentSetPlan.Algorithm.kEdgeTiledPriority


cdef class IndependentSetPlan(Plan):
    cdef:
        _IndependentSetPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _IndependentSetPlanAlgorithm

    @staticmethod
    cdef IndependentSetPlan make(_IndependentSetPlan u):
        f = <IndependentSetPlan>IndependentSetPlan.__new__(IndependentSetPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> _IndependentSetPlanAlgorithm:
        return _IndependentSetPlanAlgorithm(self.underlying_.algorithm())

    @staticmethod
    def serial():
        return IndependentSetPlan.make(_IndependentSetPlan.Serial())

    @staticmethod
    def pull():
        return IndependentSetPlan.make(_IndependentSetPlan.Pull())

    @staticmethod
    def priority():
        return IndependentSetPlan.make(_IndependentSetPlan.Priority())

    @staticmethod
    def edge_tiled_priority():
        return IndependentSetPlan.make(_IndependentSetPlan.EdgeTiledPriority())


def independent_set(PropertyGraph pg, str output_property_name,
             IndependentSetPlan plan = IndependentSetPlan()):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(IndependentSet(pg.underlying.get(), output_property_name_cstr, plan.underlying_))


def independent_set_assert_valid(PropertyGraph pg, str output_property_name):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(IndependentSetAssertValid(pg.underlying.get(), output_property_name_cstr))


cdef _IndependentSetStatistics handle_result_IndependentSetStatistics(std_result[_IndependentSetStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class IndependentSetStatistics:
    cdef _IndependentSetStatistics underlying

    def __init__(self, PropertyGraph pg, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_IndependentSetStatistics(_IndependentSetStatistics.Compute(
                pg.underlying.get(), output_property_name_cstr))

    @property
    def cardinality(self) -> int:
        return self.underlying.cardinality

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
