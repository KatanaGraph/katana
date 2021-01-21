from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

from katana.cpp.libstd.boost cimport handle_result_void, handle_result_assert, raise_error_code, std_result
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libgalois.graphs.Graph cimport PropertyFileGraph
from katana.analytics.plan cimport Plan, _Plan
from katana.property_graph cimport PropertyGraph

from enum import Enum


cdef extern from "katana/analytics/k_truss/k_truss.h" namespace "katana::analytics" nogil:
    cppclass _KTrussPlan "katana::analytics::KTrussPlan" (_Plan):
        enum Algorithm:
            kBsp "katana::analytics::KTrussPlan::kBsp"
            kBspJacobi "katana::analytics::KTrussPlan::kBspJacobi"
            kBspCoreThenTruss "katana::analytics::KTrussPlan::kBspCoreThenTruss"

        _KTrussPlan.Algorithm algorithm() const

        KTrussPlan()

        @staticmethod
        _KTrussPlan Bsp()
        @staticmethod
        _KTrussPlan BspJacobi()
        @staticmethod
        _KTrussPlan BspCoreThenTruss()

    std_result[void] KTruss(PropertyFileGraph* pfg, uint32_t k_truss_number,string output_property_name, _KTrussPlan plan)

    std_result[void] KTrussAssertValid(PropertyFileGraph* pfg, uint32_t k_truss_number,
                                      string output_property_name)

    cppclass _KTrussStatistics "katana::analytics::KTrussStatistics":
        uint64_t number_of_edges_left

        void Print(ostream os)

        @staticmethod
        std_result[_KTrussStatistics] Compute(PropertyFileGraph* pfg, uint32_t k_truss_number,
                                             string output_property_name)


class _KTrussPlanAlgorithm(Enum):
    Bsp = _KTrussPlan.Algorithm.kBsp
    BspJacobi = _KTrussPlan.Algorithm.kBspJacobi
    BspCoreThenTruss = _KTrussPlan.Algorithm.kBspCoreThenTruss


cdef class KTrussPlan(Plan):
    cdef:
        _KTrussPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _KTrussPlanAlgorithm

    @staticmethod
    cdef KTrussPlan make(_KTrussPlan u):
        f = <KTrussPlan>KTrussPlan.__new__(KTrussPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> KTrussPlan.Algorithm:
        return self.underlying_.algorithm()

    @staticmethod
    def bsp() -> KTrussPlan:
        return KTrussPlan.make(_KTrussPlan.Bsp())
    @staticmethod
    def bsp_jacobi() -> KTrussPlan:
        return KTrussPlan.make(_KTrussPlan.BspJacobi())
    @staticmethod
    def bsp_core_then_truss() -> KTrussPlan:
        return KTrussPlan.make(_KTrussPlan.BspCoreThenTruss())


def k_truss(PropertyGraph pg, uint32_t k_truss_number, str output_property_name, KTrussPlan plan = KTrussPlan()) -> int:
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        v = handle_result_void(KTruss(pg.underlying.get(),k_truss_number,output_property_name_str, plan.underlying_))
    return v


def k_truss_assert_valid(PropertyGraph pg, uint32_t k_truss_number, str output_property_name):
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        handle_result_assert(KTrussAssertValid(pg.underlying.get(), k_truss_number, output_property_name_str))


cdef _KTrussStatistics handle_result_KTrussStatistics(std_result[_KTrussStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class KTrussStatistics:
    cdef _KTrussStatistics underlying

    def __init__(self, PropertyGraph pg, uint32_t k_truss_number, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_KTrussStatistics(_KTrussStatistics.Compute(
                pg.underlying.get(), k_truss_number, output_property_name_str))

    @property
    def number_of_edges_left(self) -> uint64_t:
        return self.underlying.number_of_edges_left

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
