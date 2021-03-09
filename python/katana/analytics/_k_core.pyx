from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport handle_result_void, handle_result_assert, raise_error_code, Result
from katana.analytics.plan cimport Plan, _Plan
from katana.property_graph cimport PropertyGraph

from enum import Enum


cdef extern from "katana/analytics/k_core/k_core.h" namespace "katana::analytics" nogil:
    cppclass _KCorePlan "katana::analytics::KCorePlan" (_Plan):
        enum Algorithm:
            kSynchronous "katana::analytics::KCorePlan::kSynchronous"
            kAsynchronous "katana::analytics::KCorePlan::kAsynchronous"

        _KCorePlan.Algorithm algorithm() const

        KCorePlan()

        @staticmethod
        _KCorePlan Synchronous()
        @staticmethod
        _KCorePlan Asynchronous()

    Result[void] KCore(_PropertyGraph* pg, uint32_t k_core_number, string output_property_name, _KCorePlan plan)


    Result[void] KCoreAssertValid(_PropertyGraph* pg, uint32_t k_core_number, string output_property_name)

    cppclass _KCoreStatistics "katana::analytics::KCoreStatistics":
        uint64_t number_of_nodes_in_kcore

        void Print(ostream os)

        @staticmethod
        Result[_KCoreStatistics] Compute(_PropertyGraph* pg, uint32_t k_core_number, string output_property_name)


class _KCorePlanAlgorithm(Enum):
    Synchronous = _KCorePlan.Algorithm.kSynchronous
    Asynchronous = _KCorePlan.Algorithm.kAsynchronous


cdef class KCorePlan(Plan):
    cdef:
        _KCorePlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _KCorePlanAlgorithm

    @staticmethod
    cdef KCorePlan make(_KCorePlan u):
        f = <KCorePlan>KCorePlan.__new__(KCorePlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> KCorePlan.Algorithm:
        return self.underlying_.algorithm()

    @staticmethod
    def synchronous() -> KCorePlan:
        return KCorePlan.make(_KCorePlan.Synchronous())
    @staticmethod
    def asynchronous() -> KCorePlan:
        return KCorePlan.make(_KCorePlan.Asynchronous())


def k_core(PropertyGraph pg, uint32_t k_core_number, str output_property_name, KCorePlan plan = KCorePlan()) -> int:
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        v = handle_result_void(KCore(pg.underlying.get(), k_core_number, output_property_name_str, plan.underlying_))
    return v


def k_core_assert_valid(PropertyGraph pg, uint32_t k_core_number, str output_property_name):
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        handle_result_assert(KCoreAssertValid(pg.underlying.get(), k_core_number, output_property_name_str))


cdef _KCoreStatistics handle_result_KCoreStatistics(Result[_KCoreStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class KCoreStatistics:
    cdef _KCoreStatistics underlying

    def __init__(self, PropertyGraph pg, uint32_t k_core_number, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_KCoreStatistics(_KCoreStatistics.Compute(
                pg.underlying.get(), k_core_number, output_property_name_str))

    @property
    def number_of_nodes_in_kcore(self) -> uint64_t:
        return self.underlying.number_of_nodes_in_kcore

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
