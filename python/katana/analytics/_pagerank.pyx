from libcpp.string cimport string

from katana.cpp.libstd.boost cimport handle_result_void, handle_result_assert, raise_error_code, std_result
from katana.cpp.libstd.iostream cimport ostringstream, ostream
from katana.cpp.libgalois.graphs.Graph cimport PropertyFileGraph
from katana.analytics.plan cimport Plan, _Plan
from katana.property_graph cimport PropertyGraph

from enum import Enum


cdef extern from "katana/analytics/pagerank/pagerank.h" namespace "katana::analytics" nogil:
    cppclass _PagerankPlan "katana::analytics::PagerankPlan" (_Plan):
        enum Algorithm:
            kPullTopological "katana::analytics::PagerankPlan::kPullTopological"
            kPullResidual "katana::analytics::PagerankPlan::kPullResidual"
            kPushSynchronous "katana::analytics::PagerankPlan::kPushSynchronous"
            kPushAsynchronous "katana::analytics::PagerankPlan::kPushAsynchronous"

        # unsigned int kChunkSize

        _PagerankPlan.Algorithm algorithm() const
        float tolerance() const
        unsigned int max_iterations() const
        float alpha() const
        float initial_residual() const

        PagerankPlan()

        @staticmethod
        _PagerankPlan PullTopological(float tolerance, unsigned int max_iterations, float alpha)
        @staticmethod
        _PagerankPlan PullResidual(float tolerance, unsigned int max_iterations, float alpha)
        @staticmethod
        _PagerankPlan PushAsynchronous(float tolerance, float alpha)
        @staticmethod
        _PagerankPlan PushSynchronous(float tolerance, unsigned int max_iterations, float alpha)

    std_result[void] Pagerank(PropertyFileGraph* pfg, string output_property_name, _PagerankPlan plan)

    std_result[void] PagerankAssertValid(PropertyFileGraph* pfg, string output_property_name)

    cppclass _PagerankStatistics "katana::analytics::PagerankStatistics":
        float max_rank
        float min_rank
        float average_rank

        void Print(ostream os)

        @staticmethod
        std_result[_PagerankStatistics] Compute(PropertyFileGraph* pfg, string output_property_name)


class _PagerankPlanAlgorithm(Enum):
    PullTopological = _PagerankPlan.Algorithm.kPullTopological
    PullResidual = _PagerankPlan.Algorithm.kPullResidual
    PushSynchronous = _PagerankPlan.Algorithm.kPushSynchronous
    PushAsynchronous = _PagerankPlan.Algorithm.kPushAsynchronous


cdef class PagerankPlan(Plan):
    cdef:
        _PagerankPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _PagerankPlanAlgorithm

    @staticmethod
    cdef PagerankPlan make(_PagerankPlan u):
        f = <PagerankPlan>PagerankPlan.__new__(PagerankPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> _PagerankPlanAlgorithm:
        return _PagerankPlanAlgorithm(self.underlying_.algorithm())

    @property
    def tolerance(self) -> float:
        return self.underlying_.tolerance()

    @property
    def max_iterations(self) -> int:
        return self.underlying_.max_iterations()

    @property
    def alpha(self) -> float:
        return self.underlying_.alpha()

    @property
    def initial_residual(self) -> float:
        return self.underlying_.initial_residual()

    @staticmethod
    def pull_topological(float tolerance, unsigned int max_iterations, float alpha):
        return PagerankPlan.make(_PagerankPlan.PullTopological(tolerance, max_iterations, alpha))

    @staticmethod
    def pull_residual(float tolerance, unsigned int max_iterations, float alpha):
        return PagerankPlan.make(_PagerankPlan.PullResidual(tolerance, max_iterations, alpha))

    @staticmethod
    def push_asynchronous(float tolerance, float alpha):
        return PagerankPlan.make(_PagerankPlan.PushAsynchronous(tolerance, alpha))

    @staticmethod
    def push_synchronous(float tolerance, unsigned int max_iterations, float alpha):
        return PagerankPlan.make(_PagerankPlan.PushSynchronous(tolerance, max_iterations, alpha))


def pagerank(PropertyGraph pg, str output_property_name,
             PagerankPlan plan = PagerankPlan()):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Pagerank(pg.underlying.get(), output_property_name_cstr, plan.underlying_))


def pagerank_assert_valid(PropertyGraph pg, str output_property_name):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(PagerankAssertValid(pg.underlying.get(), output_property_name_cstr))


cdef _PagerankStatistics handle_result_PagerankStatistics(std_result[_PagerankStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class PagerankStatistics:
    cdef _PagerankStatistics underlying

    def __init__(self, PropertyGraph pg, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_PagerankStatistics(_PagerankStatistics.Compute(
                pg.underlying.get(), output_property_name_cstr))

    @property
    def max_rank(self) -> float:
        return self.underlying.max_rank

    @property
    def min_rank(self) -> float:
        return self.underlying.min_rank

    @property
    def average_rank(self) -> float:
        return self.underlying.average_rank

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
