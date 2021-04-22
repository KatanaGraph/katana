from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport handle_result_void, handle_result_assert, raise_error_code, Result
from katana.analytics.plan cimport Plan, _Plan
from katana.property_graph cimport PropertyGraph

from libcpp.vector cimport vector
from libc.stdint cimport uint32_t
from pyarrow.lib cimport to_shared
from libcpp.memory cimport shared_ptr, unique_ptr
from katana.cpp.libsupport.result cimport Result

from enum import Enum

# TODO(amp): Module needs documenting.


cdef extern from "katana/analytics/subgraph_extraction/subgraph_extraction.h" namespace "katana::analytics" nogil:
    cppclass _SubGraphExtractionPlan "katana::analytics::SubGraphExtractionPlan" (_Plan):
        enum Algorithm:
            kNodeSet "katana::analytics::SubGraphExtractionPlan::kNodeSet"

        _SubGraphExtractionPlan.Algorithm algorithm() const

        # SubGraphExtractionPlan()

        @staticmethod
        _SubGraphExtractionPlan NodeSet(
            )

    Result[unique_ptr[_PropertyGraph]] SubGraphExtraction(_PropertyGraph* pfg, const vector[uint32_t]& node_vec, _SubGraphExtractionPlan plan)


class _SubGraphExtractionPlanAlgorithm(Enum):
    NodeSet = _SubGraphExtractionPlan.Algorithm.kNodeSet


cdef class SubGraphExtractionPlan(Plan):
    cdef:
        _SubGraphExtractionPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _SubGraphExtractionPlanAlgorithm

    @staticmethod
    cdef SubGraphExtractionPlan make(_SubGraphExtractionPlan u):
        f = <SubGraphExtractionPlan>SubGraphExtractionPlan.__new__(SubGraphExtractionPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> Algorithm:
        return _SubGraphExtractionPlanAlgorithm(self.underlying_.algorithm())

    @staticmethod
    def node_set() -> SubGraphExtractionPlan:
        return SubGraphExtractionPlan.make(_SubGraphExtractionPlan.NodeSet())


cdef shared_ptr[_PropertyGraph] handle_result_property_graph(Result[unique_ptr[_PropertyGraph]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return to_shared(res.value())


def subgraph_extraction(PropertyGraph pg, node_vec, SubGraphExtractionPlan plan = SubGraphExtractionPlan()) -> PropertyGraph:
    cdef vector[uint32_t] vec = [<uint32_t>n for n in node_vec]
    with nogil:
        v = handle_result_property_graph(SubGraphExtraction(pg.underlying.get(), vec, plan.underlying_))
    return PropertyGraph.make(v)
