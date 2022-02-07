"""
Subgraph Extraction
-------------------

.. autoclass:: katana.local.analytics.SubGraphExtractionPlan


.. autofunction:: katana.local.analytics.subgraph_extraction
"""
from libc.stdint cimport uint32_t, uintptr_t
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector
from pyarrow.lib cimport to_shared

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result, raise_error_code
from katana.local import Graph, TxnContext
from katana.local._graph cimport underlying_property_graph
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


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
    """
    A computational :ref:`Plan` for subgraph extraction.

    Static methods construct SubGraphExtractionPlan. The constructor will select a reasonable default plan.
    """
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
        """
        The node-set algorithm.
        """
        return SubGraphExtractionPlan.make(_SubGraphExtractionPlan.NodeSet())


cdef shared_ptr[_PropertyGraph] handle_result_property_graph(Result[unique_ptr[_PropertyGraph]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return to_shared(res.value())


def subgraph_extraction(pg, node_vec, SubGraphExtractionPlan plan = SubGraphExtractionPlan()) -> Graph:
    """
    Given a set of node ids, this algorithm constructs a new sub-graph which contains all nodes in the set and edges
    between them.
    """
    cdef vector[uint32_t] vec = [<uint32_t>n for n in node_vec]
    with nogil:
        v = handle_result_property_graph(SubGraphExtraction(underlying_property_graph(pg), vec, plan.underlying_))
    return Graph._make_from_address_shared(<uintptr_t>&v)
