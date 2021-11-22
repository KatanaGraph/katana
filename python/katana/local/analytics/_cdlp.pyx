"""
Community Detection using Label Propagation (CDLP)
--------------------------------------------------

.. autoclass:: katana.local.analytics.CdlpPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. [Raghavan] U. N. Raghavan, R. Albert and S. Kumara, "Near linear time algorithm
            to detect community structures in large-scale networks,"  In: Physical
            Review E 76.3 (2007), p. 036106.

.. [Graphalytics] A. Iosup , A. Musaafir, A. Uta et al., "The LDBC Graphalytics
                  Benchmark," arXiv preprint arXiv:2011.15028 (2020).

.. autoclass:: katana.local.analytics._cdlp._CdlpPlanAlgorithm
    :members:
    :undoc-members:


.. autofunction:: katana.local.analytics.cdlp

.. autoclass:: katana.local.analytics.CdlpStatistics
    :members:
    :undoc-members:
"""
from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._graph cimport Graph
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/cdlp/cdlp.h" namespace "katana::analytics" nogil:
    cppclass _CdlpPlan "katana::analytics::CdlpPlan"(_Plan):
        enum Algorithm:
            kSynchronous "katana::analytics::CdlpPlan::kSynchronous"
            #kAsynchronous "katana::analytics::CdlpPlan::kAsynchronous"

        _CdlpPlan.Algorithm algorithm() const

        CdlpPlan()

        @staticmethod
        _CdlpPlan Synchronous()

        #@staticmethod
        #_CdlpPlan Asynchronous()

    Result[void] Cdlp(_PropertyGraph*pg, string output_property_name, int max_iteration,
                                     _CdlpPlan plan)

    cppclass _CdlpStatistics "katana::analytics::CdlpStatistics":
        uint64_t total_communities
        uint64_t total_non_trivial_communities
        uint64_t largest_community_size
        double largest_community_ratio

        void Print(ostream os)

        @staticmethod
        Result[_CdlpStatistics] Compute(_PropertyGraph*pg, string output_property_name)


class _CdlpPlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.CdlpPlan` constructors for algorithm documentation.
    """
    Synchronous = _CdlpPlan.Algorithm.kSynchronous
    #Asynchronous = _CdlpPlan.Algorithm.kAsynchronous


cdef class CdlpPlan(Plan):
    """
    A computational :ref:`Plan` for CDLP.

    Static method construct CdlpPlans using specific algorithms with their required parameters. All
    parameters are optional and have reasonable defaults.
    """
    cdef:
        _CdlpPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _CdlpPlanAlgorithm

    @staticmethod
    cdef CdlpPlan make(_CdlpPlan u):
        f = <CdlpPlan> CdlpPlan.__new__(CdlpPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> _CdlpPlanAlgorithm:
        return self.underlying_.algorithm()

    @staticmethod
    def synchronous() -> CdlpPlan:
        """
        Synchronous community detection algorithm. A deterministic synchronous algorithm
        [Graphalytics]_ based on the algorithm introduced in [Raghavan]_.
        """
        return CdlpPlan.make(_CdlpPlan.Synchronous())

    #@staticmethod
    #def asynchronous() -> CdlpPlan:
        #"""
        #Unlike Synchronous algorithm, Asynchronous can use the current iteration
        #updated community IDs for some of the neighbors that have been already
        #updated in the current iteration and use the old values for the other neighbors
        #based on [Raghavan]_.
        #"""
        #return CdlpPlan.make(_CdlpPlan.Asynchronous())

def cdlp(Graph pg, str output_property_name,
                         int max_iteration = 10 ,CdlpPlan plan = CdlpPlan()) -> int:
    """
    Compute the CDLP for `pg`.

    :type pg: katana.local.Graph
    :param pg: The graph to analyze.
    :type output_property_name: str
    :param output_property_name: The output property to write path lengths into. This property must not already exist.
    :type plan: CdlpPlan
    :param plan: The execution plan to use. Defaults to heuristically selecting the plan.

    .. code-block:: python

        import katana.local
        from katana.example_data import get_input
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_input("propertygraphs/ldbc_003"))
        from katana.analytics import cdlp, CdlpStatistics
        cdlp(graph, "output")

        stats = CdlpStatistics(graph, "output")

        print("Total Communities:", stats.total_communitiess)
        print("Total Non-Trivial Communities:", stats.total_non_trivial_communities)
        print("Largest Community Size:", stats.largest_community_size)
        print("Largest Community Ratio:", stats.largest_community_ratio)

    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        v = handle_result_void(Cdlp(pg.underlying_property_graph(), output_property_name_str, max_iteration, plan.underlying_))
    return v

cdef _CdlpStatistics handle_result_CdlpStatistics(
        Result[_CdlpStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()

cdef class CdlpStatistics:
    """
    Compute the :ref:`statistics` of a CDLP computation on a graph.
    """
    cdef _CdlpStatistics underlying

    def __init__(self, Graph pg, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_CdlpStatistics(_CdlpStatistics.Compute(
                pg.underlying_property_graph(), output_property_name_str))

    @property
    def total_communities(self) -> uint64_t:
        return self.underlying.total_communities

    @property
    def total_non_trivial_communities(self) -> uint64_t:
        return self.underlying.total_non_trivial_communities

    @property
    def largest_community_size(self) -> uint64_t:
        return self.underlying.largest_community_size

    @property
    def largest_community_ratio(self) -> double:
        """
        The fraction of the entire graph that is part of the largest community.
        """
        return self.underlying.largest_community_ratio

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
