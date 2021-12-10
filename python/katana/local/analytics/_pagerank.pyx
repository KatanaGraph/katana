"""
Page Rank
---------

.. autoclass:: katana.local.analytics.PagerankPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. [WHANG] WHANG, Joyce Jiyoung, et al. Scalable data-driven pagerank: Algorithms,
    system issues, and lessons learned. In: European Conference on Parallel
    Processing. Springer, Berlin, Heidelberg, 2015. p. 438-450.

.. autoclass:: katana.local.analytics._pagerank._PagerankPlanAlgorithm
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.pagerank

.. autoclass:: katana.local.analytics.PagerankStatistics
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.pagerank_assert_valid
"""
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._graph cimport Graph, TxnContext
from katana.local.analytics.plan cimport Plan, _Plan

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

    double kDefaultTolerance "katana::analytics::PagerankPlan::kDefaultTolerance"
    int kDefaultMaxIterations "katana::analytics::PagerankPlan::kDefaultMaxIterations"
    double kDefaultAlpha "katana::analytics::PagerankPlan::kDefaultAlpha"

    Result[void] Pagerank(_PropertyGraph* pg, string output_property_name, CTxnContext* txn_ctx, _PagerankPlan plan)

    Result[void] PagerankAssertValid(_PropertyGraph* pg, string output_property_name)

    cppclass _PagerankStatistics "katana::analytics::PagerankStatistics":
        float max_rank
        float min_rank
        float average_rank

        void Print(ostream os)

        @staticmethod
        Result[_PagerankStatistics] Compute(_PropertyGraph* pg, string output_property_name)


class _PagerankPlanAlgorithm(Enum):
    PullTopological = _PagerankPlan.Algorithm.kPullTopological
    PullResidual = _PagerankPlan.Algorithm.kPullResidual
    PushSynchronous = _PagerankPlan.Algorithm.kPushSynchronous
    PushAsynchronous = _PagerankPlan.Algorithm.kPushAsynchronous


cdef class PagerankPlan(Plan):
    """
    A computational :ref:`Plan` for Page Rank.

    Static methods construct PagerankPlans.
    """
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
    def pull_topological(float tolerance = kDefaultTolerance, unsigned int max_iterations = kDefaultMaxIterations, float alpha = kDefaultAlpha):
        """
        Topological pull algorithm

        The graph must be transposed to use this algorithm.
        """
        return PagerankPlan.make(_PagerankPlan.PullTopological(tolerance, max_iterations, alpha))

    @staticmethod
    def pull_residual(float tolerance = kDefaultTolerance, unsigned int max_iterations = kDefaultMaxIterations, float alpha = kDefaultAlpha):
        """
        Delta-residual pull algorithm

        The graph must be transposed to use this algorithm.
        """
        return PagerankPlan.make(_PagerankPlan.PullResidual(tolerance, max_iterations, alpha))

    @staticmethod
    def push_asynchronous(float tolerance = kDefaultTolerance, float alpha = kDefaultAlpha):
        """
        Asynchronous push algorithm

        This implementation is based on the Push-based PageRank computation
        (Algorithm 4) as described in the PageRank Europar 2015 paper [WHANG]_.

        """
        return PagerankPlan.make(_PagerankPlan.PushAsynchronous(tolerance, alpha))

    @staticmethod
    def push_synchronous(float tolerance = kDefaultTolerance, unsigned int max_iterations = kDefaultMaxIterations, float alpha = kDefaultAlpha):
        """
        Synchronous push algorithm

        This implementation is based on the Push-based PageRank computation
        (Algorithm 4) as described in the PageRank Europar 2015 paper [WHANG]_.
        """
        return PagerankPlan.make(_PagerankPlan.PushSynchronous(tolerance, max_iterations, alpha))


def pagerank(Graph pg, str output_property_name, PagerankPlan plan = PagerankPlan(), *, TxnContext txn_ctx = None):
    """
    Compute the Page Rank of each node in the graph.

    :type pg: katana.local.Graph
    :param pg: The graph to analyze.
    :type output_property_name: str
    :param output_property_name: The output property to store the rank. This property must not already exist.
    :type plan: PagerankPlan
    :param plan: The execution plan to use.
    :param txn_ctx: The tranaction context for passing read write sets.

    .. code-block:: python

        import katana.local
        from katana.example_data import get_input
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_input("propertygraphs/ldbc_003"))
        from katana.analytics import pagerank, PagerankStatistics
        property_name = "NewProp"

        pagerank(graph, property_name)

        stats = PagerankStatistics(graph, property_name)
        print("Min Rank:", stats.min_rank)
        print("Max Rank:", stats.max_rank)
        print("Average Rank:", stats.average_rank)

    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        handle_result_void(Pagerank(pg.underlying_property_graph(), output_property_name_cstr, &txn_ctx._txn_ctx, plan.underlying_))


def pagerank_assert_valid(Graph pg, str output_property_name):
    """
    Raise an exception if the pagerank results in `pg` are invalid. This is not an exhaustive check, just a sanity check.

    :raises: AssertionError
    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(PagerankAssertValid(pg.underlying_property_graph(), output_property_name_cstr))


cdef _PagerankStatistics handle_result_PagerankStatistics(Result[_PagerankStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class PagerankStatistics:
    """
    Compute the :ref:`statistics` of a Page Rank result.

    All statistics are `float`.
    """
    cdef _PagerankStatistics underlying

    def __init__(self, Graph pg, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_PagerankStatistics(_PagerankStatistics.Compute(
                pg.underlying_property_graph(), output_property_name_cstr))

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
