"""
Jaccard Similarity
------------------

.. autoclass:: katana.local.analytics.JaccardPlan


.. autoclass:: katana.local.analytics._jaccard._JaccardEdgeSorting


.. autofunction:: katana.local.analytics.jaccard

.. autoclass:: katana.local.analytics.JaccardStatistics


.. autofunction:: katana.local.analytics.jaccard_assert_valid
"""

from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code

from katana.local import Graph, TxnContext

from katana.local._graph cimport underlying_property_graph, underlying_txn_context
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/jaccard/jaccard.h" namespace "katana::analytics" nogil:
    cppclass _JaccardPlan "katana::analytics::JaccardPlan" (_Plan):
        enum EdgeSorting:
            kSorted "katana::analytics::JaccardPlan::kSorted"
            kUnsorted "katana::analytics::JaccardPlan::kUnsorted"
            kUnknown "katana::analytics::JaccardPlan::kUnknown"

        _JaccardPlan.EdgeSorting edge_sorting() const

        _JaccardPlan()

        @staticmethod
        _JaccardPlan Sorted()

        @staticmethod
        _JaccardPlan Unsorted()

    Result[void] Jaccard(_PropertyGraph* pg, size_t compare_node,
        string output_property_name, CTxnContext* txn_ctx, _JaccardPlan plan)

    Result[void] JaccardAssertValid(_PropertyGraph* pg, size_t compare_node,
        string output_property_name)

    cppclass _JaccardStatistics  "katana::analytics::JaccardStatistics":
        double max_similarity
        double min_similarity
        double average_similarity
        void Print(ostream os)

        @staticmethod
        Result[_JaccardStatistics] Compute(_PropertyGraph* pg, size_t compare_node,
            string output_property_name)


class _JaccardEdgeSorting(Enum):
    """
    The ordering of edges in the input graph.

    :see: :py:class:`~katana.local.analytics.JaccardPlan` constructors for algorithm documentation.
    """
    Sorted = _JaccardPlan.EdgeSorting.kSorted
    Unsorted = _JaccardPlan.EdgeSorting.kUnsorted
    Unknown = _JaccardPlan.EdgeSorting.kUnknown


cdef class JaccardPlan(Plan):
    """
    A computational :ref:`Plan` for Jaccard Similarity.

    Static methods construct JaccardPlans. The constructor will handle any edge ordering.
    """
    cdef:
        _JaccardPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    EdgeSorting = _JaccardEdgeSorting

    @staticmethod
    cdef JaccardPlan make(_JaccardPlan u):
        f = <JaccardPlan>JaccardPlan.__new__(JaccardPlan)
        f.underlying_ = u
        return f

    @property
    def edge_sorting(self) -> _JaccardEdgeSorting:
        """
        The ordering of input graph edges.

        :rtype: EdgeSorting
        """
        return _JaccardEdgeSorting(self.underlying_.edge_sorting())

    def __init__(self):
        """
        May attempt sorted intersections, but will fall back on exhaustive intersections.

        The edge ordering is not known.
        """
        super(JaccardPlan, self).__init__()

    @staticmethod
    def sorted():
        """
        Use the sorted-list intersection algorithm .

        The edges of the graph must be ordered by target node ID.
        """
        return JaccardPlan.make(_JaccardPlan.Sorted())

    @staticmethod
    def unsorted():
        """
        Use the exhaustive intersection algorithm.

        The edges need not be ordered.
        """
        return JaccardPlan.make(_JaccardPlan.Unsorted())


def jaccard(pg, size_t compare_node, str output_property_name,
            JaccardPlan plan = JaccardPlan(), *, txn_ctx = None):
    """
    Compute the Jaccard Similarity between `compare_node` and all nodes in the graph.

    :type pg: katana.local.Graph
    :param pg: The graph to analyze.
    :type compare_node: node ID
    :param compare_node: The node to compare to all nodes.
    :type output_property_name: str
    :param output_property_name: The output property for similarities. This property must not already exist.
    :type plan: JaccardPlan
    :param plan: The execution plan to use.
    :param txn_ctx: The transaction context for passing read write sets.

    .. code-block:: python

        import katana.local
        from katana.example_data import get_rdg_dataset
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_rdg_dataset("ldbc_003"))
        from katana.analytics import jaccard, JaccardStatistics
        property_name = "NewProp"
        compare_node = 0

        jaccard(graph, compare_node, property_name)
        stats = JaccardStatistics(graph, compare_node, property_name)

        print("Max Similarity:", stats.max_similarity)
        print("Min Similarity:", stats.min_similarity)
        print("Average Similarity:", stats.average_similarity)

    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        handle_result_void(Jaccard(underlying_property_graph(pg), compare_node, output_property_name_cstr, underlying_txn_context(txn_ctx), plan.underlying_))


def jaccard_assert_valid(pg, size_t compare_node, str output_property_name):
    """
    Raise an exception if the Jaccard Similarity results in `pg` are invalid. This is not an exhaustive check, just a
    sanity check.

    :raises: AssertionError
    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(JaccardAssertValid(underlying_property_graph(pg), compare_node, output_property_name_cstr))


cdef _JaccardStatistics handle_result_JaccardStatistics(Result[_JaccardStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class JaccardStatistics:
    """
    Compute the :ref:`statistics` of a Jaccard Similarity result.

    All statistics are `floats`.
    """
    cdef _JaccardStatistics underlying

    def __init__(self, pg, size_t compare_node, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_JaccardStatistics(_JaccardStatistics.Compute(
                underlying_property_graph(pg), compare_node, output_property_name_cstr))

    @property
    def max_similarity(self):
        return self.underlying.max_similarity

    @property
    def min_similarity(self):
        return self.underlying.min_similarity

    @property
    def average_similarity(self):
        return self.underlying.average_similarity

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
