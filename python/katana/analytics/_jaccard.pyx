"""
Jaccard Similarity
------------------

.. autoclass:: katana.analytics.JaccardPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. autoclass:: katana.analytics._jaccard._JaccardEdgeSorting

.. autofunction:: katana.analytics.jaccard

.. autoclass:: katana.analytics.JaccardStatistics
    :members:
    :undoc-members:

.. autofunction:: katana.analytics.jaccard_assert_valid
"""

from libcpp.string cimport string

from katana._property_graph cimport PropertyGraph
from katana.analytics.plan cimport Plan, _Plan
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code

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
        string output_property_name, _JaccardPlan plan)

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

    Sorted
        The edges are ordered by target node ID. A sorted-list intersection algorithm is used.
    Unsorted
        The edges are not ordered. An exhaustive intersection algorithm is used.
    Unknown
        The edge ordering is not known. May attempt sorted intersections, but will fall back on exhaustive intersections.
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

    @staticmethod
    def sorted():
        return JaccardPlan.make(_JaccardPlan.Sorted())

    @staticmethod
    def unsorted():
        return JaccardPlan.make(_JaccardPlan.Unsorted())


def jaccard(PropertyGraph pg, size_t compare_node, str output_property_name,
            JaccardPlan plan = JaccardPlan()):
    """
    Compute the Jaccard Similarity between `compare_node` and all nodes in the graph.

    :type pg: PropertyGraph
    :param pg: The graph to analyze.
    :type compare_node: node ID
    :param compare_node: The node to compare to all nodes.
    :type output_property_name: str
    :param output_property_name: The output property for similarities. This property must not already exist.
    :type plan: JaccardPlan
    :param plan: The execution plan to use.
    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Jaccard(pg.underlying_property_graph(), compare_node, output_property_name_cstr, plan.underlying_))


def jaccard_assert_valid(PropertyGraph pg, size_t compare_node, str output_property_name):
    """
    Raise an exception if the Jaccard Similarity results in `pg` are invalid. This is not an exhaustive check, just a
    sanity check.

    :raises: AssertionError
    """
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(JaccardAssertValid(pg.underlying_property_graph(), compare_node, output_property_name_cstr))


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

    def __init__(self, PropertyGraph pg, size_t compare_node, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_JaccardStatistics(_JaccardStatistics.Compute(
                pg.underlying_property_graph(), compare_node, output_property_name_cstr))

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
