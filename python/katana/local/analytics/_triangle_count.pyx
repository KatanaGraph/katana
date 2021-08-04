"""
Triangle Counting
-----------------

.. autoclass:: katana.local.analytics.TriangleCountPlan
    :members:
    :special-members: __init__
    :undoc-members:

.. [Schank] Thomas Schank. Algorithmic Aspects of Triangle-Based Network Analysis. PhD
    Thesis. Universitat Karlsruhe. 2007.

.. autoclass:: katana.local.analytics._triangle_count._TriangleCountPlanAlgorithm
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.triangle_count
"""
from libc.stdint cimport uint64_t
from libcpp cimport bool

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._property_graph cimport PropertyGraph
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/triangle_count/triangle_count.h" namespace "katana::analytics" nogil:
    cppclass _TriangleCountPlan "katana::analytics::TriangleCountPlan" (_Plan):
        enum Algorithm:
            kNodeIteration "katana::analytics::TriangleCountPlan::kNodeIteration"
            kEdgeIteration "katana::analytics::TriangleCountPlan::kEdgeIteration"
            kOrderedCount "katana::analytics::TriangleCountPlan::kOrderedCount"

        enum Relabeling:
            kRelabel "katana::analytics::TriangleCountPlan::kRelabel"
            kNoRelabel "katana::analytics::TriangleCountPlan::kNoRelabel"
            kAutoRelabel "katana::analytics::TriangleCountPlan::kAutoRelabel"

        _TriangleCountPlan.Algorithm algorithm() const
        _TriangleCountPlan.Relabeling relabeling() const
        bool edges_sorted() const

        TriangleCountPlan()

        @staticmethod
        _TriangleCountPlan NodeIteration(bool edges_sorted, _TriangleCountPlan.Relabeling relabeling)
        @staticmethod
        _TriangleCountPlan EdgeIteration(bool edges_sorted, _TriangleCountPlan.Relabeling relabeling)
        @staticmethod
        _TriangleCountPlan OrderedCount(bool edges_sorted, _TriangleCountPlan.Relabeling relabeling)


    _TriangleCountPlan.Relabeling kDefaultRelabeling "katana::analytics::TriangleCountPlan::kDefaultRelabeling"
    bool kDefaultEdgeSorted "katana::analytics::TriangleCountPlan::kDefaultEdgeSorted"

    Result[uint64_t] TriangleCount(_PropertyGraph* pg, _TriangleCountPlan plan)


class _TriangleCountPlanAlgorithm(Enum):
    NodeIteration = _TriangleCountPlan.Algorithm.kNodeIteration
    EdgeIteration = _TriangleCountPlan.Algorithm.kEdgeIteration
    OrderedCount = _TriangleCountPlan.Algorithm.kOrderedCount


cdef _relabeling_to_python(v):
    if v == _TriangleCountPlan.Relabeling.kRelabel:
        return True
    elif v == _TriangleCountPlan.Relabeling.kNoRelabel:
        return False
    else:
        return None


cdef _relabeling_from_python(v):
    if v is None:
        return _TriangleCountPlan.Relabeling.kAutoRelabel
    elif v:
        return _TriangleCountPlan.Relabeling.kRelabel
    else:
        return _TriangleCountPlan.Relabeling.kNoRelabel


cdef class TriangleCountPlan(Plan):
    """
    A computational :ref:`Plan` for Triangle Counting.

    Static methods construct TriangleCountPlans.
    """
    cdef:
        _TriangleCountPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _TriangleCountPlanAlgorithm

    @staticmethod
    cdef TriangleCountPlan make(_TriangleCountPlan u):
        f = <TriangleCountPlan>TriangleCountPlan.__new__(TriangleCountPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> _TriangleCountPlanAlgorithm:
        return _TriangleCountPlanAlgorithm(self.underlying_.algorithm())

    @property
    def edges_sorted(self) -> bool:
        """
        Are the edges of the graph already sorted?

        :rtype: bool
        """
        return self.underlying_.edges_sorted()

    @property
    def relabeling(self):
        """
        Should the algorithm relabel the nodes? Or `None` to decide heuristically.

        :rtype: Optional[bool]
        """
        return _relabeling_to_python(self.underlying_.relabeling())

    @staticmethod
    def node_iteration(bool edges_sorted = kDefaultEdgeSorted,
                       relabeling = _relabeling_to_python(kDefaultRelabeling)):
        """
        The node-iterator algorithm due to Thomas Schank [Schank]_.

        :type edges_sorted: bool
        :param edges_sorted: Are the edges of the graph already sorted?
        :type relabeling: Optional[bool]
        :param relabeling: Should the algorithm relabel the nodes? Or `None` to decide heuristically.
        """
        return TriangleCountPlan.make(_TriangleCountPlan.NodeIteration(
            edges_sorted, _relabeling_from_python(relabeling)))

    @staticmethod
    def edge_iteration(bool edges_sorted = kDefaultEdgeSorted,
                       relabeling = _relabeling_to_python(kDefaultRelabeling)):
        """
        The edge-iterator algorithm due to Thomas Schank [Schank]_.

        :type edges_sorted: bool
        :param edges_sorted: Are the edges of the graph already sorted?
        :type relabeling: Optional[bool]
        :param relabeling: Should the algorithm relabel the nodes? Or `None` to decide heuristically.
        """
        return TriangleCountPlan.make(_TriangleCountPlan.EdgeIteration(
            edges_sorted, _relabeling_from_python(relabeling)))

    @staticmethod
    def ordered_count(bool edges_sorted = kDefaultEdgeSorted,
                      relabeling = _relabeling_to_python(kDefaultRelabeling)):
        """
        An ordered count algorithm that sorts the nodes by degree before
        execution. This has been found to give good performance. We implement the
        ordered count algorithm from: http://gap.cs.berkeley.edu/benchmark.html

        :type edges_sorted: bool
        :param edges_sorted: Are the edges of the graph already sorted?
        :type relabeling: Optional[bool]
        :param relabeling: Should the algorithm relabel the nodes? Or `None` to decide heuristically.
        """
        return TriangleCountPlan.make(_TriangleCountPlan.OrderedCount(
            edges_sorted, _relabeling_from_python(relabeling)))

    def __str__(self):
        return "TriangleCountPlan({}, {}, {})".format(self.algorithm.name, self.edges_sorted, self.relabeling)


cdef uint64_t handle_result_int(Result[uint64_t] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


def triangle_count(PropertyGraph pg,  TriangleCountPlan plan = TriangleCountPlan()) -> int:
    """
    Count the triangles in `pg`.

    :type pg: PropertyGraph
    :param pg: The graph to analyze.
    :type plan: TriangleCountPlan
    :param plan: The execution plan to use.
    :return: The number of triangles found.
    """
    with nogil:
        v = handle_result_int(TriangleCount(pg.underlying_property_graph(), plan.underlying_))
    return v
