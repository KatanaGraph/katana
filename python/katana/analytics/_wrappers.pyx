from libc.stddef cimport ptrdiff_t
from libc.stdint cimport uint64_t, uint32_t
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, static_pointer_cast

from pyarrow.lib cimport CArray, CUInt64Array, pyarrow_wrap_array

from katana.cpp.libstd.boost cimport std_result, handle_result_void, handle_result_assert, raise_error_code
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libgalois.graphs.Graph cimport PropertyFileGraph
from katana.property_graph cimport PropertyGraph
from katana.analytics.plan cimport _Plan, Plan

from enum import Enum

cdef inline default_value(v, d):
    if v is None:
        return d
    return v

cdef shared_ptr[CUInt64Array] handle_result_shared_cuint64array(std_result[shared_ptr[CUInt64Array]] res) \
        nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


# "Algorithms" from PropertyFileGraph

cdef extern from "katana/PropertyFileGraph.h" namespace "katana" nogil:
    std_result[shared_ptr[CUInt64Array]] SortAllEdgesByDest(PropertyFileGraph* pfg);

    uint64_t FindEdgeSortedByDest(const PropertyFileGraph* graph, uint32_t node, uint32_t node_to_find);

    std_result[void] SortNodesByDegree(PropertyFileGraph* pfg);


def sort_all_edges_by_dest(PropertyGraph pg):
    with nogil:
        res = handle_result_shared_cuint64array(SortAllEdgesByDest(pg.underlying.get()))
    return pyarrow_wrap_array(static_pointer_cast[CArray, CUInt64Array](res))


def find_edge_sorted_by_dest(PropertyGraph pg, uint32_t node, uint32_t node_to_find):
    with nogil:
        res = FindEdgeSortedByDest(pg.underlying.get(), node, node_to_find)
    if res == pg.edges(node)[-1] + 1:
        return None
    return res


def sort_nodes_by_degree(PropertyGraph pg):
    with nogil:
        handle_result_void(SortNodesByDegree(pg.underlying.get()))


# Jaccard


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

    std_result[void] Jaccard(PropertyFileGraph* pfg, size_t compare_node,
        string output_property_name, _JaccardPlan plan)

    std_result[void] JaccardAssertValid(PropertyFileGraph* pfg, size_t compare_node,
        string output_property_name)

    cppclass _JaccardStatistics  "katana::analytics::JaccardStatistics":
        double max_similarity
        double min_similarity
        double average_similarity
        void Print(ostream)

        @staticmethod
        std_result[_JaccardStatistics] Compute(PropertyFileGraph* pfg, size_t compare_node,
            string output_property_name)


class _JaccardEdgeSorting(Enum):
    Sorted = _JaccardPlan.EdgeSorting.kSorted
    Unsorted = _JaccardPlan.EdgeSorting.kUnsorted
    kUnknown = _JaccardPlan.EdgeSorting.kUnknown


cdef class JaccardPlan(Plan):
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
        return _JaccardEdgeSorting(self.underlying_.edge_sorting())

    @staticmethod
    def sorted():
        return JaccardPlan.make(_JaccardPlan.Sorted())

    @staticmethod
    def unsorted():
        return JaccardPlan.make(_JaccardPlan.Unsorted())


def jaccard(PropertyGraph pg, size_t compare_node, str output_property_name,
            JaccardPlan plan = JaccardPlan()):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_void(Jaccard(pg.underlying.get(), compare_node, output_property_name_cstr, plan.underlying_))


def jaccard_assert_valid(PropertyGraph pg, size_t compare_node, str output_property_name):
    output_property_name_bytes = bytes(output_property_name, "utf-8")
    output_property_name_cstr = <string>output_property_name_bytes
    with nogil:
        handle_result_assert(JaccardAssertValid(pg.underlying.get(), compare_node, output_property_name_cstr))


cdef _JaccardStatistics handle_result_JaccardStatistics(std_result[_JaccardStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class JaccardStatistics:
    cdef _JaccardStatistics underlying

    def __init__(self, PropertyGraph pg, size_t compare_node, str output_property_name):
        output_property_name_bytes = bytes(output_property_name, "utf-8")
        output_property_name_cstr = <string> output_property_name_bytes
        with nogil:
            self.underlying = handle_result_JaccardStatistics(_JaccardStatistics.Compute(
                pg.underlying.get(), compare_node, output_property_name_cstr))

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


# TODO(amp): Wrap ConnectedComponents
