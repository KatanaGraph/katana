from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport handle_result_void, handle_result_assert, raise_error_code, Result
from katana.analytics.plan cimport Plan, _Plan
from katana.property_graph cimport PropertyGraph

from libcpp.string cimport string
from libcpp cimport bool

from enum import Enum

# TODO(amp): Module needs documenting.


cdef extern from "katana/analytics/local_clustering_coefficient/local_clustering_coefficient.h" namespace "katana::analytics" nogil:
    cppclass _LocalClusteringCoefficientPlan "katana::analytics::LocalClusteringCoefficientPlan" (_Plan):
        enum Algorithm:
            kOrderedCountAtomics "katana::analytics::LocalClusteringCoefficientPlan::kOrderedCountAtomics"
            kOrderedCountPerThread "katana::analytics::LocalClusteringCoefficientPlan::kOrderedCountPerThread"

        enum Relabeling:
            kRelabel "katana::analytics::LocalClusteringCoefficientPlan::kRelabel"
            kNoRelabel "katana::analytics::LocalClusteringCoefficientPlan::kNoRelabel"
            kAutoRelabel "katana::analytics::LocalClusteringCoefficientPlan::kAutoRelabel"

        _LocalClusteringCoefficientPlan.Algorithm algorithm() const
        _LocalClusteringCoefficientPlan.Relabeling relabeling() const
        bool edges_sorted() const

        # LocalClusteringCoefficientPlan()

        @staticmethod
        _LocalClusteringCoefficientPlan OrderedCountAtomics(
                bool edges_sorted,
                _LocalClusteringCoefficientPlan.Relabeling relabeling
        )
        @staticmethod
        _LocalClusteringCoefficientPlan OrderedCountPerThread(
                bool edges_sorted,
                _LocalClusteringCoefficientPlan.Relabeling relabeling
            )

    _LocalClusteringCoefficientPlan.Relabeling kDefaultRelabeling "katana::analytics::LocalClusteringCoefficientPlan::kDefaultRelabeling"
    bool kDefaultEdgesSorted "katana::analytics::LocalClusteringCoefficientPlan::kDefaultEdgesSorted"

    Result[void] LocalClusteringCoefficient(_PropertyGraph* pfg, const string& output_property_name, _LocalClusteringCoefficientPlan plan)


class _LocalClusteringCoefficientPlanAlgorithm(Enum):
    OrderedCountAtomics = _LocalClusteringCoefficientPlan.Algorithm.kOrderedCountAtomics
    OrderedCountPerThread = _LocalClusteringCoefficientPlan.Algorithm.kOrderedCountPerThread


cdef _relabeling_to_python(v):
    if v == _LocalClusteringCoefficientPlan.Relabeling.kRelabel:
        return True
    elif v == _LocalClusteringCoefficientPlan.Relabeling.kNoRelabel:
        return False
    else:
        return None


cdef _relabeling_from_python(v):
    if v is None:
        return _LocalClusteringCoefficientPlan.Relabeling.kAutoRelabel
    elif v:
        return _LocalClusteringCoefficientPlan.Relabeling.kRelabel
    else:
        return _LocalClusteringCoefficientPlan.Relabeling.kNoRelabel


cdef class LocalClusteringCoefficientPlan(Plan):
    cdef:
        _LocalClusteringCoefficientPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _LocalClusteringCoefficientPlanAlgorithm

    @staticmethod
    cdef LocalClusteringCoefficientPlan make(_LocalClusteringCoefficientPlan u):
        f = <LocalClusteringCoefficientPlan>LocalClusteringCoefficientPlan.__new__(LocalClusteringCoefficientPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> Algorithm:
        return _LocalClusteringCoefficientPlanAlgorithm(self.underlying_.algorithm())

    @property
    def relabeling(self):
        return self.underlying_.relabeling()

    @property
    def edges_sorted(self) -> bool:
        return self.underlying_.edges_sorted()

    @staticmethod
    def ordered_count_atomics(
                relabeling = _relabeling_to_python(kDefaultRelabeling),
                bool edges_sorted = kDefaultEdgesSorted
            ):
        return LocalClusteringCoefficientPlan.make(_LocalClusteringCoefficientPlan.OrderedCountAtomics(
             edges_sorted, _relabeling_from_python(relabeling)))

    @staticmethod
    def ordered_count_per_thread(
                relabeling = _relabeling_to_python(kDefaultRelabeling),
                bool edges_sorted = kDefaultEdgesSorted
            ):
        return LocalClusteringCoefficientPlan.make(_LocalClusteringCoefficientPlan.OrderedCountPerThread(
             edges_sorted, _relabeling_from_python(relabeling)))


def local_clustering_coefficient(PropertyGraph pg, str output_property_name, LocalClusteringCoefficientPlan plan = LocalClusteringCoefficientPlan()):
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    with nogil:
        handle_result_void(LocalClusteringCoefficient(pg.underlying.get(), output_property_name_str, plan.underlying_))
