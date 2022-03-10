"""
Local Clustering Coefficient
----------------------------

.. autoclass:: katana.local.analytics.LocalClusteringCoefficientPlan


.. autoclass:: katana.local.analytics._local_clustering_coefficient._LocalClusteringCoefficientPlanAlgorithm


.. autofunction:: katana.local.analytics.local_clustering_coefficient
"""
from libcpp cimport bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libsupport.result cimport Result, handle_result_void

from katana.local import Graph, TxnContext

from katana.local._graph cimport underlying_property_graph_shared_ptr, underlying_txn_context
from katana.local.analytics.plan cimport Plan, _Plan

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

    Result[void] LocalClusteringCoefficient(const shared_ptr[_PropertyGraph]& pfg, const string& output_property_name, CTxnContext* txn_ctx, _LocalClusteringCoefficientPlan plan)


class _LocalClusteringCoefficientPlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.LocalClusteringCoefficientPlan` constructors for algorithm documentation.
    """
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
        """
        An ordered count algorithm that sorts the nodes by degree before
        execution. This has been found to give good performance. We implement the
        ordered count algorithm from the following:
        http://gap.cs.berkeley.edu/benchmark.html

        This algorithm uses atomic instructions to update counts.

        :param relabeling: Should the algorithm relabel the nodes.
        :param edges_sorted: Are the edges of the graph already sorted.
        """
        return LocalClusteringCoefficientPlan.make(_LocalClusteringCoefficientPlan.OrderedCountAtomics(
             edges_sorted, _relabeling_from_python(relabeling)))

    @staticmethod
    def ordered_count_per_thread(
                relabeling = _relabeling_to_python(kDefaultRelabeling),
                bool edges_sorted = kDefaultEdgesSorted
            ):
        """
        An ordered count algorithm that sorts the nodes by degree before
        execution. This has been found to give good performance. We implement the
        ordered count algorithm from the following:
        http://gap.cs.berkeley.edu/benchmark.html

        This algorithm uses thread-local counters for parallel counting.

        :param relabeling: Should the algorithm relabel the nodes.
        :param edges_sorted: Are the edges of the graph already sorted.
        """
        return LocalClusteringCoefficientPlan.make(_LocalClusteringCoefficientPlan.OrderedCountPerThread(
             edges_sorted, _relabeling_from_python(relabeling)))


def local_clustering_coefficient(pg, str output_property_name, LocalClusteringCoefficientPlan plan = LocalClusteringCoefficientPlan(), *, txn_ctx = None):
    cdef string output_property_name_str = bytes(output_property_name, "utf-8")
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        handle_result_void(LocalClusteringCoefficient(underlying_property_graph_shared_ptr(pg), output_property_name_str, underlying_txn_context(txn_ctx), plan.underlying_))
