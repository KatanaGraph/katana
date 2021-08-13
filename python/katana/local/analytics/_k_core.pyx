"""
k-Core
---------------

.. autoclass:: katana.local.analytics.KCorePlan
    :members:
    :special-members: __init__
    :undoc-members:

.. autoclass:: katana.local.analytics._k_core._KCorePlanAlgorithm
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.k_core

.. autoclass:: katana.local.analytics.KCoreStatistics
    :members:
    :undoc-members:

.. autofunction:: katana.local.analytics.k_core_assert_valid
"""
from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code
from katana.local._property_graph cimport PropertyGraph
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/k_core/k_core.h" namespace "katana::analytics" nogil:
    cppclass _KCorePlan "katana::analytics::KCorePlan" (_Plan):
        enum Algorithm:
            kSynchronous "katana::analytics::KCorePlan::kSynchronous"
            kAsynchronous "katana::analytics::KCorePlan::kAsynchronous"

        _KCorePlan.Algorithm algorithm() const

        KCorePlan()

        @staticmethod
        _KCorePlan Synchronous()
        @staticmethod
        _KCorePlan Asynchronous()

    Result[void] KCore(_PropertyGraph* pg, uint32_t k_core_number, string output_property_name, _KCorePlan plan)


    Result[void] KCoreAssertValid(_PropertyGraph* pg, uint32_t k_core_number, string output_property_name)

    cppclass _KCoreStatistics "katana::analytics::KCoreStatistics":
        uint64_t number_of_nodes_in_kcore

        void Print(ostream os)

        @staticmethod
        Result[_KCoreStatistics] Compute(_PropertyGraph* pg, uint32_t k_core_number, string output_property_name)


class _KCorePlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.KCorePlan` constructors for algorithm documentation.
    """
    Synchronous = _KCorePlan.Algorithm.kSynchronous
    Asynchronous = _KCorePlan.Algorithm.kAsynchronous


cdef class KCorePlan(Plan):
    """
    A computational :ref:`Plan` for k-Core.

    Static methods construct KCorePlans.
    """
    cdef:
        _KCorePlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _KCorePlanAlgorithm

    @staticmethod
    cdef KCorePlan make(_KCorePlan u):
        f = <KCorePlan>KCorePlan.__new__(KCorePlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> KCorePlan.Algorithm:
        return self.underlying_.algorithm()

    @staticmethod
    def synchronous() -> KCorePlan:
        """
        Bulk-synchronous
        """
        return KCorePlan.make(_KCorePlan.Synchronous())
    @staticmethod
    def asynchronous() -> KCorePlan:
        """
        Asynchronous
        """
        return KCorePlan.make(_KCorePlan.Asynchronous())


def k_core(PropertyGraph pg, uint32_t k_core_number, str output_property_name, KCorePlan plan = KCorePlan()) -> int:
    """
    Description
    -----------
    Compute nodes which are in the k-core of pg. The pg must be symmetric.
    
    Parameters
    ----------
    :type pg: PropertyGraph
    :param pg: The graph to analyze.
    :param k_core_number: k. The minimum degree of nodes in the resulting core.
    :type output_property_name: str
    :param output_property_name: The output property holding an indicator 1 if the node is in the k-core, 0 otherwise.
        This property must not already exist.
    :type plan: KCorePlan
    :param plan: The execution plan to use.

    Examples
    --------
    .. code-block:: python
        import katana.local
        from katana.example_utils import get_input
        from katana.property_graph import PropertyGraph
        katana.local.initialize()

        property_graph = PropertyGraph(get_input("propertygraphs/ldbc_003"))
        from katana.analytics import k_core, KCoreStatistics
        k_core(property_graph, 10, "output")

        stats = KCoreStatistics(property_graph, 10, "output")
        print("Number of Nodes in K-core:", stats.number_of_nodes_in_kcore)
    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        v = handle_result_void(KCore(pg.underlying_property_graph(), k_core_number, output_property_name_str, plan.underlying_))
    return v


def k_core_assert_valid(PropertyGraph pg, uint32_t k_core_number, str output_property_name):
    """
    Raise an exception if the k-core results in `pg` are invalid. This is not an exhaustive check, just a sanity check.

    :raises: AssertionError
    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        handle_result_assert(KCoreAssertValid(pg.underlying_property_graph(), k_core_number, output_property_name_str))


cdef _KCoreStatistics handle_result_KCoreStatistics(Result[_KCoreStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class KCoreStatistics:
    """
    Compute the :ref:`statistics` of a k-Core result.
    """
    cdef _KCoreStatistics underlying

    def __init__(self, PropertyGraph pg, uint32_t k_core_number, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_KCoreStatistics(_KCoreStatistics.Compute(
                pg.underlying_property_graph(), k_core_number, output_property_name_str))

    @property
    def number_of_nodes_in_kcore(self) -> uint64_t:
        return self.underlying.number_of_nodes_in_kcore

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
