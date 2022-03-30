"""
k-Truss
-------

The k-Truss is a maximal connected subgraph in which all edges are part of at least (k-2) triangles.

.. autoclass:: katana.local.analytics.KTrussPlan


.. autoclass:: katana.local.analytics._k_truss._KTrussPlanAlgorithm


.. autofunction:: katana.local.analytics.k_truss

.. autoclass:: katana.local.analytics.KTrussStatistics


.. autofunction:: katana.local.analytics.k_truss_assert_valid
"""
from libc.stdint cimport uint32_t, uint64_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string

from katana.cpp.libgalois.graphs.Graph cimport TxnContext as CTxnContext
from katana.cpp.libgalois.graphs.Graph cimport _PropertyGraph
from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libsupport.result cimport Result, handle_result_assert, handle_result_void, raise_error_code

from katana.local import Graph, TxnContext

from katana.local._graph cimport underlying_property_graph_shared_ptr, underlying_txn_context
from katana.local.analytics.plan cimport Plan, _Plan

from enum import Enum


cdef extern from "katana/analytics/k_truss/k_truss.h" namespace "katana::analytics" nogil:
    cppclass _KTrussPlan "katana::analytics::KTrussPlan" (_Plan):
        enum Algorithm:
            kBsp "katana::analytics::KTrussPlan::kBsp"
            kBspJacobi "katana::analytics::KTrussPlan::kBspJacobi"
            kBspCoreThenTruss "katana::analytics::KTrussPlan::kBspCoreThenTruss"

        _KTrussPlan.Algorithm algorithm() const

        KTrussPlan()

        @staticmethod
        _KTrussPlan Bsp()
        @staticmethod
        _KTrussPlan BspJacobi()
        @staticmethod
        _KTrussPlan BspCoreThenTruss()

    Result[void] KTruss(CTxnContext* txn_ctx, const shared_ptr[_PropertyGraph]& pg, uint32_t k_truss_number, string output_property_name, _KTrussPlan plan)

    Result[void] KTrussAssertValid(const shared_ptr[_PropertyGraph]& pg, uint32_t k_truss_number,
                                   string output_property_name)

    cppclass _KTrussStatistics "katana::analytics::KTrussStatistics":
        uint64_t number_of_edges_left

        void Print(ostream os)

        @staticmethod
        Result[_KTrussStatistics] Compute(const shared_ptr[_PropertyGraph]& pg, uint32_t k_truss_number,
                                          string output_property_name)


class _KTrussPlanAlgorithm(Enum):
    """
    :see: :py:class:`~katana.local.analytics.KTrussPlan` constructors for algorithm documentation.
    """
    Bsp = _KTrussPlan.Algorithm.kBsp
    BspJacobi = _KTrussPlan.Algorithm.kBspJacobi
    BspCoreThenTruss = _KTrussPlan.Algorithm.kBspCoreThenTruss


cdef class KTrussPlan(Plan):
    """
    A computational :ref:`Plan` for k-truss.

    Static methods construct KTrussPlans. The constructor will select a reasonable default plan.
    """
    cdef:
        _KTrussPlan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _KTrussPlanAlgorithm

    @staticmethod
    cdef KTrussPlan make(_KTrussPlan u):
        f = <KTrussPlan>KTrussPlan.__new__(KTrussPlan)
        f.underlying_ = u
        return f

    @property
    def algorithm(self) -> KTrussPlan.Algorithm:
        return self.underlying_.algorithm()

    @staticmethod
    def bsp() -> KTrussPlan:
        """
        Bulk-synchronous parallel algorithm.
        """
        return KTrussPlan.make(_KTrussPlan.Bsp())

    @staticmethod
    def bsp_jacobi() -> KTrussPlan:
        """
        Bulk-synchronous parallel with separated edge removal algorithm.
        """
        return KTrussPlan.make(_KTrussPlan.BspJacobi())

    @staticmethod
    def bsp_core_then_truss() -> KTrussPlan:
        """
        Compute k-1 core and then k-truss algorithm.
        """
        return KTrussPlan.make(_KTrussPlan.BspCoreThenTruss())


def k_truss(pg, uint32_t k_truss_number, str output_property_name, KTrussPlan plan = KTrussPlan(), *, txn_ctx = None) -> int:
    """
    Compute the k-truss for pg. `pg` must be symmetric.

    :type pg: katana.local.Graph
    :param pg: The graph to analyze.
    :param k_truss_number: k. The number of triangles that each edge must be part of.
    :type output_property_name: str
    :param output_property_name: The output edge property specifying if that edge is in the k-truss.
        This property must not already exist.
    :type plan: KTrussPlan
    :param plan: The execution plan to use.
    :param txn_ctx: The transaction context for passing read write sets.

    .. code-block:: python

        import katana.local
        from katana.example_data import get_rdg_dataset
        from katana.local import Graph
        katana.local.initialize()

        graph = Graph(get_rdg_dataset("ldbc_003"))
        from katana.analytics import k_truss, KTrussStatistics
        k_truss(graph, 10, "output")

        stats = KTrussStatistics(graph, 10, "output")
        print("Number of Edges Left:", stats.number_of_edges_left)

    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    txn_ctx = txn_ctx or TxnContext()
    with nogil:
        v = handle_result_void(KTruss(underlying_txn_context(txn_ctx), underlying_property_graph_shared_ptr(pg),k_truss_number,output_property_name_str, plan.underlying_))
    return v


def k_truss_assert_valid(pg, uint32_t k_truss_number, str output_property_name):
    """
    Raise an exception if the k-truss results in `pg` are invalid. This is not an exhaustive check, just a sanity check.

    :raises: AssertionError
    """
    cdef string output_property_name_str = output_property_name.encode("utf-8")
    with nogil:
        handle_result_assert(KTrussAssertValid(underlying_property_graph_shared_ptr(pg), k_truss_number, output_property_name_str))


cdef _KTrussStatistics handle_result_KTrussStatistics(Result[_KTrussStatistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef class KTrussStatistics:
    """
    Compute the :ref:`statistics` of a k-truss.
    """
    cdef _KTrussStatistics underlying

    def __init__(self, pg, uint32_t k_truss_number, str output_property_name):
        cdef string output_property_name_str = output_property_name.encode("utf-8")
        with nogil:
            self.underlying = handle_result_KTrussStatistics(_KTrussStatistics.Compute(
                underlying_property_graph_shared_ptr(pg), k_truss_number, output_property_name_str))

    @property
    def number_of_edges_left(self) -> uint64_t:
        return self.underlying.number_of_edges_left

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
