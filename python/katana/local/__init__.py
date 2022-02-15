"""
:py:mod:`katana.local` provides single-machine (local) graph data access, graph loading, and analytics. This API
supports writing new graph algorithms using high-performance parallel loops. This API does not require or utilize a
remote server and cannot load or process graphs that do not fit in memory.
"""

# Register numba overloads
import katana.native_interfacing.pyarrow
from katana.local import graph_adds
from katana.local._shared_mem_sys import initialize
from katana.local.barrier import Barrier, SimpleBarrier, get_fast_barrier
from katana.local.datastructures import AllocationPolicy, InsertBag, NUMAArray
from katana.local.dynamic_bitset import DynamicBitset
from katana.local.entity_type_array import EntityTypeArray
from katana.local_native import (
    AtomicEntityType,
    EntityType,
    EntityTypeManager,
    Graph,
    ReduceAnd,
    ReduceMax,
    ReduceMin,
    ReduceOr,
    ReduceSum,
    TxnContext,
)
from katana.native_interfacing.numpy_atomic import atomic_add, atomic_max, atomic_min, atomic_sub

__all__ = [
    "Barrier",
    "DynamicBitset",
    "ReduceSum",
    "ReduceAnd",
    "ReduceOr",
    "ReduceMax",
    "ReduceMin",
    "InsertBag",
    "NUMAArray",
    "Graph",
    "TxnContext",
    "SimpleBarrier",
    "atomic_add",
    "atomic_max",
    "atomic_min",
    "atomic_sub",
    "get_fast_barrier",
    "initialize",
    "AllocationPolicy",
    "EntityType",
    "AtomicEntityType",
    "EntityTypeManager",
    "EntityTypeArray",
]

Graph.out_edges = graph_adds.out_edges
