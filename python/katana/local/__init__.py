"""
:py:mod:`katana.local` provides single-machine (local) graph data access, graph loading, and analytics. This API
supports writing new graph algorithms using high-performance parallel loops. This API does not require or utilize a
remote server and cannot load or process graphs that do not fit in memory.
"""

# Register numba overloads
import katana.native_interfacing.pyarrow
from katana.local._shared_mem_sys import initialize
from katana.local.atomic import (
    ReduceLogicalAnd,
    ReduceLogicalOr,
    ReduceMax,
    ReduceMin,
    ReduceSum,
    atomic_add,
    atomic_max,
    atomic_min,
    atomic_sub,
)
from katana.local.barrier import Barrier, SimpleBarrier, get_fast_barrier
from katana.local.datastructures import AllocationPolicy, InsertBag, NUMAArray
from katana.local.dynamic_bitset import DynamicBitset
from katana.local.entity_type import EntityType
from katana.local.graph import Graph, TxnContext

__all__ = [
    "Barrier",
    "DynamicBitset",
    "ReduceSum",
    "ReduceLogicalAnd",
    "ReduceLogicalOr",
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
]
