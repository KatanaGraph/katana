=================
Atomic Operations
=================

`katana.local` provides atomic values and atomic operations on numpy arrays.
The atomic values are available for several element types, but cannot support custom element types.
The element type is selected by indexing the type, for example ``ReduceSum[int]`` or ``ReduceMin[np.uint64]``.
The supported types are: Unsigned and signed int 8- through 64-bits, 32-bit float, and 64-bit double float.
Atomic operations on numpy arrays support integer and floating point.

.. py:currentmodule:: katana.local

.. autoclass:: ReduceAnd

.. autoclass:: ReduceMax

.. autoclass:: ReduceMin

.. autoclass:: ReduceOr

.. autoclass:: ReduceSum

.. autofunction:: atomic_add

.. autofunction:: atomic_max

.. autofunction:: atomic_min

.. autofunction:: atomic_sub
