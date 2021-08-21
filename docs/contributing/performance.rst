===========
Performance
===========

Arrays and Vectors
==================

We have different specializations or types for arrays/vectors of
objects/elements that are efficient in different use cases. These types can be
differentiated along these dimensions:

Array/Vector
   the abstraction that the type provides - a *vector* can be resized
   dynamically and its allocation grows in powers-of-2, whereas an *array*
   cannot be resized (i.e., its allocation size must be known at runtime).

Concurrent/Scalable Allocation
   whether the type supports efficient concurrent allocation in a parallel
   region.

NUMA-aware Allocation
   whether the type supports allocation of objects in different NUMA
   sockets/regions.

Uninitialized Allocation
   whether the type does not initialize the objects during allocation.

Object Construction/Destruction
   whether the type supports calling each object's constructor/destructor.

Parallel Construction/Destruction
   whether the type uses threads during its construction and destruction.

.. list-table::

   - * Name
     * Array/Vector
     * Concurrent/Scalable Allocation
     * NUMA-aware Allocation
     * Uninitialized Allocation
     * Object Construction/Destruction
     * Parallel Construction/Destruction
   - * ``std::vector``
     * Vector
     * No
     * No
     * No
     * Yes
     * No
   - * ``katana::gstl::Vector``
     * Vector
     * Yes
     * No
     * No
     * Yes
     * No
   - * ``katana::PODVector``
     * Vector
     * No
     * No
     * Yes
     * No
     * No
   - * ``katana::NUMAArray``
     * Array
     * No
     * Yes
     * Yes
     * Yes
     * Yes

Here is a brief description of the specializations:

:class:`katana::gstl::Vector`
   A specialization of ``std::vector`` that uses a concurrent, scalable
   allocator: the allocator is composed of thread-local allocators that manage
   thread-local pages and only use a global lock to get allocation in chunks of
   (huge) pages. When a ``katana::gstl::Vector`` is destroyed, its memory is
   reclaimed by the thread that destroys it.

:class:`katana::PODVector`
   A specialization of ``std::vector`` of plain-old-datatype (POD) objects that
   does not initialize/construct or destruct the objects. It does not support
   concurrent/scalable or NUMA-aware allocation.

:class:`katana::NUMAArray`
   An array of objects that is distributed among NUMA sockets/regions but
   cannot be resized. Different NUMA-aware allocation policies are supported.
   The allocation is uninitialized but objects of any type can be constructed
   after allocation using member functions. Allocations and deallocations are
   parallel operations because threads are used to allocate pages in each
   thread's NUMA region and destroy objects in parallel respectively.

Here is a rule of thumb to choose among these different options:

- Use ``katana::gstl::Vector`` when allocations and deallocations can occur in
  a parallel region. As the memory allocated can be reused for another
  allocation only by the thread that deallocated it, this is not suitable for
  use cases where the main thread always does the deallocation (after the
  parallel region).

- Use ``katana::PODVector`` when the object type is a POD and when the
  allocation is done in a serial region but the assignment/construction is done
  in a parallel region. In other words, when ``resize()`` is done on the main
  thread and values are assigned in parallel (instead of the typical
  ``reserve()`` and ``emplace_back()`` on the main thread).

- Use ``katana::NUMAArray`` when the allocation size is large (in the order of
  nodes/edges). Allocation size must be known at runtime (allocation cannot
  grow dynamically). Allocations and deallocations must occur on the main
  thread.

Note the design space of the different dimensions is not yet fully explored. We
may create more specializations in the future when the need arises.
