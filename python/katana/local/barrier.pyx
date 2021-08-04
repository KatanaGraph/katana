from libc.stdint cimport uintptr_t
from libcpp.memory cimport unique_ptr

from katana.cpp.libgalois.Barrier cimport Barrier as CBarrier
from katana.cpp.libgalois.Barrier cimport CreateSimpleBarrier, GetBarrier

from katana.galois import get_active_threads


cdef class Barrier:
    def __init__(self, active_threads):
        self.active_threads = active_threads

    cdef CBarrier* underlying(self) nogil except NULL:
        with gil:
            raise NotImplementedError()

    def reset(self, active_threads = None):
        """
        Reset the barrier to its empty state. This is not safe if any thread is in wait.

        :param active_threads: The number of threads that will wait on this barrier. By default the same as before
            the reset.
        """
        if active_threads is None:
            active_threads = self.active_threads
        active_threads_c = <unsigned int>active_threads
        self.active_threads = active_threads
        with nogil:
            self.underlying().Reinit(active_threads_c)

    def wait(self):
        """
        Wait at this barrier for all threads to arrive.
        """
        with nogil:
            self.underlying().Wait()

    def __str__(self):
        name = self.underlying().name()
        return "<" + str(name, "ascii") + " @ " + hex(<size_t>self.underlying()) + ">"

    @property
    def address(self):
        """
        Internal.
        """
        return <uintptr_t>self.underlying()

cdef class _FastBarrier(Barrier):
    """
    A pre-instantiated barrier managed by the system. This barrier is designed to be fast and should be used in the
    common case.

    However, there is a race if the number of active threads is modified after using this barrier: some threads may
    still be in the barrier while the main thread reinitializes this barrier to the new number of active threads. If
    that may happen, use SimpleBarrier instead.
    """
    cdef CBarrier* _underlying

    def __init__(self, active_threads=None):
        if active_threads is None:
            active_threads = get_active_threads()
        super().__init__(active_threads)
        self._underlying = &GetBarrier(self.active_threads)

    cdef invalidate(self):
        self._underlying = NULL

    cdef CBarrier* underlying(self) nogil except NULL:
        if self._underlying == NULL:
            with gil:
                raise ValueError("This barrier has been invalidated.")
        return self._underlying

    def reset(self, active_threads = None):
        super(_FastBarrier, self).reset(active_threads)

    def __str__(self):
        if self._underlying == NULL:
            return "<invalidated FastBarrier>"
        return super().__str__()


cdef current_fast_barrier = None


def get_fast_barrier(active_threads = None):
    """
    A pre-instantiated barrier managed by the system. This is initialized to the current number of active threads. This
    barrier is designed to be fast and should be used in the common case.

    However, there is a race if the number of active threads is modified after using this barrier: some threads may
    still be in the barrier while the main thread reinitializes this barrier to the new number of active threads. If
    that may happen, use SimpleBarrier instead.

    If this is called with different arguments it will invalidate the existing barrier returned by a previous call to
    this function. This is because a single global barrier is managed by the native library.

    :param active_threads: The number of threads that will wait on this barrier. By default the number of active threads
        in the Katana thread pool.
    """
    if active_threads is None:
        active_threads = get_active_threads()
    active_threads_c = <unsigned int>active_threads
    global current_fast_barrier
    if current_fast_barrier is not None:
        if active_threads_c == (<_FastBarrier>current_fast_barrier).active_threads:
            return current_fast_barrier
        (<_FastBarrier>current_fast_barrier).invalidate()
    current_fast_barrier = _FastBarrier(active_threads_c)
    return current_fast_barrier


cdef class SimpleBarrier(Barrier):
    """
    This barrier is not designed to be fast but does guarantee that all threads have left the barrier before returning
    control. Useful when the number of active threads is modified to avoid a race in FastBarrier.
    """
    cdef unique_ptr[CBarrier] _underlying

    def __init__(self, active_threads = None):
        """
        :param active_threads: The number of threads that will wait on this barrier. By default the number of active
            threads in the Katana thread pool.
        """
        if active_threads is None:
            active_threads = get_active_threads()
        super().__init__(active_threads)
        self._underlying = CreateSimpleBarrier(active_threads)

    cdef CBarrier* underlying(self) nogil except NULL:
        return self._underlying.get()


# Numba Wrappers

import ctypes

from katana.native_interfacing.wrappers import SimpleNumbaPointerWrapper

Barrier_numba_type_wrapper = SimpleNumbaPointerWrapper(Barrier)
Barrier_numba_type = Barrier_numba_type_wrapper.Type


cdef void _numba_Barrier_wait(CBarrier *self) nogil:
    self.Wait()

Barrier_numba_type_wrapper.register_method(
    "wait",
    ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_void_p),
    addr=<uintptr_t>&_numba_Barrier_wait,
)
