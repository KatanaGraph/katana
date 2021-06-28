from katana.cpp.libgalois.Barrier cimport Barrier as CBarrier


cdef class Barrier:
    cdef unsigned int active_threads

    cdef CBarrier* underlying(self) nogil except NULL
