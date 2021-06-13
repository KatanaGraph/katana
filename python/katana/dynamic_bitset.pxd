from .cpp.libgalois.DynamicBitset cimport DynamicBitset as CDynamicBitset


cdef class DynamicBitset:
    cdef CDynamicBitset underlying
