cdef extern from "galois/analytics/Plan.h" namespace "galois::analytics" nogil:
    enum _Architecture "galois::analytics::Architecture":
        kCPU
        kGPU
        kDistributed

    cppclass _Plan "galois::analytics::Plan":
        _Architecture architecture() const

cdef class Plan:
    cdef _Plan* underlying(self) except NULL
